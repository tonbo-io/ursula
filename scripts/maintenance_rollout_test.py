#!/usr/bin/env python3
"""Actual chart shell + native reservation CLI with synthetic transport fixtures.

The transport has atomic whole-object CAS and process token preconditions. Raft
proofs are synthetic: this is controller sequencing evidence, not live recovery.
Run with URSULA_CTL_BINARY pointing to the built, source-matched ursulactl.
"""
import fcntl
import json
import os
import signal
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'charts/ursula/files/maintenance-rollout.sh'


def load(path):
    return json.loads(Path(path).read_text())


def write_db(path, value):
    path = Path(path)
    temporary = path.with_suffix(f'.{os.getpid()}.pending')
    temporary.write_text(json.dumps(value))
    os.replace(temporary, path)


def emit(value):
    print(json.dumps(value, separators=(',', ':')))


def option(args, name, default=None):
    return args[args.index(name) + 1] if name in args else default


def state(db):
    return json.loads(db['store']['data']['reservation'])


def pod(db, node):
    entry = db['nodes'][str(node)]
    return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
        'namespace': 'test', 'name': f'voters-{node - 1}', 'uid': entry['uid'],
        'ownerReferences': [{'kind': 'StatefulSet', 'uid': 'sts-uid', 'controller': True}],
        'labels': {'controller-revision-hash': entry['revision']}},
        'spec': {'nodeName': f'host-{node}', 'containers': [{'name': 'ursula', 'image': entry['image']}]}}


def proof(db, nodes):
    now = time.time_ns() // 1_000_000
    retired = all(db['nodes'][str(n['id'])]['phase'] == 'retired' for n in nodes)
    return {'started_ms': now, 'completed_ms': now, 'verification': {
        'version': 3, 'participation_certified': True,
        'process_incarnations_certified': True,
        'process_incarnations': {str(n['id']): n['expected_process_incarnation'] for n in nodes},
        'maintenance_executor_certified': bool(nodes[0].get('expected_maintenance_fence')) and not retired,
        'maintenance_executor_retired_certified': bool(nodes[0].get('expected_maintenance_fence')) and retired,
        'maintenance_fence': nodes[0]['expected_maintenance_fence'],
        'prefixes': {str(g): {'raft_group_id': g, 'leader_id': 1,
                            'leader_term': 1, 'required_applied_index': 50} for g in range(4)},
        'applied': {str(n['id']): {str(g): 50 for g in range(4)} for n in nodes}}}


def shim(role):
    args = sys.argv[1:]
    if role == 'ctl' and args[0].startswith('reservation-'):
        os.execv(os.environ['URSULA_CTL_BINARY'], [os.environ['URSULA_CTL_BINARY'], *args])
    if role == 'kubectl' and 'port-forward' in args:
        print('Forwarding from 127.0.0.1', flush=True)
        time.sleep(45)
        return
    database = Path(os.environ['FIXTURE_DB'])
    offered = None
    if role == 'kubectl' and 'replace' in args:
        offered = load(option(args, '-f'))
        # Both executors submit proposals for the same actual store revision.
        # Delay commitment until the competing proposal has arrived.
        with database.with_suffix('.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            db = load(database)
            racing = db['mode'] == 'race' and state(db)['operation'] is None
            if racing:
                db['arrivals'] += 1
                write_db(database, db)
        if racing:
            deadline = time.monotonic() + 15
            while load(database)['arrivals'] < 2:
                if time.monotonic() > deadline:
                    raise RuntimeError('second competing CAS did not arrive')
                time.sleep(.02)
    failure = None
    result = None
    with database.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        db = load(database)
        event = {'role': role, 'args': args}
        db['events'].append(event)
        if role == 'kubectl':
            clean = list(args)
            if '-n' in clean:
                i = clean.index('-n')
                del clean[i:i + 2]
            command = clean[0]
            if command == 'get':
                kind, name = clean[1:3]
                selector = option(clean, '-o')
                if kind == 'configmap':
                    if db['store'] is None:
                        failure = 'missing reservation store'
                    else:
                        result = db['store']
                elif kind == 'namespace':
                    result = {'kind': 'Namespace', 'metadata': {'name': 'test', 'uid': 'ns-uid'}}
                elif kind == 'node':
                    node = int(name.split('-')[-1])
                    result = {'kind': 'Node', 'metadata': {'name': name, 'uid': f'node-{node}',
                              'labels': {'topology.kubernetes.io/zone': f'zone-{node}'}},
                              'spec': {'providerID': f'instance-{node}'},
                              'status': {'conditions': [{'type': 'Ready', 'status': 'True'}]}}
                elif kind == 'statefulset':
                    result = {'kind': 'StatefulSet', 'metadata': {'name': 'voters', 'namespace': 'test',
                              'uid': 'sts-uid', 'generation': 1}, 'spec': {'replicas': 3},
                              'status': {'updateRevision': 'new', 'observedGeneration': 1}}
                    if selector != 'json':
                        result = {'{.spec.template.spec.containers[?(@.name=="ursula")].image}': 'candidate',
                                  '{.spec.updateStrategy.type}': 'OnDelete', '{.spec.replicas}': '3',
                                  '{.metadata.generation}': '1', '{.status.observedGeneration}': '1',
                                  '{.status.updateRevision}': 'new'}[selector.removeprefix('jsonpath=')]
                elif kind == 'pod':
                    node = int(name.split('-')[-1]) + 1
                    result = pod(db, node)
                    if selector != 'json':
                        entry = db['nodes'][str(node)]
                        result = {'{.metadata.uid}': entry['uid'], '{.spec.nodeName}': f'host-{node}',
                                  '{.spec.containers[?(@.name=="ursula")].image}': entry['image'],
                                  '{.metadata.labels.controller-revision-hash}': entry['revision']}[selector.removeprefix('jsonpath=')]
                else:
                    raise RuntimeError(args)
            elif command == 'replace':
                old = db['store']['metadata']
                new = offered['metadata']
                if db['mode'] == 'conflict' or any(old[k] != new[k] for k in ('uid', 'resourceVersion')):
                    failure = '409: stale UID/resourceVersion'
                else:
                    db['store'] = offered
                    db['store']['metadata']['resourceVersion'] = str(int(old['resourceVersion']) + 1)
                    event['committed_stage'] = state(db)['operation']
                    result = db['store']
                    if db['mode'] == 'after-bind' and state(db)['operation']['replacement']:
                        db['mode'] = 'normal'
                        failure = 'transport lost after binding committed'
            elif command == 'delete':
                options = json.load(sys.stdin)
                node = int(next(a for a in clean if a.startswith('--raw=')).split('-')[-1]) + 1
                current = db['nodes'][str(node)]
                operation = state(db)['operation']
                if options['preconditions']['uid'] != current['uid']:
                    failure = '409: source Pod UID changed'
                elif operation['source']['node_id'] != node or operation['admission'] is None:
                    failure = 'unadmitted physical delete'
                else:
                    current.update(uid=str(uuid.uuid4()), boot=f'{node + 100:032x}',
                                   phase='unclaimed', token=None, image='candidate', revision='new')
                    event['deleted_node'] = node
                    event['generation'] = operation['fence']['generation']
                    event['admission'] = operation['admission']
                    if db['mode'] == 'startup-bind':
                        # A separately admitted server commits its boot before
                        # listening, while this hook still holds the old snapshot.
                        plan = operation['process_plan']
                        plan[node - 1]['expected_process_incarnation'] = current['boot']
                        request = {'action': 'bind_pod_replacement', 'fence': operation['fence'],
                                   'pod': pod(db, node), 'node': {'kind': 'Node', 'metadata': {
                                       'name': f'host-{node}', 'uid': f'node-{node}',
                                       'labels': {'topology.kubernetes.io/zone': f'zone-{node}'}},
                                       'spec': {'providerID': f'instance-{node}'},
                                       'status': {'conditions': [{'type': 'Ready', 'status': 'True'}]}},
                                   'process_plan': plan}
                        cell_file = database.with_suffix('.cell.json')
                        snapshot_file = database.with_suffix('.snapshot.json')
                        request_file = database.with_suffix('.request.json')
                        cell_file.write_text(json.dumps(state(db)['cell']))
                        snapshot_file.write_text(json.dumps(db['store']))
                        request_file.write_text(json.dumps(request))
                        bound = subprocess.run([os.environ['URSULA_CTL_BINARY'], 'reservation-propose',
                                                '--cell', str(cell_file), '--snapshot', str(snapshot_file),
                                                '--request', str(request_file)], capture_output=True,
                                               text=True, check=False)
                        if bound.returncode:
                            raise RuntimeError(bound.stderr + bound.stdout)
                        offered = json.loads(bound.stdout)
                        offered['metadata']['resourceVersion'] = str(int(db['store']['metadata']['resourceVersion']) + 1)
                        db['store'] = offered
                        current.update(phase='activating', token=operation['fence'])
                        event['startup_bound'] = True
                    if db['mode'] == 'after-delete':
                        db['mode'] = 'normal'
                        failure = 'transport lost after API accepted UID deletion'
            elif command == 'wait':
                pass
            else:
                raise RuntimeError(args)
        else:
            command = args[0]
            nodes = load(option(args, '--config'))['nodes']
            replacement = option(args, '--replace-node')
            excluded = option(args, '--excluded-node-id')
            for n in nodes:
                if str(n['id']) == excluded:
                    continue
                entry = db['nodes'][str(n['id'])]
                if str(n['id']) != replacement and n.get('expected_process_incarnation') not in (None, entry['boot']):
                    failure = 'fixed surviving process changed'
            if command == 'pin-incarnations' and not failure:
                for n in nodes:
                    entry = db['nodes'][str(n['id'])]
                    if replacement and str(n['id']) != replacement and entry['token'] != n['expected_maintenance_fence']:
                        failure = 'survivor executor changed'
                    n['expected_process_incarnation'] = entry['boot']
                    n.setdefault('expected_maintenance_fence', None)
                result = {'nodes': nodes}
            elif command in ('activate-maintenance-fence', 'retire-maintenance-fence') and not failure:
                for n in nodes:
                    entry = db['nodes'][str(n['id'])]
                    token = n['expected_maintenance_fence']
                    if command.startswith('activate'):
                        if entry['token'] and entry['token'] != token and entry['token']['generation'] >= token['generation']:
                            failure = 'stale maintenance generation'
                        entry['token'], entry['phase'] = token, 'active'
                    elif entry['token'] != token:
                        failure = 'retirement token mismatch'
                    else:
                        entry['phase'] = 'retired'
                        if db['mode'] == 'retire-failure':
                            db['mode'] = 'normal'
                            failure = 'retirement interrupted after one process'
                            break
            elif command != 'pin-incarnations' and not failure:
                retired = command in ('verify-quorum', 'verify-cluster') and all(db['nodes'][str(n['id'])]['phase'] == 'retired' for n in nodes)
                selected = option(args, '--excluded-node-id')
                for n in nodes:
                    if str(n['id']) == selected:
                        continue
                    entry = db['nodes'][str(n['id'])]
                    if not (command in ('verify-quorum', 'verify-cluster') and n.get('expected_maintenance_fence') is None) and (entry['token'] != n['expected_maintenance_fence'] or (entry['phase'] != 'active' and not retired)):
                        failure = 'mutation/proof uses stale or inactive executor'
                if command == 'verify-quorum':
                    if db['mode'] == 'proof-failure':
                        failure = 'incomplete all-group prefix'
                    result = proof(db, nodes)
                elif command == 'verify-survivors':
                    result = {'synthetic_survivors': True}
                elif command not in ('drain', 'wait', 'undrain', 'verify-cluster'):
                    raise RuntimeError(args)
        paused = role == 'ctl' and args[0] == 'drain' and db['mode'] == 'pause-drain'
        if paused:
            db['paused'] = True
        write_db(database, db)
    if paused:
        time.sleep(45)
    if failure:
        print(failure, file=sys.stderr)
        sys.exit(1)
    if result is not None:
        emit(result) if isinstance(result, (dict, list)) else print(result)


class RolloutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.env = {**os.environ, 'FIXTURE_DB': str(self.directory / 'db.json'),
                    'NAMESPACE': 'test', 'STATEFULSET': 'voters', 'REPLICAS': '3',
                    'EXPECTED_GROUPS': '4', 'CORE_COUNT': '2', 'TARGET_IMAGE': 'candidate',
                    'CTL': str(self.directory / 'ctl'), 'MAINTENANCE_SOURCE_ONLY': '1',
                    'ROLLOUT_LIBRARY_DIR': str(SCRIPT.parent),
                    'PATH': str(self.directory) + os.pathsep + os.environ['PATH']}
        for role in ('ctl', 'kubectl'):
            p = self.directory / role
            p.write_text(f'#!/usr/bin/env python3\nimport runpy\nmodule=runpy.run_path({str(Path(__file__).resolve())!r})\nmodule["shim"]({role!r})\n')
            p.chmod(0o755)
        cell = {'namespace': 'test', 'namespace_uid': 'ns-uid', 'statefulset': 'voters',
                'statefulset_uid': 'sts-uid', 'group_count': 4, 'core_count': 2, 'voter_ids': [1, 2, 3]}
        cell_path = self.directory / 'cell.json'
        cell_path.write_text(json.dumps(cell))
        bootstrap = subprocess.run([self.env['URSULA_CTL_BINARY'], 'reservation-bootstrap', '--cell', str(cell_path),
                                    '--confirm-new-cell'], capture_output=True, text=True, check=True)
        store = json.loads(bootstrap.stdout)
        self.assertNotIn('uid', store['metadata'])
        self.assertNotIn('ownerReferences', store['metadata'])
        store['metadata'].update(uid='store-uid', resourceVersion='1')
        self.db = {'store': store, 'mode': 'normal', 'arrivals': 0, 'events': [], 'nodes': {
            str(n): {'uid': str(uuid.uuid4()), 'boot': f'{n:032x}', 'token': None, 'phase': 'unclaimed',
                     'image': 'candidate' if n != 3 else 'old', 'revision': 'new' if n != 3 else 'old'} for n in (1, 2, 3)}}
        self.save()

    def save(self):
        write_db(self.env['FIXTURE_DB'], self.db)

    def run_shell(self, suffix='maintenance_main', asynchronous=False):
        directory = self.directory / uuid.uuid4().hex
        directory.mkdir()
        env = {**self.env, 'MAINTENANCE_WORK_DIR': str(directory)}
        program = '. "$1"; maintenance_new_id() { python3 -c "import uuid; print(uuid.uuid4().hex)"; }; ' + suffix
        command = ['/bin/sh', '-ec', program, 'fixture', str(SCRIPT)]
        process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, start_new_session=True)
        def cleanup_process_group():
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            if process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
        self.addCleanup(cleanup_process_group)
        if asynchronous:
            return process
        stdout, stderr = process.communicate(timeout=90)
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)

    def refresh(self):
        self.db = load(self.env['FIXTURE_DB'])
        return state(self.db) if self.db['store'] else None

    def deletions(self):
        return [e for e in self.db['events'] if 'deleted_node' in e]

    def test_complete_real_shell_pipeline_retires_before_next_source(self):
        for node in self.db['nodes'].values():
            node.update(image='old', revision='old')
        self.save()
        run = self.run_shell()
        self.assertEqual(run.returncode, 0, run.stderr + run.stdout)
        final = self.refresh()
        self.assertIsNone(final['operation'])
        self.assertEqual(final['generation'], 3)
        self.assertEqual([e['deleted_node'] for e in self.deletions()], [3, 2, 1])
        self.assertEqual([e['generation'] for e in self.deletions()], [1, 2, 3])
        for entry in self.db['nodes'].values():
            self.assertEqual(entry['phase'], 'retired')
        # Each voter is drained before its Pod is deleted, and undrained only
        # after it caught up as a voter again.
        restart_steps = [e['args'][0] if e['role'] == 'ctl' else 'delete'
                         for e in self.db['events']
                         if (e['role'] == 'ctl' and e['args'][0] in ('drain', 'wait', 'undrain'))
                         or 'deleted_node' in e]
        self.assertEqual(restart_steps, ['drain', 'delete', 'wait', 'undrain'] * 3)
        # Every admission included every configured group and every fixed boot.
        for deletion in self.deletions():
            self.assertEqual(len(deletion['admission']['verification']['prefixes']), 4)
            self.assertEqual(len(deletion['admission']['verification']['applied']), 3)

    def test_hook_accepts_exact_same_executor_pretransport_startup_binding(self):
        self.db['mode'] = 'startup-bind'
        self.save()
        run = self.run_shell()
        self.assertEqual(run.returncode, 0, run.stderr + run.stdout)
        final = self.refresh()
        self.assertIsNone(final['operation'])
        self.assertEqual(final['generation'], 1)
        self.assertEqual(len(self.deletions()), 1)
        self.assertTrue(self.deletions()[0]['startup_bound'])
        self.assertEqual(final['completion']['replacement']['process_incarnation'], self.db['nodes']['3']['boot'])

    def test_native_catalog_accepts_hook_tunnels_and_same_executor_startup_binding(self):
        plan = [{'id': n, 'host': f'10.0.0.{n}',
                 'admin_url': f'http://10.0.0.{n}:4438/',
                 'http_url': f'http://10.0.0.{n}:4437/',
                 'expected_process_incarnation': self.db['nodes'][str(n)]['boot'],
                 'expected_maintenance_fence': None} for n in (1, 2, 3)]
        pods = [pod(self.db, n) for n in (1, 2, 3)]
        for p in pods:
            p['status'] = {'conditions': [{'type': 'Ready', 'status': 'True'}]}
        nodes = [{'kind': 'Node', 'metadata': {'name': f'host-{n}', 'uid': f'node-{n}',
                 'labels': {'topology.kubernetes.io/zone': f'zone-{n}'}},
                 'spec': {'providerID': f'instance-{n}'},
                 'status': {'conditions': [{'type': 'Ready', 'status': 'True'}]}}
                 for n in (1, 2, 3)]
        request = {'action': 'publish_host_inventory', 'pods': pods, 'nodes': nodes,
                   'process_plan': plan, 'observation': proof(self.db, plan),
                   'now_ms': time.time_ns() // 1_000_000}
        request_path = self.directory / 'request.json'
        snapshot_path = self.directory / 'snapshot.json'
        request_path.write_text(json.dumps(request))
        snapshot_path.write_text(json.dumps(self.db['store']))
        result = subprocess.run([self.env['URSULA_CTL_BINARY'], 'reservation-propose',
                                 '--cell', str(self.directory / 'cell.json'),
                                 '--snapshot', str(snapshot_path), '--request', str(request_path)],
                                capture_output=True, text=True, check=True)
        self.db['store'] = json.loads(result.stdout)
        self.db['store']['metadata']['resourceVersion'] = '2'
        self.db['mode'] = 'startup-bind'
        self.save()
        run = self.run_shell()
        self.assertEqual(run.returncode, 0, run.stderr + run.stdout)
        final = self.refresh()
        self.assertEqual(final['version'], 2)
        self.assertEqual(final['generation'], 1)
        self.assertIsNone(final['operation'])
        self.assertEqual(len(self.deletions()), 1)
        self.assertTrue(self.deletions()[0]['startup_bound'])
        self.assertEqual(final['hosts']['voters'][2]['source']['pod_uid'], self.db['nodes']['3']['uid'])

    def test_legacy_consumer_cannot_bypass_an_existing_shared_reservation(self):
        run = self.run_shell('main')
        self.assertNotEqual(run.returncode, 0)
        self.refresh()
        self.assertFalse(self.deletions())
        self.assertFalse(any(e['role'] == 'ctl' for e in self.db['events']))
        self.assertIn('persistent maintenance store exists', run.stdout)

    def test_planned_rollout_cannot_take_over_host_recovery(self):
        # Drive real offline CLI transitions; only the transport/healthy proof
        # is synthetic. Even an unadmitted host reservation blocks rollout.
        plan = [{'id': n, 'host': f'voters-{n - 1}',
                 'admin_url': f'http://localhost:{1000 + n}/',
                 'expected_process_incarnation': self.db['nodes'][str(n)]['boot'],
                 'expected_maintenance_fence': None} for n in (1, 2, 3)]
        pods = [pod(self.db, n) for n in (1, 2, 3)]
        for p in pods:
            p['status'] = {'conditions': [{'type': 'Ready', 'status': 'True'}]}
        nodes = [{'kind': 'Node', 'metadata': {'name': f'host-{n}', 'uid': f'node-{n}',
                 'labels': {'topology.kubernetes.io/zone': f'zone-{n}'}},
                 'spec': {'providerID': f'instance-{n}'},
                 'status': {'conditions': [{'type': 'Ready', 'status': 'True'}]}}
                 for n in (1, 2, 3)]
        for request in [
                {'action': 'publish_host_inventory', 'pods': pods, 'nodes': nodes,
                 'process_plan': plan, 'observation': proof(self.db, plan),
                 'now_ms': time.time_ns() // 1_000_000},
                {'action': 'reserve_host_recovery', 'operation_id': uuid.uuid4().hex,
                 'executor_id': uuid.uuid4().hex, 'node_id': 3, 'process_plan': plan,
                 'now_ms': time.time_ns() // 1_000_000}]:
            request_path = self.directory / 'request.json'
            snapshot_path = self.directory / 'snapshot.json'
            request_path.write_text(json.dumps(request))
            snapshot_path.write_text(json.dumps(self.db['store']))
            result = subprocess.run([self.env['URSULA_CTL_BINARY'], 'reservation-propose',
                                     '--cell', str(self.directory / 'cell.json'),
                                     '--snapshot', str(snapshot_path), '--request', str(request_path)],
                                    capture_output=True, text=True, check=True)
            offered = json.loads(result.stdout)
            offered['metadata']['resourceVersion'] = str(int(self.db['store']['metadata']['resourceVersion']) + 1)
            self.db['store'] = offered
        self.save()
        original = self.db['store']
        run = self.run_shell()
        self.assertNotEqual(run.returncode, 0)
        self.assertIn('host recovery owns the reservation', run.stdout)
        self.refresh()
        self.assertEqual(self.db['store'], original)
        self.assertFalse(self.deletions())
        self.assertFalse(any(e['role'] == 'ctl' for e in self.db['events']))

    def test_noop_rollout_still_requires_all_group_health(self):
        for node in self.db['nodes'].values():
            node.update(image='candidate', revision='new')
        self.db['mode'] = 'proof-failure'
        self.save()
        run = self.run_shell()
        self.assertNotEqual(run.returncode, 0)
        saved = self.refresh()
        self.assertIsNone(saved['operation'])
        self.assertFalse(self.deletions())
        self.assertTrue(any('verify-quorum' in e['args'] for e in self.db['events']))

    def test_missing_store_is_never_created(self):
        self.db['store'] = None
        self.save()
        run = self.run_shell()
        self.assertNotEqual(run.returncode, 0)
        self.refresh()
        self.assertFalse(self.deletions())
        self.assertIsNone(self.db['store'])
        self.assertFalse(any('create' in e['args'] or 'apply' in e['args'] for e in self.db['events']))

    def test_conflict_and_failed_prefix_do_not_delete_or_release(self):
        for mode in ('conflict', 'proof-failure'):
            with self.subTest(mode=mode):
                self.setUp()
                self.db['mode'] = mode
                self.save()
                run = self.run_shell()
                self.assertNotEqual(run.returncode, 0)
                saved = self.refresh()
                self.assertFalse(self.deletions())
                self.assertFalse(any('undrain' in e['args'] for e in self.db['events']))
                if mode == 'proof-failure':
                    self.assertIsNotNone(saved['operation'])
                    self.assertIsNone(saved['operation']['admission'])

    def test_ambiguous_delete_resumes_same_operation_and_uid_once(self):
        self.db['mode'] = 'after-delete'
        self.save()
        failed = self.run_shell()
        self.assertNotEqual(failed.returncode, 0)
        saved = self.refresh()
        original = saved['operation']
        self.assertEqual(original['source']['node_id'], 3)
        self.assertIsNotNone(original['admission'])
        self.assertIsNone(original['replacement'])
        self.assertEqual(len(self.deletions()), 1)
        resumed = self.run_shell()
        self.assertEqual(resumed.returncode, 0, resumed.stderr + resumed.stdout)
        final = self.refresh()
        self.assertIsNone(final['operation'])
        self.assertEqual(final['generation'], 2)
        self.assertEqual(final['completion']['fence']['reservation_id'], original['fence']['reservation_id'])
        self.assertEqual(final['completion']['source'], original['source'])
        self.assertEqual(len(self.deletions()), 1)

    def test_sigterm_keeps_admitted_operation_and_does_not_undrain(self):
        self.db['mode'] = 'pause-drain'
        self.save()
        worker = self.run_shell(asynchronous=True)
        deadline = time.monotonic() + 25
        while not load(self.env['FIXTURE_DB']).get('paused'):
            self.assertIsNone(worker.poll())
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.05)
        os.killpg(worker.pid, signal.SIGTERM)
        worker.communicate(timeout=10)
        self.assertNotEqual(worker.returncode, 0)
        saved = self.refresh()
        self.assertIsNotNone(saved['operation']['admission'])
        self.assertIsNone(saved['operation']['replacement'])
        self.assertFalse(self.deletions())
        self.assertFalse(any('undrain' in e['args'] for e in self.db['events']))

    def test_bound_replacement_container_restart_cannot_refresh_the_plan(self):
        self.db['mode'] = 'after-bind'
        self.save()
        failed = self.run_shell()
        self.assertNotEqual(failed.returncode, 0)
        saved = self.refresh()
        original = saved['operation']
        self.assertIsNotNone(original['replacement'])
        original_uid = original['replacement']['pod_uid']
        self.db['nodes']['3']['boot'] = f'{999:032x}'
        self.save()
        resumed = self.run_shell()
        self.assertNotEqual(resumed.returncode, 0)
        saved = self.refresh()
        self.assertIsNotNone(saved['operation'])
        self.assertEqual(saved['operation']['replacement']['pod_uid'], original_uid)
        self.assertEqual(saved['operation']['replacement']['process_incarnation'],
                         original['replacement']['process_incarnation'])
        self.assertEqual(len(self.deletions()), 1)

    def test_partial_retirement_takeover_restores_same_operation_before_release(self):
        self.db['mode'] = 'retire-failure'
        self.save()
        failed = self.run_shell()
        self.assertNotEqual(failed.returncode, 0)
        saved = self.refresh()
        original = saved['operation']
        self.assertIsNotNone(original['replacement'])
        self.assertEqual(self.db['nodes']['1']['phase'], 'retired')
        resumed = self.run_shell()
        self.assertEqual(resumed.returncode, 0, resumed.stderr + resumed.stdout)
        saved = self.refresh()
        self.assertIsNone(saved['operation'])
        self.assertEqual(saved['completion']['source'], original['source'])
        self.assertEqual(saved['completion']['replacement'], original['replacement'])
        self.assertEqual(len(self.deletions()), 1)

    def test_competing_actual_consumers_have_one_acknowledged_executor(self):
        self.db['mode'] = 'race'
        self.save()
        prefix = ('kubectl get namespace test -o json >"${WORK}/namespace.json"; '
                  'kubectl -n test get statefulset voters -o json >"${WORK}/statefulset.json"; '
                  '"${CTL}" reservation-cell --namespace-object "${WORK}/namespace.json" '
                  '--statefulset-object "${WORK}/statefulset.json" --group-count 4 --core-count 2 >"${CELL}"; '
                  'maintenance_executor=$(maintenance_new_id); maintenance_reserve 3')
        workers = [self.run_shell(prefix, asynchronous=True) for _ in range(2)]
        reports = [w.communicate(timeout=40) for w in workers]
        self.assertEqual(sorted(w.returncode for w in workers), [0, 1], reports)
        saved = self.refresh()
        self.assertEqual(saved['generation'], 1)
        self.assertIsNotNone(saved['operation']['admission'])
        self.assertEqual(self.db['arrivals'], 2)
        self.assertFalse(self.deletions())
        activations = [e for e in self.db['events'] if 'activate-maintenance-fence' in e['args']]
        self.assertEqual(len(activations), 1)


if __name__ == '__main__':
    if not os.environ.get('URSULA_CTL_BINARY'):
        raise SystemExit('URSULA_CTL_BINARY must point to the source-matched native CLI')
    unittest.main()
