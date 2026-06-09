# Dynamic Group Membership Phase 18 Manual Migration Runbook Docs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Document the manual-first dynamic node registration and one-group-at-a-time migration workflow now exposed through `ursulactl`.

**Architecture:** Keep the operator runbook on the `ursulactl` page because every step is a CLI command. Update deploy and operations pages only where their current text says membership changes are not exposed or where the admin endpoint list is stale.

**Tech Stack:** MDX docs, Vite docs build, `rg` verification.

---

## File Structure

- `docs/web/src/content/docs/pages/cli.mdx`: add the manual group migration runbook and extend the HTTP surface table.
- `docs/web/src/content/docs/pages/deploy-cluster.mdx`: update cluster operations and current-limit text to mention manual dynamic migration.
- `docs/web/src/content/docs/pages/operations.mdx`: update the tooling map and admin endpoint examples.

### Task 1: Documentation Red Checks

**Files:**
- Modify: none

- [x] **Step 1: Confirm the CLI page does not yet document the new commands**

Run:

```bash
rg -n "group placement commit|group migration finish" docs/web/src/content/docs/pages/cli.mdx
```

Expected: exit 1 with no output, because the runbook is not documented yet.

- [x] **Step 2: Confirm deploy docs still contain stale membership wording**

Run:

```bash
rg -n "Membership changes .*not yet exposed" docs/web/src/content/docs/pages/deploy-cluster.mdx
```

Expected: one match in the current "Limits in the current build" section.

### Task 2: Add The Manual Migration Runbook

**Files:**
- Modify: `docs/web/src/content/docs/pages/cli.mdx`

- [x] **Step 1: Add a `Group placement and migration` section after `wait-ready`**

Add a section that documents:

- one active group migration at a time;
- initial bootstrap nodes are both meta-group voters and data-capable nodes;
- the operator starts any new node process out-of-band before registering it;
- the new node is registered in the meta group with `ursulactl node register`;
- placement is inspected with `ursulactl group placement get`;
- migration intent is started with `ursulactl group migration begin`;
- the target node warms a local engine with `ursulactl group local-engine prepare`;
- the data-group leader adds learners with `ursulactl group learner add`;
- the data-group leader changes multiple voters in one OpenRaft membership operation with `ursulactl group membership change`;
- final placement is committed with `ursulactl group placement commit`;
- the migration lock is released with `ursulactl group migration finish --success`;
- omitted `--success` finishes the migration as failed.

Use this example command sequence:

```bash
ursulactl node register \
  --admin-url http://node1:4437 \
  --node-id 4 \
  --client-url http://node4:4437 \
  --cluster-url http://node4:4437

ursulactl group placement get \
  --admin-url http://node1:4437 \
  --raft-group-id 2

ursulactl group migration begin \
  --admin-url http://node1:4437 \
  --raft-group-id 2 \
  --target-voters 2,3,4 \
  --retain-removed

ursulactl group local-engine prepare \
  --admin-url http://node4:4437 \
  --raft-group-id 2

ursulactl group learner add \
  --leader-url http://node2:4437 \
  --raft-group-id 2 \
  --node-id 4 \
  --cluster-url http://node4:4437

ursulactl group membership change \
  --leader-url http://node2:4437 \
  --raft-group-id 2 \
  --voters 2,3,4

ursulactl group placement commit \
  --admin-url http://node1:4437 \
  --raft-group-id 2 \
  --voters 2,3,4 \
  --learners 1 \
  --draining 1

ursulactl group migration finish \
  --admin-url http://node1:4437 \
  --migration-id 1 \
  --success
```

- [x] **Step 2: Extend the underlying HTTP surface table**

Add rows for:

- `node register`: `POST /__ursula/admin/nodes/{node_id}/register`
- `group placement get`: `GET /__ursula/admin/groups/{raft_group_id}/placement`
- `group migration begin`: `POST /__ursula/admin/groups/{raft_group_id}/migrations`
- `group local-engine prepare`: `POST /__ursula/admin/groups/{raft_group_id}/local-engine`
- `group learner add`: `POST /__ursula/raft/{raft_group_id}/learners/{node_id}`
- `group membership change`: `POST /__ursula/raft/{raft_group_id}/membership`
- `group placement commit`: `POST /__ursula/admin/groups/{raft_group_id}/placement/commit`
- `group migration finish`: `POST /__ursula/admin/migrations/{migration_id}/finish`

### Task 3: Update Deployment And Operations Docs

**Files:**
- Modify: `docs/web/src/content/docs/pages/deploy-cluster.mdx`
- Modify: `docs/web/src/content/docs/pages/operations.mdx`

- [x] **Step 1: Update deploy cluster operation bullets**

Add a bullet for manual group migration and replace the stale limit about membership changes not being exposed with:

```markdown
- Group membership migration is manual-first: register the new node, migrate one group at a time with `ursulactl group ...`, and let the meta group serialize active migration intent. There is no automatic scheduler or balancer yet.
```

- [x] **Step 2: Update operations admin endpoint examples**

Add examples for:

```bash
curl -X POST 'http://127.0.0.1:4437/__ursula/admin/nodes/4/register?client_url=http://node4:4437&cluster_url=http://node4:4437'
curl -X POST 'http://127.0.0.1:4437/__ursula/admin/groups/2/migrations?target_voters=2,3,4&retain_removed=true'
curl -X POST 'http://127.0.0.1:4437/__ursula/admin/groups/2/placement/commit?voters=2,3,4&learners=1&draining=1'
curl -X POST 'http://127.0.0.1:4437/__ursula/admin/migrations/1/finish?success=true'
```

Also update the opening text so `ursulactl` lists manual group migration alongside restart, status, and wait-ready.

### Task 4: Verification And Commit

**Files:**
- Modify: none

- [x] **Step 1: Verify new docs content is present**

Run:

```bash
rg -n "Group placement and migration|group placement commit|group migration finish" docs/web/src/content/docs/pages/cli.mdx
rg -n "manual-first|automatic scheduler|balancer" docs/web/src/content/docs/pages/deploy-cluster.mdx docs/web/src/content/docs/pages/operations.mdx
```

Expected: both commands print matches.

- [x] **Step 2: Run docs build and diff check**

Run:

```bash
npm run build
git diff --check
```

Run `npm run build` from `docs/web`. Run `git diff --check` from the repository root.

- [x] **Step 3: Commit**

Run:

```bash
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase18-manual-migration-runbook-docs.md docs/web/src/content/docs/pages/cli.mdx docs/web/src/content/docs/pages/deploy-cluster.mdx docs/web/src/content/docs/pages/operations.mdx
git commit -m "docs(web): add manual group migration runbook"
```
