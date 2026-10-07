# Maintenance authority migration

The Kubernetes ConfigMap reservation and provider-specific startup protocol have
been removed. Meta Raft is the authority for placement, process epochs and
maintenance operations. See [meta control-plane operations and migration](meta-control-plane.md)
for the replacement protocol and coordinated restart procedure.

Old `reservation-*`, `startup-admit`, and external maintenance-fence CLI commands
are unsupported. Existing-PVC ordinary restarts claim a fresh epoch directly;
lost-volume replacement requires an explicit `RebuildReplica` operation.
