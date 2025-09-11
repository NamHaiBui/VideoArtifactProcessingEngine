# Video Artifacts Processing Engine

A video processing application.

## Fargate SPOT Optimization

This service can run on a mixed capacity strategy (On-Demand + FARGATE_SPOT) for cost reduction while preserving safe processing of critical sessions.

### Key enhancements

* Mixed capacity provider strategy (1 base on-demand task + weighted Spot tasks) for baseline availability.
* Spot interruption (SIGTERM) triggers expedited voluntary shutdown path instead of being fully ignored.
* Dynamic drain timeout: default 30s, extended to 95s for Spot interruption to fit inside the 2m termination window.
* ECS Task Protection still used during critical sessions; protection is voluntarily lifted during Spot drain.

### Environment variables

* FARGATE_SPOT=true (or CAPACITY_PROVIDER=FARGATE_SPOT) marks runtime as Spot mode for signal semantics.
* CRITICAL_SESSION_DRAIN_TIMEOUT (seconds) override normal drain wait.
* SPOT_DRAIN_TIMEOUT (seconds) override Spot interruption drain wait (default 95).

### Deployment steps (example)

1. Register / update task definition (includes protection + health check).
2. Create or update service using `ecs-service-definition-spot.json` (after templating variables) – ensures capacityProviderStrategy mixes FARGATE and FARGATE_SPOT.
3. Tag tasks with `TaskProtection=enabled` (already in task def) for observability.
4. Monitor CloudWatch Logs for lines containing `FARGATE_SPOT interruption notice` to audit drains.

### Operational notes

* A Spot SIGTERM sets `spot_termination_imminent` and calls the voluntary shutdown path.
* If no critical session is active, the task exits quickly allowing service replacement.
* If a critical session is active, we wait up to SPOT_DRAIN_TIMEOUT or its completion, whichever comes first.
* Emergency protection still applies for non-Spot external signals.

### Metrics / Alarms (recommended)

* Alarm on high ratio of interruption drains to total tasks (capacity churn).
* Alarm if average drain duration hits the timeout (indicates tasks not finishing promptly).

To simulate locally, send SIGTERM with `FARGATE_SPOT=true` env var set.
