# Complete Shutdown Protection Documentation

## Overview

This application implements **COMPLETE EXTERNAL SHUTDOWN PROTECTION** - it will **ONLY** shut down when explicitly requested by the application itself. All external shutdown signals are completely ignored to prevent data corruption and maintain processing integrity.

## Key Protection Features

### 1. Complete Signal Blocking

The application completely ignores **ALL** external shutdown signals:

- **SIGTERM**: Completely ignored (ECS termination requests)
- **SIGINT**: Completely ignored (Ctrl+C interrupts)  
- **SIGHUP**: Completely ignored (Hangup signals)
- **SIGQUIT**: Completely ignored (Quit signals)
- **SIGUSR1**: **ONLY** accepted signal for voluntary shutdown

### 2. Self-Invocation Only Shutdown

The application **ONLY** shuts down through these methods:
- **SIGUSR1 signal**: Voluntary shutdown request
- **Application completion**: Natural process completion
- **Programmatic request**: Internal `request_voluntary_shutdown()` call

### 3. Enhanced ECS Task Protection

When running in AWS ECS with strengthened settings:
- **Protection Extension**: 10 minutes
- **Protection Buffer**: 3 minutes  
- **Check Interval**: 30 seconds
- **Maximum Protection**: 2 hours
- **Baseline Protection**: Always active on startup

## Protection Modes

### Critical Session Protection

When video processing begins, the session is marked as "critical":

```python
session.set_critical(True)  # Enables ECS task protection
```

During critical sessions:
- All external shutdown signals (SIGTERM, SIGINT, etc.) are **completely ignored**
- ECS task protection prevents AWS from terminating the task
- The application logs detailed protection status information

### Voluntary Shutdown

For controlled shutdowns, use the voluntary shutdown mechanism:

```bash
# Send voluntary shutdown signal
kill -USR1 <pid>
```

Or programmatically:
```python
request_voluntary_shutdown()
```

This removes baseline protection and allows clean shutdown after critical sessions complete.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ECS_PROACTIVE_PROTECTION` | `true` | Enable baseline protection on startup |
| `ECS_ENABLE_TASK_PROTECTION` | `false` | Force enable ECS task protection |
| `ECS_CLUSTER_NAME` | `video-processing-cluster` | ECS cluster name |

## Logging and Monitoring

The application provides detailed logging for protection events:

### Protection Status Logs
```
ECS Task Protection Manager initialized with enhanced settings:
- Protection extension interval: 600s
- Protection buffer: 180s
- Check interval: 30s
- Max protection duration: 7200s
- Proactive protection: true
```

### Signal Blocking Logs
```
SHUTDOWN BLOCKED: 1 critical processing sessions active
Active sessions: ['session_abc12345']
Signal ignored to prevent data corruption and maintain processing integrity.
ECS task protection is ACTIVE - termination requests will be blocked by AWS
```

### Critical Session Logs
```
Session session_abc12345 marked as CRITICAL - ECS task protection enabled
Total critical sessions now: 1
ECS task protection is active and will prevent termination
Gap protection: SAFE (margin: 150s)
```

## Operational Guidelines

### Normal Operation
1. Application starts with baseline protection (if enabled)
2. External shutdown signals are ignored during critical processing
3. Processing completes naturally and protection is removed
4. Application continues polling for new messages

### Planned Maintenance
1. Send voluntary shutdown signal: `kill -USR1 <pid>`
2. Application removes baseline protection
3. Wait for critical sessions to complete
4. Application shuts down cleanly

### Emergency Shutdown
1. Critical sessions will complete naturally (up to 2 hours max)
2. ECS task protection prevents premature termination
3. Use force disable only in extreme circumstances

### Scale-in Events
1. ECS scale-in requests are blocked by task protection
2. Critical processing continues uninterrupted
3. Task is removed from scale-in only after processing completes

## Troubleshooting

### Protection Not Working
- Check if running in ECS environment
- Verify ECS permissions include `ecs:UpdateTaskProtection`
- Check logs for ECS client initialization errors

### Tasks Not Terminating
- Check for stuck critical sessions in logs
- Verify max protection duration settings
- Use voluntary shutdown signal for clean termination

### High Protection Duration
- Monitor for long-running processing sessions
- Check for processing errors that prevent completion
- Adjust max protection duration if needed

## Safety Features

### Maximum Protection Duration
Protection is automatically disabled after 2 hours to prevent indefinite protection due to bugs or stuck processes.

### Gap Protection
The protection manager checks every 30 seconds and maintains a 3-minute buffer to prevent protection gaps during renewal.

### Failure Recovery
The protection manager includes exponential backoff and retry logic for API failures, with detailed error logging.

### Force Disable
In extreme cases, protection can be force-disabled during controlled application shutdown.

## Best Practices

1. **Always use voluntary shutdown** for planned maintenance
2. **Monitor protection duration** to identify long-running or stuck processes
3. **Review protection logs** regularly to ensure proper operation
4. **Test protection** in development environments before production deployment
5. **Set appropriate timeouts** for video processing operations

## Signal Reference

| Signal | Behavior | Use Case |
|--------|----------|----------|
| SIGTERM | Blocked during critical processing | ECS task termination, scale-in |
| SIGINT | Blocked during critical processing | Manual interruption (Ctrl+C) |
| SIGHUP | Blocked during critical processing | Terminal disconnection |
| SIGQUIT | Blocked during critical processing | Quit request |
| SIGUSR1 | Initiates voluntary shutdown | Planned maintenance shutdown |

This enhanced protection system ensures that video processing operations complete successfully even in the face of external termination requests, maintaining data integrity and preventing processing failures.
