# Operational & Infrastructure Notes

---

## Cloud Provider

**AWS** — native integration across S3, SQS, MediaConvert, ECS, RDS, and CloudFront. Strong regional presence in `ca-central-1` and `eu-west-1`/`eu-central-1`. Core infrastructure uses AWS first-party services; Slack is only used as an alert destination.

---

## Infrastructure as Code

In a production implementation, AWS resources would be managed with Terraform or AWS CDK. This repository is a system design artifact, so IaC files are intentionally not included.

---

## CI/CD Pipeline

In production, GitHub Actions or a similar CI/CD system would build and deploy the Upload Service, Lambda functions, and database migrations. The exact workflow files are intentionally out of scope for this design submission.

---

## Autoscaling

### Upload Service (ECS Fargate)
- Min: 2 tasks, Max: 10 tasks
- Scale out: CPU > 70% for 2 minutes
- Scale in: CPU < 30% for 5 minutes
- The service is lightweight (no video bytes, no heavy compute) — 2 tasks handle normal load comfortably

### MediaConvert
- Fully managed — scales automatically with job volume
- No worker fleet to configure or monitor
- Handles upload spikes natively; SQS buffers jobs during bursts

### Lambda (Job Submission Lambda + MediaConvert Completion Lambda)
- Scales automatically with SQS and EventBridge event volume
- Concurrency limit: 50 (prevents DB connection exhaustion)

---

## Fault Tolerance & Retry Strategies

### SQS Dead-Letter Queue (DLQ)

The `transcoding-jobs` queue has a DLQ configured:

```
maxReceiveCount: 3
```

After 3 failed job submission attempts, the message moves to the DLQ.
A CloudWatch alarm fires when DLQ depth > 0, alerting the on-call engineer.

Recovery steps:
1. Inspect the failed message to diagnose the issue
2. Fix the bug and redeploy the Lambda
3. Move messages back to the main queue via the SQS console or a script

### MediaConvert Job Failures

- MediaConvert emits job state change events via EventBridge for `PROGRESSING`, `COMPLETE`, and `ERROR`
- The MediaConvert Completion Lambda handles all three: `PROGRESSING` → `processing`, `COMPLETE` → `ready`, `ERROR` → `failed`
- On `ERROR`, the error message from MediaConvert is stored in `transcoding_jobs.error_msg`
- A CloudWatch alarm fires on MediaConvert error events for visibility

### Job Submission Idempotency

- Before submitting a MediaConvert job, the Job Submission Lambda checks if a job already exists for the video in `queued`, `processing`, or `completed` state
- Duplicate SQS deliveries (at-least-once) are safely handled without double-billing

### Database

- **RDS Multi-AZ:** Automatic failover to standby replica in ~60 seconds on primary failure
- **Connection pooling:** Upload Service uses `pg-pool` (Node.js). Lambda functions reuse connections across warm invocations
- **Automated backups:** Daily snapshots retained for 7 days. Point-in-time recovery enabled

### S3

- **Versioning:** Enabled on `processed-videos` bucket — accidental overwrites are recoverable
- **Lifecycle rules:** Raw uploads are deleted 7 days after processing completes. The MediaConvert Completion Lambda tags the raw object with `status=processed` on a successful `COMPLETE` event. The S3 lifecycle rule on `raw-uploads` uses this tag as its filter, so only confirmed-processed objects are expired. Tagging is best-effort — a failure is logged but does not affect video status.

### Upload Service

- **Stateless:** Multiple ECS tasks behind API Gateway — any task handles any request
- **Health checks:** ECS checks `GET /health` every 30 seconds; unhealthy tasks are replaced automatically

---

## Observability

### Structured Logging

All services emit JSON logs to CloudWatch Logs:

```json
{
  "time": "2026-05-12T14:05:23Z",
  "level": "INFO",
  "service": "mediaconvert-submit",
  "action": "job_created",
  "videoId": "a1b2c3d4-...",
  "mcJobId": "1234567890123-abcdef"
}
```

### Key CloudWatch Alarms

| Alarm | Threshold | Action |
|-------|-----------|--------|
| DLQ depth > 0 | Any message in DLQ | SNS → Slack #alerts |
| SQS queue depth > 100 | Sustained 10 min | SNS → Slack (possible scaling issue) |
| MediaConvert ERROR events | Any error | SNS → Slack |
| RDS CPU > 80% | Sustained 5 min | SNS → Slack |
| API Gateway 5xx rate > 1% | 5 min window | SNS → Slack |
| Lambda error rate > 5% | 5 min window | SNS → Slack |

### CloudWatch Dashboard

Single dashboard showing:
- SQS queue depth (transcoding-jobs, DLQ)
- MediaConvert job counts (submitted, complete, error) — via CloudWatch Metrics
- ECS task count and CPU (upload service)
- API Gateway request rate, latency (p50/p95/p99), error rate
- RDS connections, CPU, read/write IOPS
- S3 upload rate (PutObject requests/min)

---

## Security

### Network
- Upload Service and RDS run in a private VPC subnet — not publicly accessible
- API Gateway is the only public entry point
- S3 buckets are private; access is via presigned URLs or IAM roles only
- CloudFront serves processed videos — S3 bucket policy denies direct public access

### IAM (Least Privilege)
- **Upload Service role:** `s3:PutObject` on `raw-uploads/*` (for presigned URL generation only)
- **Job Submission Lambda role:** `sqs:ReceiveMessage` + `sqs:DeleteMessage`, `mediaconvert:CreateJob`, `iam:PassRole` (to pass MediaConvert role)
- **MediaConvert role:** `s3:GetObject` on `raw-uploads/*`, `s3:PutObject` on `processed-videos/*`
- **MediaConvert Completion Lambda role:** `rds-db:connect` (via IAM auth) or `DATABASE_URL` via Secrets Manager; `s3:PutObjectTagging` on `raw-uploads/*` (to tag processed objects for lifecycle expiry)

### Data
- S3 server-side encryption: SSE-S3 (AES-256) on both buckets
- RDS encryption at rest: enabled
- All traffic over HTTPS/TLS (API Gateway enforces TLS 1.2+)
- Presigned URLs are scoped to a specific S3 key and expire in 60 minutes
- Database credentials stored in AWS Secrets Manager, not environment variables

### Auth
- All API endpoints require a valid Cognito JWT validated by API Gateway
- Upload ownership enforced in the service layer — users can only access their own videos

---

## Cost Considerations

Transcoding and CDN egress are the dominant cost drivers. Everything else is relatively minor at this scale.

**Rough directional ranges at 20–50k DAU:**

| Service | Notes |
|---------|-------|
| AWS MediaConvert | Billed per minute of output video. At ~10k uploads/day averaging 10 min each, across 3 renditions, expect this to be the largest line item. Actual cost depends heavily on video length and rendition count. |
| S3 storage | Raw uploads are short-lived (7-day lifecycle). Processed video storage grows over time and depends on average video length and bitrate. |
| CloudFront | Relevant for playback egress, not the upload flow. Cost depends on how much processed video is actually watched. |
| ECS Fargate (upload service) | Lightweight service — modest cost. |
| RDS PostgreSQL (db.t3.medium Multi-AZ) | Fixed monthly cost, predictable. |
| SQS + Lambda | Negligible at this scale. |

**Key point:** Actual costs are highly workload-dependent. Run a cost estimate in the AWS Pricing Calculator once you have real data on average video length and upload volume. If MediaConvert costs become significant at scale, migrating to self-managed Fargate workers (see `code/transcoding_worker.py`) can reduce per-minute transcoding cost substantially.
