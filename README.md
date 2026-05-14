# Video Upload Platform — System Design

## Overview

This repository documents the backend architecture for a video upload platform — think YouTube-style upload flow. It covers the full pipeline from a user initiating an upload to a processed, playback-ready video in object storage.

The design prioritizes operational simplicity over theoretical optimality. With 5 engineers and a 6-month launch timeline, the right call is managed services, a low component count, and deferring complexity until there's evidence it's actually needed.

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Upload Flow — Step by Step](#upload-flow--step-by-step)
3. [Technology Stack](#technology-stack)
4. [Design Trade-offs & Assumptions](#design-trade-offs--assumptions)
5. [API Specification](#api-specification)
6. [Code Samples](#code-samples)
7. [Operational & Infrastructure Notes](#operational--infrastructure-notes)
8. [AI Usage Disclosure](#ai-usage-disclosure)

---

## System Architecture

See [`architecture/diagram.md`](architecture/diagram.md) for the annotated architecture diagram and component descriptions.

**Flow summary:** Client → API Gateway → Upload Service → S3 (direct upload) → SQS → Job Submission Lambda → MediaConvert → EventBridge → Completion Lambda → PostgreSQL. Client polls status via the Upload Service.

---

## Upload Flow — Step by Step

| Step | Actor | Action |
|------|-------|--------|
| 1 | Client | Authenticates via Cognito, receives JWT |
| 2 | Client | `POST /videos/upload-url` with filename, size, MIME type |
| 3 | Upload Service | Validates request, creates `video` record (status: `pending`), returns presigned S3 PUT URL + `videoId` |
| 4 | Client | Uploads file **directly** to S3 — no bytes touch our servers. Small files use a single presigned PUT; large files (>100 MB) use S3 multipart upload for resumability. |
| 5 | S3 | Fires `s3:ObjectCreated` → SQS `transcoding-jobs` queue |
| 6 | Client | (Optional) `POST /videos/:id/metadata` to submit title/description |
| 7 | Job Submission Lambda | Triggered by SQS; submits MediaConvert job; updates video status → `queued` |
| 8 | MediaConvert | Transcodes to HLS (360p/720p/1080p), extracts thumbnail, writes output to `processed-videos` S3 |
| 9 | MediaConvert | Emits job state change events via EventBridge: `PROGRESSING` when the job starts, `COMPLETE` or `ERROR` on finish |
| 10 | Completion Lambda | `PROGRESSING` → status `processing`. `COMPLETE` → status `ready`, stores output paths, tags raw S3 object for lifecycle expiry. `ERROR` → status `failed`. |
| 11 | Client | Polls `GET /videos/:id/status` — receives `ready` with HLS output URLs |

---

## Technology Stack

### API Layer

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Runtime | **Node.js 20 (TypeScript)** | Fast I/O, strong ecosystem, easy to hire for. TypeScript catches a meaningful class of bugs at compile time — worth it on a small team. |
| Framework | **Express.js** | Minimal, well-understood, easy to onboard onto. No framework magic to debug when something goes wrong. |
| API Gateway | **AWS API Gateway (HTTP API)** | JWT validation, rate limiting, and TLS termination without writing any of it. Removes a whole category of infra code. |

### Storage

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Object Storage | **Amazon S3** | Industry standard for this use case. Native SQS event integration and presigned URL support. Two buckets: `raw-uploads` (lifecycle: delete 7 days post-processing) and `processed-videos` (permanent). |
| Metadata DB | **PostgreSQL on Amazon RDS** | Relational model fits video metadata well. RDS handles backups, patching, and failover — the team doesn't run a database. At 20–50k DAU with 5–10s polling intervals, direct reads are well within capacity without a cache layer. |

### Processing

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Queue | **Amazon SQS (Standard)** | Decouples upload from transcoding and absorbs traffic spikes. At-least-once delivery is fine; job submission is idempotent. |
| Transcoding | **AWS MediaConvert**  | No workers to build or scale, no ffmpeg to manage, no on-call burden for processing failures. Handles HLS at multiple renditions plus thumbnail extraction in one job. Pay-per-minute, so no idle compute cost. |
| Event Routing | **Amazon EventBridge** | Routes MediaConvert job state changes (`PROGRESSING`, `COMPLETE`, `ERROR`) to the Completion Lambda. Native integration — no polling or custom webhook plumbing needed. |
| Output Format | **HLS (HTTP Live Streaming)** | Adaptive bitrate — poor mobile connections get 360p, desktop gets 1080p. Native iOS support without a JS player. |

### Auth

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Authentication | **Amazon Cognito** | Managed user pools, JWT issuance, social login. Saves weeks of auth work on a 6-month timeline. |

### CDN

| Component | Choice | Rationale |
|-----------|--------|-----------|
| CDN | **Amazon CloudFront** | Origin for the `processed-videos` bucket. Edge locations in Canada and Europe match the target user base. Relevant for playback — not a core concern of the upload flow itself. |

### Observability

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Logging | **AWS CloudWatch Logs** | Zero-config with ECS and Lambda. Structured JSON logs. |
| Metrics & Alarms | **CloudWatch Metrics + Alarms** | SQS queue depth, ECS CPU, RDS connections, MediaConvert job failures. |
| Alerting | **CloudWatch Alarms → SNS → Slack** | Alerts on DLQ depth, job failures, API error rates. |

---

## Design Trade-offs & Assumptions

### Assumptions

| Assumption | Value |
|------------|-------|
| Max video file size | 5 GB |
| Typical video duration | 1–30 minutes |
| Estimated upload volume | ~5,000–15,000 uploads/day at peak DAU |
| Output formats | HLS with 360p, 720p, 1080p renditions |
| Supported input formats | MP4, MOV, AVI, MKV, WebM |
| Presigned URL expiry | 60 minutes |
| Processing SLA | Video ready within ~10 minutes of upload completion |
| Auth | Users are authenticated before uploading |

### Trade-offs

#### 1. Presigned URL (client → S3 direct) vs. Proxy Upload through API

**Chosen:** Presigned URL direct-to-S3

Upload traffic never touches our servers — no bandwidth cost, no server bottleneck, and it scales to any file size without changes. The tradeoff is less control over the upload stream mid-flight, but that's mitigated by post-upload validation in the processing step. At 5 GB max files and 50k DAU, proxying would require significant bandwidth and beefy instances. Direct upload is the obvious call.

**Multipart upload for large files:**
For uploads above ~100 MB, clients should use S3 multipart upload rather than a single PUT. The file is split into independently uploaded parts, making the transfer resumable if the connection drops — which matters for mobile users on unreliable networks. Each part gets its own presigned URL; the client finalizes with a `CompleteMultipartUpload` call. The `ObjectCreated` event fires only after the multipart upload completes, so nothing downstream changes.

#### 2. AWS MediaConvert vs. Self-Managed ffmpeg Workers (ECS Fargate)

**Chosen for launch:** AWS MediaConvert

Running a transcoding fleet means managing autoscaling, handling spot interruptions, monitoring worker health, and keeping ffmpeg pinned and patched across container images. That's a real operational surface area that a 5-person team on a 6-month timeline can't absorb. MediaConvert eliminates all of it — no containers to build, no workers to scale, no 2 AM pages when a job fails in a weird way.

It also handles thumbnail extraction in the same job. Pay-per-minute pricing means no idle compute during off-peak hours.

Optimizing transcoding cost before there's production data on actual video lengths and upload volumes is premature. The time to evaluate self-managed workers is when MediaConvert shows up as a meaningful line item in the bill.

**Future path — ECS Fargate + ffmpeg workers:**
- At scale, self-managed workers on Fargate Spot can cut per-minute transcoding cost significantly.
- If the pipeline needs custom logic (watermarking, proprietary codecs, content analysis), a custom worker gives full control.
- A reference implementation lives in [`code/transcoding_worker.py`](code/transcoding_worker.py) — not the launch path, but ready if the team needs to migrate.

#### 3. PostgreSQL for Status Polling vs. Redis Cache

**Chosen:** PostgreSQL directly, no cache layer at launch

Even at 50k DAU, only a fraction of users are actively uploading and polling at any given moment. Processing windows are ~5–10 minutes; clients poll every 5–10 seconds for that window, then stop. The status query is a single indexed primary key lookup — a `db.t3.medium` handles thousands of those per second.

Adding Redis means operating a second stateful service, keeping it consistent with the DB, and monitoring it — real overhead for a problem that doesn't exist yet.

**If it ever becomes a concern:**
- A read replica absorbs polling load with zero application code changes.
- A short-lived in-process cache (e.g., 3-second TTL) cuts DB hits further without adding a new service.
- WebSockets or SSE via API Gateway can replace polling entirely if real-time status UX becomes a product priority — without touching the data model or the `GET /videos/:id/status` contract.

#### 4. SQS Standard vs. FIFO Queue

**Chosen:** SQS Standard

Job submission to MediaConvert is idempotent, so at-least-once delivery is fine — duplicate messages are detected and skipped. Standard queues have higher throughput and handle upload spikes better than FIFO. Per-video jobs are independent, so ordering doesn't matter.

#### 5. Managed Services vs. Self-Hosted

**Chosen:** Heavily managed (RDS, SQS, MediaConvert, Cognito, ECS Fargate)

A 5-person team with a 6-month deadline can't afford to operate Kafka, self-hosted Postgres, custom auth, and a transcoding fleet simultaneously. Managed services cost more per unit, but at this scale the cost delta is small and the time savings are large. Optimize when there's data to justify it.

#### 6. Polling vs. WebSockets for Status Updates

**Chosen:** Polling

`GET /videos/:id/status` every 5–10 seconds is simple to implement, debug, and reason about. The processing window is 5–10 minutes — a few extra seconds of status latency isn't a meaningful UX problem at this stage. Each request is a cheap indexed PK read, and since uploads are a minority of DAU at any moment, the aggregate load stays modest even at 50k DAU.

WebSocket support via API Gateway can be layered on later without changing the backend data model or the polling endpoint.

---

## API Specification

See [`api/openapi.yaml`](api/openapi.yaml) for the full OpenAPI 3.0 spec.

### Key Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/videos/upload-url` | Presigned S3 URL for single-PUT upload (small files) |
| `POST` | `/videos/multipart/init` | Initialize S3 multipart upload (large files >100 MB) |
| `GET` | `/videos/:id/multipart/:uploadId/url?partNumber=N` | Presigned URL for one multipart part |
| `POST` | `/videos/:id/multipart/:uploadId/complete` | Finalize multipart upload |
| `DELETE` | `/videos/:id/multipart/:uploadId/abort` | Abort multipart upload |
| `POST` | `/videos/:id/metadata` | Submit title, description, tags |
| `GET` | `/videos/:id/status` | Poll processing status |
| `DELETE` | `/videos/:id` | Cancel/delete a video |

---

## Code Samples

| File | Description |
|------|-------------|
| [`code/presigned_url.js`](code/presigned_url.js) | Upload Service — presigned URL generation and status polling |
| [`code/mediaconvert_job.py`](code/mediaconvert_job.py) | Lambda functions — Job Submission (SQS trigger) and Completion Handler (EventBridge trigger) |
| [`code/transcoding_worker.py`](code/transcoding_worker.py) | **Alternative path** — self-managed ECS worker with ffmpeg (future optimization) |

---

## AI Usage Disclosure

AI tools were used during development as an engineering assistant for:

- brainstorming architecture alternatives
- refining documentation wording and organization
- reviewing tradeoffs and operational considerations
- generating and refining example code snippets

All architectural decisions, technology selections, tradeoff analysis, and final refinements were reviewed and intentionally chosen by me.

---

## Operational & Infrastructure Notes

See [`infrastructure/notes.md`](infrastructure/notes.md) for full details.

### Summary

- **Cloud Provider:** AWS (Canada/Europe coverage via CloudFront + multi-AZ RDS)
- **IaC:** Terraform (proposed — not included in this repo)
- **CI/CD:** GitHub Actions → ECR → ECS rolling deploy (proposed)
- **Autoscaling:** Upload Service scales on CPU; MediaConvert scales automatically; Lambda scales with event volume
- **Fault Tolerance:** SQS DLQ after 3 retries, CloudWatch alarm on DLQ depth, RDS Multi-AZ
- **Backups:** RDS automated daily snapshots, S3 versioning on processed-videos bucket
