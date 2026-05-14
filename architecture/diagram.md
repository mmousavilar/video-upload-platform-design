# System Architecture Diagram

The diagram shows the complete upload flow from client to playback-ready storage.

![System Architecture Diagram](diagram.png)

> **Future optimization path:** If transcoding costs become significant at scale or custom pipeline logic is needed, the MediaConvert step can be replaced with self-managed ECS Fargate workers running ffmpeg. See [`code/transcoding_worker.py`](../code/transcoding_worker.py) for a reference implementation.

---

## Component Descriptions

### API Gateway
- Single public entry point; validates Cognito JWTs before forwarding
- Rate limiting per user, TLS termination

### Upload Service (Node.js/TypeScript on ECS Fargate)
- Stateless REST service, min 2 tasks
- Generates presigned S3 PUT URLs (60-minute TTL)
- Creates video records in PostgreSQL (`pending`) on upload initiation
- Serves status polling directly from PostgreSQL
- Does **not** handle video bytes; does **not** receive MediaConvert callbacks

### Amazon S3 (raw-uploads)
- Receives video files directly from clients via presigned URL
- Fires `s3:ObjectCreated` to SQS on new uploads
- Lifecycle rule: expires raw objects 7 days after processing. The rule filters on the `status=processed` tag applied by the Completion Lambda on success — so only confirmed-processed objects are expired.
- For files above ~100 MB, clients should use S3 multipart upload. Each part gets its own presigned URL; the `ObjectCreated` event fires only after `CompleteMultipartUpload`, so the downstream pipeline is unchanged.

### SQS (transcoding-jobs)
- Decouples upload from transcoding; absorbs traffic spikes
- Standard queue, at-least-once delivery, visibility timeout 15 min
- DLQ after 3 failed attempts; CloudWatch alarm on DLQ depth > 0

### Job Submission Lambda
- Triggered by SQS; checks idempotency before submitting (skips if job already exists in `queued`, `processing`, or `completed` state)
- Submits MediaConvert job with `videoId` and `rawS3Key` in `UserMetadata`
- Updates video status to `queued`

### AWS MediaConvert
- Fully managed transcoding — no workers, no ffmpeg, no autoscaling to configure
- Produces HLS output at 360p, 720p, 1080p; extracts thumbnail in the same job
- Emits `PROGRESSING`, `COMPLETE`, and `ERROR` state change events via EventBridge
- Pay-per-minute; no idle compute cost

### EventBridge
- Routes MediaConvert job state changes (`PROGRESSING`, `COMPLETE`, `ERROR`) to the Completion Lambda
- Rule scoped to `aws.mediaconvert` source — no custom webhook infrastructure needed

### Completion Lambda
- `PROGRESSING` → sets video and job status to `processing`, records `started_at`
- `COMPLETE` → sets video status to `ready`, stores output paths and thumbnail URL, tags raw S3 object `status=processed` for lifecycle expiry (best-effort — tagging failure is logged but does not affect video status)
- `ERROR` → sets video status to `failed`, stores error message in `transcoding_jobs.error_msg`

### PostgreSQL (Amazon RDS)
- Stores video metadata, job status, and user associations
- Multi-AZ; status polling reads directly from this DB
- Schema: `videos`, `transcoding_jobs`, `users`

### CloudFront (CDN)
- Origin: `processed-videos` S3 bucket
- Serves HLS segments for playback — not in scope for the upload flow itself

---

## Status Lifecycle

```
Video:           pending → queued → processing → ready | failed
Transcoding job: queued  → processing → completed | failed
```

`ready` is the user-facing video state. `completed` is the internal transcoding job state set on a successful MediaConvert `COMPLETE` event.

---

## Data Model

```sql
CREATE TABLE users (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cognito_sub VARCHAR(255) UNIQUE NOT NULL,
    email       VARCHAR(255) UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE videos (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          UUID NOT NULL REFERENCES users(id),
    title            VARCHAR(500),
    description      TEXT,
    status           VARCHAR(50) NOT NULL DEFAULT 'pending',
                     -- pending | queued | processing | ready | failed
    raw_s3_key       VARCHAR(1000),
    output_s3_prefix VARCHAR(1000),
    thumbnail_url    VARCHAR(1000),
    duration_seconds INTEGER,
    file_size_bytes  BIGINT,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE transcoding_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    video_id            UUID NOT NULL REFERENCES videos(id),
    mediaconvert_job_id VARCHAR(255),          -- MediaConvert job ARN/ID
    status              VARCHAR(50) NOT NULL DEFAULT 'queued',
                        -- queued | processing | completed | failed
    attempts            INTEGER DEFAULT 0,
    error_msg           TEXT,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_videos_user_id ON videos(user_id);
CREATE INDEX idx_videos_status  ON videos(status);
CREATE INDEX idx_jobs_video_id  ON transcoding_jobs(video_id);
```
