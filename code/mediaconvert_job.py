"""
MediaConvert Job Submission & Completion Handler

This module covers two responsibilities:

1. submit_job(event, context)
   AWS Lambda triggered by SQS (transcoding-jobs queue).
   Reads the S3 ObjectCreated event, submits an AWS MediaConvert job,
   and updates the video record status to 'queued'.

   MediaConvert handles:
     - HLS transcoding at 360p, 720p, 1080p
     - Thumbnail extraction (JPEG frame at 5 seconds)
   No ffmpeg, no containers, no scaling to manage.

2. handle_completion(event, context)
   AWS Lambda triggered by EventBridge on MediaConvert job state changes.
   Handles three terminal/intermediate states:
     - PROGRESSING → status = 'processing', sets started_at
     - COMPLETE    → status = 'ready', stores output paths, tags raw S3 object
     - ERROR       → status = 'failed', stores error message

   On COMPLETE, the raw upload object in S3 is tagged with status=processed
   so the bucket lifecycle rule can safely expire it after 7 days.

Why Lambda here instead of the Upload Service directly?
  The SQS consumer and the EventBridge handler are both short-lived,
  event-driven tasks. Lambda is the right fit — no always-on container
  needed, and it scales automatically with event volume.

Environment variables:
  AWS_REGION                e.g. ca-central-1
  MEDIACONVERT_ENDPOINT     Account-specific endpoint (from describe-endpoints)
  MEDIACONVERT_ROLE_ARN     IAM role MediaConvert assumes to read/write S3
  RAW_UPLOADS_BUCKET        Source bucket — used to tag raw object after processing
  PROCESSED_VIDEOS_BUCKET   Destination bucket for HLS output + thumbnails
  DATABASE_URL              PostgreSQL connection string

Dependencies:
  pip install boto3 psycopg2-binary
"""

import json
import logging
import os

import boto3
import psycopg2

log = logging.getLogger()
log.setLevel(logging.INFO)

AWS_REGION = os.environ["AWS_REGION"]
MC_ENDPOINT = os.environ["MEDIACONVERT_ENDPOINT"]
MC_ROLE_ARN = os.environ["MEDIACONVERT_ROLE_ARN"]
RAW_BUCKET = os.environ["RAW_UPLOADS_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_VIDEOS_BUCKET"]
DATABASE_URL = os.environ["DATABASE_URL"]

mediaconvert = boto3.client(
    "mediaconvert",
    region_name=AWS_REGION,
    endpoint_url=MC_ENDPOINT,
)

s3 = boto3.client("s3", region_name=AWS_REGION)

# DB connection reused across warm Lambda invocations
_db_conn = None


def get_db():
    global _db_conn
    if _db_conn is None or _db_conn.closed:
        _db_conn = psycopg2.connect(DATABASE_URL)
        _db_conn.autocommit = False
    return _db_conn


# ---------------------------------------------------------------------------
# Handler 1: SQS → submit MediaConvert job
# ---------------------------------------------------------------------------

def submit_job(event, context):
    """
    Triggered by SQS transcoding-jobs queue.
    Parses the S3 ObjectCreated event and submits a MediaConvert job.
    """
    for record in event.get("Records", []):
        try:
            _process_sqs_record(record)
        except Exception as exc:
            log.error(f"Failed to submit job: {exc}", exc_info=True)
            # Re-raise so Lambda does not delete the SQS message.
            # SQS will redeliver up to maxReceiveCount (3), then move to DLQ.
            raise

    return {"statusCode": 200}


def _process_sqs_record(record: dict):
    body = json.loads(record["body"])

    # S3 event may be wrapped in an SNS notification
    if "Message" in body:
        s3_event = json.loads(body["Message"])
    else:
        s3_event = body

    s3_record = s3_event["Records"][0]
    bucket = s3_record["s3"]["bucket"]["name"]
    key = s3_record["s3"]["object"]["key"]

    # Key format: uploads/{videoId}/original.mp4
    video_id = key.split("/")[1]
    input_uri = f"s3://{bucket}/{key}"
    output_prefix = f"s3://{PROCESSED_BUCKET}/videos/{video_id}/"
    thumbnail_prefix = f"s3://{PROCESSED_BUCKET}/thumbnails/{video_id}/"

    log.info(json.dumps({"action": "submit_job", "videoId": video_id, "input": input_uri}))

    # --- Idempotency: skip if a job is already queued/running for this video ---
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM transcoding_jobs WHERE video_id = %s ORDER BY created_at DESC LIMIT 1",
            (video_id,)
        )
        row = cur.fetchone()
        if row and row[0] in ("queued", "processing", "completed"):
            log.info(json.dumps({"action": "skip_duplicate", "videoId": video_id, "existingStatus": row[0]}))
            return

    job_settings = _build_job_settings(input_uri, output_prefix, thumbnail_prefix)

    response = mediaconvert.create_job(
        Role=MC_ROLE_ARN,
        Settings=job_settings,
        UserMetadata={"videoId": video_id, "rawS3Key": key},  # passed through to completion event
    )

    mc_job_id = response["Job"]["Id"]
    log.info(json.dumps({"action": "job_created", "videoId": video_id, "mcJobId": mc_job_id}))

    # Update DB: record the MediaConvert job ID and set status to queued
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO transcoding_jobs (video_id, mediaconvert_job_id, status, created_at)
                   VALUES (%s, %s, 'queued', NOW())""",
                (video_id, mc_job_id),
            )
            cur.execute(
                "UPDATE videos SET status = 'queued', updated_at = NOW() WHERE id = %s",
                (video_id,),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _build_job_settings(input_uri: str, output_prefix: str, thumbnail_prefix: str) -> dict:
    """
    Build the MediaConvert job settings for HLS output at 360p/720p/1080p
    plus a JPEG thumbnail at the 5-second mark.

    This is a representative configuration — adjust bitrates and segment
    duration to match your quality/cost targets.
    """
    return {
        "Inputs": [
            {
                "FileInput": input_uri,
                "AudioSelectors": {"Audio Selector 1": {"DefaultSelection": "DEFAULT"}},
                "VideoSelector": {},
            }
        ],
        "OutputGroups": [
            # --- HLS Adaptive Bitrate Group ---
            {
                "Name": "HLS",
                "OutputGroupSettings": {
                    "Type": "HLS_GROUP_SETTINGS",
                    "HlsGroupSettings": {
                        "Destination": output_prefix,
                        "SegmentLength": 6,
                        "MinSegmentLength": 0,
                    },
                },
                "Outputs": [
                    _hls_output("360p",  360,  800_000,  96_000),
                    _hls_output("720p",  720,  2_500_000, 128_000),
                    _hls_output("1080p", 1080, 5_000_000, 192_000),
                ],
            },
            # --- Thumbnail Group ---
            {
                "Name": "Thumbnails",
                "OutputGroupSettings": {
                    "Type": "FILE_GROUP_SETTINGS",
                    "FileGroupSettings": {"Destination": thumbnail_prefix},
                },
                "Outputs": [
                    {
                        "NameModifier": "thumbnail",
                        "ContainerSettings": {"Container": "RAW"},
                        "VideoDescription": {
                            "Width": 1280,
                            "Height": 720,
                            "CodecSettings": {
                                "Codec": "FRAME_CAPTURE",
                                "FrameCaptureSettings": {
                                    "FramerateNumerator": 1,
                                    "FramerateDenominator": 1,
                                    "MaxCaptures": 1,
                                    "Quality": 80,
                                },
                            },
                        },
                        # Capture at 5 seconds via input clipping
                        "InputClippings": [
                            {"StartTimecode": "00:00:05:00", "EndTimecode": "00:00:06:00"}
                        ],
                    }
                ],
            },
        ],
        "TimecodeConfig": {"Source": "ZEROBASED"},
    }


def _hls_output(name: str, height: int, video_bps: int, audio_bps: int) -> dict:
    return {
        "NameModifier": f"_{name}",
        "ContainerSettings": {"Container": "M3U8", "M3u8Settings": {}},
        "VideoDescription": {
            "Height": height,
            "CodecSettings": {
                "Codec": "H_264",
                "H264Settings": {
                    "Bitrate": video_bps,
                    "RateControlMode": "CBR",
                    "CodecProfile": "MAIN",
                    "CodecLevel": "AUTO",
                    "FramerateControl": "INITIALIZE_FROM_SOURCE",
                },
            },
        },
        "AudioDescriptions": [
            {
                "CodecSettings": {
                    "Codec": "AAC",
                    "AacSettings": {
                        "Bitrate": audio_bps,
                        "SampleRate": 48000,
                        "CodingMode": "CODING_MODE_2_0",
                    },
                }
            }
        ],
    }


# ---------------------------------------------------------------------------
# Handler 2: EventBridge → handle MediaConvert job state changes
# ---------------------------------------------------------------------------

def handle_completion(event, context):
    """
    Triggered by EventBridge rule on MediaConvert job state changes.

    Handles the full status lifecycle:
      pending → queued → processing → ready | failed

    EventBridge rule pattern (covers all three states):
      {
        "source": ["aws.mediaconvert"],
        "detail-type": ["MediaConvert Job State Change"],
        "detail": { "status": ["PROGRESSING", "COMPLETE", "ERROR"] }
      }
    """
    detail = event.get("detail", {})
    status = detail.get("status")
    mc_job_id = detail.get("jobId")
    user_metadata = detail.get("userMetadata", {})
    video_id = user_metadata.get("videoId")

    if not video_id:
        log.error(f"No videoId in userMetadata for job {mc_job_id}")
        return

    log.info(json.dumps({"action": "job_event", "videoId": video_id, "mcJobId": mc_job_id, "status": status}))

    if status == "PROGRESSING":
        _handle_progressing(video_id, mc_job_id)
    elif status == "COMPLETE":
        raw_s3_key = user_metadata.get("rawS3Key")
        _handle_success(video_id, mc_job_id, detail, raw_s3_key)
    elif status == "ERROR":
        error_msg = detail.get("errorMessage", "MediaConvert job failed")
        _handle_failure(video_id, mc_job_id, error_msg)
    else:
        log.info(f"Ignoring unhandled status: {status}")


def _handle_progressing(video_id: str, mc_job_id: str):
    """
    MediaConvert has started processing the job.
    Transition: queued → processing.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videos SET status = 'processing', updated_at = NOW() WHERE id = %s",
                (video_id,),
            )
            cur.execute(
                """UPDATE transcoding_jobs
                   SET status = 'processing', started_at = NOW()
                   WHERE mediaconvert_job_id = %s""",
                (mc_job_id,),
            )
        conn.commit()
        log.info(json.dumps({"action": "status_processing", "videoId": video_id}))
    except Exception:
        conn.rollback()
        raise


def _handle_success(video_id: str, mc_job_id: str, detail: dict, raw_s3_key: str | None):
    """
    MediaConvert completed successfully.
    Transition: processing → ready.
    Stores output paths, then tags the raw S3 object so the bucket
    lifecycle rule can expire it after 7 days.
    """
    output_prefix = f"videos/{video_id}"
    thumbnail_url = f"https://cdn.example.com/thumbnails/{video_id}/thumbnail.jpg"

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE videos
                   SET status = 'ready',
                       output_s3_prefix = %s,
                       thumbnail_url = %s,
                       updated_at = NOW()
                   WHERE id = %s""",
                (output_prefix, thumbnail_url, video_id),
            )
            cur.execute(
                """UPDATE transcoding_jobs
                   SET status = 'completed', completed_at = NOW()
                   WHERE mediaconvert_job_id = %s""",
                (mc_job_id,),
            )
        conn.commit()
        log.info(json.dumps({"action": "status_ready", "videoId": video_id}))
    except Exception:
        conn.rollback()
        raise

    # Tag the raw upload so the S3 lifecycle rule can expire it after 7 days.
    # Best-effort — a failure is logged but does not affect video status.
    if raw_s3_key:
        try:
            s3.put_object_tagging(
                Bucket=RAW_BUCKET,
                Key=raw_s3_key,
                Tagging={
                    "TagSet": [
                        {"Key": "videoId", "Value": video_id},
                        {"Key": "status",  "Value": "processed"},
                    ]
                },
            )
            log.info(json.dumps({"action": "raw_tagged", "videoId": video_id, "key": raw_s3_key}))
        except Exception as exc:
            # Non-fatal: log and continue. The video is already marked ready.
            log.warning(json.dumps({"action": "raw_tag_failed", "videoId": video_id, "error": str(exc)}))
    else:
        log.warning(json.dumps({"action": "raw_tag_skipped", "videoId": video_id, "reason": "rawS3Key missing from userMetadata"}))


def _handle_failure(video_id: str, mc_job_id: str, error_msg: str):
    """
    MediaConvert job failed.
    Transition: processing → failed.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videos SET status = 'failed', updated_at = NOW() WHERE id = %s",
                (video_id,),
            )
            cur.execute(
                """UPDATE transcoding_jobs
                   SET status = 'failed', error_msg = %s, completed_at = NOW()
                   WHERE mediaconvert_job_id = %s""",
                (error_msg, mc_job_id),
            )
        conn.commit()
        log.info(json.dumps({"action": "status_failed", "videoId": video_id, "error": error_msg}))
    except Exception:
        conn.rollback()
        raise
