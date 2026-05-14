"""
Transcoding Worker — ECS Fargate Container (Alternative / Future Path)

NOTE: This is NOT the primary transcoding path at launch.
The production system uses AWS MediaConvert (see mediaconvert_job.py),
which handles transcoding and thumbnail extraction as a fully managed service.

This worker is documented as a future optimization path for when:
  - Transcoding costs at scale justify self-managed compute (Fargate Spot)
  - Custom pipeline logic is needed (watermarking, proprietary codecs, etc.)
  - Full control over the ffmpeg command is required

If you migrate to this path, replace the MediaConvert Lambda with this
worker running on ECS Fargate, auto-scaled on SQS queue depth.

---

For each job this worker:
  1. Downloads raw video from S3 to ephemeral container storage
  2. Runs ffmpeg to produce HLS output at 360p, 720p, 1080p
     (thumbnail extraction is included as a separate ffmpeg pass)
  3. Uploads HLS segments + master manifest + thumbnail to processed-videos S3
  4. Updates video status in PostgreSQL
  5. Deletes SQS message on success

Idempotency: checks if master.m3u8 already exists in S3 before processing.

Environment variables:
  AWS_REGION              e.g. ca-central-1
  SQS_QUEUE_URL           transcoding-jobs queue URL
  RAW_UPLOADS_BUCKET      source bucket name
  PROCESSED_VIDEOS_BUCKET destination bucket name
  DATABASE_URL            PostgreSQL connection string

Dependencies:
  pip install boto3 psycopg2-binary
  ffmpeg must be installed in the container image
"""

import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path

import boto3
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
)
log = logging.getLogger(__name__)

# --- Config ---
AWS_REGION = os.environ["AWS_REGION"]
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
RAW_BUCKET = os.environ["RAW_UPLOADS_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_VIDEOS_BUCKET"]
DATABASE_URL = os.environ["DATABASE_URL"]

# HLS rendition ladder: (name, height, video_bitrate_kbps, audio_bitrate_kbps)
RENDITIONS = [
    ("360p",  360,  800,  96),
    ("720p",  720,  2500, 128),
    ("1080p", 1080, 5000, 192),
]

THUMBNAIL_TIMESTAMP = "00:00:05"  # Extract frame at 5 seconds
THUMBNAIL_WIDTH = 1280

# --- AWS clients ---
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3  = boto3.client("s3",  region_name=AWS_REGION)

# --- DB ---
db_conn = psycopg2.connect(DATABASE_URL)
db_conn.autocommit = False


def main():
    log.info("Transcoding worker started. Polling SQS...")
    while True:
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,      # long polling — reduces empty receives
            VisibilityTimeout=1800,  # 30 min — covers max transcoding time
        )

        messages = response.get("Messages", [])
        if not messages:
            continue

        message = messages[0]
        receipt_handle = message["ReceiptHandle"]

        try:
            job = parse_sqs_message(message)
            process_job(job)
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            log.info(f"Job completed. videoId={job['video_id']}")
        except Exception as exc:
            log.error(f"Job failed: {exc}", exc_info=True)
            # Do NOT delete — SQS redelivers up to maxReceiveCount (3), then DLQ


def parse_sqs_message(message: dict) -> dict:
    """
    SQS message body is an SNS notification wrapping an S3 event.
    Extract the S3 bucket and key.
    """
    body = json.loads(message["Body"])

    if "Message" in body:
        s3_event = json.loads(body["Message"])
    else:
        s3_event = body

    record = s3_event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    # Key format: uploads/{videoId}/original.mp4
    video_id = key.split("/")[1]

    return {
        "video_id": video_id,
        "raw_bucket": bucket,
        "raw_key": key,
    }


def process_job(job: dict):
    video_id = job["video_id"]
    raw_key = job["raw_key"]

    log.info(f"Processing job. videoId={video_id} key={raw_key}")

    # --- Idempotency check ---
    output_prefix = f"videos/{video_id}"
    master_key = f"{output_prefix}/master.m3u8"

    try:
        s3.head_object(Bucket=PROCESSED_BUCKET, Key=master_key)
        log.info(f"Output already exists — skipping. videoId={video_id}")
        return
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise

    update_video_status(video_id, "processing")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        raw_path = tmp / "original.mp4"

        # 1. Download raw video
        log.info(f"Downloading raw video. videoId={video_id}")
        s3.download_file(Bucket=job["raw_bucket"], Key=raw_key, Filename=str(raw_path))

        # 2. Transcode to HLS renditions
        rendition_manifests = []
        for name, height, vbr, abr in RENDITIONS:
            rendition_dir = tmp / name
            rendition_dir.mkdir()

            log.info(f"Transcoding {name}. videoId={video_id}")
            run_ffmpeg_hls(raw_path, rendition_dir, height, vbr, abr)

            upload_directory(rendition_dir, PROCESSED_BUCKET, f"{output_prefix}/{name}")
            rendition_manifests.append((name, f"{output_prefix}/{name}/index.m3u8"))

        # 3. Extract thumbnail
        thumb_path = tmp / "thumbnail.jpg"
        extract_thumbnail(raw_path, thumb_path)
        thumbnail_key = f"thumbnails/{video_id}/thumbnail.jpg"
        s3.upload_file(
            Filename=str(thumb_path),
            Bucket=PROCESSED_BUCKET,
            Key=thumbnail_key,
            ExtraArgs={"ContentType": "image/jpeg"},
        )

        # 4. Write master HLS manifest
        master_content = build_master_manifest(rendition_manifests)
        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=master_key,
            Body=master_content.encode("utf-8"),
            ContentType="application/vnd.apple.mpegurl",
        )
        log.info(f"Master manifest uploaded. videoId={video_id}")

    thumbnail_url = f"https://cdn.example.com/{thumbnail_key}"
    update_video_status(video_id, "ready", output_s3_prefix=output_prefix, thumbnail_url=thumbnail_url)

def run_ffmpeg_hls(input_path: Path, output_dir: Path, height: int, vbr_kbps: int, abr_kbps: int):
    """Transcode input video to HLS segments at the given resolution."""
    manifest = output_dir / "index.m3u8"
    segment_pattern = str(output_dir / "segment_%03d.ts")

    cmd = [
        "ffmpeg",
        "-i", str(input_path),
        "-vf", f"scale=-2:{height}",
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-maxrate", f"{vbr_kbps}k",
        "-bufsize", f"{vbr_kbps * 2}k",
        "-c:a", "aac",
        "-b:a", f"{abr_kbps}k",
        "-ar", "48000",
        "-hls_time", "6",
        "-hls_playlist_type", "vod",
        "-hls_segment_filename", segment_pattern,
        "-hls_flags", "independent_segments",
        str(manifest),
        "-y",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    if result.returncode != 0:
        log.error(f"ffmpeg stderr: {result.stderr}")
        raise RuntimeError(f"ffmpeg failed with exit code {result.returncode}")


def extract_thumbnail(input_path: Path, output_path: Path):
    """Extract a single JPEG frame at 5 seconds. Falls back to first frame."""
    cmd = [
        "ffmpeg",
        "-ss", THUMBNAIL_TIMESTAMP,
        "-i", str(input_path),
        "-vframes", "1",
        "-vf", f"scale={THUMBNAIL_WIDTH}:-2",
        "-q:v", "2",
        str(output_path),
        "-y",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

    if result.returncode != 0 or not output_path.exists():
        log.warning("5-second seek failed, falling back to first frame")
        fallback = [
            "ffmpeg",
            "-i", str(input_path),
            "-vframes", "1",
            "-vf", f"scale={THUMBNAIL_WIDTH}:-2",
            "-q:v", "2",
            str(output_path),
            "-y",
        ]
        fb_result = subprocess.run(fallback, capture_output=True, text=True, timeout=120)
        if fb_result.returncode != 0:
            raise RuntimeError(f"Thumbnail extraction failed: {fb_result.stderr}")


def upload_directory(local_dir: Path, bucket: str, s3_prefix: str):
    """Upload all files in a local directory to S3 under the given prefix."""
    for file_path in local_dir.iterdir():
        if file_path.is_file():
            s3_key = f"{s3_prefix}/{file_path.name}"
            content_type = "video/MP2T" if file_path.suffix == ".ts" else "application/vnd.apple.mpegurl"
            s3.upload_file(
                Filename=str(file_path),
                Bucket=bucket,
                Key=s3_key,
                ExtraArgs={"ContentType": content_type},
            )


def build_master_manifest(renditions: list[tuple[str, str]]) -> str:
    """Build an HLS master manifest referencing each rendition playlist."""
    bandwidth_map  = {"360p": 896000, "720p": 2628000, "1080p": 5192000}
    resolution_map = {"360p": "640x360", "720p": "1280x720", "1080p": "1920x1080"}

    lines = ["#EXTM3U", "#EXT-X-VERSION:3"]
    for name, key in sorted(renditions, key=lambda x: int(x[0].replace("p", ""))):
        bw  = bandwidth_map[name]
        res = resolution_map[name]
        playlist_filename = "/".join(key.split("/")[-2:])
        lines.append(f'#EXT-X-STREAM-INF:BANDWIDTH={bw},RESOLUTION={res},CODECS="avc1.42e01e,mp4a.40.2"')
        lines.append(playlist_filename)

    return "\n".join(lines) + "\n"


def update_video_status(
    video_id: str,
    status: str,
    output_s3_prefix: str = None,
    thumbnail_url: str = None,
):
    """
    Update video status in PostgreSQL.
    When marking a video ready, the transcoding job is set to 'completed'
    (the canonical job terminal state) rather than 'ready' (a video-only state).
    """
    # Transcoding job uses 'completed' as its success state; 'ready' is video-only.
    job_status = "completed" if status == "ready" else status

    try:
        with db_conn.cursor() as cur:
            if output_s3_prefix:
                cur.execute(
                    """UPDATE videos
                       SET status = %s, output_s3_prefix = %s, thumbnail_url = %s, updated_at = NOW()
                       WHERE id = %s""",
                    (status, output_s3_prefix, thumbnail_url, video_id),
                )
                cur.execute(
                    """UPDATE transcoding_jobs
                       SET status = %s, completed_at = NOW()
                       WHERE video_id = %s""",
                    (job_status, video_id),
                )
            else:
                cur.execute(
                    "UPDATE videos SET status = %s, updated_at = NOW() WHERE id = %s",
                    (status, video_id),
                )
                cur.execute(
                    """UPDATE transcoding_jobs SET status = %s, started_at = NOW()
                       WHERE video_id = %s AND status = 'queued'""",
                    (status, video_id),
                )
        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise


if __name__ == "__main__":
    main()
