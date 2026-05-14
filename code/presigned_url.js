/**
 * Upload Service — Presigned URL Generation & Status Polling
 *
 * Express route handlers for:
 *   POST /videos/upload-url  — generate presigned S3 PUT URL (small files)
 *   GET  /videos/:id/status  — poll processing status
 *
 * Flow:
 *   1. Resolve internal user ID from Cognito subject
 *   2. Validate request (file size, MIME type)
 *   3. Create a video record in PostgreSQL (status: pending)
 *   4. Generate a presigned S3 PUT URL (60-minute TTL)
 *   5. Return videoId + presigned URL to client
 *
 * The client uploads the file directly to S3 — no video bytes
 * ever pass through this service.
 *
 * For large files (>100 MB), clients use the multipart upload endpoints
 * instead, which issue per-part presigned URLs and support resumable transfers.
 *
 * Dependencies:
 *   npm install @aws-sdk/client-s3 @aws-sdk/s3-request-presigner
 *               express pg uuid
 */

import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { v4 as uuidv4 } from 'uuid';
import { pool } from '../db/pool.js'; // pg Pool instance

const s3 = new S3Client({ region: process.env.AWS_REGION ?? 'ca-central-1' });

const RAW_BUCKET = process.env.RAW_UPLOADS_BUCKET;
const PRESIGNED_URL_TTL_SECONDS = 3600;               // 60 minutes
const MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024 * 1024;  // 5 GB

const ALLOWED_MIME_TYPES = new Set([
  'video/mp4',
  'video/quicktime',
  'video/x-msvideo',
  'video/x-matroska',
  'video/webm',
]);

/**
 * Resolve the internal users.id UUID from a Cognito subject claim.
 *
 * The JWT `sub` claim is the Cognito identity, stored in users.cognito_sub.
 * The videos table references users.id (an internal UUID) as its FK, so we
 * map cognito_sub → users.id before creating any video records. This keeps
 * the auth identity decoupled from the internal data model.
 */
async function resolveUserId(cognitoSub) {
  const result = await pool.query(
    'SELECT id FROM users WHERE cognito_sub = $1',
    [cognitoSub]
  );
  return result.rows[0]?.id ?? null;
}

/**
 * POST /videos/upload-url
 *
 * Request body:
 *   { filename: string, fileSizeBytes: number, mimeType: string }
 *
 * Response 201:
 *   { videoId, presignedUrl, expiresAt, uploadInstructions }
 */
export async function createUploadUrl(req, res) {
  const { filename, fileSizeBytes, mimeType } = req.body;
  const cognitoSub = req.user.sub;

  // Map Cognito sub to internal user ID
  const userId = await resolveUserId(cognitoSub);
  if (!userId) {
    return res.status(401).json({
      error: 'USER_NOT_FOUND',
      message: 'No account found for this identity.',
    });
  }

  // --- Validation ---
  const errors = [];

  if (!filename || typeof filename !== 'string' || filename.length > 255) {
    errors.push({ field: 'filename', message: 'Required string, max 255 characters' });
  }

  if (!Number.isInteger(fileSizeBytes) || fileSizeBytes <= 0) {
    errors.push({ field: 'fileSizeBytes', message: 'Must be a positive integer' });
  } else if (fileSizeBytes > MAX_FILE_SIZE_BYTES) {
    return res.status(413).json({
      error: 'FILE_TOO_LARGE',
      message: 'Maximum file size is 5 GB',
    });
  }

  if (!ALLOWED_MIME_TYPES.has(mimeType)) {
    errors.push({
      field: 'mimeType',
      message: `Must be one of: ${[...ALLOWED_MIME_TYPES].join(', ')}`,
    });
  }

  if (errors.length > 0) {
    return res.status(400).json({
      error: 'VALIDATION_ERROR',
      message: 'Request validation failed',
      details: errors,
    });
  }

  // --- Create video record in DB ---
  const videoId = uuidv4();
  const s3Key = `uploads/${videoId}/original${getExtension(filename)}`;

  const client = await pool.connect();
  try {
    await client.query(
      `INSERT INTO videos (id, user_id, status, raw_s3_key, file_size_bytes, created_at, updated_at)
       VALUES ($1, $2, 'pending', $3, $4, NOW(), NOW())`,
      [videoId, userId, s3Key, fileSizeBytes]
    );
  } finally {
    client.release();
  }

  // --- Generate presigned S3 PUT URL ---
  const command = new PutObjectCommand({
    Bucket: RAW_BUCKET,
    Key: s3Key,
    ContentType: mimeType,
    ContentLength: fileSizeBytes,
    // Tag the object so the S3 lifecycle rule can clean it up after processing
    Tagging: `videoId=${videoId}&status=raw`,
    Metadata: {
      'video-id': videoId,
      'user-id': userId,
    },
  });

  const presignedUrl = await getSignedUrl(s3, command, {
    expiresIn: PRESIGNED_URL_TTL_SECONDS,
  });

  const expiresAt = new Date(Date.now() + PRESIGNED_URL_TTL_SECONDS * 1000).toISOString();

  return res.status(201).json({
    videoId,
    presignedUrl,
    expiresAt,
    uploadInstructions: {
      method: 'PUT',
      headers: {
        'Content-Type': mimeType,
        'Content-Length': String(fileSizeBytes),
      },
    },
  });
}

/**
 * GET /videos/:id/status
 *
 * Returns video processing status. Reads directly from PostgreSQL.
 */
export async function getVideoStatus(req, res) {
  const { id: videoId } = req.params;
  const cognitoSub = req.user.sub;

  const userId = await resolveUserId(cognitoSub);
  if (!userId) {
    return res.status(401).json({ error: 'USER_NOT_FOUND', message: 'No account found for this identity.' });
  }

  const result = await pool.query(
    `SELECT v.id, v.status, v.output_s3_prefix, v.thumbnail_url, v.user_id,
            tj.started_at, tj.completed_at, tj.error_msg
     FROM videos v
     LEFT JOIN transcoding_jobs tj ON tj.video_id = v.id
     WHERE v.id = $1`,
    [videoId]
  );

  if (result.rows.length === 0) {
    return res.status(404).json({ error: 'NOT_FOUND', message: 'Video not found' });
  }

  const video = result.rows[0];

  if (video.user_id !== userId) {
    return res.status(403).json({ error: 'FORBIDDEN', message: 'Access denied' });
  }

  return res.json(buildStatusPayload(video));
}

// --- Helpers ---

function getExtension(filename) {
  const match = filename.match(/\.[^.]+$/);
  return match ? match[0].toLowerCase() : '.mp4';
}

function buildStatusPayload(video) {
  const base = {
    videoId: video.id,
    status: video.status,
  };

  if (video.status === 'ready') {
    const prefix = video.output_s3_prefix;
    const cdnBase = `https://cdn.example.com/${prefix}`;
    return {
      ...base,
      outputs: {
        hlsManifestUrl: `${cdnBase}/master.m3u8`,
        thumbnailUrl: video.thumbnail_url,
        renditions: ['1080p', '720p', '360p'].map((res) => ({
          resolution: res,
          url: `${cdnBase}/${res}/index.m3u8`,
        })),
      },
      completedAt: video.completed_at,
    };
  }

  if (video.status === 'failed') {
    return {
      ...base,
      error: {
        code: 'TRANSCODING_FAILED',
        message: video.error_msg ?? 'Processing failed',
      },
    };
  }

  // pending | queued | processing
  return base;
}
