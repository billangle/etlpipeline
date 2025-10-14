
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";
const s3 = new S3Client({});

export const handler = async () => {
  const bucket = process.env.RAW_BUCKET!;
  const prefix = 'input/';
  const res = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, MaxKeys: 1 }));
  const ok = (res.KeyCount ?? 0) > 0;
  if (!ok) throw new Error(`No input objects found in s3://${bucket}/${prefix}. Upload a file and retry.`);
  return { ok, bucket, prefix };
};
