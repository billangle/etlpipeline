
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";
import {SSMClient, GetParameterCommand} from "@aws-sdk/client-ssm";
const s3 = new S3Client({});
const ssmClient = new SSMClient({region: process.env.AWS_REGION});

export const handler = async (event) => {

  let bucketName="";
     const input = {
        Name: "fpacFsaLandingBucketSSMName"
    }

    const envBucket = process.env.LANDING_BUCKET;

    console.log ("Event: " + envBucket);
    const command = new GetParameterCommand(input);

    try {
      const res = await ssmClient.send(command);
      bucketName = res.Parameter.Value;

    } catch (e) {
        console.error ("ERROR on SSM: " + e);
    }


  const bucket = bucketName;
  const prefix = 'dbo';
  const res = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, MaxKeys: 1 }));
  const ok = (res.KeyCount ?? 0) > 0;
  if (!ok) throw new Error(`No input objects found in s3://${bucket}/${prefix}. Upload a file and retry.`);
  return { ok, bucket, bucketName, prefix, envBucket};
};
