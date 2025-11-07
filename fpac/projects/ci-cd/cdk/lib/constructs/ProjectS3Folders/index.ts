import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import { Construct } from 'constructs';
import {
    Bucket, IBucket
  } from 'aws-cdk-lib/aws-s3';



  interface Props {
   project: string
   landingBucket: IBucket;
   cleanBucket: IBucket;
   finalBucket: IBucket;
  }


  
  export class ProjectS3Folders extends Construct {
  


    constructor(scope: Construct, id: string, props: Props) {
      super(scope, id);
            // Create folder-like prefixes in S3 buckets to organize data
            //
            new s3deploy.BucketDeployment(this, `Create-${props.project}-DBO`, {
              destinationBucket: props.landingBucket,
              destinationKeyPrefix: `${props.project}/dbo/`,  // <-- creates a folder-like prefix
              sources: [s3deploy.Source.data('dbo-ignore.txt', 'NA')],
            });


            new s3deploy.BucketDeployment(this, `Create-${props.project}-ETLJobs`, {
              destinationBucket: props.landingBucket,
              destinationKeyPrefix: `${props.project}/etl-jobs/`,  // <-- creates a folder-like prefix
              sources: [s3deploy.Source.data('etl-jobs-ignore.txt', 'NA')],
            });


            new s3deploy.BucketDeployment(this, `Create-${props.project}-CleanETLJobs`, {
              destinationBucket: props.cleanBucket,
              destinationKeyPrefix: `${props.project}/etl-jobs/`,  // <-- creates a folder-like prefix
              sources: [s3deploy.Source.data('clean-etl-jobs-ignore.txt', 'NA')],
            });
  

            new s3deploy.BucketDeployment(this, `Create-${props.project}-Final`, {
              destinationBucket: props.cleanBucket,
              destinationKeyPrefix: `${props.project}/`,  // <-- creates a folder-like prefix
              sources: [s3deploy.Source.data('clean-etl-jobs-ignore.txt', 'NA')],
            });
    }
  }
  