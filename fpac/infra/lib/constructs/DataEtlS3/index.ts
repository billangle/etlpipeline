import {
    BlockPublicAccess,
    Bucket,
    BucketAccessControl,
  } from 'aws-cdk-lib/aws-s3';
  import { BucketDeployment, Source} from 'aws-cdk-lib/aws-s3-deployment';
  import { Construct } from 'constructs';
  import {  RemovalPolicy } from 'aws-cdk-lib';



  interface Props {
    env: string;
    base_name: string;
  }


  
  export class S3 extends Construct {
  
    public readonly DataBucket: Bucket;

    constructor(scope: Construct, id: string, props: Props) {
      super(scope, id);
  
     let bucketName=`${props.env}-${props.base_name}`;

    this.DataBucket = new Bucket(
        scope,
        `ETL-Data-${bucketName}`,
        {
          bucketName: bucketName,
          publicReadAccess: false,
          accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
          removalPolicy: RemovalPolicy.DESTROY,
          autoDeleteObjects: true,
        },
      );
   
  
    }
  }
  