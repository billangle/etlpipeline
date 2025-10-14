
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
//import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as glue_l1 from 'aws-cdk-lib/aws-glue'; // L1s for database/crawler/workflow/trigger
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { S3 } from './constructs/DataEtlS3';

interface ConfigurationData {
  inputBucketName: string;
  outputBucketName: string;
  databaseName: string;
  region: string;
};

interface EtlStackProps extends cdk.StackProps {
  configData: ConfigurationData;
  deployEnv: string;
}

export class EtlInfraStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: EtlStackProps) {
    super(scope, id, props);

    // Buckets for raw input and processed output data
    /*
    const rawBucket = new s3.Bucket(this, `${props.configData.inputBucketName}-${props.deployEnv}` , {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const processedBucket = new s3.Bucket(this, `${props.configData.outputBucketName}-${props.deployEnv}`, {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
    */

    const rawBucketConstruct = new S3(this, `${props.configData.inputBucketName}-${props.deployEnv}` , {
      env: props.deployEnv,
      base_name: props.configData.inputBucketName,
    }); 
    const rawBucket = rawBucketConstruct.DataBucket;

    const processedBucketConstruct = new S3(this, `${props.configData.outputBucketName}-${props.deployEnv}` , {
      env: props.deployEnv,
      base_name: props.configData.outputBucketName,
    });
    const processedBucket = processedBucketConstruct.DataBucket;

    // Role for the Glue job (L2 requires a role)
    const glueJobRole = new iam.Role(this, 'EtlStackGlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });

    // Allow Glue job to read the script and buckets
    rawBucket.grantRead(glueJobRole);
    processedBucket.grantReadWrite(glueJobRole);

    
    // ===== Glue Data Catalog (Database) – L1 =====
   // const databaseName = `${cdk.Stack.of(this).stackName.toLowerCase()}_db`;
     const databaseName = props.configData.databaseName;
    const glueDb = new glue_l1.CfnDatabase(this, databaseName, {
      catalogId: cdk.Stack.of(this).account,
      databaseInput: { name: databaseName },
    });

    const glueRoleARN = glueJobRole.roleArn;
    const ssmEtlGlueJobRole = new StringParameter (this, 'etlGlueJobRoleSSM', {
      parameterName: 'etlGlueJobRoleSSMArn',
      stringValue: glueRoleARN
   });

    // Outputs


  
    new cdk.CfnOutput(this, 'RawBucketName', { value: rawBucket.bucketName });
    new cdk.CfnOutput(this, 'ProcessedBucketName', { value: processedBucket.bucketName });
  
  }
}
