
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
//import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as glue_l1 from 'aws-cdk-lib/aws-glue'; // L1s for database/crawler/workflow/trigger
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { S3 } from './constructs/DataEtlS3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';

interface ConfigurationData {
  landingBucketName: string;
  cleanBucketName: string;
  finalBucketName: string;
  databaseName: string;
  region: string;
};

interface EtlStackProps extends cdk.StackProps {
  configData: ConfigurationData;
  deployEnv: string;
}

export class FpacFsaInfraStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: EtlStackProps) {
    super(scope, id, props);

    // Buckets for raw input and processed output data

    const landingBucketConstruct = new S3(this, `${props.deployEnv}-${props.configData.landingBucketName}-` , {
      env: props.deployEnv,
      base_name: props.configData.landingBucketName,
    }); 
    const landingBucket = landingBucketConstruct.DataBucket;

    const cleanBucketConstruct = new S3(this, `${props.deployEnv}-${props.configData.cleanBucketName}` , {
      env: props.deployEnv,
      base_name: props.configData.cleanBucketName,
    });
    const cleanBucket = cleanBucketConstruct.DataBucket;

    const finalBucketConstruct = new S3(this, `${props.deployEnv}-${props.configData.finalBucketName}` , {
      env: props.deployEnv,
      base_name: props.configData.finalBucketName,
    });
    const finalBucket = finalBucketConstruct.DataBucket;

    // Create folder-like prefixes in S3 buckets to organize data
    //
    new s3deploy.BucketDeployment(this, 'CreateDBO', {
      destinationBucket: landingBucket,
      destinationKeyPrefix: 'dbo/',  // <-- creates a folder-like prefix
      sources: [s3deploy.Source.data('dbo-ignore.txt', 'NA')],
    });


    new s3deploy.BucketDeployment(this, 'CreateETLJobs', {
      destinationBucket: landingBucket,
      destinationKeyPrefix: 'etl-jobs/',  // <-- creates a folder-like prefix
      sources: [s3deploy.Source.data('etl-jobs-ignore.txt', 'NA')],
    });


    new s3deploy.BucketDeployment(this, 'CreateCleanETLJobs', {
      destinationBucket: cleanBucket,
      destinationKeyPrefix: 'etl-jobs/',  // <-- creates a folder-like prefix
      sources: [s3deploy.Source.data('clean-etl-jobs-ignore.txt', 'NA')],
    });

// IAM Role for Glue Jobs

    const glueJobRole = new iam.Role(this, 'FpacFsaStackGlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
        });

        // Add S3 and SSM permissions
      glueJobRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        's3:GetObject',
        's3:ListBucket',
        'ssm:GetParameter',
      ],
      resources: ['*']
        }));


    // Allow Glue job to read the script and buckets
    landingBucket.grantReadWrite(glueJobRole);
    cleanBucket.grantReadWrite(glueJobRole);
    finalBucket.grantReadWrite(glueJobRole);

    const fpacFsaLambdaExecutionRole = new iam.Role(this, 'FpacFsaLambdaExecutionRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    fpacFsaLambdaExecutionRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        's3:GetObject',
        's3:ListBucket',
        'lambda:InvokeFunction',
        'ssm:GetParameter',
        'ssm:PutParameter'
      ],
      resources: ['*']
    }));

    
    // ===== Glue Data Catalog (Database) – L1 =====
   
    const databaseName = props.configData.databaseName;
    const glueDb = new glue_l1.CfnDatabase(this, databaseName, {
      catalogId: cdk.Stack.of(this).account,
      databaseInput: { name: databaseName },
    });


    // Store important resource ARNs/names in SSM Parameters for retrieval by ETL jobs
    const glueRoleARN = glueJobRole.roleArn;
    const ssmEtlGlueJobRole = new StringParameter (this, 'fpacFsaGlueJobRoleSSM', {
      parameterName: 'fpacFsaGlueJobRoleSSMArn',
      stringValue: glueRoleARN
   });


   const landingBucketName = landingBucket.bucketName
    const ssmInputBucketARN = new StringParameter (this, 'fpacFsaInputBucketSSM', {
      parameterName: 'fpacFsaLandingBucketSSMName',
      stringValue: landingBucketName
   });

   const cleanBucketName = cleanBucket.bucketName;
    const ssmOutputBucketName = new StringParameter (this, 'fpacFsaOutputBucketSSM', {
      parameterName: 'fpacFsaCleanBucketSSMName',
      stringValue: cleanBucketName
   });

    const finalBucketName = finalBucket.bucketName;
    const ssmFinalBucketName = new StringParameter (this, 'fpacFsaFinalBucketSSM', {
      parameterName: 'fpacFsaFinalBucketSSMName',
      stringValue: finalBucketName
    });

    const databaseARN = `arn:aws:glue:${props.configData.region}:${cdk.Stack.of(this).account}:database/${databaseName}`;
    const ssmDatabaseARN = new StringParameter (this, 'fpacFsaDatabaseARNSSM', {
      parameterName: 'fpacFsaDatabaseARNSSM',
      stringValue: databaseARN
   });

  const fpacFsaLambdaExecutionRoleARN = fpacFsaLambdaExecutionRole.roleArn;
  const ssmEtlRoleARN = new StringParameter(this, 'fpacFsaLambdaExecuteRoleARN', {
    parameterName: 'fpacFsaLambdaExecuteRoleARN',
    stringValue: fpacFsaLambdaExecutionRoleARN
  });

 
    // Outputs



    new cdk.CfnOutput(this, 'LandingBucketName', { value: landingBucket.bucketName });
    new cdk.CfnOutput(this, 'CleanBucketName', { value: cleanBucket.bucketName });
    new cdk.CfnOutput(this, 'FinalBucketName', { value: finalBucket.bucketName });

  }
}
