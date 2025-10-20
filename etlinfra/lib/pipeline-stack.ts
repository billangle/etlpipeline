
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

    const glueJobRole = new iam.Role(this, 'EtlStackGlueJobRole', {
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
    rawBucket.grantRead(glueJobRole);
    processedBucket.grantReadWrite(glueJobRole);

    const etlRole = new iam.Role(this, 'EtlExecutionRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    etlRole.addToPolicy(new iam.PolicyStatement({
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


   const inputBucketName = rawBucket.bucketName
    const ssmInputBucketARN = new StringParameter (this, 'etlInputBucketSSM', {
      parameterName: 'etlInputBucketSSMName',
      stringValue: inputBucketName
   });
   
   const outputBucketName = processedBucket.bucketName;
    const ssmOutputBucketName = new StringParameter (this, 'etlOutputBucketSSM', {
      parameterName: 'etlOuputBucketSSMName',
      stringValue: outputBucketName
   });


    const databaseARN = `arn:aws:glue:${props.configData.region}:${cdk.Stack.of(this).account}:database/${databaseName}`;
    const ssmDatabaseARN = new StringParameter (this, 'etlDatabaseARNSSM', {
      parameterName: 'etlDatabaseARNSSM',
      stringValue: databaseARN
   });

  const etlRoleARN = etlRole.roleArn;
  const ssmEtlRoleARN = new StringParameter(this, 'etlLambdaExecuteRoleARN', {
    parameterName: 'etlLambdaExecuteRoleARN',
    stringValue: etlRoleARN
  });

 
    // Outputs


  
    new cdk.CfnOutput(this, 'RawBucketName', { value: rawBucket.bucketName });
    new cdk.CfnOutput(this, 'ProcessedBucketName', { value: processedBucket.bucketName });
  
  }
}
