import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as glue_l1 from 'aws-cdk-lib/aws-glue'; // L1s for database/crawler/workflow/trigger
// Glue L2 (alpha) constructs
//import * as glue from 'aws-cdk-lib/aws-glue';
import * as glue from '@aws-cdk/aws-glue-alpha';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { FpacGlueJob } from './constructs/FpacGlueJob';
import { ProjectS3Folders } from './constructs/ProjectS3Folders';


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
  project: string;
}

export class CarsDataPipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: EtlStackProps) {
    super(scope, id, props);


    // === Get S3 bucket names and role ARNs from SSM ===
    const landingBucketName = StringParameter.valueForStringParameter(this, 'fpacFsaLandingBucketSSMName');
    const cleanBucketName = StringParameter.valueForStringParameter(this, 'fpacFsaCleanBucketSSMName');
    const finalBucketName = StringParameter.valueForStringParameter(this, 'fpacFsaFinalBucketSSMName');
    const glueJobRoleArn = StringParameter.valueForStringParameter(this, 'fpacFsaGlueJobRoleSSMArn');
    const databaseArn = StringParameter.valueForStringParameter(this, 'fpacFsaDatabaseARNSSM');
    const etlRoleArn = StringParameter.valueForStringParameter(this, 'fpacFsaLambdaExecuteRoleARN');

    // Reference existing S3 buckets

     const landingBucket = s3.Bucket.fromBucketName(this, 'LandingDataBucket', landingBucketName);
     const cleanBucket = s3.Bucket.fromBucketName(this, 'CleanDataBucket', cleanBucketName);
     const finalBucket = s3.Bucket.fromBucketName(this, 'FinalDataBucket', finalBucketName);

    // Reference existing IAM roles
    const glueJobRole = iam.Role.fromRoleArn(this, 'GlueJobRole', glueJobRoleArn, { mutable: false });
    const crawlerRole = iam.Role.fromRoleArn(this, 'CrawlerRole', glueJobRoleArn, { mutable: false });
    const etlLambdaRole = iam.Role.fromRoleArn(this, 'EtlLambdaRole', etlRoleArn, { mutable: false });

    const projectS3Folders = new ProjectS3Folders(this, 'CarsProjectS3Folders', {
      project: props.project,
      landingBucket: landingBucket,
      cleanBucket: cleanBucket,
      finalBucket: finalBucket,
    });

    // ===== Glue ETL Jobs =====

    // The Glue ETL script for moving Landing Zone 
   
    const step1GlueJob = new FpacGlueJob(this, 'LandingFilesGlueJob', {
      env: props.deployEnv,
      jobName: `FSA-${props.deployEnv}-CARS-LandingFiles`,
      role: glueJobRole,
      scriptLocation: 'glue/landingFiles/landing_job.py',
      landingBucket: landingBucket.bucketName,
      cleanBucket: cleanBucket.bucketName,
      finalBucket: finalBucket.bucketName,
      jobType: 'CARS-LandingFiles',
      stepName: 'Step1',
      project: props.project,
    });

    // Step 2:The Glue ETL script for cleaningJob data from Landing to Cleaned zone
   
    const step2GlueJob = new FpacGlueJob(this, 'CleansedFilesGlueJob', {
      env: props.deployEnv,
      jobName: `FSA-${props.deployEnv}-CARS-CleansedFiles`,
      role: glueJobRole,
      scriptLocation: 'glue/cleaningFiles/cleaning_job.py',
      landingBucket: landingBucket.bucketName,
      cleanBucket: cleanBucket.bucketName,
      finalBucket: finalBucket.bucketName,
      jobType: 'CARS-CleansedFiles',
      stepName: 'Step2',
      project: props.project,
    });

     // Step 3 The Glue ETL script for processing data from Cleaned to Final zone
    
    const step3GlueJob = new FpacGlueJob(this, 'FinalFilesGlueJob', {
      env: props.deployEnv,
      jobName: `FSA-${props.deployEnv}-cars`,
      role: glueJobRole,
      scriptLocation: 'glue/finalFiles/final_job.py',
      landingBucket: landingBucket.bucketName,
      cleanBucket: cleanBucket.bucketName,
      finalBucket: finalBucket.bucketName,
      jobType: 'CARS',
      stepName: 'Step3',
      project: props.project,
    });


    // ===== Glue Data Catalog (Database) – L1 =====
    
    class ExistingGlueDatabase extends Construct {
      public readonly databaseArn: string;
      public readonly databaseName: string;
      constructor(scope: Construct, id: string, props: { databaseArn: string; databaseName: string }) {
        super(scope, id);
        this.databaseArn = props.databaseArn;
        this.databaseName = props.databaseName;
      }
    }

    const databaseName = props.configData.databaseName;
    const glueDb = new ExistingGlueDatabase(this, 'FpacFsaGlueDatabase', { databaseArn, databaseName });


    // ===== Glue Crawler (targets processed/output) – L1 =====
    finalBucket.grantRead(crawlerRole);

    const crawlerName = `FSA-${props.deployEnv}-CARS-CRAWLER`;
    const crawler = new glue_l1.CfnCrawler(this, 'FpacFsaProcessedCrawler', {
      name: crawlerName,
      role: crawlerRole.roleArn,
      databaseName: databaseName,
      targets: {
        s3Targets: [{
          path: `s3://${finalBucket.bucketName}/`,
        }],
      },
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',
        deleteBehavior: 'DEPRECATE_IN_DATABASE',
      },
    });


    // Conditional trigger: run crawler when ETL job succeeds
    const crawlerTrigger = new glue_l1.CfnTrigger(this, 'CrawlerTrigger', {
      name: `${cdk.Stack.of(this).stackName}-crawler-trigger`,
      type: 'CONDITIONAL',
      workflowName: step3GlueJob.workflow.name!,
      predicate: {
        conditions: [{
          jobName: step3GlueJob.job.jobName!,
          state: 'SUCCEEDED',
          logicalOperator: 'EQUALS',
        }],
      },
      actions: [{
        crawlerName: crawler.name,
      }],
    });
    crawlerTrigger.addDependency(step3GlueJob.trigger);
    crawlerTrigger.addDependency(crawler);

    // ===== Validator Lambda =====
    const validatorFn = new lambda.Function(this, 'ValidatorFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      functionName: `FSA-${props.deployEnv}-CARS-Validator`,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/validate'),
      environment: { LANDING_BUCKET: landingBucket.bucketName, PROJECT: props.project },
      role: etlLambdaRole,
    });
    landingBucket.grantRead(validatorFn);

    // ===== Step Functions: Validate → Run ETL → Start Crawler → Success/Fail =====
    const validateTask = new tasks.LambdaInvoke(this, 'Validate input', {
      lambdaFunction: validatorFn,
      outputPath: '$.Payload',
    });

   

    // Start crawler (does not wait for completion)
    const startCrawler = new tasks.GlueStartCrawlerRun(this, 'Start Processed Crawler', {
      crawlerName: crawler.name!,
      resultPath: '$.glueResult',
    });

 
    const success = new sfn.Succeed(this, 'Success');
    const fail = new sfn.Fail(this, 'Fail');

      

    const logGlueResults = new sfn.Pass(this, 'Log Glue Results', {
        parameters: {
          'jobDetails.$': '$.glueResult',
          'timestamp.$': '$$.State.EnteredTime'
        },
        resultPath: '$.logged'
    });


      const definition = validateTask
        .next(step1GlueJob.task)
        .next(step2GlueJob.task)
        .next(step3GlueJob.task)
        .next(logGlueResults)
        .next(startCrawler)
        .next(new sfn.Choice(this, 'Was Glue successful?')
          .when(sfn.Condition.stringEquals('$.logged.jobDetails.JobRunState', 'SUCCEEDED'), success)
          .otherwise(fail));
     



    const stateMachine = new sfn.StateMachine(this, 'CarsPipelineStateMachine', {
      stateMachineName: `FSA-${props.deployEnv}-CARS-Pipeline`,
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      logs: {
        destination: new logs.LogGroup(this, 'StateMachineLogs', {
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        }),
        level: sfn.LogLevel.ALL,
      },
      tracingEnabled: true,
    });


    new cdk.CfnOutput(this, 'CarsStateMachineArn', { value: stateMachine.stateMachineArn });

    
  }
}
