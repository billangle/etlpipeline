
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

export class EtlDataPipe1Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Buckets for raw input and processed output data
    const rawBucket = new s3.Bucket(this, 'RawDataBucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const processedBucket = new s3.Bucket(this, 'ProcessedDataBucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // Role for the Glue job (L2 requires a role)
    const glueJobRole = new iam.Role(this, 'GlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });

    // Allow Glue job to read the script and buckets
    rawBucket.grantRead(glueJobRole);
    processedBucket.grantReadWrite(glueJobRole);

    // The Glue ETL script packaged from local repo (L2 uses glue.Code)
    const scriptCode = glue.Code.fromAsset('glue/etl_job.py');

    // Create a Glue PySpark ETL Job USING L2 CONSTRUCTS
    const job = new glue.PySparkEtlJob(this, 'EtlJob', {
      jobName: `${cdk.Stack.of(this).stackName}-etl-job`,
      role: glueJobRole,
      script: scriptCode,
      numberOfWorkers: 2,
      workerType: glue.WorkerType.G_1X,
      timeout: cdk.Duration.minutes(30),
      defaultArguments: {
        '--raw_bucket': rawBucket.bucketName,
        '--processed_bucket': processedBucket.bucketName,
      },
    });

    // ===== Glue Data Catalog (Database) – L1 =====
    const databaseName = `${cdk.Stack.of(this).stackName.toLowerCase()}_db`;
    const glueDb = new glue_l1.CfnDatabase(this, 'CatalogDatabase', {
      catalogId: cdk.Stack.of(this).account,
      databaseInput: { name: databaseName },
    });

    // ===== Glue Crawler (targets processed/output) – L1 =====
    const crawlerRole = new iam.Role(this, 'CrawlerRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });
    processedBucket.grantRead(crawlerRole);

    const crawlerName = `${cdk.Stack.of(this).stackName}-crawler`;
    const crawler = new glue_l1.CfnCrawler(this, 'ProcessedCrawler', {
      name: crawlerName,
      role: crawlerRole.roleArn,
      databaseName: databaseName,
      targets: {
        s3Targets: [{
          path: `s3://${processedBucket.bucketName}/output/`,
        }],
      },
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',
        deleteBehavior: 'DEPRECATE_IN_DATABASE',
      },
    });
    crawler.addDependency(glueDb);

    // ===== Optional: Glue Workflow + Triggers (L1) =====
    const workflow = new glue_l1.CfnWorkflow(this, 'PipelineWorkflow', {
      name: `${cdk.Stack.of(this).stackName}-workflow`,
    });

    // On-demand trigger for the ETL job
    const jobTrigger = new glue_l1.CfnTrigger(this, 'EtlJobTrigger', {
      name: `${cdk.Stack.of(this).stackName}-job-trigger`,
      type: 'ON_DEMAND',
      workflowName: workflow.name,
      actions: [{
        jobName: job.jobName,
        timeout: 30,
      }],
    });

    // Conditional trigger: run crawler when ETL job succeeds
    const crawlerTrigger = new glue_l1.CfnTrigger(this, 'CrawlerTrigger', {
      name: `${cdk.Stack.of(this).stackName}-crawler-trigger`,
      type: 'CONDITIONAL',
      workflowName: workflow.name,
      predicate: {
        conditions: [{
          jobName: job.jobName,
          state: 'SUCCEEDED',
          logicalOperator: 'EQUALS',
        }],
      },
      actions: [{
        crawlerName: crawler.name,
      }],
    });
    crawlerTrigger.addDependency(jobTrigger);
    crawlerTrigger.addDependency(crawler);

    // ===== Validator Lambda =====
    const validatorFn = new lambda.Function(this, 'ValidatorFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/validate'),
      environment: { RAW_BUCKET: rawBucket.bucketName },
    });
    rawBucket.grantRead(validatorFn);

    // ===== Step Functions: Validate → Run ETL → Start Crawler → Success/Fail =====
    const validateTask = new tasks.LambdaInvoke(this, 'Validate input', {
      lambdaFunction: validatorFn,
      outputPath: '$.Payload',
    });

    const startGlueTask = new tasks.GlueStartJobRun(this, 'Run Glue ETL', {
      glueJobName: job.jobName,
      arguments: sfn.TaskInput.fromObject({
        '--raw_bucket': rawBucket.bucketName,
        '--processed_bucket': processedBucket.bucketName,
      }),
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      resultPath: '$.glueResult',
    });

    // Start crawler (does not wait for completion)
    const startCrawler = new tasks.GlueStartCrawlerRun(this, 'Start Processed Crawler', {
      crawlerName: crawler.name!,
    });

    const success = new sfn.Succeed(this, 'Success');
    const fail = new sfn.Fail(this, 'Fail');

    const definition = validateTask
      .next(startGlueTask)
      .next(startCrawler)
      .next(new sfn.Choice(this, 'Was Glue successful?')
        .when(sfn.Condition.isPresent('$.glueResult.JobRunId'), success)
        .otherwise(fail));

    const stateMachine = new sfn.StateMachine(this, 'PipelineStateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      logs: {
        destination: new logs.LogGroup(this, 'StateMachineLogs', {
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        }),
        level: sfn.LogLevel.ALL,
      },
      tracingEnabled: true,
    });

    new cdk.CfnOutput(this, 'RawBucketName', { value: rawBucket.bucketName });
    new cdk.CfnOutput(this, 'ProcessedBucketName', { value: processedBucket.bucketName });
    new cdk.CfnOutput(this, 'GlueJobName', { value: job.jobName });
    new cdk.CfnOutput(this, 'CrawlerName', { value: crawler.name! });
    new cdk.CfnOutput(this, 'WorkflowName', { value: workflow.name! });
    new cdk.CfnOutput(this, 'JobTriggerName', { value: jobTrigger.name! });
    new cdk.CfnOutput(this, 'CrawlerTriggerName', { value: crawlerTrigger.name! });
    new cdk.CfnOutput(this, 'StateMachineArn', { value: stateMachine.stateMachineArn });
  }
}
