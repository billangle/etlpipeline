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

export class EtlDataPipe1Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: EtlStackProps) {
    super(scope, id, props);

    // === Get S3 bucket names and role ARNs from SSM ===
    const rawBucketName = StringParameter.valueForStringParameter(this, 'etlInputBucketSSMName');
    const processedBucketName = StringParameter.valueForStringParameter(this, 'etlOuputBucketSSMName');
    const glueJobRoleArn = StringParameter.valueForStringParameter(this, 'etlGlueJobRoleSSMArn');
    const databaseArn = StringParameter.valueForStringParameter(this, 'etlDatabaseARNSSM');
    const etlRoleArn = StringParameter.valueForStringParameter(this, 'etlLambdaExecuteRoleARN');



    // Reference existing S3 buckets

     const rawBucket = s3.Bucket.fromBucketName(this, 'RawDataBucket', rawBucketName);
      const processedBucket = s3.Bucket.fromBucketName(this, 'ProcessedDataBucket', processedBucketName);

    // Reference existing IAM roles
    const glueJobRole = iam.Role.fromRoleArn(this, 'GlueJobRole', glueJobRoleArn, { mutable: false });
    const crawlerRole = iam.Role.fromRoleArn(this, 'CrawlerRole', glueJobRoleArn, { mutable: false });
    const etlLambdaRole = iam.Role.fromRoleArn(this, 'EtlLambdaRole', etlRoleArn, { mutable: false });

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
    

    // Wrapper Construct that represents an existing Glue database by ARN/name.
    // This does NOT create a new CloudFormation resource; it only exposes the ARN and name
    // so other constructs (like the crawler) can depend on it logically in code.
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
    const glueDb = new ExistingGlueDatabase(this, 'ExistingGlueDatabase', { databaseArn, databaseName });




    // ===== Glue Crawler (targets processed/output) – L1 =====
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
      role: etlLambdaRole,
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
        .next(startGlueTask)
        .next(logGlueResults)
        .next(startCrawler)
        .next(new sfn.Choice(this, 'Was Glue successful?')
          .when(sfn.Condition.stringEquals('$.logged.jobDetails.JobRunState', 'SUCCEEDED'), success)
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

    new StringParameter(this, 'GlueJobNameParam', {
      parameterName: `/etlpipe1/glue-job-name`,
      stringValue: job.jobName,
    });
    new StringParameter(this, 'CrawlerNameParam', {
      parameterName: `/etlpipe1/crawler-name`,
      stringValue: crawler.name!,
    });
    new StringParameter(this, 'WorkflowNameParam', {
      parameterName: `/etlpipe1/workflow-name`,
      stringValue: workflow.name!,
    });
    new StringParameter(this, 'JobTriggerNameParam', {
      parameterName: `/etlpipe1/job-trigger-name`,
      stringValue: jobTrigger.name!,
    });
    new StringParameter(this, 'CrawlerTriggerNameParam', {
      parameterName: `/etlpipe1/crawler-trigger-name`,
      stringValue: crawlerTrigger.name!,
    });
    new StringParameter(this, 'StateMachineArnParam', {
      parameterName: `/etlpipe1/state-machine-arn`,
      stringValue: stateMachine.stateMachineArn,
    });
  }
}
