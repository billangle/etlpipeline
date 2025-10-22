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
import { RemovalPolicy, Duration } from 'aws-cdk-lib';


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

export class EtlDataPipe2Stack extends cdk.Stack {
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
    const job = new glue.PySparkEtlJob(this, 'EtlJobPipe2', {
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
    const crawler = new glue_l1.CfnCrawler(this, 'ProcessedCrawlerPipe2', {
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
    const workflow = new glue_l1.CfnWorkflow(this, 'Pipeline2Workflow', {
      name: `${cdk.Stack.of(this).stackName}-workflow`,
    });

    // On-demand trigger for the ETL job
    const jobTrigger = new glue_l1.CfnTrigger(this, 'EtlJobTrigger2', {
      name: `${cdk.Stack.of(this).stackName}-job-trigger`,
      type: 'ON_DEMAND',
      workflowName: workflow.name,
      actions: [{
        jobName: job.jobName,
        timeout: 30,
      }],
    });

    // Conditional trigger: run crawler when ETL job succeeds
    const crawlerTrigger = new glue_l1.CfnTrigger(this, 'CrawlerTrigger2', {
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
    const validatorFn = new lambda.Function(this, 'ValidatorFn2', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/validate'),
      environment: { RAW_BUCKET: rawBucket.bucketName },
      role: etlLambdaRole,
    });
    rawBucket.grantRead(validatorFn);

    // ===== Step Functions: Validate → Run ETL → Start Crawler → Success/Fail =====
    const validateTask = new tasks.LambdaInvoke(this, 'Validate input Pipe2', {
      lambdaFunction: validatorFn,
      outputPath: '$.Payload',
    });



    const startGlueTask = new tasks.GlueStartJobRun(this, 'Run Glue ETL Pipe2', {
      glueJobName: job.jobName,
      arguments: sfn.TaskInput.fromObject({
        '--raw_bucket': rawBucket.bucketName,
        '--processed_bucket': processedBucket.bucketName,
      }),
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      resultPath: '$.glueResult',
    });

   

    // Start crawler (does not wait for completion)
    const startCrawler = new tasks.GlueStartCrawlerRun(this, 'Start Processed Crawler Pipe2', {
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

      // Build FAILURE log payload (captures error/cause)
    const buildFailureLog = new sfn.Pass(this, 'Build Failure Log', {
      parameters: {
        'runId.$': '$.logBase.runId',
        'stateEntered.$': '$.logBase.stateEntered',
        status: 'FAILED',
        // $.Cause and $.Error come from the Catch
        'error.$': '$.Cause',
        'errorType.$': '$.Error',
        // Include any partial Glue info if present:
        'glue.$': '$.logBase.glue',
      },
      resultPath: '$.log',
    });

    // --- A small logger Lambda that just console.logs the incoming payload ---
    const loggerFn = new lambda.Function(this, 'OutcomeLoggerFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
        exports.handler = async (event) => {
          // event is the structured log object you pass in below
          console.log(JSON.stringify({ level: "info", ...event }));
          return { ok: true };
        };
      `),
      timeout: Duration.seconds(10),
      memorySize: 128,
    });

    // Logger (re-usable). We feed it a fully-built $.log object.
    const logOutcome = new tasks.LambdaInvoke(this, 'Log Outcome', {
      lambdaFunction: loggerFn,
      payload: sfn.TaskInput.fromJsonPathAt('$.log'),
      payloadResponseOnly: true,
      // Never let logging break the workflow it’s attached to:
      resultPath: sfn.JsonPath.DISCARD,
    }).addCatch(new sfn.Pass(this, 'Ignore Log Errors'), { resultPath: sfn.JsonPath.DISCARD });

    // Build SUCCESS log payload
    const buildSuccessLog = new sfn.Pass(this, 'Build Success Log', {
      parameters: {
        // Merge base + success status
        'runId.$': '$.logBase.runId',
        'stateEntered.$': '$.logBase.stateEntered',
        status: 'SUCCEEDED',
        'glue.$': '$.logBase.glue',
      },
      resultPath: '$.log',
    });

    // Failure terminator
    const failed = new sfn.Fail(this, 'PipelineFailed', {
      error: 'PipelineFailed',
      cause: 'One or more steps failed',
    });



    const definition = sfn.Chain
      .start(validateTask)
      .next(startGlueTask)
      .next(logGlueResults)
      .next(startCrawler)
      .next(new sfn.Choice(this, 'Was Glue successful?')
        .when(sfn.Condition.stringEquals('$.logged.jobDetails.JobRunState', 'SUCCEEDED'), success)
        .otherwise(// ANY error from the above goes here
            buildFailureLog
            .next(logOutcome)
            .next(failed)
        ));

  
    const stateMachine = new sfn.StateMachine(this, 'Pipeline2StateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      logs: {
        destination: new logs.LogGroup(this, 'StateMachineLogsPipe2', {
          removalPolicy: cdk.RemovalPolicy.DESTROY,
          retention: logs.RetentionDays.ONE_WEEK,
          logGroupName: `${cdk.Stack.of(this).stackName}-state-machine-logs`,
        }),
        level: sfn.LogLevel.ALL,
        includeExecutionData: true,
      },
      tracingEnabled: true,
    });

     

/***** */





   
  }
}
