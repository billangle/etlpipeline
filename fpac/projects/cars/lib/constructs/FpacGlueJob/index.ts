
  import { Construct } from 'constructs';
  import * as cdk from 'aws-cdk-lib';
  import * as glue_l1 from 'aws-cdk-lib/aws-glue';
  import * as glue from '@aws-cdk/aws-glue-alpha';
  import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
  import * as sfn from 'aws-cdk-lib/aws-stepfunctions';



  interface Props {
    env: string;
    jobName: string;
    role: cdk.aws_iam.IRole;
    scriptLocation: string;
    landingBucket: string;
    cleanBucket: string;
    finalBucket: string;
    jobType: string;
    stepName: string;
  }

  
  export class FpacGlueJob extends Construct {

        public readonly job: glue.PySparkEtlJob;
        public readonly workflow: glue_l1.CfnWorkflow;
        public readonly trigger: glue_l1.CfnTrigger;
        public readonly task: tasks.GlueStartJobRun;

    constructor(scope: Construct, id: string, props: Props) {
      super(scope, id);



        const scriptCode = glue.Code.fromAsset(props.scriptLocation);

        this.job = new glue.PySparkEtlJob(this, `FSA-${props.env}-${props.jobType}`, {
            jobName: `FSA-${props.env}-${props.jobType}`,
            role: props.role,
            script: scriptCode,
            numberOfWorkers: 2,
            workerType: glue.WorkerType.G_1X,
            timeout: cdk.Duration.minutes(30),
            defaultArguments: {
                '--landing_bucket': props.landingBucket,
                '--clean_bucket': props.cleanBucket,
                '--final_bucket': props.finalBucket,
            },
        });



        this.workflow = new glue_l1.CfnWorkflow(this, `${props.jobName}-PipelineWorkflow`, {
           name: `${cdk.Stack.of(this).stackName}-${props.jobName}-workflow`,
        });

        this.trigger = new glue_l1.CfnTrigger(this, `${props.jobName}-Trigger`, {
            name: `${cdk.Stack.of(this).stackName}-${props.jobName}-trigger`,
            type: 'ON_DEMAND',
            actions: [{
                jobName: this.job.jobName!,
                timeout: 30,
            }],
            workflowName: this.workflow.name!,
        });

        
       this.task = new tasks.GlueStartJobRun(this, `${props.stepName}`, {
            glueJobName: this.job.jobName,
            arguments: sfn.TaskInput.fromObject({
                '--landing_bucket': props.landingBucket,
                '--clean_bucket': props.cleanBucket,
                '--final_bucket': props.finalBucket,
            }),
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            resultPath: '$.glueResult',
        });


   
  
    }
  }
  