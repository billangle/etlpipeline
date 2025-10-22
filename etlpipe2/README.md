
# Glue + Step Functions + CDK (L2) Starter

A minimal starter that uses **AWS Glue L2 (alpha) constructs** and **Step Functions L2 constructs** to orchestrate a PySpark ETL job via AWS CDK v2 (TypeScript).

## What it deploys
- **S3 buckets** for raw and processed data (auto-delete on destroy — demo only).
- **Glue PySpark ETL Job (Glue 4.0)** defined with `@aws-cdk/aws-glue-alpha` (L2).
- **Validator Lambda (Node.js 20)** to check for input files.
- **Step Functions State Machine** using `aws-stepfunctions` and `aws-stepfunctions-tasks` constructs.

## Quick start
```bash
npm install
npm run build
npx cdk synth
npx cdk deploy
```
Upload a CSV to `s3://<RawBucketName>/input/` and start a state machine execution. Parquet will appear in `s3://<ProcessedBucketName>/output/`.

## References
- Glue L2 (alpha) docs: https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_glue_alpha/README.html
- GlueStartJobRun task: https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_stepfunctions_tasks.GlueStartJobRun.html


## Added components
- **Glue Catalog Database (L1)** and **Glue Crawler (L1)** to catalog the processed Parquet output.
- **Glue Workflow (L1) + Triggers (L1)**: an on-demand trigger starts the ETL job; a conditional trigger starts the crawler when the job succeeds.
- **Step Functions** flow now also **starts the crawler** after the ETL job.

### Run with Glue Workflow (optional)
You can also run via Glue Workflow:
1. In the Glue console, start the on-demand trigger `${StackName}-job-trigger` in workflow `${StackName}-workflow`.
2. When the job succeeds, the conditional trigger will start the crawler.

### Notes
- We intentionally use **L2 constructs** for the Glue **Job** and Step Functions tasks, and fall back to **L1** for entities that don't have stable L2 (Database, Crawler, Workflow, Trigger).
