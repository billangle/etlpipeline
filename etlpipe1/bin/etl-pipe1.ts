#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { DataPipelineStack } from '../lib/pipeline-stack';

const app = new cdk.App();

new DataPipelineStack(app, 'GlueStepFunctionsCdkStarterL2', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
