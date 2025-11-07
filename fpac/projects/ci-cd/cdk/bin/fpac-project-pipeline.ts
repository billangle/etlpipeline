#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import * as fs from 'fs';
import { FpacProjectDataPipelineStack } from '../lib/fpac-project-data-pipeline';

const app = new cdk.App();

let environment = process.env.DEPLOY_ENV || '';
const envdata = fs.readFileSync("../config/" + environment + '/cdk-spec.json', 'utf8');
const configData = JSON.parse(envdata);
const project = process.env.PROJECT || '';

new FpacProjectDataPipelineStack(app, `${project}-DataPipelineStack`, {
  env: {
    account: process.env.CDK_ACCOUNT,
    region: configData.region,
  },
  configData: configData,
  deployEnv: environment,
  project: project,
});
