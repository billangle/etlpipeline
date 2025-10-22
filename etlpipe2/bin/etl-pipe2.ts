#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import * as fs from 'fs';
import { EtlDataPipe2Stack } from '../lib/etl-data-pipe2stack.';

const app = new cdk.App();

let environment = process.env.DEPLOY_ENV || '';
const envdata = fs.readFileSync("../" + environment + '/cdk-spec.json', 'utf8');
const configData = JSON.parse(envdata);

new EtlDataPipe2Stack(app, 'EtlDataPipe2Stack', {
  env: {
    account: process.env.CDK_ACCOUNT,
    region: configData.region,
  },
  configData: configData,
  deployEnv: environment,
});
