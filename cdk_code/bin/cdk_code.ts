#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { ApplicationStack } from '../lib/application-stack';

const app = new cdk.App();
new ApplicationStack(app, 'ApplicationStack', {
    applicationName: 'source-test'
  });
