#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { ApplicationStack } from '../lib/application-stack';
import { CodePipelineStack } from '..//lib/cicd-stack';
import { dev } from './environments';

const prefix_dev = dev.app + "-" + dev.environment;

const app = new cdk.App();
new ApplicationStack(app, 'ApplicationStack', dev);

new CodePipelineStack(app,'CodePipelineStack', dev);
