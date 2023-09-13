import { Stack, StackProps } from 'aws-cdk-lib';
import { PipelineProject, BuildSpec, LinuxBuildImage } from 'aws-cdk-lib/aws-codebuild';
import { Artifact } from 'aws-cdk-lib/aws-codepipeline';
import * as iam from 'aws-cdk-lib/aws-iam';
import { CodeStarConnectionsSourceAction, CodeBuildAction } from 'aws-cdk-lib/aws-codepipeline-actions';
import { Pipeline } from 'aws-cdk-lib/aws-codepipeline';
import { Construct } from 'constructs';

export interface CodePipelineStackProps extends StackProps {
  region: string,
  app: string,
  version: string,
  environment: string
}

export class CodePipelineStack extends Stack {
    constructor(scope: Construct, id: string, props: CodePipelineStackProps) {
    super(scope, id, props);
    const prefix = props.app + "-" + props.environment;

      
    // Create a CodeBuild project
    const codeBuildProject = new PipelineProject(this, 'CodeBuildProject', {
        environment: {
        buildImage: LinuxBuildImage.STANDARD_6_0,
        },
        buildSpec: BuildSpec.fromObject({
        version: '0.2',
        phases: {
            install: {
            'runtime-versions': {
                nodejs: '16', 
            },
            commands: [
                'node -v',
            ],
            },
            build: {
            commands: [
                'sudo npm install -g aws-cdk',
                'cdk --version',
                'cd cdk_code',
                'npm install',
                'ls',
                'cdk deploy ApplicationStack --require-approval never', 
            ],
            },
        },
        }),
    });
    
    // Grant permissions to the CodeBuild project
    codeBuildProject.addToRolePolicy(new iam.PolicyStatement({
      actions: ['sts:AssumeRole'],
      resources: ['*'],
    }));
    
    // Source artifact
    const sourceArtifact = new Artifact();

    // Source action for GitHub
    const sourceAction = new CodeStarConnectionsSourceAction({
        actionName: prefix + '-Github-Deployment',
        owner: 'JugueDev',
        repo: 'automated-data-pipeline-with-cdk',
        branch: 'main',
        triggerOnPush: true,
        output: sourceArtifact,
        connectionArn: 'arn:aws:codestar-connections:us-east-1:539548017896:connection/1dcf1f84-2491-4206-9144-144500b67da0',
      });

    // CodeBuild artifact
    const buildArtifact = new Artifact("task_definition");

    // CodeBuild action
    const buildAction = new CodeBuildAction({
      actionName: prefix + '-CodeBuild',
      project: codeBuildProject,
      input: sourceArtifact,
      outputs: [buildArtifact],
    });


    // Create CodePipeline
    const pipeline = new Pipeline(this, 'MyCodePipeline', {
      pipelineName: prefix + 'Pipeline',
      stages: [
        {
          stageName: prefix + 'Source',
          actions: [sourceAction],
        },
        {
          stageName: prefix + 'Build',
          actions: [buildAction],
        },
      ],
    });

  }
}