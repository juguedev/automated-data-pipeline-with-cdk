import { Construct } from 'constructs';
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";

export interface TransformStageProps {
    assetsBucket: s3.Bucket,
    businessLogicGlueJobName: string,
    aggregationGlueJobName: string,
    executionGlueJobsRole: iam.Role
  }
  
  export class TransformStageConstruct extends Construct {
    constructor(scope: Construct, id: string, props: TransformStageProps) {
      super(scope, id);


    // Job de Lógica de negocio
    const businessLogicJob = new glue.CfnJob(this, 'business-logic-data-job-id', {
        command: {
          name: 'glueetl',
          pythonVersion: '3',
          scriptLocation: "s3://" + props.assetsBucket.bucketName + "/assets/sourceTest/business-logic-job.py",
        },
        defaultArguments: {
          "--BUCKET": "s3://obds3131226-dev/",
        },
        role: props.executionGlueJobsRole.roleArn,
        description: 'Este job se encarga de ejecutar las transformaciones de la lógica de negocio.',
        executionProperty: {
          maxConcurrentRuns: 3,
        },
        maxCapacity: 2,
        maxRetries: 0,
        name: props.businessLogicGlueJobName,
        timeout: 5,
        glueVersion: "3.0",
      });

      
    // Job de Agregación
    const aggregationJob = new glue.CfnJob(this, 'aggregation-data-job-id', {
        command: {
          name: 'glueetl',
          pythonVersion: '3',
          scriptLocation: "s3://" + props.assetsBucket.bucketName + "/assets/sourceTest/aggregation-job.py",
        },
        defaultArguments: {
          "--BUCKET": "s3://obds3131226-dev/",
        },
        role: props.executionGlueJobsRole.roleArn,
        description: 'Este job se encarga de ejecutar las transformaciones de agregación de datos.',
        executionProperty: {
          maxConcurrentRuns: 3,
        },
        maxCapacity: 2,
        maxRetries: 0,
        name: props.aggregationGlueJobName,
        timeout: 5,
        glueVersion: "3.0",
      });


    }
  }