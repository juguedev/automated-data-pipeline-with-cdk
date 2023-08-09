import { Construct } from 'constructs';
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";

export interface IngestStageProps {
    assetsBucket: s3.Bucket,
    dataBucket: s3.Bucket,
    validationGlueJobName: string,
    executionGlueJobsRole: iam.Role
  }
  
  export class IngestStageConstruct extends Construct {
    constructor(scope: Construct, id: string, props: IngestStageProps) {
      super(scope, id);
      

    // Job de validación
    const validationJob = new glue.CfnJob(this, 'validation-data-job-id', {
        command: {
          name: 'pythonshell',
          pythonVersion: '3.9',
          scriptLocation: "s3://" + props.assetsBucket.bucketName + "/assets/sourceTest/validation-job.py",
        },
        defaultArguments: {
          "--BUCKET":  "s3://" + props.dataBucket.bucketName + "/",
        },
        role: props.executionGlueJobsRole.roleArn,
        description: 'Este job se encarga de validar que el archivo ingresado cumple con las reglas.',
        executionProperty: {
          maxConcurrentRuns: 3,
        },
        maxCapacity: 1,
        maxRetries: 0,
        name: props.validationGlueJobName,
        timeout: 5,
        glueVersion: "3.0",
      });

    }
  }