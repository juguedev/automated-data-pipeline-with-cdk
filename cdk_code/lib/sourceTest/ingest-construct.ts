import { Construct } from 'constructs';
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";

export interface IngestStageProps {
    assetsBucket: s3.Bucket,
    validationGlueJobName: string
  }
  
  export class IngestStageConstruct extends Construct {
    constructor(scope: Construct, id: string, props: IngestStageProps) {
      super(scope, id);
      
    // Creamos un rol para asignarlo a los jobs
    const executeGlueJobsRole = new iam.Role(this, "glue-job-role-id", {
        assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
        roleName: "Glue-Jobs-Role",
        description: "Rol de IAM para que los Glue Jobs puedan leer y escribir en los buckets de S3 asi como guardar logs.",
    });

    executeGlueJobsRole.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName(
            'service-role/AWSGlueServiceRole', 
          )
    );

    // Job de validación
    const validationJob = new glue.CfnJob(this, 'validation-data-job-id', {
        command: {
          name: 'glueetl',
          pythonVersion: '3',
          scriptLocation: "s3://" + props.assetsBucket.bucketName + "/assets/sourceTest/validation-job.py",
        },
        defaultArguments: {
          "--BUCKET": "s3://obds3131226-dev/",
        },
        role: executeGlueJobsRole.roleArn,
        description: 'Este job se encarga de validar que el archivo ingresado cumple con las reglas.',
        executionProperty: {
          maxConcurrentRuns: 3,
        },
        maxCapacity: 2,
        maxRetries: 0,
        name: props.validationGlueJobName,
        timeout: 5,
        glueVersion: "3.0",
      });

    }
  }