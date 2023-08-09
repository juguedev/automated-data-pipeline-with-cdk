import { Construct } from 'constructs';
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";
import * as sns from 'aws-cdk-lib/aws-sns';

export interface ConsumeStageProps {
    assetsBucket: s3.Bucket,
    dataBucket: s3.Bucket,
    loadGlueJobName: string,
    notificationGlueJobName: string,
    executionGlueJobsRole: iam.Role,
    snsNotificationTopic: sns.Topic,

  }
  
  export class ConsumeStageConstruct extends Construct {
    constructor(scope: Construct, id: string, props: ConsumeStageProps) {
      super(scope, id);


    // Job de Carga de Datos
    const loadJob = new glue.CfnJob(this, 'load-data-job-id', {
        command: {
          name: 'pythonshell',
          pythonVersion: '3.9',
          scriptLocation: "s3://" + props.assetsBucket.bucketName + "/assets/sourceTest/load-job.py",
        },
        defaultArguments: {
          "--BUCKET": "s3://" + props.dataBucket.bucketName + "/",
        },
        role: props.executionGlueJobsRole.roleArn,
        description: 'Este job se encarga de ejecutar la carga de los datos al destino final.',
        executionProperty: {
          maxConcurrentRuns: 3,
        },
        maxCapacity: 1,
        maxRetries: 0,
        name: props.loadGlueJobName,
        timeout: 5,
        glueVersion: "3.0",
      });

      
    // Job de Notificación
    const notificationJob = new glue.CfnJob(this, 'notification-data-job-id', {
        command: {
          name: 'pythonshell',
          pythonVersion: '3.9',
          scriptLocation: "s3://" + props.assetsBucket.bucketName + "/assets/sourceTest/notification-job.py",
        },
        defaultArguments: {
          "--BUCKET": "s3://" + props.dataBucket.bucketName + "/",
          "--SNS_ARN": props.snsNotificationTopic.topicArn,
        },
        role: props.executionGlueJobsRole.roleArn,
        description: 'Este job se encarga de ejecutar las Consumeaciones de agregación de datos.',
        executionProperty: {
          maxConcurrentRuns: 3,
        },
        maxCapacity: 1,
        maxRetries: 0,
        name: props.notificationGlueJobName,
        timeout: 5,
        glueVersion: "3.0",
      });


    }
  }