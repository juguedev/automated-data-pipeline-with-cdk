import { Construct } from 'constructs';
import * as s3 from "aws-cdk-lib/aws-s3";
import { IngestStageConstruct } from "./ingest-construct";
import { TransformStageConstruct } from "./transform-construct";
import { ConsumeStageConstruct } from "./consume-construct";
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import { Duration } from 'aws-cdk-lib';
import * as iam from "aws-cdk-lib/aws-iam";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";

export interface DataPipelineConstructProps {
    assetsBucket: s3.Bucket,
    dataBucket: s3.Bucket,
  }
export class DataPipelineConstruct extends Construct {
  constructor(scope: Construct, id: string, props: DataPipelineConstructProps) {
    super(scope, id);

    const validationGlueJobName = 'validation-data-job';
    const businessLogicGlueJobName = 'business-logic-data-job';
    const aggregationGlueJobName = 'aggregation-data-job';
    const loadGlueJobName = 'load-data-job';
    const notificationGlueJobName = 'notification-data-job';

    // Creamos un rol para asignarlo a los jobs
    const executeGlueJobsRole = new iam.Role(this, "glue-job-role-id", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      roleName: "Glue-Jobs-Role",
      description: "Rol de IAM para que los Glue Jobs puedan leer y escribir en los buckets de S3 asi como guardar logs.",
    });
  
      executeGlueJobsRole.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          'service-role/AWSGlueServiceRole', 
      ));
      executeGlueJobsRole.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          'CloudWatchFullAccess', 
      ));
      executeGlueJobsRole.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          'AmazonS3FullAccess', 
      ));

      const topic = new sns.Topic(this, 'sns-topic', {
        displayName: 'Topico de notificacion del data pipeline.',
      });

      // Añadimos los subscriptores
      // topic.addSubscription(new subs.EmailSubscription("jurgen.guerra@unmsm.edu.pe"));
          

    const ingestStageConstruct = new IngestStageConstruct(this, 'ingestStageConstruct', {
        assetsBucket: props.assetsBucket, 
        dataBucket: props.dataBucket,  
        validationGlueJobName: validationGlueJobName,
        executionGlueJobsRole: executeGlueJobsRole
    });

    const transformStageConstruct = new TransformStageConstruct(this, 'transformStageConstruct', {
        assetsBucket: props.assetsBucket, 
        dataBucket: props.dataBucket,  
        businessLogicGlueJobName: businessLogicGlueJobName,
        aggregationGlueJobName: aggregationGlueJobName,
        executionGlueJobsRole: executeGlueJobsRole
    });

    const consumeStageConstruct = new ConsumeStageConstruct(this, 'consumeStageConstruct', {
      assetsBucket: props.assetsBucket, 
      dataBucket: props.dataBucket,  
      loadGlueJobName: loadGlueJobName,
      notificationGlueJobName: notificationGlueJobName,
      executionGlueJobsRole: executeGlueJobsRole,
      snsNotificationTopic: topic,

  });




    /*                      STEP FUNCTION CONFIGURATION                     */

    const validation_job_task = new tasks.GlueStartJobRun(this, "sf-validation-data-job", {
      glueJobName: validationGlueJobName,
      arguments: sfn.TaskInput.fromObject({
          "--KEY.$": '$.detail.object.key',
      }),
      outputPath: "$",
      inputPath: "$",
      resultPath: "$",
      integrationPattern: sfn.IntegrationPattern.RUN_JOB, //runJob.sync
        
    })

    const business_logic_job_task = new tasks.GlueStartJobRun(this, "sf-business-logic-data-job", {
      glueJobName: businessLogicGlueJobName,
      arguments: sfn.TaskInput.fromObject({
          "--KEY.$": "$.Arguments.--KEY",
      }),
      outputPath: "$",
      inputPath: "$",
      resultPath: "$",
      integrationPattern: sfn.IntegrationPattern.RUN_JOB, //runJob.sync    
    })

    const aggregation_job_task = new tasks.GlueStartJobRun(this, "sf-aggregation-data-job", {
      glueJobName: aggregationGlueJobName,
      arguments: sfn.TaskInput.fromObject({
          "--KEY.$": "$.Arguments.--KEY",
      }),
      outputPath: "$",
      inputPath: "$",
      resultPath: "$",
      integrationPattern: sfn.IntegrationPattern.RUN_JOB, //runJob.sync    
    })

    const load_job_task = new tasks.GlueStartJobRun(this, "sf-load-data-job", {
      glueJobName: loadGlueJobName,
      arguments: sfn.TaskInput.fromObject({
          "--KEY.$": "$.Arguments.--KEY",
      }),
      outputPath: "$",
      inputPath: "$",
      resultPath: "$",
      integrationPattern: sfn.IntegrationPattern.RUN_JOB, //runJob.sync    
    })

    const notification_job_task = new tasks.GlueStartJobRun(this, "sf-notification-data-job", {
      glueJobName: notificationGlueJobName,
      arguments: sfn.TaskInput.fromObject({
          "--KEY.$": "$.Arguments.--KEY",
      }),
      outputPath: "$",
      inputPath: "$",
      resultPath: "$",
      integrationPattern: sfn.IntegrationPattern.RUN_JOB, //runJob.sync    
    })

    const jobFailedTask = new tasks.SnsPublish(this, 'publish-notification', {
      topic: topic,
      message: sfn.TaskInput.fromJsonPathAt('$.error'),
      resultPath: '$.error',
      subject: "Failed Pipeline",
    });


    const stepFunctionDefinition = validation_job_task
    .addCatch(jobFailedTask, {   
        resultPath: '$.error',
    })
    .next(business_logic_job_task.addCatch(jobFailedTask, {   
      resultPath: '$.error',
    }))
    .next(aggregation_job_task.addCatch(jobFailedTask, {   
      resultPath: '$.error',
    }))
    .next(load_job_task.addCatch(jobFailedTask, {   
      resultPath: '$.error',
    }))
    .next(notification_job_task.addCatch(jobFailedTask, {   
      resultPath: '$.error',
    }))

    const SFMachine = new sfn.StateMachine(this, "state-machine-id", {
        stateMachineName: "data-pipeline",
        definitionBody: sfn.DefinitionBody.fromChainable(stepFunctionDefinition),
        timeout: Duration.minutes(10),
    });


    // Regla de eventbridge para ejecutar el SM
    const eventRule = new events.Rule(this, "s3-object-created", {
      ruleName: "s3-object-created",
      eventPattern: {
          source: ["aws.s3"],
          detailType: ["Object Created"],
          detail: {
              bucket: {
                  name: [props.dataBucket.bucketName]
              },
              object: {
                  key: [{
                      prefix: "charged"
                  }]
              }
          }
      }
  });

  // Creamos un rol para ejecutar el step function 
  const invokeStepFunctionRole = new iam.Role(this, "invoke-step-function-role-id", {
      assumedBy: new iam.ServicePrincipal("events.amazonaws.com"),
      roleName: "Invoke-Step-Function-Role",
      description: "Rol de IAM para invocar el Step Function.",
  });

  // Añademos un Policy al rol de IAM
  invokeStepFunctionRole.addToPolicy(
      new iam.PolicyStatement({
          resources: [SFMachine.stateMachineArn],
          actions: ["states:StartExecution"],
      })
  );

  // Añadimos un target a la regla del evento
  eventRule.addTarget(new targets.SfnStateMachine(SFMachine, {
      role: invokeStepFunctionRole
    }));
  
  }
}
