import { Construct } from 'constructs';
import * as s3 from "aws-cdk-lib/aws-s3";
import { IngestStageConstruct } from "./ingest-construct";
import { TransformStageConstruct } from "./transform-construct";
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import { Duration } from 'aws-cdk-lib';
import * as iam from "aws-cdk-lib/aws-iam";

export interface DataPipelineConstructProps {
    assetsBucket: s3.Bucket,
  }
export class DataPipelineConstruct extends Construct {
  constructor(scope: Construct, id: string, props: DataPipelineConstructProps) {
    super(scope, id);

    const validationGlueJobName = 'validation-data-job';
    const businessLogicGlueJobName = 'business-logic-data-job';
    const aggregationGlueJobName = 'aggregation-data-job';

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
        validationGlueJobName: validationGlueJobName,
        executionGlueJobsRole: executeGlueJobsRole
    });

    const transformStageConstruct = new TransformStageConstruct(this, 'transformStageConstruct', {
        assetsBucket: props.assetsBucket, 
        businessLogicGlueJobName: businessLogicGlueJobName,
        aggregationGlueJobName: aggregationGlueJobName,
        executionGlueJobsRole: executeGlueJobsRole
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
          "--KEY.$": '$.detail.object.key',
      }),
      outputPath: "$",
      inputPath: "$",
      resultPath: "$",
      integrationPattern: sfn.IntegrationPattern.RUN_JOB, //runJob.sync    
    })

    const aggregation_job_task = new tasks.GlueStartJobRun(this, "sf-aggregation-data-job", {
      glueJobName: aggregationGlueJobName,
      arguments: sfn.TaskInput.fromObject({
          "--KEY.$": '$.detail.object.key',
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

    const SFMachine = new sfn.StateMachine(this, "state-machine-id", {
        stateMachineName: "data-pipeline",
        definition: stepFunctionDefinition,
        timeout: Duration.minutes(10),
    });


      }
}
