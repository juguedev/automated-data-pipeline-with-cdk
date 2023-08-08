import { Construct } from 'constructs';
import * as s3 from "aws-cdk-lib/aws-s3";
import { IngestStageConstruct } from "./ingest-construct";
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import { Duration } from 'aws-cdk-lib';

export interface DataPipelineConstructProps {
    assetsBucket: s3.Bucket,
  }
export class DataPipelineConstruct extends Construct {
  constructor(scope: Construct, id: string, props: DataPipelineConstructProps) {
    super(scope, id);

    const validationGlueJobName = 'validation-data-job';

    const ingestStageConstruct = new IngestStageConstruct(this, 'ingestStageConstruct', {
        assetsBucket: props.assetsBucket, 
        validationGlueJobName: validationGlueJobName
      });


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
    
    const topic = new sns.Topic(this, 'sns-topic', {
      displayName: 'Topico de notificacion del data pipeline.',
    });
    // Añadimos los subscriptores
    // topic.addSubscription(new subs.EmailSubscription("jurgen.guerra@unmsm.edu.pe"));

      
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

    const SFMachine = new sfn.StateMachine(this, "state-machine-id", {
        stateMachineName: "data-pipeline",
        definition: stepFunctionDefinition,
        timeout: Duration.minutes(10),
    });


      }
}
