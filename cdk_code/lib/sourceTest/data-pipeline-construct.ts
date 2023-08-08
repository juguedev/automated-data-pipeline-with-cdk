import { Construct } from 'constructs';
import * as s3 from "aws-cdk-lib/aws-s3";
import {IngestStageConstruct} from "./ingest-construct"

export interface DataPipelineConstructProps {
    assetsBucket: s3.Bucket,
  }
export class DataPipelineConstruct extends Construct {
  constructor(scope: Construct, id: string, props: DataPipelineConstructProps) {
    super(scope, id);

    const ingestStageConstruct = new IngestStageConstruct(this, 'ingestStageConstruct', {
        assetsBucket: props.assetsBucket, 
      });

      }
}
