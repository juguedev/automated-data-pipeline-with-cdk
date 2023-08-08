import {  Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from "aws-cdk-lib/aws-s3";
import {DataPipelineConstruct} from "./sourceTest/data-pipeline-construct"
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";

export class ApplicationStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Bucket donde se almacenará el código fuente de los  
    const assetsBucket = new s3.Bucket(this, "assets-bucket-id", {
      bucketName: "assets-bucket-for-test-jg",
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });  

    // Se copian los scripts y demas assets a este bucket
    new s3deploy.BucketDeployment(this, 'DeployWebsite', {
        sources: [s3deploy.Source.asset('../assets')],
        destinationBucket: assetsBucket,
        destinationKeyPrefix: 'assets', 
      });

    const dataPipelineConstruct = new DataPipelineConstruct(this, 'dataPipelineConstruct', {
      assetsBucket: assetsBucket, 
    });
    
    }
}
