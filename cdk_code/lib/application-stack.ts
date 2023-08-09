import {  Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from "aws-cdk-lib/aws-s3";
import {DataPipelineConstruct} from "./sourceTest/data-pipeline-construct"
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";


export interface ApplicationStackProps {
  applicationName: string
}

export class ApplicationStack extends Stack {
  constructor(scope: Construct, id: string, props: ApplicationStackProps) {
    super(scope, id);

    // Bucket donde se almacenará el código fuente de las funciones en glue
    const assetsBucket = new s3.Bucket(this, "assets-bucket-id", {
      bucketName: props.applicationName + "-assets-bucket",
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });  

    // Bucket donde se almacenarán los datos de la aplicación  
    const dataBucket = new s3.Bucket(this, "data-bucket-id", {
      bucketName: props.applicationName + "-data-bucket",
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      eventBridgeEnabled: true
    });  
    

    // Se copian los scripts y demas assets a este bucket
    new s3deploy.BucketDeployment(this, 'DeployWebsite', {
        sources: [s3deploy.Source.asset('../assets')],
        destinationBucket: assetsBucket,
        destinationKeyPrefix: 'assets', 
      });

    const dataPipelineConstruct = new DataPipelineConstruct(this, 'dataPipelineConstruct', {
      assetsBucket: assetsBucket, 
      dataBucket: dataBucket, 
    });
    
    }
}
