
import * as iam from "aws-cdk-lib/aws-iam"
import { Function, Runtime, AssetCode  } from "aws-cdk-lib/aws-lambda"
import { Duration, StackProps } from "aws-cdk-lib"
import { Construct } from "constructs"


interface LambdaApiStackProps extends StackProps {
    functionName: string,
    functionCode: string,
    role: iam.IRole,
    environment: any,
}

export class FpacLambda extends Construct {
    public lambdaFunction: Function

    constructor(scope: Construct, id: string, props: LambdaApiStackProps) {
        super(scope, id)


        this.lambdaFunction = new Function(this, props.functionName, {
            functionName: props.functionName,
            handler: "index.handler",
            runtime: Runtime.NODEJS_LATEST,
            code: new AssetCode(props.functionCode),
            memorySize: 512,
            role: props.role,
            timeout: Duration.seconds(30),
            environment: props.environment,
       
        })



    }
}