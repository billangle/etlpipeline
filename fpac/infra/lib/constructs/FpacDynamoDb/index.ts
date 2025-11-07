import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';



  interface Props {
     tablename: string;
     key: string;
  }


  
  export class FpacDynamoDb extends Construct {
  
    public readonly table: dynamodb.Table;

    constructor(scope: Construct, id: string, props: Props) {
      super(scope, id);
  
     this.table = new dynamodb.Table(this, `${props.tablename}`, {
        partitionKey: { name: props.key, type: dynamodb.AttributeType.STRING },
        tableName: props.tablename,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        deletionProtection: false,
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      });


      // stream requires TableV2 - must implment the billing differently
      // this.table.addStream(dynamodb.StreamViewType.NEW_AND_OLD_IMAGES);  
      // billing: dynamodb.Billing.provisiosoned(  // TableV2
      //   {
      //     readCapacity: 5,
      //     writeCapacity: 5,
      //   }
      // ),
   
  
    }
  }
  