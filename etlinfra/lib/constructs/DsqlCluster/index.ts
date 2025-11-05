// lib/dsql-cluster.ts
import { Construct } from 'constructs';
import {
  aws_dsql as dsql,
  aws_ec2 as ec2,
  CfnOutput,
  Tags,
} from 'aws-cdk-lib';

export interface DsqlClusterProps {
  vpc: ec2.IVpc;
  // Optional: allow inbound from these SGs to the Interface VPC Endpoint (tcp/5432)
  allowedSecurityGroups?: ec2.ISecurityGroup[];
  // Optional: bring your own KMS key (ARN). If omitted, AWS-owned key is used.
  kmsKeyArn?: string;
  // Optional: turn on deletion protection
  deletionProtection?: boolean;
  // Optional: tags to apply to the DSQL cluster
  tags?: Record<string, string>;
}

/**
 * Creates an Aurora DSQL cluster and a VPC Interface Endpoint to reach it privately.
 * Connect with any PostgreSQL client to the cluster hostname via your VPC.
 */
export class DsqlCluster extends Construct {
  public readonly cluster: dsql.CfnCluster;
  public readonly vpcEndpoint: ec2.InterfaceVpcEndpoint;

  constructor(scope: Construct, id: string, props: DsqlClusterProps) {
    super(scope, id);

    // 1) Create the Aurora DSQL cluster (single-Region)
    this.cluster = new dsql.CfnCluster(this, 'Cluster', {
      deletionProtectionEnabled: props.deletionProtection ?? true,
      kmsEncryptionKey: props.kmsKeyArn ?? 'AWS_OWNED_KMS_KEY',
      // For multi-Region, add multiRegionProperties here.
      // multiRegionProperties: { witnessRegion: 'us-west-2', peeredClusters: [...] }
    });

    // Tag the cluster (useful for ownership/cost)
    if (props.tags) {
      for (const [k, v] of Object.entries(props.tags)) {
        Tags.of(this.cluster).add(k, v);
      }
    }

    // 2) Create an Interface VPC Endpoint to the cluster’s PrivateLink service
    // CFN exposes the service name; CDK reads it as an attribute.
    // (If your CDK version doesn’t surface this attribute yet, you can fetch it
    // with a Custom Resource calling dsql:GetVpcEndpointServiceName.)
    const endpointSg = new ec2.SecurityGroup(this, 'DsqlEndpointSg', {
      vpc: props.vpc,
      description: 'Security group for DSQL Interface VPC Endpoint',
      allowAllOutbound: true,
    });

    // Allow app SGs to reach the endpoint on Postgres port 5432
    for (const sg of props.allowedSecurityGroups ?? []) {
      endpointSg.addIngressRule(sg, ec2.Port.tcp(5432), 'App -> DSQL (postgres)');
    }
    // If you prefer CIDR-based access, uncomment:
    // endpointSg.addIngressRule(ec2.Peer.ipv4(props.vpc.vpcCidrBlock), ec2.Port.tcp(5432));

    // Create the endpoint using the cluster’s service name
    // (attrVpcEndpointServiceName is provided by AWS::DSQL::Cluster)
    this.vpcEndpoint = new ec2.InterfaceVpcEndpoint(this, 'DsqlInterfaceEndpoint', {
      vpc: props.vpc,
      service: new ec2.InterfaceVpcEndpointService(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (this.cluster as any).attrVpcEndpointServiceName
      ),
      privateDnsEnabled: true,
      securityGroups: [endpointSg],
      subnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    });

    // Useful outputs
    new CfnOutput(this, 'DsqlClusterArn', { value: (this.cluster as any).attrArn });
    new CfnOutput(this, 'VpcEndpointId', { value: this.vpcEndpoint.vpcEndpointId });
  }
}
