import * as path from 'node:path';
import * as glue from '@aws-cdk/aws-glue-alpha';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as cdk from 'aws-cdk-lib/core';
import type { Construct } from 'constructs';

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Glue実行用のIAMロール
    const glueRole = new iam.Role(this, 'GlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          'service-role/AWSGlueServiceRole',
        ),
      ],
    });

    // 検証用Glueジョブ（G.2X、2ワーカー）
    const verificationJob = new glue.PySparkEtlJob(
      this,
      'WorkerVerificationJob',
      {
        role: glueRole,
        script: glue.Code.fromAsset(
          path.join(__dirname, '../assets/scripts/worker_verification.py'),
        ),
        glueVersion: glue.GlueVersion.V5_0,
        workerType: glue.WorkerType.G_1X,
        numberOfWorkers: 2,
        jobName: 'glue-worker-verification',
        description: 'AWS Glueワーカー・並列処理の検証用ジョブ',
      },
    );

    // 出力
    new cdk.CfnOutput(this, 'GlueJobName', {
      value: verificationJob.jobName,
      description: '検証用Glueジョブ名',
    });

    new cdk.CfnOutput(this, 'GlueJobArn', {
      value: verificationJob.jobArn,
      description: '検証用GlueジョブARN',
    });
  }
}
