import os

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_redshiftserverless as redshift,
    RemovalPolicy,
    CfnOutput
)
from constructs import Construct

class InfrastructureStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, client_name: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── S3 Staging Bucket ──────────────────────────────────────
        staging_bucket = s3.Bucket(
            self, "StagingBucket",
            bucket_name=f"{client_name}-airbyte-staging",
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True
        )

        # ── IAM User for Airbyte ───────────────────────────────────
        airbyte_user = iam.User(
            self, "AirbyteUser",
            user_name=f"{client_name}-airbyte-user"
        )

        # S3 access for Airbyte
        staging_bucket.grant_read_write(airbyte_user)

        # Redshift access for Airbyte
        airbyte_user.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftFullAccess")
        )
        airbyte_user.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftDataFullAccess")
        )

        # Redshift Serverless inline policy
        airbyte_user.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["redshift-serverless:GetCredentials"],
                resources=["*"]
            )
        )

        # ── Redshift Serverless Namespace ──────────────────────────
        namespace = redshift.CfnNamespace(
            self, "RedshiftNamespace",
            namespace_name=f"{client_name}-namespace",
            admin_username="admin",
            admin_user_password= os.environ.get("REDSHIFT_PASSWORD"),
            db_name="dev"
        )

        # ── Redshift Serverless Workgroup ──────────────────────────
        workgroup = redshift.CfnWorkgroup(
            self, "RedshiftWorkgroup",
            workgroup_name=f"{client_name}-workgroup",
            namespace_name=f"{client_name}-namespace",
            base_capacity=8,
            publicly_accessible=True
        )
        workgroup.add_dependency(namespace)

        # ── Outputs (printed after cdk deploy) ─────────────────────
        CfnOutput(self, "BucketName", value=staging_bucket.bucket_name)
        CfnOutput(self, "AirbyteUserName", value=airbyte_user.user_name)
        CfnOutput(self, "RedshiftWorkgroupName", value=f"{client_name}-workgroup")