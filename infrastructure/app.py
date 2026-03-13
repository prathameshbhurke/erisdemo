import aws_cdk as cdk
from infrastructure.infrastructure_stack import InfrastructureStack
from dotenv import load_dotenv
import os

load_dotenv()

app = cdk.App()

InfrastructureStack(app, "ErisStack",
    client_name="acme",
    env=cdk.Environment(
        account="680019129594",
        region="us-east-1"
    )
)

app.synth()