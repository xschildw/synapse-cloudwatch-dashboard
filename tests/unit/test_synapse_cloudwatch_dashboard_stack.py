import aws_cdk as core
import aws_cdk.assertions as assertions

from synapse_cloudwatch_dashboard.synapse_cloudwatch_dashboard_stack import SynapseCloudwatchDashboardStack

# example tests. To run these tests, uncomment this file along with the example
# resource in synapse_cloudwatch_dashboard/synapse_cloudwatch_dashboard_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = SynapseCloudwatchDashboardStack(app, "synapse-cloudwatch-dashboard")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
