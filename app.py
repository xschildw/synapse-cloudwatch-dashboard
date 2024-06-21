#!/usr/bin/env python3
import os

import aws_cdk as cdk

from synapse_cloudwatch_dashboard.synapse_cloudwatch_dashboard_stack import SynapseCloudwatchDashboardStack

app = cdk.App()

SynapseCloudwatchDashboardStack(scope=app, construct_id="SynapseCloudwatchDashboardStack")

app.synth()
