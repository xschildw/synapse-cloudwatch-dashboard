import boto3
from configuration import ConfigurationProvider, AwsProvider
from aws_cdk import (
    Duration,
    Stack,
    aws_cloudwatch as cw
)
from constructs import Construct


def init_config(stack, profile_name):
  BUCKET_NAME = f'{stack}.cloudwatch.metrics.sagebase.org'
  FILE_KEY = f'{stack}_cw_configuration.json'
  if profile_name:
    session = boto3.Session(profile_name=profile_name, region_name='us-east-1')
  else:
    session = boto3.Session(region_name='us-east-1')
  aws_provider = AwsProvider(session=session)
  s3_client = aws_provider.get_client(client_type='s3')
  configuration_provider = ConfigurationProvider(s3_client=s3_client, bucket_name=BUCKET_NAME, file_key=FILE_KEY)
  config = configuration_provider.load_raw_configuration()
  return config


def create_graph_widget(namespace, metric_name, dimension_name, values, title='Title', width=24, height=6):
  metrics = [
    cw.Metric(
      namespace=namespace,
      metric_name=metric_name,
      dimensions_map={dimension_name: instance_id}
    ) for instance_id in values
  ]
  widget = cw.GraphWidget(title=title, width=width, height=height, stacked=False, left=metrics, view=cw.GraphWidgetView.TIME_SERIES)
  return widget


def create_worker_stats_widget(title, config, stack_versions, metric_name):
  metrics = []
  for sv in stack_versions:
    namespace = f'Worker-Statistics-{sv}'
    config_key = f'{sv}-workers-names'
    version_metrics = [cw.Metric(namespace=namespace, metric_name=metric_name,
                        dimensions_map={"Worker Name": value}) for value in config[config_key]]
    metrics.extend(version_metrics)
  return cw.GraphWidget(title=title, width=24, height=3,
                        view=cw.GraphWidgetView.TIME_SERIES, stacked=False, period=Duration.seconds(300),
                        left=metrics)


def create_memory_widget(title, config, stack_versions, environment):
  ENV_KEYS = {"Repository": "repo", "Workers": "workers"}
  metrics = []
  for sv in stack_versions:
    namespace = f'{environment}-Memory-{sv}'
    config_key = f'{sv}-{ENV_KEYS[environment]}-vmids'
    version_metrics = [cw.Metric(namespace=namespace, metric_name='used',
                        dimensions_map={"instance": value}) for value in config[config_key]]
    metrics.extend(version_metrics)
  return cw.GraphWidget(title=title, width=24, height=3,
                        view=cw.GraphWidgetView.TIME_SERIES, stacked=False, period=Duration.seconds(300),
                        left=metrics)


def create_ec2_cpu_utilization_widget(title, ec2_instance_ids):
  return create_graph_widget("AWS/EC2", "CPUUtilization", "InstanceId", ec2_instance_ids, title, 24, 6)


def create_ec2_network_out_widget(title, ec2_instance_ids):
  return create_graph_widget("AWS/EC2", "NetworkOut", "InstanceId", ec2_instance_ids, title, 24, 3)


'''
  RDS
'''
def rds_ids_from_stack_versions(stack, stack_versions):
  db_types = ['db', 'table-0']
  ids = [f'{stack}-{sv}-{dbt}' for sv in stack_versions for dbt in db_types]
  ids.append(f'{stack}-id-generator-db-orange')
  return ids


def create_rds_cpu_utilization_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="CPUUtilization", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=24, height=6)


def create_rds_free_storage_space_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="FreeStorageSpace", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=24, height=3)

def create_rds_read_throughput_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="ReadThroughput", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


def create_rds_write_throughput_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="WriteThroughput", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


def create_rds_read_latency_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="ReadLatency", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


def create_rds_write_latency_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="WriteLatency", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


def create_rds_read_iops_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="ReadIOPS", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


def create_rds_write_iops_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="WriteIOPS", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


'''
  QueryPerf
'''
def create_query_performance_widget(title, stack, stack_versions):
  metrics = [cw.Metric(namespace="AWS/SQS",
                       metric_name="ApproximateAgeOfOldestMessage",
                       dimensions_map={"QueueName": f'{stack}-{sv}-QUERY'}) for sv in stack_versions]
  widget = cw.GraphWidget(title=title, width=24, height=6, view=cw.GraphWidgetView.TIME_SERIES,
                          left=metrics, period=Duration.seconds(300), stacked=False, statistic='Average')
  return widget

'''
  SES
'''
def create_ses_widget(title):
  bounce_rate_metric = cw.Metric(
    namespace="AWS/SES",
    metric_name="Reputation.BounceRate",
    label="Bounce Rate",
    statistic="Maximum",
    region="us-east-1"
  )
  complaint_rate_metric = cw.Metric(
    namespace="AWS/SES",
    metric_name="Reputation.ComplaintRate",
    label="Complaint Rate",
    statistic="Maximum",
    color="#d62728",
    region="us-east-1"
  )
  bounce_rate_expression = cw.MathExpression(
    expression="100 * m1",
    using_metrics={"m1": bounce_rate_metric},
    period=Duration.hours(1),
    label="Bounce Rate",
    color="#1f77b4"
  )
  complaint_rate_expression = cw.MathExpression(
    expression="100 * m2",
    using_metrics={"m2": complaint_rate_metric},
    period=Duration.hours(1),
    label="Complaint Rate",
    color="#d62728"
  )
  bounce_count_metric = cw.Metric(
    namespace="AWS/SES",
    metric_name="Bounce",
    label="Bounced Count",
    statistic="Sum",
    color="#ff7f0e",
  )
  send_count_metric = cw.Metric(
    namespace="AWS/SES",
    metric_name="Send",
    label="Sent Count",
    statistic="Sum",
    color="#2ca02c",
  )
  widget = cw.GraphWidget(title=title, width=24, height=4,
                          view=cw.GraphWidgetView.TIME_SERIES,
                          left=[
                            bounce_rate_metric,
                            complaint_rate_metric,
                            bounce_rate_expression,
                            complaint_rate_expression
                          ],
                          right=[
                            bounce_count_metric,
                            send_count_metric
                          ],
                          period=Duration.hours(1),
                          left_y_axis=cw.YAxisProps(label="Rate", min=0, show_units=False),
                          right_y_axis=cw.YAxisProps(label="Count", min=0, show_units=False)
                          )
  return widget

'''
  FileScanner
'''
def create_filescanner_widget(title, stack_versions):
  left_metrics = []
  right_metrics = []

  for stack_version in stack_versions:
    namespace = f'Asynchronous Workers - {stack_version}'
    left_metrics.extend([
      cw.Metric(namespace=namespace, metric_name="JobCompletedCount",
                        dimensions_map={"workerClass": "FileHandleAssociationScanRangeWorker"},
                        label=f"Jobs Completed - {stack_version}", color="#1f77b4"),
      cw.Metric(namespace=namespace, metric_name="JobFailedCount",
                        label=f"Jobs Failed - {stack_version}", color="#d62728")
    ])
    right_metrics.extend([cw.Metric(namespace=namespace, metric_name="AllJobsCompletedCount",
                        dimensions_map={"workerClass": "FileHandleAssociationScanRangeWorker"},
                        label=f"Scans Completed - {stack_version}", color="#2ca02c")])

  widget = cw.GraphWidget(title=title, width=24, height=4, view=cw.GraphWidgetView.TIME_SERIES,
                          left=left_metrics, right=right_metrics, stacked=False,
                          set_period_to_time_range=True,
                          statistic="Sum")
  return widget


'''
  Active connections
'''
def create_active_connections_metric(namespace, db):
  metric = cw.Metric(
    namespace=namespace,
    metric_name="activeConnectionsCount",
    dimensions_map={"dataSourceId": db}
  )
  return metric

def create_active_connections_widget(title, environment, stack_versions):
  dbs = ["idgen", "main", "tables"]
  metrics = [create_active_connections_metric(f'{environment}-Database-{sv}', db) for sv in stack_versions for db in dbs]
  widget = cw.GraphWidget(title=title, width=24, height=6, left=metrics, statistic="Maximum", view=cw.GraphWidgetView.TIME_SERIES)
  return widget


def create_repo_active_connections_widget(title, stack_versions):
  return create_active_connections_widget(title, "Repository", stack_versions)


def create_workers_active_connections_widget(title, stack_versions):
  return create_active_connections_widget(title, "Workers", stack_versions)


'''
  CloudSearch
'''
def create_cloudsearch_metric(dimension_value):
  metric = cw.Metric(
    namespace="AWS/CloudSearch",
    metric_name="SearchableDocuments",
    dimensions_map={"DomainName": dimension_value, "ClientId": "325565585839"}
  )
  return metric


def create_cloudsearch_widget(title, stack_versions):
  dimension_values = [f'prod-{sv}-sagebase-org' for sv in stack_versions]
  metrics = [create_cloudsearch_metric(dv) for dv in dimension_values]
  widget = cw.GraphWidget(title=title, width=24, height=4, left=metrics, view=cw.GraphWidgetView.TIME_SERIES)
  return widget


def create_repo_alb_response_widget(title, config, stack_versions):
  metrics = []
  dimensions_values = [item for sublist in [config[f'{sv}-repo-alb-name'] for sv in stack_versions if f'{sv}-repo-alb-name' in config] for item in sublist]
  for dv in dimensions_values:
    metric1 = cw.Metric(namespace='AWS/ApplicationELB', metric_name='TargetResponseTime', dimensions_map={'LoadBalancer': dv},
                       period=Duration.seconds(300), statistic='Average')
    metric2 = cw.Metric(namespace='AWS/ApplicationELB', metric_name='TargetResponseTime', dimensions_map={'LoadBalancer': dv},
                          period=Duration.seconds(300), statistic='p95')
    metrics.append(metric1)
    metrics.append(metric2)
  widget = cw.GraphWidget(title=title, width=24, height=4, view=cw.GraphWidgetView.TIME_SERIES,
                          stacked=False, set_period_to_time_range=True,
                          left=metrics)
  return widget

def create_docker_cpu_widget():
  metric1 = cw.Metric(namespace='AWS/EC2', metric_name="CPUUtilization", dimensions_map={'InstanceId': 'i-03caba8ba8027dcdb'},
                     period=Duration.seconds(300))
  metric2 = cw.Metric(namespace='AWS/EC2', metric_name="CPUUtilization", dimensions_map={'InstanceId': 'i-0e72eb7485bf626fd'},
                      period=Duration.seconds(300))
  metrics = [metric1, metric2]
  widget = cw.GraphWidget(title='Docker - CPU utilization', width=12, height=4, view=cw.GraphWidgetView.TIME_SERIES,
                          stacked=False, set_period_to_time_range=True,
                          left=metrics)
  return widget

def create_docker_network_widget():
  metric1 = cw.Metric(namespace='AWS/EC2', metric_name="NetworkOut", dimensions_map={'InstanceId': 'i-03caba8ba8027dcdb'},
                     period=Duration.seconds(300))
  metric2 = cw.Metric(namespace='AWS/EC2', metric_name="NetworkOut", dimensions_map={'InstanceId': 'i-0e72eb7485bf626fd'},
                      period=Duration.seconds(300))
  metrics = [metric1, metric2]
  widget = cw.GraphWidget(title='Docker - Network out', width=12, height=4, view=cw.GraphWidgetView.TIME_SERIES,
                          stacked=False, set_period_to_time_range=True,
                          left=metrics)
  return widget


class SynapseCloudwatchDashboardStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
      super().__init__(scope, construct_id, **kwargs)

      stack = self.node.try_get_context(key='stack')
      if stack is None:
        raise ValueError('No stack specified')

      stack_versions_str = self.node.try_get_context(key='stack_versions')
      if stack_versions_str is None:
        raise ValueError('No stack versions specified')

      # Profile name can be undefined if run on EC2
      profile_name = self.node.try_get_context(key='profile_name')

      stack_versions = stack_versions_str.split(',')

      dashboard = cw.Dashboard(
        self,
        id="stack-status",
        dashboard_name="Stack-Status",
        default_interval=Duration.days(35),
      )

      config = init_config(stack=stack, profile_name=profile_name)

      filescanner_widget = create_filescanner_widget(title='FileScanner', stack_versions=stack_versions)
      cloudsearch_widget = create_cloudsearch_widget(title='CloudSearch - searchableDocuments', stack_versions=stack_versions)
      repo_active_connections_widget = create_repo_active_connections_widget(title='Repo-Active-Connections', stack_versions=stack_versions)
      workers_active_connections_widget = create_workers_active_connections_widget(title='Workers-Active-Connections', stack_versions=stack_versions)
      query_perf_widget = create_query_performance_widget(title="Query Performance", stack=stack, stack_versions=stack_versions)
      ses_widget = create_ses_widget(title='SES')
      rds_cpu_widget = create_rds_cpu_utilization_widget(title='RDS - CPU Utilization', stack=stack, stack_versions=stack_versions)
      rds_freestorage_widget = create_rds_free_storage_space_widget(title='RDS - Free Storage Space', stack=stack, stack_versions=stack_versions)
      repo_ec2_ids = [s for vp in stack_versions for s in config.get(f'{vp}-repo-ec2-instances', [])]
      cpu_repo_widget = create_ec2_cpu_utilization_widget(title="Repo - CPU Utilization", ec2_instance_ids=repo_ec2_ids)
      workers_ec2_ids = [s for vp in stack_versions for s in config.get(f'{vp}-workers-ec2-instances', [])]
      cpu_workers_widget = create_ec2_cpu_utilization_widget(title="Workers - CPU Utilization", ec2_instance_ids=workers_ec2_ids)
      portal_ec2_ids = [s for vp in stack_versions for s in config.get(f'{vp}-portal-ec2-instances', [])]
      cpu_portal_widget = create_ec2_cpu_utilization_widget(title="Portal - CPU Utilization", ec2_instance_ids=portal_ec2_ids)
      network_out_portal_widget = create_ec2_network_out_widget(title="Portal - Network out", ec2_instance_ids=portal_ec2_ids)
      repo_memory_widget = create_memory_widget(title='Repo - Memory used', config=config, stack_versions=stack_versions, environment='Repository')
      workers_memory_widget = create_memory_widget(title='Workers - Memory used', config=config, stack_versions=stack_versions, environment='Workers')
      workers_jobs_completed_widget = create_worker_stats_widget(title="Workers stats - Jobs completed", config=config, stack_versions=stack_versions, metric_name='Completed Job Count')
      workers_pc_time_widget = create_worker_stats_widget(title="Workers stats - % time running", config=config, stack_versions=stack_versions, metric_name='% Time Running')
      workers_cumulative_time_widget = create_worker_stats_widget(title="Workers stats - Cumulative time", config=config, stack_versions=stack_versions, metric_name='Cumulative runtime')
      repo_alb_rtime_widget = create_repo_alb_response_widget(title='Repo ALB response time', config=config, stack_versions=stack_versions)
      docker_cpu_widget = create_docker_cpu_widget()
      docker_network_widget = create_docker_network_widget()
      rds_read_throughput_widget = create_rds_read_throughput_widget(title="RDS Read Throughput", stack=stack, stack_versions=stack_versions)
      rds_write_throughput_widget = create_rds_write_throughput_widget(title="RDS Write Throughput", stack=stack, stack_versions=stack_versions)
      rds_read_latency_widget = create_rds_read_latency_widget(title="RDS Read Latency", stack=stack, stack_versions=stack_versions)
      rds_write_latency_widget = create_rds_write_latency_widget(title="RDS Write Latency", stack=stack, stack_versions=stack_versions)
      rds_read_iops_widget = create_rds_read_iops_widget(title="RDS Read Iops", stack=stack, stack_versions=stack_versions)
      rds_write_iops_widget = create_rds_write_iops_widget(title="RDS Write Iops", stack=stack, stack_versions=stack_versions)

      dashboard.add_widgets(cpu_repo_widget)
      dashboard.add_widgets(cpu_workers_widget)
      dashboard.add_widgets(rds_cpu_widget)
      dashboard.add_widgets(rds_freestorage_widget)
      dashboard.add_widgets(cpu_portal_widget)
      dashboard.add_widgets(network_out_portal_widget)
      dashboard.add_widgets(docker_cpu_widget, docker_network_widget)
      dashboard.add_widgets(repo_memory_widget)
      dashboard.add_widgets(workers_memory_widget)
      dashboard.add_widgets(repo_active_connections_widget)
      dashboard.add_widgets(workers_active_connections_widget)
      dashboard.add_widgets(workers_jobs_completed_widget)
      dashboard.add_widgets(workers_pc_time_widget)
      dashboard.add_widgets(workers_cumulative_time_widget)
      dashboard.add_widgets(query_perf_widget)
      dashboard.add_widgets(repo_alb_rtime_widget)
      dashboard.add_widgets(ses_widget)
      dashboard.add_widgets(filescanner_widget)
      dashboard.add_widgets(cloudsearch_widget)
      dashboard.add_widgets(rds_read_throughput_widget, rds_write_throughput_widget)
      dashboard.add_widgets(rds_read_latency_widget, rds_write_latency_widget)
      dashboard.add_widgets(rds_read_iops_widget, rds_write_iops_widget)


