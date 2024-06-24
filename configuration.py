import sys
import logging
import json
import re
import boto3
from botocore import args


class AwsProvider:
  def __init__(self, session=None):
    """Initialize the AwsProvider with an optional boto3 session."""
    self.session = session
    self.clients = {}
    self.resources = {}

    if self.session is not None:
      self.clients['s3'] = self.session.client('s3')
      self.clients['rds'] = self.session.client('rds')
      self.clients['ec2'] = self.session.client('ec2')
      self.clients['cloudwatch'] = self.session.client('cloudwatch')
      self.clients['resourcegroupstaggingapi'] = self.session.client('resourcegroupstaggingapi')
      self.resources['s3'] = self.session.resource('s3')
      self.resources['ec2'] = self.session.resource('ec2')

  def get_client(self, client_type):
    """Return a boto3 client for the given type."""
    if (client_type != 's3'
        and client_type != 'ec2'
        and client_type != 'rds'
        and client_type != 'cloudwatch'
        and client_type != 'resourcegroupstaggingapi'):
      raise ValueError("Client type error, valid client types are 's3', 'ec2, 'rds' and 'cloudwatch.")
    if client_type not in self.clients.keys():
      return ValueError(f"Client type error, {client_type} not found in AWS clients")
    return self.clients[client_type]

  def get_resource(self, resource_type):
    """Return a boto3 resource for the given type."""
    if resource_type != 's3' and resource_type != 'ec2' and resource_type != 'rds':
      raise ValueError("Resource type error, valid resource types are 's3', 'ec2, 'rds'.")
    if resource_type not in self.resources.keys():
      return ValueError(f"Client type error, {resource_type} not found in AWS clients")
    return self.resources[resource_type]


class ConfigurationProvider:
  def __init__(self, s3_client, bucket_name=None, file_key=None):
    self.s3_client = s3_client
    self.bucket_name = bucket_name
    self.file_key = file_key

  def get_bucket_name(self):
    return self.bucket_name

  def set_file_key(self, file_key):
    self.file_key = file_key

  def get_file_key(self):
    return self.file_key

  def set_bucket_name(self, bucket_name):
    self.bucket_name = bucket_name

  def get_s3_client(self):
    return self.s3_client

  def set_s3_client(self, s3_client):
    self.s3_client = s3_client

  def load_raw_configuration(self):
    """Load the configuration from an S3 file"""
    if self.file_key is None or self.bucket_name is None or self.s3_client is None:
      raise ValueError("Provider not initialized")
    if self.file_key == '' or self.bucket_name == '':
      raise ValueError('Bucket name and file key cannot be empty')
    try:
      resp = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_key)
      file_content = resp.get('Body').read().decode('utf-8')
      configuration = json.loads(file_content)
      return configuration
    except json.decoder.JSONDecodeError as e:
      logging.error(f'Invalid JSON configuration: {e}')
    except Exception as e:
      logging.error(f'Error loading configuration from S3: {e}')

  def save_raw_configuration(self, configuration):
    """Save the configuration to an S3 file"""
    try:
      self.s3_client.put_object(
        Bucket=self.bucket_name,
        Key=self.file_key,
        Body=json.dumps(configuration, indent=4).encode('utf-8'))
    except Exception as e:
      logging.error(f'Error saving configuration to S3: {e}')


class RealTimeConfiguration:
  def __init__(self, aws_provider=None):
    self.aws_provider = aws_provider

  @staticmethod
  def get_instance_from_stack_instance(stack_instance):
    idx = stack_instance.find("-")
    if idx == -1:
      raise ValueError("stack_instance expected format is 'xxx-y'")
    version = stack_instance[:idx]
    return version

  @staticmethod
  def get_worker_stats_namespace(stack_instance):
    instance = RealTimeConfiguration.get_instance_from_stack_instance(stack_instance)
    namespace = f"Worker-Statistics-{instance}"
    return namespace

  @staticmethod
  def get_async_workers_namespace(stack_instance):
    instance = RealTimeConfiguration.get_instance_from_stack_instance(stack_instance)
    namespace = f"Asynchronous Workers - {instance}"
    return namespace

  @staticmethod
  def get_async_job_stats_namespace(stack_instance):
    instance = RealTimeConfiguration.get_instance_from_stack_instance(stack_instance)
    namespace = f"Asynchronous-Jobs-{instance}"
    return namespace

  @staticmethod
  def get_memory_namespace(stack_instance, instance_type):
    """
    instance_type ::= [R, W]
    """
    namespace_prefix = ""
    if instance_type == "R":
      namespace_prefix = "Repository"
    elif instance_type == "W":
      namespace_prefix = "Workers"
    else:
      raise ValueError("instance_type can be 'R' or 'W'")
    instance = RealTimeConfiguration.get_instance_from_stack_instance(stack_instance)
    namespace = f"{namespace_prefix}-Memory-{instance}"
    return namespace

  def get_cloudwatch_memory_instances(self, stack_instance, instance_type):
    cw_client = self.aws_provider.get_client('cloudwatch')
    namespace = RealTimeConfiguration.get_memory_namespace(stack_instance, instance_type)
    res = cw_client.list_metrics(Namespace=namespace, MetricName='used')
    instances = [metric["Dimensions"][0]["Value"] for metric in res["Metrics"]]
    return instances

  def get_cloudwatch_worker_stats_instances(self, stack_instance, metric_name):
    cw_client = self.aws_provider.get_client('cloudwatch')
    namespace = self.get_worker_stats_namespace(stack_instance)
    res = cw_client.list_metrics(Namespace=namespace, MetricName=metric_name)
    instances = [metric["Dimensions"][0]["Value"] for metric in res["Metrics"]]
    return instances


  def get_cloudwatch_worker_stats_completed_job_count_instances(self, stack_instance):
    return self.get_cloudwatch_worker_stats_instances(stack_instance, "Completed Job Count")

  def get_cloudwatch_worker_stats_time_running_instances(self, stack_instance):
    return self.get_cloudwatch_worker_stats_instances(stack_instance, "% Time Running")

  def get_cloudwatch_worker_stats_cumulative_time_instances(self, stack_instance):
    return self.get_cloudwatch_worker_stats_instances(stack_instance, "Cumulative runtime")

  def get_ec2_instance_ids(self, environment, stack, stack_instance):
    name_tag_value = f"{environment}-{stack}-{stack_instance}"
    instances = self.get_ec2_instances_by_name(name_tag_value)
    return [inst.id for inst in instances]

  def get_ec2_instances_by_name(self, name):
    ec2 = self.aws_provider.get_resource('ec2')
    filters = [{"Name": "tag:Name", "Values": [name]}]
    instances = ec2.instances.filter(Filters=filters)
    return instances

  def get_rds_instance_ids(self, stack, release_num):
    rds_client = self.aws_provider.get_client('rds')
    db_name = f'{stack}{release_num}'
    res = rds_client.describe_db_instances()
    db_instances = res['DBInstances']
    instance_ids = [inst['DBInstanceIdentifier'] for inst in db_instances if inst['DBName'] == db_name]
    return instance_ids

  def get_rds_idgen_id(self, stack):
    rds_client = self.aws_provider.get_client('rds')
    db_name = f"{stack}idgen"
    res = rds_client.describe_db_instances()
    db_instances = res['DBInstances']
    instance_ids = [inst['DBInstanceIdentifier'] for inst in db_instances if inst['DBName'] == db_name]
    return instance_ids[0]

  def get_repo_alb_name(self, stack, stack_instance):
    env_name = f'repo-{stack}-{stack_instance}'
    rgtapi_client = aws_provider.get_client('resourcegroupstaggingapi')
    tag_filters = [{'Key':'elasticbeanstalk:environment-name', 'Values': [env_name]}]
    resp = rgtapi_client.get_resources(
        TagFilters=tag_filters,
        ResourceTypeFilters = ['elasticloadbalancing:loadbalancer'],
        IncludeComplianceDetails=False,
        ExcludeCompliantResources=False
    )
    alb_name = ''
    if resp["ResourceTagMappingList"]:
      arn = resp["ResourceTagMappingList"][0]["ResourceARN"]
      p = re.compile('arn:aws:elasticloadbalancing:us-east-1:\d+:loadbalancer/(.+)')
      m = p.match(arn)
      alb_name = m.groups()[0]
    return alb_name


class AppConfiguration:
  def __init__(self, configuration_provider, realtime_configuration, stack, version, instances):
    self.configuration_provider = configuration_provider
    self.realtime_configuration = realtime_configuration
    self.stack = stack
    self.version = version
    self.instances = instances  # instances for each environment (repo, workers, portal)
    self.configuration = {}
    if self.configuration_provider is not None:
      self.configuration = configuration_provider.load_raw_configuration()

  def update_ec2_instances(self, env_type):
    current_ec2_instances = self.realtime_configuration.get_ec2_instance_ids(env_type, self.stack, self.instances[env_type])
    self.update_configuration_entry(f'{self.version}-{env_type}-ec2-instances', current_ec2_instances)

  def update_configuration(self):
    # Update EC2 instance ids
    self.update_ec2_instances("repo")
    self.update_ec2_instances("workers")
    self.update_ec2_instances("portal")

    # # Update VMids for memory
    vm_ids = realtime_config.get_cloudwatch_memory_instances(stack_instance=self.instances['repo'], instance_type='R')
    app_config.update_configuration_entry(key=f'{self.version}-repo-vmids', values=vm_ids)
    vm_ids = realtime_config.get_cloudwatch_memory_instances(stack_instance=self.instances['workers'], instance_type='W')
    app_config.update_configuration_entry(key=f'{self.version}-workers-vmids', values=vm_ids)

    # worker stats series are the same for all metric names - only need to call and save once
    worker_names = realtime_config.get_cloudwatch_worker_stats_instances(stack_instance=self.instances['workers'],
                                                                         metric_name='Completed Job Count')
    app_config.update_configuration_entry(key=f'{self.version}-workers-names', values=worker_names)

    # Docker instances are fixed and don't need to be saved here
    # SES instances are fixed and don't need to be saved here
    # SQS query performance format is known and does not need to be saved here
    # FileScanner name format is known and does not need to be saved here

    # repo ALB name
    repo_alb_name = realtime_config.get_repo_alb_name(stack=self.stack, stack_instance=self.instances['repo'])
    app_config.update_configuration_entry(key=f'{self.version}-repo-alb-name', values=[repo_alb_name])

    # Save config
    self.configuration_provider.save_raw_configuration(self.configuration)

  def update_configuration_entry(self, key, values):
    if key not in self.configuration.keys():
      self.configuration[key] = values
    else:
      existing_values = self.configuration[key]
      to_add = []
      for v in values:
        if v not in existing_values:
          to_add.append(v)
      existing_values.extend(to_add)


if __name__ == '__main__':

  if len(sys.argv) != 5:
    raise ValueError('Usage: python configuration.py <stack> <stack_version> <env_instances>')

  stack = sys.argv[1]
  stack_version = sys.argv[2]
  stack_versions_str = sys.argv[3]
  profile_name = sys.argv[4]
  stack_versions = stack_versions_str.split(',')
  env_keys = ['repo', 'workers', 'portal']
  env_instances = dict(zip(env_keys, stack_versions))

  BUCKET_NAME = f'{stack}.cloudwatch.metrics.sagebase.org'
  FILE_KEY = f'{stack}_cw_configuration.json'

  session = boto3.Session(profile_name=profile_name, region_name='us-east-1')
  aws_provider = AwsProvider(session=session)
  s3_client = aws_provider.get_client(client_type='s3')
  configuration_provider = ConfigurationProvider(s3_client=s3_client, bucket_name=BUCKET_NAME, file_key=FILE_KEY)
  config = configuration_provider.load_raw_configuration()

  realtime_config = RealTimeConfiguration(aws_provider=aws_provider)
  app_config = AppConfiguration(configuration_provider=configuration_provider,
                                realtime_configuration=realtime_config,
                                stack=stack, version=stack_version, instances=env_instances)
  app_config.update_configuration()

