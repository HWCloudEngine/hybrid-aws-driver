#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import boto3

from botocore import exceptions
from jacket.db.extend import api as db_api
from jacket.drivers.aws import exception_ex
from jacket.i18n import _LE
from oslo_log import log as logging
from oslo_utils import excutils

LOG = logging.getLogger(__name__)


class AwsClient(object):

    def __init__(self, *args, **kwargs):
        self._boto3client = None
        super(AwsClient, self).__init__(*args, **kwargs)

    def create_ec2_client(self, context=None):
        project_info = db_api.project_mapper_get(context, context.project_id)
        if not project_info:
            project_info = db_api.project_mapper_get(context,
                                                     "aws_default")
        if not project_info:
            raise exception_ex.AccountNotConfig()

        username = project_info.pop('aws_access_key_id', None)
        password = project_info.pop("aws_secret_access_key", None)
        region_name = project_info.pop("region", None)
        kwargs = {}
        kwargs['aws_access_key_id'] = username
        kwargs['aws_secret_access_key'] = password
        kwargs['region_name'] = region_name
        return boto3.client('ec2', **kwargs)

    def create_resource_client(self, context=None):
        project_info = db_api.project_mapper_get(context, context.project_id)
        if not project_info:
            project_info = db_api.project_mapper_get(context,
                                                     "aws_default")
        if not project_info:
            raise exception_ex.AccountNotConfig()

        username = project_info.pop('aws_access_key_id', None)
        password = project_info.pop("aws_secret_access_key", None)
        region_name = project_info.pop("region", None)
        kwargs = {}
        kwargs['aws_access_key_id'] = username
        kwargs['aws_secret_access_key'] = password
        kwargs['region_name'] = region_name
        return boto3.resource('ec2', **kwargs)

    def get_aws_client(self, context):
        if self._boto3client is None:
            try:
                ec2_client = self.create_ec2_client(context)
                resource_client = self.create_resource_client(context)
                self._boto3client = AwsClientPlugin(ec2_client,
                                                    resource_client)
            except Exception:
                LOG.error(_LE('Create aws client failed.'))
                raise exception_ex.OsAwsConnectFailed

        return self._boto3client


class AwsClientPlugin(object):

    def __init__(self, ec2_client=None, res_client=None, **kwargs):
        self._ec2_client = ec2_client
        self._ec2_resource = res_client

    def create_tags(self, **kwargs):
        self._ec2_client.create_tags(**kwargs)

    def create_volume(self, **kwargs):
        vol = None
        try:
            vol = self._ec2_client.create_volume(**kwargs)
            waiter = self._ec2_client.get_waiter('volume_available')
            waiter.wait(VolumeIds=[vol['VolumeId']])
        except Exception as e:
            if vol:
                self.delete_volume(VolumeId=vol['VolumeId'])
            if isinstance(e, exceptions.ClientError):
                reason = e.response.get('Error', {}).get('Message', 'Unkown')
                LOG.error(_LE("Aws create snapshot failed! error_msg: %s"),
                          reason)
                raise exception_ex.ProviderCreateVolumeFailed(reason=reason)
            else:
                raise
        else:
            return vol

    def delete_volume(self, **kwargs):
        try:
            self._ec2_client.delete_volume(**kwargs)
            waiter = self._ec2_client.get_waiter('volume_deleted')
            waiter.wait(VolumeIds=[kwargs['VolumeId']])
        except Exception as e:
            if isinstance(e, exceptions.ClientError):
                reason = e.response.get('Error', {}).get('Message', 'Unkown')
                LOG.error(_LE("Aws create snapshot failed! error_msg: %s"),
                          reason)
                raise exception_ex.ProviderDeleteVolumeFailed(reason=reason)
            else:
                raise

    def create_snapshot(self, **kwargs):
        snapshot = None
        try:
            snapshot = self._ec2_client.create_snapshot(**kwargs)
            waiter = self._ec2_client.get_waiter('snapshot_completed')
            waiter.wait(VolumeIds=[snapshot['SnapshotId']])
        except Exception as e:
            if snapshot:
                self.delete_snapshot(SnapshotId=snapshot['SnapshotId'])
            if isinstance(e, exceptions.ClientError):
                reason = e.response.get('Error', {}).get('Message', 'Unkown')
                LOG.error(_LE("Aws create snapshot failed! error_msg: %s"),
                          reason)
                raise exception_ex.ProviderCreateSnapshotFailed(reason=reason)
            else:
                raise
        else:
            return snapshot

    def describe_volumes(self, **kwargs):
        response = self._ec2_client.describe_volumes(**kwargs)
        volumes = response.get('Volumes', [])
        return volumes

    def describe_snapshots(self, **kwargs):
        response = self._ec2_client.describe_snapshots(**kwargs)
        snapshots = response.get('Snapshots', [])
        return snapshots

    def delete_snapshot(self, **kwargs):
        try:
            self._ec2_client.delete_snapshot(**kwargs)
        except Exception as e:
            if isinstance(e, exceptions.ClientError):
                reason = e.response.get('Error', {}).get('Message', 'Unkown')
                LOG.error(_LE("Aws create snapshot failed! error_msg: %s"),
                          reason)
                raise exception_ex.ProviderDeleteSnapshotFailed(reason=reason)
            else:
                raise

    def create_instance(self, **kwargs):
        instance_ids = []
        try:
            response = self._ec2_client.run_instances(**kwargs)
            instances = response.get('Instances', [])
            for instance in instances:
                instance_ids.append(instance.get('InstanceId'))
            waiter = self._ec2_client.get_waiter('instance_running')
            waiter.wait(InstanceIds=instance_ids)
            return instance_ids
        except Exception:
            with excutils.save_and_reraise_exception():
                if instance_ids:
                    self.delete_instances(InstanceIds=instance_ids)
        return instance_ids

    def start_instances(self, **kwargs):
        self._ec2_client.start_instances(**kwargs)
        instance_ids = kwargs.get('InstanceIds', [])
        if instance_ids:
            waiter = self._ec2_client.get_waiter('instance_running')
            waiter.wait(InstanceIds=instance_ids)

    def stop_instances(self, **kwargs):
        self._ec2_client.stop_instances(**kwargs)
        instance_ids = kwargs.get('InstanceIds', [])
        if instance_ids:
            waiter = self._ec2_client.get_waiter('instance_stopped')
            waiter.wait(InstanceIds=instance_ids)

    def delete_instances(self, **kwargs):
        self._ec2_client.terminate_instances(**kwargs)
        instance_ids = kwargs.get('InstanceIds', [])
        if instance_ids:
            waiter = self._ec2_client.get_waiter('instance_terminated')
            waiter.wait(InstanceIds=instance_ids)

    def describe_instances(self, **kwargs):
        instances = []
        response = self._ec2_client.describe_instances(**kwargs)
        reservations = response.get('Reservations', [])
        for reservation in reservations:
            instances.extend(reservation.get('Instances'))
        return instances

    def reboot_instances(self, **kwargs):
        self._ec2_client.reboot_instances(**kwargs)

    def detach_volume(self, **kwargs):
        self._ec2_client.detach_volume(**kwargs)
        volume_id = kwargs.get('VolumeId')
        if volume_id:
            volume_ids = [volume_id]
            waiter = self._ec2_client.get_waiter('volume_available')
            waiter.wait(VolumeIds=volume_ids)

    def attach_volume(self, **kwargs):
        self._ec2_client.attach_volume(**kwargs)
        volume_id = kwargs.get('VolumeId')
        if volume_id:
            volume_ids = [volume_id]
            waiter = self._ec2_client.get_waiter('volume_in_use')
            waiter.wait(VolumeIds=volume_ids)

    def describe_images(self, **kwargs):
        response = self._ec2_client.describe_images(**kwargs)
        volumes = response.get('Images', [])
        return volumes
