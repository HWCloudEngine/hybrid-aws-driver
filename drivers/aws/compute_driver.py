# All Rights Reserved.
#
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

""" A connection to aws through boto3.
"""

import botocore
import copy
from jacket.compute.cloud import power_state
from jacket.compute import exception
from jacket.compute.virt import driver
from jacket.compute.virt import hardware
from jacket import conf
from jacket import context as req_context
from jacket.db.extend import api as caa_db_api
from jacket.drivers.aws import client
from jacket.drivers.aws import exception_ex
from jacket.i18n import _LE
from jacket.i18n import _LI
from oslo_log import log as logging
from oslo_serialization import jsonutils
from oslo_utils import excutils
import re
import string
import traceback
import uuid

LOG = logging.getLogger(__name__)
CONF = conf.CONF

AWS_INSTANCE_PENDING = 0
AWS_INSTANCE_RUNNING = 16
AWS_INSTANCE_SHUTTING_DOWN = 32
AWS_INSTANCE_TERMINATED = 48
AWS_INSTANCE_STOPPING = 64
AWS_INSTANCE_STOPPED = 80

AWS_POWER_STATE = {
    AWS_INSTANCE_RUNNING: power_state.RUNNING,
    AWS_INSTANCE_STOPPED: power_state.SHUTDOWN,
    AWS_INSTANCE_TERMINATED: power_state.CRASHED,
}

AWS_INSTANCE_TAG = 'caa_instance_id'
AWS_VOLUME_TAG = 'caa_volume_id'


class AwsComputeDriver(driver.ComputeDriver):
    def __init__(self, virtapi):
        self.caa_db_api = caa_db_api
        self.aws_client = client.AwsClient()
        super(AwsComputeDriver, self).__init__(virtapi)

    def after_detach_volume_fail(self, job_detail_info, **kwargs):
        pass

    def after_detach_volume_success(self, job_detail_info, **kwargs):
        pass

    def list_instance_uuids(self):
        """List VM instances from all nodes."""
        uuids = []
        try:
            context = req_context.RequestContext(is_admin=True,
                                                 project_id='aws_default')
            servers = self.aws_client.get_aws_client(context)\
                                     .describe_instances()
        except botocore.exceptions.ClientError as e:
            reason = e.response.get('Error', {}).get('Message', 'Unkown')
            LOG.warn('List instances failed, the error is: %s' % reason)
            return uuids
        for server in servers:
            server_id = server.get('InstanceId')
            uuids.append(server_id)
        LOG.debug('List_instance_uuids: %s' % uuids)
        return uuids

    def list_instances(self):
        """List VM instances from all nodes.

        :return: list of instance id. e.g.['id_001', 'id_002', ...]
        """
        instances = []
        context = req_context.RequestContext(is_admin=True,
                                             project_id='default')
        try:
            servers = self.aws_client.get_aws_client(context)\
                                     .describe_instances()
        except botocore.exceptions.ClientError as e:
            reason = e.response.get('Error', {}).get('Message', 'Unkown')
            LOG.warn('List instances failed, the error is: %s' % reason)
            return instances
        for server in servers:
            tags = server.get('Tags')
            server_name = None
            for tag in tags:
                if tag.get('key') == 'Name':
                    server_name = tag.get('Value')
                    break
            if server_name:
                instances.append(server_name)
        LOG.debug('List_instance: %s' % instances)
        return instances

    def list_instances_stats(self):
        """List VM instances from all nodes.

        :return: list of instance id. e.g.['id_001', 'id_002', ...]
        """
        pass

    def get_console_output(self, context, instance):
        pass

    def volume_create(self, context, instance, image_id=None, size=None):
        '''Create volume for lxc in image sys or creating hybridcontainer '''

        # 1. volume size check
        root_size = instance.get_flavor().get('root_gb', None)
        vol_size = size or root_size

        if not vol_size:
            _msg = "volume size input is None."
            raise exception_ex.ProviderCreateVolumeFailed(reason=_msg)

        kwargs = {}
        kwargs['Size'] = vol_size
        # 2. available zone from configure file or BD
        # if change from parameter, here should modify.
        try:
            project_mapper = \
                self._get_project_mapper(context, context.project_id)
        except exception_ex.AccountNotConfig:
            LOG.warn(_LE("Get project mapper failed in db."))
            project_mapper = None

        if not project_mapper:
            az = CONF.availability_zone or None
        else:
            az = project_mapper.get('availability_zone', None)
        if not az:
            LOG.error(_LE("Create volume error: availability zone is none."))
            raise exception_ex.AvailabilityZoneNotFountError
        kwargs['AvailabilityZone'] = az

        # volume id for caa
        volume_id = str(uuid.uuid4())
        provider_snapshot_id = None
        # if image id is not None and image id is base vm
        if image_id and image_id == "base":
            # get base vm image id in aws
            try:
                provider_image_id = \
                    self._get_provider_base_image_id(context, image_id)
                # get base vm image snapshot id in aws
                kwargs = {'ImageIds': [provider_image_id]}
                images = self.aws_client.get_aws_client(context)\
                                        .describe_images(**kwargs)
                image = images[0]
                block_device_mappings = image.get('BlockDeviceMappings')
                for bdm in block_device_mappings:
                    device_name = bdm.get('DeviceName')
                    if device_name == '/dev/sda1' or \
                       device_name == '/dev/xvda':
                        provider_snapshot_id = \
                            bdm.get('Ebs', {})['SnapshotId']
                        break
            except Exception as e:
                LOG.error(_LE('Query basevm image %(i)s in aws error: %(e)s'),
                          {'i': image_id, 'e': e})
                raise exception.ImageNotFound(image_id=image_id)
        elif image_id:
            provider_snapshot_id = \
                self._get_provider_image_id(context, image_id)
        else:
            provider_snapshot_id = None

        if provider_snapshot_id:
            kwargs['SnapshotId'] = provider_snapshot_id

        # 3. create volume
        volume = None
        try:
            # 3.1 create volume
            aws_client = self.aws_client.get_aws_client(context)
            volume = aws_client.create_volume(**kwargs)
            # 3.2. create volume tag
            tags = [{'Key': 'caa_volume_id', 'Value': volume_id}]
            aws_client.create_tags(Resources=[volume['VolumeId']],
                                   Tags=tags)
        except Exception as e:
            _msg = "Aws create volume error: %s" % traceback.format_exc(e)
            if volume:
                LOG.error(_msg)
                self.aws_client.get_aws_client(context)\
                               .delete_volume(VolumeId=volume['VolumeId'])
            raise exception_ex.ProviderCreateVolumeFailed(reason=_msg)

        # 4. create volume mapper
        try:
            values = {'provider_volume_id': volume['VolumeId']}
            self.caa_db_api.volume_mapper_create(context, volume_id,
                                                 context.project_id,
                                                 values)
        except Exception as ex:
            _msg = (_LE("volume_mapper_create failed! vol: %(id)s,ex: %(ex)s"),
                    {'id': volume['VolumeId'], 'ex': ex})
            LOG.error(_msg)
            aws_client = self.aws_client.get_aws_client(context)
            aws_client.delete_volume(VolumeId=volume['VolumeId'])
            raise exception_ex.ProviderCreateVolumeFailed(reason=_msg)

        return volume_id

    def volume_delete(self, context, instance, volume_id):
        """Delete specified volume."""
        try:
            aws_volume_id = self._get_provider_volume_id(context,
                                                         volume_id)
            if not aws_volume_id:
                aws_volumes = self._get_provider_volume(context, volume_id)
                if not aws_volumes:
                    LOG.error('the volume %s not found' % volume_id)
                    return
                volume_ids = []
                for aws_volume in aws_volumes:
                    volume_ids.append(aws_volume.get('VolumeId'))
                self.aws_client.get_aws_client(context)\
                               .delete_volume(VolumeIds=volume_ids)
            else:
                LOG.debug('Delete the volume %s on aws',
                          aws_volume_id)
                self.aws_client.get_aws_client(context)\
                               .delete_volume(VolumeIds=[aws_volume_id])
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from delete volume. '
                              'Error=%(e)s'), {'e': e},
                          instance=instance)
        try:
            # delelte volume mapper
            self.caa_db_api.volume_mapper_delete(context, volume_id,
                                                 context.project_id)
        except Exception as ex:
            LOG.error(_LE("volume_mapper_delete failed! ex = %s"), ex)

    def attach_volume(self, context, connection_info, instance,
                      mountpoint=None,
                      disk_bus=None, device_type=None,
                      encryption=None):
        """Attach the disk to the instance at mountpoint using info."""
        LOG.debug('Start to attach volume %s to instance %s'
                  % (instance.uuid, connection_info['data']['volume_id']))
        try:
            caa_volume_id = connection_info['data']['volume_id']
            aws_instance_id = self._get_provider_instance_id(context,
                                                             instance.uuid)
            aws_volume_id = self._get_provider_volume_id(context,
                                                         caa_volume_id)
            if not aws_instance_id:
                raise exception.InstanceNotFound(instance_id=instance.uuid)
            if not aws_volume_id:
                raise exception.VolumeNotFound(volume_id=caa_volume_id)
            if mountpoint:
                device_name = self._trans_device_name(mountpoint)
            else:
                device_name = self._get_device_name(context, aws_instance_id)
            LOG.debug('Attach volume %s to instance %s on aws'
                      % (aws_volume_id, aws_instance_id))
            self.aws_client.get_aws_client(context)\
                           .attach_volume(VolumeId=aws_volume_id,
                                          InstanceId=aws_instance_id,
                                          Device=device_name)
            LOG.debug('Attach volume %s to instance %s success'
                      % (instance.uuid, connection_info['data']['volume_id']))
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from attach volume. '
                              'Error=%(e)s'), {'e': e},
                          instance=instance)

    def _get_device_name(self, context, instance_id):
        try:
            kwargs = {'InstanceIds': [instance_id]}
            instances = self.aws_client.get_aws_client(context)\
                            .describe_instances(**kwargs)
            bdms = instances[0].get('BlockDeviceMappings')
            used_letters = set()
            if bdms:
                for bdm in bdms:
                    device_name = bdm.get('DeviceName')
                    used_letters.add(self._get_device_letter(device_name))
            device_name = 'dev/sd' + self._get_unused_letter(used_letters)
            return device_name
        except Exception as e:
            LOG.error(_LE('Get device name error. '
                          'Error=%(e)s'), {'e': e})
            raise exception_ex.AttachVolumeFailed()

    def _strip_dev(self, device_name):
        """remove leading '/dev/'."""
        _dev = re.compile('^/dev/')
        return _dev.sub('', device_name) if device_name else device_name

    def _strip_prefix(self, device_name):
        """remove both leading /dev/ and xvd or sd or hd."""
        _pref = re.compile('^((x?h|s)d)')
        device_name = self._strip_dev(device_name)
        return _pref.sub('', device_name)

    def _get_device_letter(self, device_name):
        _nums = re.compile('\d+')
        letter = self._strip_prefix(device_name)
        # NOTE(vish): delete numbers in case we have something like
        #             /dev/sda1
        return _nums.sub('', letter)

    def _get_unused_letter(self, used_letters):
        all_letters = set(list(string.ascii_lowercase))
        letters = list(all_letters - used_letters)
        # NOTE(vish): prepend ` so all shorter sequences sort first
        letters.sort(key=lambda x: x.rjust(2, '`'))
        return letters[0]

    def destroy(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True, migrate_data=None):
        """Destroy the specified instance from the Hypervisor."""
        LOG.debug('Start to delete server: %s' % instance.uuid)
        instance_ids = []
        try:
            aws_instance_id = self._get_provider_instance_id(context,
                                                             instance.uuid)
            if not aws_instance_id:
                filters = [{'Name': 'tag:caa_instance_id',
                           'Values': [instance.uuid]}]
                instances = self.aws_client.get_aws_client(context)\
                                           .describe_instances(Filters=filters)
                if not instances:
                    LOG.warn('Instance %s not found on aws' % instance.uuid)
                else:
                    for node in instances:
                        instance_ids.append(node.get('InstanceId'))
            else:
                LOG.debug('delete the instance %s on aws',
                          aws_instance_id)
                instance_ids = [aws_instance_id]
            if instance_ids:
                self.aws_client.get_aws_client(context)\
                               .delete_instances(InstanceIds=instance_ids)
        except botocore.exceptions.ClientError as e:
            reason = e.response.get('Error', {}).get('Message', 'Unkown')
            LOG.error('Delete instance failed, the error is: %s' % reason)
            error_code = e.response.get('Error', {}).get('Code', 'Unkown')
            if error_code == 'InvalidInstanceID.NotFound':
                LOG.warn('The instance %s not found on aws' % instance.uuid)
            else:
                raise exception.InstanceTerminationFailure(reason=reason)
        except botocore.exceptions.WaiterError as e:
            reason = e.message
            LOG.warn('Cannot delete instance,operation time out')
            raise exception.InstanceTerminationFailure(reason=reason)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from delete instance. '
                              'Error=%(e)s'), {'e': e},
                          instance=instance)
        try:
            # delete instance mapper
            self.caa_db_api.instance_mapper_delete(context,
                                                   instance.uuid,
                                                   instance.project_id)
        except Exception as ex:
            LOG.warn(_LE("Instance_mapper_delete failed! ex = %s"), ex)

        LOG.debug('Success to delete instance: %s' % instance.uuid)

    def detach_volume(self, connection_info, instance, mountpoint,
                      encryption=None):
        """Detach the disk attached to the instance."""
        LOG.debug('Start to detach volume %s from instance %s'
                  % (instance.uuid, connection_info['data']['volume_id']))
        project_id = instance.project_id
        context = req_context.RequestContext(is_admin=True,
                                             project_id=project_id)
        try:
            caa_volume_id = connection_info['data']['volume_id']
            aws_instance_id = self._get_provider_instance_id(context,
                                                             instance.uuid)
            aws_volume_id = self._get_provider_volume_id(context,
                                                         caa_volume_id)
            if not aws_instance_id:
                aws_instances = self._get_provider_instance(context,
                                                            instance.uuid)
                if not aws_instances:
                    LOG.error('The instances %s not found' % instance.uuid)
                    raise exception.InstanceNotFound(instance_id=instance.uuid)
                elif len(aws_instances) > 1:
                    raise exception_ex.MultiInstanceConfusion
                else:
                    aws_instance_id = aws_instances[0].get('InstanceId')
            if not aws_volume_id:
                aws_volumes = self._get_provider_volume(context, caa_volume_id)
                if not aws_volumes:
                    LOG.error('The volume %s not found' % caa_volume_id)
                    return
                elif len(aws_volumes) > 1:
                    raise exception_ex.MultiVolumeConfusion
                else:
                    aws_volume_id = aws_volumes[0].get('VolumeId')
            LOG.debug('Detach volume %s from instance %s on aws'
                      % (aws_volume_id, aws_instance_id))
            self.aws_client.get_aws_client(context)\
                           .detach_volume(VolumeId=aws_volume_id,
                                          InstanceId=aws_instance_id)
            LOG.debug('Detach volume %s from instance %s success'
                      % (instance.uuid, connection_info['data']['volume_id']))
        except botocore.exceptions.ClientError as e:
            reason = e.response.get('Error', {}).get('Message', 'Unkown')
            LOG.error('Detach volume failed, the error is: %s' % reason)
            error_code = e.response.get('Error', {}).get('Code', 'Unkown')
            if error_code == 'InvalidVolume.NotFound':
                LOG.warn('The volume %s not found on aws' % caa_volume_id)
            elif error_code == 'InvalidInstanceID.NotFound':
                LOG.error('Detach volume failed, the error is: %s' % reason)
                raise exception.InstanceNotFound(instance_id=instance.uuid)
            elif error_code == 'IncorrectState':
                kwargs = {'VolumeIds': [aws_volume_id]}
                volumes = self.aws_client.get_aws_client(context)\
                                         .describe_volumes(**kwargs)
                volume_state = volumes[0].get('State')
                if volume_state == 'available':
                    LOG.warn('The volume %s is available on aws'
                             % caa_volume_id)
                else:
                    with excutils.save_and_reraise_exception():
                        pass
            else:
                with excutils.save_and_reraise_exception():
                        pass
        except botocore.exceptions.WaiterError as e:
            reason = e.message
            LOG.warn('Cannot detach volume,operation time out')
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from detach volume. '
                              'Error=%(e)s'), {'e': e},
                          instance=instance)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from detach volume '
                              'Error=%(e)s'), {'e': e},
                          instance=instance)

    def get_available_nodes(self, refresh=False):
        pass

    def get_available_resource(self, nodename):
        """Retrieve resource information."""
        return {'vcpus': 999999,
                'memory_mb': 999999,
                'local_gb': 99999999,
                'vcpus_used': 0,
                'memory_mb_used': 99999999,
                'local_gb_used': 99999999,
                'hypervisor_type': 'aws',
                'hypervisor_version': 5005000,
                'hypervisor_hostname': nodename,
                'cpu_info': '{"model": ["Intel(R) Xeon(R) CPU E5-2670 0 @ 2.60GHz"], \
                "vendor": ["Huawei Technologies Co., Ltd."], \
                "topology": {"cores": 16, "threads": 32}}',
                'supported_instances': jsonutils.dumps(
                    [["i686", "ec2", "hvm"], ["x86_64", "ec2", "hvm"]]),
                'numa_topology': None,
                }

    def get_info(self, instance):
        """Retrieve information from aws for a specific instance name."""
        LOG.debug('Get info of server: %s' % instance.uuid)
        context = req_context.RequestContext(is_admin=True,
                                             project_id=instance.project_id)
        state = power_state.NOSTATE
        aws_instance_id = self._get_provider_instance_id(context,
                                                         instance.uuid)
        if not aws_instance_id:
            LOG.error('Cannot get the aws_instance_id of % s'
                      % instance.uuid)
            raise exception.InstanceNotFound(instance_id=instance.uuid)
        try:
            LOG.debug('Get info the instance %s on aws',
                      aws_instance_id)
            kwargs = {'InstanceIds': [aws_instance_id]}
            instances = self.aws_client.get_aws_client(context)\
                            .describe_instances(**kwargs)
            if not instances:
                LOG.error('Instance %s not found on aws' % instance.uuid)
                raise exception.InstanceNotFound(instance_id=instance.uuid)
            instance = instances[0]
            state = AWS_POWER_STATE.get(instance.get('State').get('Code'))
        except botocore.exceptions.ClientError as e:
            reason = e.response.get('Error', {}).get('Message', 'Unkown')
            with excutils.save_and_reraise_exception():
                LOG.error('Get instance failed on aws, the error is: %s'
                          % reason)
        except KeyError:
            state = power_state.NOSTATE
        return hardware.InstanceInfo(
            state=state,
            max_mem_kb=0,
            mem_kb=0,
            num_cpu=1)

    def get_instance_macs(self, instance):
        pass

    def get_volume_connector(self, instance):
        return {'ip': CONF.my_block_storage_ip,
                'initiator': 'fake',
                'host': 'fakehost'}

    def init_host(self, host):
        pass

    def power_off(self, instance, timeout=0, retry_interval=0):
        """Power off the specified instance."""
        LOG.debug('Start to stop server: %s' % instance.uuid)
        try:
            project_id = instance.project_id
            context = req_context.RequestContext(is_admin=True,
                                                 project_id=project_id)
            aws_instance_id = self._get_provider_instance_id(context,
                                                             instance.uuid)
            if aws_instance_id:
                LOG.debug('Power off the instance %s on aws',
                          aws_instance_id)
                instance_ids = [aws_instance_id]
                self.aws_client.get_aws_client(context)\
                               .stop_instances(InstanceIds=instance_ids)
                LOG.debug('Stop server: %s success' % instance.uuid)
            else:
                LOG.error('Cannot get the aws_instance_id of % s'
                          % instance.uuid)
                raise exception.InstanceNotFound(instance_id=instance.uuid)
        except botocore.exceptions.ClientError as e:
            reason = e.response.get('Error', {}).get('Message', 'Unkown')
            LOG.error('Power off instance failed, the error is: %s' % reason)
            error_code = e.response.get('Error', {}).get('Code', 'Unkown')
            if error_code == 'InvalidInstanceID.NotFound':
                raise exception.InstanceNotFound(instance_id=instance.uuid)
            else:
                raise exception.InstancePowerOffFailure(reason=reason)
        except botocore.exceptions.WaiterError as e:
            reason = e.message
            LOG.warn('Cannot power_off instance,operation time out')
            raise exception.InstancePowerOffFailure(reason=reason)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from power off instance. '
                              'Error=%(e)s'), {'e': e},
                          instance=instance)

    def power_on(self, context, instance, network_info,
                 block_device_info=None):
        """Power on the specified instance."""
        LOG.debug('Start to start server: %s' % instance.uuid)
        try:
            aws_instance_id = self._get_provider_instance_id(context,
                                                             instance.uuid)
            if aws_instance_id:
                LOG.debug('Power on the instance %s on aws',
                          aws_instance_id)
                instance_ids = [aws_instance_id]
                self.aws_client.get_aws_client(context)\
                               .start_instances(InstanceIds=instance_ids)
                LOG.debug('Start server: %s success' % instance.uuid)
            else:
                LOG.error('Cannot get the aws_instance_id of % s'
                          % instance.uuid)
                raise exception.InstanceNotFound(instance_id=instance.uuid)
        except botocore.exceptions.ClientError as e:
            reason = e.response.get('Error', {}).get('Message', 'Unkown')
            LOG.error('Power on instance failed, the error is: %s' % reason)
            error_code = e.response.get('Error', {}).get('Code', 'Unkown')
            if error_code == 'InvalidInstanceID.NotFound':
                raise exception.InstanceNotFound(instance_id=instance.uuid)
            else:
                raise exception.InstancePowerOnFailure(reason=reason)
        except botocore.exceptions.WaiterError as e:
            reason = e.message
            LOG.warn('Cannot power_on instance,operation time out')
            raise exception.InstancePowerOnFailure(reason=reason)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from power on instance. '
                              'Error=%(e)s'), {'e': e},
                          instance=instance)

    def reboot(self, context, instance, network_info, reboot_type,
               block_device_info=None, bad_volumes_callback=None):
        """Reboot the specified instance."""
        LOG.debug('Start to reboot server: %s' % instance.uuid)
        try:
            aws_instance_id = self._get_provider_instance_id(context,
                                                             instance.uuid)
            if aws_instance_id:
                LOG.debug('Reboot the instance %s on aws',
                          aws_instance_id)
                instance_ids = [aws_instance_id]
                self.aws_client.get_aws_client(context)\
                               .reboot_instances(InstanceIds=instance_ids)
                LOG.debug('Reboot server: %s success' % instance.uuid)
            else:
                LOG.error('Cannot get the aws_instance_id of % s'
                          % instance.uuid)
                raise exception.InstanceNotFound(instance_id=instance.uuid)
        except botocore.exceptions.ClientError as e:
            reason = e.response.get('Error', {}).get('Message', 'Unkown')
            LOG.error('Power on instance failed, the error is: %s' % reason)
            error_code = e.response.get('Error', {}).get('Code', 'Unkown')
            if error_code == 'InvalidInstanceID.NotFound':
                raise exception.InstanceNotFound(instance_id=instance.uuid)
            else:
                raise exception.InstanceRebootFailure(reason=reason)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from power on instance. '
                              'Error=%(e)s'), {'e': e},
                          instance=instance)

    def provider_create_image(self, context, instance, image, metadata):
        pass

    def snapshot(self, context, instance, image_id, update_task_state):
        pass

    def get_provider_lxc_volume_id(self, context, instance, index):
        pass

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        LOG.debug("Start to create server", instance=instance)
        base_image_id = None
        flavor = instance.get_flavor()
        root_size = flavor.root_gb
        sub_flavor_id = self._get_provider_flavor_id(context,
                                                     flavor.flavorid)
        if not sub_flavor_id:
            raise exception.FlavorNotFound(flavor_id=flavor.flavorid)
        block_device_info = block_device_info or {}
        attached_bdms = copy.deepcopy(block_device_info
                                      .get('block_device_mapping', []))
        if instance.image_ref:
            base_image_id = self._get_provider_base_image_id(context)
        else:
            if block_device_info:
                bdms = block_device_info.get('block_device_mapping', [])
                if bdms:
                    bdms = sorted(bdms, key=lambda bdm: bdm['boot_index'])
                    bdm = bdms[0]
                    root_size = bdm.get('size')
                    base_image_id = self._get_image_id_from_bdm(context, bdm)
                    attached_bdms.remove(bdm)
        if not base_image_id:
            LOG.error(_LE('Create instance failed.The base image not found'),
                      instance=instance)
            msg = 'The base image not found on aws'
            raise exception_ex.ProviderCreateInstanceFailed(reason=msg)
        project_mapper = self._get_project_mapper(context,
                                                  context.project_id)
        nics = self._get_provider_nics(context, project_mapper)
        bdms = self._build_sub_bdm(context, base_image_id, root_size)
        availability_zone = project_mapper.get("availability_zone", None)
        security_groups = self._get_provider_security_groups_list(
            context, project_mapper
        )
        user_data = self._get_user_data(injected_files)
        create_args = self._build_create_args(base_image_id, sub_flavor_id,
                                              availability_zone, nics,
                                              security_groups=security_groups,
                                              user_data=user_data,
                                              block_device_mapping=bdms)
        instance_ids = self._create_instance(context, instance, attached_bdms,
                                             **create_args)
        try:
            kwargs = {'InstanceIds': instance_ids}
            instances = self.aws_client.get_aws_client(context)\
                            .describe_instances(**kwargs)
            nics = instances[0].get('NetworkInterfaces')
            if nics:
                for nic in nics:
                    if nic.get('SubnetId') == project_mapper.get('net_api'):
                        ip = nic.get('PrivateIpAddresses')[0]\
                                .get('PrivateIpAddress')
                        instance.system_metadata['management_ip'] = ip
            instance.system_metadata['instance_id'] = instance_ids[0]
            LOG.debug('Instance metadata info instance_id: %(id)s ,'
                      'ip: %(ip)s',
                      {'id': instance_ids[0], 'ip': ip})
            instance.save()
            # instance mapper
            values = {'provider_instance_id': instance_ids[0]}
            self.caa_db_api.instance_mapper_create(context,
                                                   instance.uuid,
                                                   instance.project_id,
                                                   values)
            LOG.info(_LI("Instance spawned successfully."), instance=instance)
        except Exception as e:
            LOG.error(_LE('save instance info failed! '
                          'Error=%(e)s'), {'e': e, },
                      instance=instance)

            self.aws_client.get_aws_client(context)\
                           .delete_instances(InstanceIds=instance_ids)
            msg = 'Instance_mapper_create failed'
            raise exception_ex.ProviderCreateInstanceFailed(reason=msg)

    def _attach_bdm_to_instance(self, context, instance, instance_id, bdms):
        try:
            pass
        except botocore.exceptions.ClientError as e:
            reason = e.response.get('Error', {}).get('Message', 'Unkown')
            LOG.error('Create instance failed, the error is: %s' % reason)
            self.aws_client.get_aws_client(context)\
                           .delete_instances(InstanceIds=[instance_id])
            raise exception_ex.ProviderCreateInstanceFailed(reason=reason)
        except botocore.exceptions.WaiterError as e:
            reason = e.message
            LOG.warn('Cannot create instance,operation time out')
            self.aws_client.get_aws_client(context)\
                           .delete_instances(InstanceIds=[instance_id])
            raise exception_ex.ProviderCreateInstanceFailed(reason=reason)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from create instance. '
                              'Error=%(e)s'), {'e': e},
                          instance=instance)

    def _check_bdms(self, bdms):
        is_right = True
        for bdm in bdms:
            volume_id = bdm.get('connection_info', {}).get('data', {})\
                           .get('volume_id', None)
            mountpoint = bdm.get('mount_device', None)
            if not volume_id or not mountpoint:
                is_right = False
                break
        return is_right

    def _create_instance(self, context, instance, bdms, **kwargs):
        LOG.debug('Create instance: %s', kwargs)
        instance_ids = []
        try:
            instance_ids = self.aws_client.get_aws_client(context)\
                                          .create_instance(**kwargs)
            if instance_ids:
                tags = [{'Key': 'caa_instance_id', 'Value': instance.uuid}]
                self.aws_client.get_aws_client(context)\
                               .create_tags(Resources=[instance_ids],
                                            Tags=tags)
                if bdms:
                    if not self._check_bdms(bdms):
                        msg = 'Create instance failed,the bdms info error'
                        LOG.error('Create instance on aws failed,'
                                  ' the bdms %s info error' % bdms)
                        raise exception_ex.ProviderCreateInstanceFailed(
                            reason=msg
                        )
                    for bdm in bdms:
                        volume_id = bdm.get('connection_info', {}).get('data', {})\
                                       .get('volume_id')
                        mountpoint = bdm.get('mount_device')
                        self.aws_client.get_aws_client(context).attach_volume(
                            VolumeId=volume_id,
                            InstanceId=instance_ids[0],
                            Device=mountpoint
                        )
                return instance_ids
            else:
                msg = 'Create instance on aws failed'
                raise exception_ex.ProviderCreateInstanceFailed(reason=msg)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from create instance. '
                              'Error=%(e)s'), {'e': e},
                          instance=instance)
                if instance_ids:
                    self.aws_client.get_aws_client(context)\
                                   .delete_instances(InstanceIds=instance_ids)

    def _build_create_args(self, image_id, instance_type, availability_zone,
                           nics, security_groups=None, user_data=None,
                           block_device_mapping=None):
        create_args = {}
        create_args['ImageId'] = image_id
        create_args['InstanceType'] = instance_type
        placement = {'AvailabilityZone': availability_zone}
        create_args['Placement'] = placement
        create_args['NetworkInterfaces'] = nics
        if user_data:
            create_args['UserData'] = user_data
        if block_device_mapping:
            create_args['BlockDeviceMappings'] = block_device_mapping
        if security_groups:
            create_args['SecurityGroupIds'] = security_groups
        return create_args

    def _get_user_data(self, injected_files):
        if injected_files:
            file_dict = dict(injected_files)
            return file_dict.get('/var/lib/wormhole/settings.json', None)

    def _get_image_id_from_bdm(self, context, bdm):
        source_type = bdm.get('source_type', None)
        provider_image_id = None
        if source_type == 'image':
            image_id = bdm.get('image_id', None)
            if image_id:
                provider_image_id = self._get_provider_image_id(context,
                                                                image_id)
            else:
                provider_image_id = self._get_provider_base_image_id(context)
        return provider_image_id

    def pause(self, instance):
        pass

    def unpause(self, instance):
        pass

    def sub_flavor_detail(self, context):
        pass

    def rename(self, ctxt, instance, display_name=None):
        pass

    def get_diagnostics(self, instance):
        pass

    def get_instance_diagnostics(self, instance):
        pass

    def resume_state_on_host_boot(self, context, instance, network_info,
                                  block_device_info=None):
        pass

    def rescue(self, context, instance, network_info, image_meta,
               rescue_password):

        pass

    def unrescue(self, instance, network_info):

        pass

    def trigger_crash_dump(self, instance):
        pass

    def set_admin_password(self, instance, new_pass):
        pass

    def get_host_uptime(self):
        pass

    def attach_interface(self, instance, image_meta, vif):
        pass

    def detach_interface(self, instance, vif):
        pass

    def upload_image(self, context, instance, image_meta):
        '''Upload image to aws in image sys. Here create snapshot '''

        # 1. Get lxc volume
        image_id = image_meta['id']
        lxc_volume_id = \
            instance.system_metadata.get('lxc_volume_id', None)
        if not lxc_volume_id:
            raise exception.LxcVolumeNotFound(instance_uuid=instance.uuid)

        # 2. Create volume snapshot
        snapshot = None
        kargs = {}

        try:
            kargs['VolumeId'] = lxc_volume_id
            # 2.1 create snapshot
            aws_client = self.aws_client.get_aws_client(context)
            snapshot = aws_client.create_snapshot(**kargs)

            # 2.2 create snapshot tag
            tags = [{'Key': 'caa_snapshot_id', 'Value': image_id}]
            aws_client.create_tags(Resources=[snapshot['SnapshotId']],
                                   Tags=tags)
        except Exception as e:
            _msg = "Upload image to aws error: %s" % traceback.format_exc(e)
            LOG.error(_msg)
            if snapshot:
                args = {}
                args['SnapshotId'] = snapshot['SnapshotId']
                self.aws_client.get_aws_client(context)\
                               .delete_snapshot(**args)
            raise exception_ex.ProviderCreateSnapshotFailed(reason=_msg)

        # 3. Create baseVM image relation with this snapshot
        if snapshot:
            try:
                values = {'provider_image_id': snapshot.get('SnapshotId')}
                self.caa_db_api.image_mapper_create(context, image_id,
                                                    context.project_id,
                                                    values)
            except Exception as e:
                LOG.error(_LE("Create image mapper error: %s"),
                          traceback.format_exc(e))
                with excutils.save_and_reraise_exception():
                    args = {}
                    args['SnapshotId'] = snapshot['SnapshotId']
                    aws_client = self.aws_client.get_aws_client(context)
                    aws_client.delete_snapshot(**args)
        else:
            _msg = _("Upload image to aws error: snapshot is None.")
            raise exception_ex.ProviderCreateSnapshotFailed(reason=_msg)

    def _get_provider_instance_id(self, context, caa_instance_id):
        instance_mapper = self.caa_db_api.instance_mapper_get(context,
                                                              caa_instance_id)
        return instance_mapper.get('provider_instance_id', None)

    def _get_provider_volume_id(self, context, caa_volume_id):
        volume_mapper = self.caa_db_api.volume_mapper_get(context,
                                                          caa_volume_id)
        provider_volume_id = volume_mapper.get('provider_volume_id', None)
        if provider_volume_id:
            return provider_volume_id

    def _get_provider_instance(self, context, instance_id):
        filters = [{'Name': 'tag:caa_instance_id',
                    'Values': [instance_id]}]
        instances = self.aws_client.get_aws_client(context)\
                                   .describe_instances(Filters=filters)
        return instances

    def _get_provider_volume(self, context, volume_id):
        filters = [{'Name': 'tag:caa_volume_id',
                    'Values': [volume_id]}]
        volumes = self.aws_client.get_aws_client(context)\
                                 .describe_volumes(Filters=filters)
        return volumes

    def _trans_device_name(self, orig_device_name):
        return '/dev/sd' + orig_device_name[-1]

    def _get_provider_flavor_id(self, context, flavor_id):
        # get dest flavor id
        flavor_mapper = self.caa_db_api.flavor_mapper_get(context,
                                                          flavor_id,
                                                          context.project_id)
        dest_flavor_id = flavor_mapper.get("dest_flavor_id", None)
        return dest_flavor_id

    def _get_provider_base_image_id(self, context, image_id=None):
        project_mapper = self._get_project_mapper(context, context.project_id)
        return project_mapper.get("base_linux_image", None)

    def _get_provider_image_id(self, context, image_id):
        image_mapper = self.caa_db_api.image_mapper_get(context, image_id)
        sub_image_id = image_mapper.get("provider_image_id")
        return sub_image_id

    def _generate_provider_instance_name(self, instance_name, instance_id):
        if not instance_name:
            instance_name = 'server'
        return '@'.join([instance_name, instance_id])

    def _get_project_mapper(self, context, project_id=None):
        if project_id is None:
            project_id = 'aws_default'

        project_mapper = self.caa_db_api.project_mapper_get(context,
                                                            project_id)
        if not project_mapper:
            raise exception_ex.AccountNotConfig()
        return project_mapper

    def _get_provider_nics(self, context, instance, project_mapper=None):
        if project_mapper is None:
            project_mapper = self._get_project_mapper(context,
                                                      context.project_id)
        provider_net_data = project_mapper.get('net_data', None)
        provider_net_api = project_mapper.get('net_api', None)
        if not provider_net_data or not provider_net_api:
            LOG.error(_LE('Create instance failed.The net not found'),
                      instance=instance)
            msg = 'The net of instance found on aws'
            raise exception_ex.ProviderCreateInstanceFailed(reason=msg)
        nics = []
        nics.append({'DeviceIndex': 0, 'SubnetId': provider_net_data,
                     'DeleteOnTermination': True})
        nics.append({'DeviceIndex': 1, 'SubnetId': provider_net_api,
                     'DeleteOnTermination': True})
        return nics

    def _build_sub_bdm(self, context, image_id, root_size):
        sub_bdms = []
        try:
            kwargs = {'ImageIds': [image_id]}
            images = self.aws_client.get_aws_client(context)\
                                    .describe_images(**kwargs)
            image = images[0]
            block_device_mappings = image.get('BlockDeviceMappings')
            for bdm in block_device_mappings:
                device_name = bdm.get('DeviceName')
                if device_name == '/dev/sda1' or device_name == '/dev/xvda':
                    bdm.get('Ebs', {})['VolumeSize'] = root_size
                    break
            sub_bdms.extend(block_device_mappings)
            return sub_bdms
        except botocore.exceptions.ClientError as e:
            reason = e.response.get('Error', {}).get('Message', 'Unkown')
            LOG.error(_LE('Error from create instance. '
                          'Error=%(error)s'), {'error': reason})
            raise exception_ex.ProviderCreateInstanceFailed(reason=reason)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Error from create instance. '
                              'Error=%(e)s'), {'e': e})

    def _get_provider_security_groups_list(self, context, project_mapper=None):
        if project_mapper is None:
            project_mapper = self._get_project_mapper(context,
                                                      context.project_id)
        provider_sg = project_mapper.get('security_groups', None)
        if provider_sg:
            security_groups = provider_sg.split(',')
        else:
            security_groups = None
        return security_groups
