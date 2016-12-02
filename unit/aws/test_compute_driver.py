# Copyright (c) 2013 Rackspace Hosting
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

import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import WaiterError
import jacket
from jacket.compute import exception
from jacket.compute.virt import fake
from jacket import context
from jacket.drivers.aws.client import AwsClientPlugin
from jacket.drivers.aws.compute_driver import AwsComputeDriver
from jacket.drivers.aws import exception_ex
from jacket.tests.compute.unit import fake_instance
import mock
from oslo_log import log as logging
import testtools


LOG = logging.getLogger(__name__)


class AwsComputeDriverTestCase(testtools.TestCase):
    """Unit tests for Driver operations."""

    def setUp(self):
        super(AwsComputeDriverTestCase, self).setUp()
        self.driver = self._get_driver()
        self.network_info = 'fake'
        self.context = context.RequestContext('fake', 'fake', is_admin=False)
        self.connection_info = self._make_connection_info()

    def _make_connection_info(self):
        target_iqn = 'iqn.2010-10.org.openstack:volume-00000001'
        return {'driver_volume_type': 'iscsi',
                'data': {'volume_id': '1',
                         'target_iqn': target_iqn,
                         'target_portal': '127.0.0.1:3260,fake',
                         'target_lun': None,
                         'auth_method': 'CHAP',
                         'auth_username': 'username',
                         'auth_password': 'password'}}

    def _make_bdms_info(self, image_id, multi_bdm):
        bdms = []
        bdm = {'device_name': 'vdb',
               'disk_bus': 'fake-bus',
               'device_type': 'fake-type',
               'boot_index': 0,
               'source_type': 'image',
               'image_id': image_id,
               'size': 10}
        bdms.append(bdm)
        connection_info = self._make_connection_info()
        if multi_bdm:
            bdm1 = {'device_name': 'vdc',
                    'disk_bus': 'fake-bus',
                    'device_type': 'fake-type',
                    'boot_index': 1,
                    'source_type': 'volume',
                    'connection_info': connection_info}
            bdms.append(bdm1)
        return bdms

    def _create_instance(self, **kwargs):
        """Create a test instance."""
        return fake_instance.fake_instance_obj(self.context, **kwargs)

    def _get_driver(self):
        driver = AwsComputeDriver(fake.FakeVirtAPI())

        def _get_aws_client(context):
            kwargs = {}
            kwargs['aws_access_key_id'] = 'fake'
            kwargs['aws_secret_access_key'] = 'fake'
            kwargs['region_name'] = 'ap-northeast-1'
            ec2_client = boto3.client('ec2', **kwargs)
            return AwsClientPlugin(ec2_client)
        setattr(driver.aws_client, 'get_aws_client', _get_aws_client)
        return driver

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    def test_power_on_not_exist_in_db(self, get_instance_id_mock):
        get_instance_id_mock.return_value = None
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound, self.driver.power_on,
                          self.context, instance, self.network_info)
        get_instance_id_mock.assert_called_once_with(self.context,
                                                     instance.uuid)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'start_instances')
    def test_power_on_not_exist_on_aws(self, start_mock):
        error_response = {'Error': {'Message': "The instance not exist",
                                    'Code': 'InvalidInstanceID.NotFound'}}
        operation_name = 'StartInstances'
        start_mock.side_effect = ClientError(error_response, operation_name)
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound, self.driver.power_on,
                          self.context, instance, self.network_info)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'start_instances')
    def test_power_on_unkown_error_on_aws(self, start_mock):
        error_response = {'Error': {'Message': "Invalid id",
                                    'Code': 'InvalidInstanceID.Malformed'}}
        operation_name = 'StartInstances'
        start_mock.side_effect = ClientError(error_response, operation_name)
        instance = self._create_instance()
        self.assertRaises(exception.InstancePowerOnFailure,
                          self.driver.power_on,
                          self.context, instance, self.network_info)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'start_instances')
    def test_power_on_time_out(self, start_mock):
        start_mock.side_effect = WaiterError(name='fake',
                                             reason='Max attempts exceeded',
                                             last_response=None)
        instance = self._create_instance()
        self.assertRaises(exception.InstancePowerOnFailure,
                          self.driver.power_on,
                          self.context, instance, self.network_info)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'start_instances')
    def test_power_on_error(self, start_mock):
        start_mock.side_effect = NoCredentialsError
        instance = self._create_instance()
        self.assertRaises(NoCredentialsError, self.driver.power_on,
                          self.context, instance, self.network_info)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'start_instances', mock.MagicMock())
    def test_power_on(self):
        instance = self._create_instance()
        self.driver.power_on(self.context, instance, self.network_info)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    def test_power_off_not_exist_in_db(self, get_instance_id_mock):
        get_instance_id_mock.return_value = None
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound, self.driver.power_off,
                          instance)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'stop_instances')
    def test_power_off_not_exist_on_aws(self, stop_mock):
        error_response = {'Error': {'Message': "The instance not exist",
                                    'Code': 'InvalidInstanceID.NotFound'}}
        operation_name = 'StopInstances'
        stop_mock.side_effect = ClientError(error_response, operation_name)
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound, self.driver.power_off,
                          instance)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'stop_instances')
    def test_power_off_unkown_error_on_aws(self, stop_mock):
        error_response = {'Error': {'Message': "Invalid id",
                                    'Code': 'InvalidInstanceID.Malformed'}}
        operation_name = 'StopInstances'
        stop_mock.side_effect = ClientError(error_response, operation_name)
        instance = self._create_instance()
        self.assertRaises(exception.InstancePowerOffFailure,
                          self.driver.power_off,
                          instance)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'stop_instances')
    def test_power_off_time_out(self, stop_mock):
        stop_mock.side_effect = WaiterError(name='fake',
                                            reason='Max attempts exceeded',
                                            last_response=None)
        instance = self._create_instance()
        self.assertRaises(exception.InstancePowerOffFailure,
                          self.driver.power_off,
                          instance)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'stop_instances')
    def test_power_off_error(self, stop_mock):
        stop_mock.side_effect = NoCredentialsError
        instance = self._create_instance()
        self.assertRaises(NoCredentialsError, self.driver.power_off,
                          instance)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'stop_instances', mock.MagicMock())
    def test_power_off(self):
        instance = self._create_instance()
        self.driver.power_off(instance)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    def test_reboot_not_exist_in_db(self, get_instance_id_mock):
        get_instance_id_mock.return_value = None
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound, self.driver.reboot,
                          self.context, instance, self.network_info, 'fake')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'reboot_instances')
    def test_reboot_not_exist_on_aws(self, reboot_mock):
        error_response = {'Error': {'Message': "The instance not exist",
                                    'Code': 'InvalidInstanceID.NotFound'}}
        operation_name = 'RebootInstances'
        reboot_mock.side_effect = ClientError(error_response, operation_name)
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound, self.driver.reboot,
                          self.context, instance, self.network_info, 'fake')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'reboot_instances')
    def test_reboot_unkown_error_on_aws(self, reboot_mock):
        error_response = {'Error': {'Message': "Invalid id",
                                    'Code': 'InvalidInstanceID.Malformed'}}
        operation_name = 'RebootInstances'
        reboot_mock.side_effect = ClientError(error_response, operation_name)
        instance = self._create_instance()
        self.assertRaises(exception.InstanceRebootFailure,
                          self.driver.reboot,
                          self.context, instance, self.network_info, 'fake')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'reboot_instances')
    def test_reboot_error(self, reboot_mock):
        reboot_mock.side_effect = NoCredentialsError
        instance = self._create_instance()
        self.assertRaises(NoCredentialsError, self.driver.reboot,
                          self.context, instance, self.network_info, 'fake')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'reboot_instances', mock.MagicMock())
    def test_reboot(self):
        instance = self._create_instance()
        self.driver.reboot(self.context, instance, self.network_info,
                           'fake')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    @mock.patch.object(jacket.db.extend.api, 'instance_mapper_delete')
    def test_destory_not_exist_in_db_and_aws(self, mapper_delete_mock,
                                             describe_instances_mock,
                                             get_instance_id_mock):
        get_instance_id_mock.return_value = None
        describe_instances_mock.return_value = []
        instance = self._create_instance()
        self.driver.destroy(self.context, instance, self.network_info)
        get_instance_id_mock.assert_called_once_with(self.context,
                                                     instance.uuid)
        filters = [{'Name': 'tag:caa_instance_id',
                    'Values': [instance.uuid]}]
        describe_instances_mock.assert_called_once_with(Filters=filters)
        mapper_delete_mock.assert_called_once_with(self.context, instance.uuid,
                                                   instance.project_id)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    @mock.patch.object(AwsClientPlugin, 'delete_instances', mock.MagicMock())
    @mock.patch('jacket.db.extend.api.instance_mapper_delete',
                mock.MagicMock())
    def test_destory_not_exist_in_db(self, describe_instances_mock,
                                     get_instance_id_mock):
        get_instance_id_mock.return_value = None
        aws_instances = [{'InstanceId': 'fake'}]
        describe_instances_mock.return_value = aws_instances
        instance = self._create_instance()
        self.driver.destroy(self.context, instance, self.network_info)
        get_instance_id_mock.assert_called_once_with(self.context,
                                                     instance.uuid)
        filters = [{'Name': 'tag:caa_instance_id',
                    'Values': [instance.uuid]}]
        describe_instances_mock.assert_called_once_with(Filters=filters)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    @mock.patch.object(AwsClientPlugin, 'delete_instances', mock.MagicMock())
    @mock.patch('jacket.db.extend.api.instance_mapper_delete',
                mock.MagicMock())
    def test_destory_exist_in_db(self,
                                 get_instance_id_mock):
        get_instance_id_mock.return_value = 'fake'
        instance = self._create_instance()
        self.driver.destroy(self.context, instance, self.network_info)
        get_instance_id_mock.assert_called_once_with(self.context,
                                                     instance.uuid)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    @mock.patch.object(AwsClientPlugin, 'delete_instances')
    @mock.patch('jacket.db.extend.api.instance_mapper_delete',
                mock.MagicMock())
    def test_destory_exist_in_db_not_on_aws(self, delete_mock,
                                            get_instance_id_mock):
        get_instance_id_mock.return_value = 'fake'
        error_response = {'Error': {'Message': "The instance not exist",
                                    'Code': 'InvalidInstanceID.NotFound'}}
        operation_name = 'TerminateInstances'
        delete_mock.side_effect = ClientError(error_response, operation_name)
        instance = self._create_instance()
        self.driver.destroy(self.context, instance, self.network_info)
        get_instance_id_mock.assert_called_once_with(self.context,
                                                     instance.uuid)
        delete_mock.assert_called_once_with(InstanceIds=['fake'])

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    @mock.patch.object(AwsClientPlugin, 'delete_instances')
    def test_destory_exist_in_db_error(self, delete_mock,
                                       get_instance_id_mock):
        get_instance_id_mock.return_value = 'fake'
        error_response = {'Error': {'Message': "Invalid id",
                                    'Code': 'InvalidInstanceID.Malformed'}}
        operation_name = 'TerminateInstances'
        delete_mock.side_effect = ClientError(error_response, operation_name)
        instance = self._create_instance()
        self.assertRaises(exception.InstanceTerminationFailure,
                          self.driver.destroy,
                          self.context, instance, self.network_info)
        get_instance_id_mock.assert_called_once_with(self.context,
                                                     instance.uuid)
        delete_mock.assert_called_once_with(InstanceIds=['fake'])

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'delete_instances')
    def test_destory_time_out(self, delete_mock):
        delete_mock.side_effect = WaiterError(name='fake',
                                              reason='Max attempts exceeded',
                                              last_response=None)
        instance = self._create_instance()
        self.assertRaises(exception.InstanceTerminationFailure,
                          self.driver.destroy,
                          self.context, instance, self.network_info)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'delete_instances')
    def test_destory_error(self, stop_mock):
        stop_mock.side_effect = NoCredentialsError
        instance = self._create_instance()
        self.assertRaises(NoCredentialsError, self.driver.destroy,
                          self.context, instance, self.network_info)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id")
    def test_attach_volume_volume_not_exist(self, get_volume_id_mock):
        get_volume_id_mock.return_value = None
        instance = self._create_instance()
        self.assertRaises(exception.VolumeNotFound, self.driver.attach_volume,
                          self.context, self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    def test_attach_volume_instance_not_exist(self, get_instance_id_mock):
        get_instance_id_mock.return_value = None
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound,
                          self.driver.attach_volume,
                          self.context, self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'attach_volume', mock.MagicMock())
    def test_attach_volume(self):
        instance = self._create_instance()
        self.driver.attach_volume(self.context, self.connection_info, instance,
                                  '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'attach_volume', mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    def test_attach_volume_no_device_name(self, describe_instances_mock):
        bdms = [{'DeviceName': '/dev/sdb'}]
        describe_instances_mock.return_value = [{'BlockDeviceMappings':
                                                 bdms,
                                                 'InstanceId': 'fake'}]
        instance = self._create_instance()
        self.driver.attach_volume(self.context, self.connection_info, instance)

    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'attach_volume')
    def test_attach_volume_error(self, attach_volume_mock):
        error_response = {'Error': {'Message': "Invalid id",
                                    'Code': 'InvalidInstanceID.Malformed'}}
        operation_name = 'AttachVolume'
        attach_volume_mock.side_effect = ClientError(error_response,
                                                     operation_name)
        instance = self._create_instance()
        self.assertRaises(ClientError, self.driver.attach_volume,
                          self.context, self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    def test_detach_instances_not_exist(self, describe_instances_mock,
                                        get_instance_id_mock):
        get_instance_id_mock.return_value = None
        describe_instances_mock.return_value = []
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound,
                          self.driver.detach_volume,
                          self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    def test_detach_instances_mul_exist(self, describe_instances_mock,
                                        get_instance_id_mock):
        get_instance_id_mock.return_value = None
        aws_instances = [{'InstanceId': 'fake'}, {'InstanceId': 'fake1'}]
        describe_instances_mock.return_value = aws_instances
        instance = self._create_instance()
        self.assertRaises(exception_ex.MultiInstanceConfusion,
                          self.driver.detach_volume,
                          self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id")
    @mock.patch.object(AwsClientPlugin, 'describe_volumes')
    def test_detach_volume_not_exist(self, describe_volumes_mock,
                                     get_volume_id_mock):
        get_volume_id_mock.return_value = None
        describe_volumes_mock.return_value = []
        instance = self._create_instance()
        self.driver.detach_volume(self.connection_info, instance,
                                  '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id")
    @mock.patch.object(AwsClientPlugin, 'describe_volumes')
    def test_detach_volume_mul_exist(self, describe_volumes_mock,
                                     get_volume_id_mock):
        get_volume_id_mock.return_value = None
        aws_volumes = [{'VolumeId': 'fake'}, {'VolumeId': 'fake1'}]
        describe_volumes_mock.return_value = aws_volumes
        instance = self._create_instance()
        self.assertRaises(exception_ex.MultiVolumeConfusion,
                          self.driver.detach_volume,
                          self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'detach_volume')
    def test_detach_volume_not_exist_on_aws(self, detach_volume_mock):
        error_response = {'Error': {'Message': "fake",
                                    'Code': 'InvalidVolume.NotFound'}}
        operation_name = 'AttachVolume'
        detach_volume_mock.side_effect = ClientError(error_response,
                                                     operation_name)
        instance = self._create_instance()
        self.driver.detach_volume(self.connection_info, instance,
                                  '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'detach_volume')
    def test_detach_instance_not_exist_on_aws(self, detach_volume_mock):
        error_response = {'Error': {'Message': "fake",
                                    'Code': 'InvalidInstanceID.NotFound'}}
        operation_name = 'AttachVolume'
        detach_volume_mock.side_effect = ClientError(error_response,
                                                     operation_name)
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound,
                          self.driver.detach_volume,
                          self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'detach_volume')
    @mock.patch.object(AwsClientPlugin, 'describe_volumes')
    def test_detach_volume_avail_exist_on_aws(self, describe_volumes_mock,
                                              detach_volume_mock):
        error_response = {'Error': {'Message': "fake",
                                    'Code': 'IncorrectState'}}
        operation_name = 'AttachVolume'
        detach_volume_mock.side_effect = ClientError(error_response,
                                                     operation_name)
        aws_volumes = [{'VolumeId': 'fake', 'State': 'available'}]
        describe_volumes_mock.return_value = aws_volumes
        instance = self._create_instance()
        self.driver.detach_volume(self.connection_info, instance,
                                  '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'detach_volume')
    @mock.patch.object(AwsClientPlugin, 'describe_volumes')
    def test_detach_volume_error_state_exist_on_aws(self,
                                                    describe_volumes_mock,
                                                    detach_volume_mock):
        error_response = {'Error': {'Message': "fake",
                                    'Code': 'IncorrectState'}}
        operation_name = 'AttachVolume'
        detach_volume_mock.side_effect = ClientError(error_response,
                                                     operation_name)
        aws_volumes = [{'VolumeId': 'fake', 'State': 'fack'}]
        describe_volumes_mock.return_value = aws_volumes
        instance = self._create_instance()
        self.assertRaises(ClientError,
                          self.driver.detach_volume,
                          self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'detach_volume')
    def test_detach_volume_error_on_aws(self,
                                        detach_volume_mock):
        error_response = {'Error': {'Message': "fake",
                                    'Code': 'fake'}}
        operation_name = 'AttachVolume'
        detach_volume_mock.side_effect = ClientError(error_response,
                                                     operation_name)
        instance = self._create_instance()
        self.assertRaises(ClientError,
                          self.driver.detach_volume,
                          self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'detach_volume')
    def test_detach_time_out(self, detach_volume_mock):
        error = WaiterError(name='fake',
                            reason='Max attempts exceeded',
                            last_response=None)
        detach_volume_mock.side_effect = error
        instance = self._create_instance()
        self.assertRaises(WaiterError,
                          self.driver.detach_volume,
                          self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_volume_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'detach_volume')
    def test_detach_error(self, detach_volume_mock):
        detach_volume_mock.side_effect = NoCredentialsError
        instance = self._create_instance()
        self.assertRaises(NoCredentialsError,
                          self.driver.detach_volume,
                          self.connection_info, instance,
                          '/dev/vdc')

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id")
    def test_get_info_not_exist_in_db(self, get_instance_id_mock):
        get_instance_id_mock.return_value = None
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound, self.driver.get_info,
                          instance)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    def test_get_info_not_exist_on_aws(self, describe_instances_mock):
        describe_instances_mock.return_value = []
        instance = self._create_instance()
        self.assertRaises(exception.InstanceNotFound, self.driver.get_info,
                          instance)

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    def test_get_info_error(self, describe_instances_mock):
        error_response = {'Error': {'Message': "fake",
                                    'Code': 'fake'}}
        operation_name = 'DescribeInstances'
        describe_instances_mock.side_effect = ClientError(error_response,
                                                          operation_name)
        instance = self._create_instance()
        self.assertRaises(ClientError, self.driver.get_info,
                          instance)
        describe_instances_mock.assert_called_once_with(InstanceIds=['fake'])

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    def test_get_info_unkown_state(self, describe_instances_mock):
        instances = [{'InstanceId': 'fake',
                      'State': {'Code': 32,
                                'Name': 'shutting-down'}
                      }]
        describe_instances_mock.return_value = instances
        instance = self._create_instance()
        self.driver.get_info(instance)
        describe_instances_mock.assert_called_once_with(InstanceIds=['fake'])

    @mock.patch.object(AwsComputeDriver, "_get_provider_instance_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    def test_get_info(self, describe_instances_mock):
        instances = [{'InstanceId': 'fake',
                      'State': {'Code': 80,
                                'Name': 'stopped'}
                      }]
        describe_instances_mock.return_value = instances
        instance = self._create_instance()
        self.driver.get_info(instance)
        describe_instances_mock.assert_called_once_with(InstanceIds=['fake'])

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id")
    def test_spawn_no_flavor(self, get_flavor_id_mock):
        get_flavor_id_mock.return_value = None
        instance = self._create_instance(image_ref='fake')
        self.assertRaises(exception.FlavorNotFound, self.driver.spawn,
                          self.context,
                          instance, None, None, None)

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_base_image_id")
    def test_spawn_from_image_no_image(self, get_image_id_mock):
        get_image_id_mock.return_value = None
        instance = self._create_instance(image_ref='fake')
        self.assertRaises(exception_ex.ProviderCreateInstanceFailed,
                          self.driver.spawn, self.context,
                          instance, None, None, None)
        get_image_id_mock.assert_called_once_with(self.context)

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_base_image_id")
    def test_spawn_from_volume_no_image(self, get_image_id_mock):
        get_image_id_mock.return_value = None
        instance = self._create_instance()
        bdms = self._make_bdms_info(None, True)
        block_device_info = {}
        block_device_info['block_device_mapping'] = bdms
        self.assertRaises(exception_ex.ProviderCreateInstanceFailed,
                          self.driver.spawn, self.context,
                          instance, None, None, None,
                          block_device_info=block_device_info)
        get_image_id_mock.assert_called_once_with(self.context)

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_image_id")
    def test_spawn_from_volume_no_provider_image(self, get_image_id_mock):
        get_image_id_mock.return_value = None
        instance = self._create_instance()
        bdms = self._make_bdms_info('fake', True)
        block_device_info = {}
        block_device_info['block_device_mapping'] = bdms
        self.assertRaises(exception_ex.ProviderCreateInstanceFailed,
                          self.driver.spawn, self.context,
                          instance, None, None, None,
                          block_device_info=block_device_info)
        get_image_id_mock.assert_called_once_with(self.context, 'fake')

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_base_image_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_project_mapper")
    def test_spawn_from_image_no_network(self, get_project_mapper_mock):
        get_project_mapper_mock.return_value = self._make_project_mapper(False)
        instance = self._create_instance(image_ref='fake')
        self.assertRaises(exception_ex.ProviderCreateInstanceFailed,
                          self.driver.spawn, self.context,
                          instance, None, None, None)
        get_project_mapper_mock.assert_called_with(self.context,
                                                   self.context.project_id
                                                   )

    def _make_project_mapper(self, have_net=True):
        project_mapper = {}
        if have_net:
            project_mapper['net_data'] = 'subnet1'
            project_mapper['net_api'] = 'subnet2'
        project_mapper['availability_zone'] = 'ap'
        return project_mapper

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_base_image_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_project_mapper")
    @mock.patch.object(AwsClientPlugin, 'describe_images')
    def test_spawn_describe_image_error_on_aws(self, describe_images_mock,
                                               get_project_mapper_mock):
        error_response = {'Error': {'Message': "fake",
                                    'Code': 'fake'}}
        operation_name = 'DescribeImages'
        describe_images_mock.side_effect = ClientError(error_response,
                                                       operation_name)
        get_project_mapper_mock.return_value = self._make_project_mapper()
        instance = self._create_instance(image_ref='fake')
        self.assertRaises(exception_ex.ProviderCreateInstanceFailed,
                          self.driver.spawn, self.context,
                          instance, None, None, None)

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_base_image_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_project_mapper")
    @mock.patch.object(AwsClientPlugin, 'describe_images')
    @mock.patch.object(AwsClientPlugin, 'create_instance')
    def test_spawn_create_instance_error_on_aws(self, create_instance_mock,
                                                describe_images_mock,
                                                get_project_mapper_mock):
        error_response = {'Error': {'Message': "fake",
                                    'Code': 'fake'}}
        operation_name = 'RunInstances'
        create_instance_mock.side_effect = ClientError(error_response,
                                                       operation_name)
        bdms = [{'DeviceName': '/dev/sda1', 'Ebs': {'VolumeSize': 1}}]
        describe_images_mock.return_value = [{'ImageId': 'fake',
                                              'BlockDeviceMappings': bdms}]
        get_project_mapper_mock.return_value = self._make_project_mapper()
        instance = self._create_instance(image_ref='fake')
        self.assertRaises(ClientError,
                          self.driver.spawn, self.context,
                          instance, None, None, None)

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_base_image_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_project_mapper")
    @mock.patch.object(AwsClientPlugin, 'describe_images')
    @mock.patch.object(AwsClientPlugin, 'create_instance')
    @mock.patch.object(AwsClientPlugin, 'create_tags')
    @mock.patch.object(AwsClientPlugin, 'delete_instances')
    def test_spawn_create_tag_error_on_aws(self, delete_instances_mock,
                                           create_tag_mock,
                                           create_instance_mock,
                                           describe_images_mock,
                                           get_project_mapper_mock):
        error_response = {'Error': {'Message': "fake",
                                    'Code': 'fake'}}
        operation_name = 'CreateTage'
        create_tag_mock.side_effect = ClientError(error_response,
                                                  operation_name)
        create_instance_mock.return_value = ['fake']
        bdms = [{'DeviceName': '/dev/sda1', 'Ebs': {'VolumeSize': 1}}]
        describe_images_mock.return_value = [{'ImageId': 'fake',
                                              'BlockDeviceMappings': bdms}]
        get_project_mapper_mock.return_value = self._make_project_mapper()
        instance = self._create_instance(image_ref='fake')
        self.assertRaises(ClientError,
                          self.driver.spawn, self.context,
                          instance, None, None, None)
        delete_instances_mock.assert_called_once_with(InstanceIds=['fake'])

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_base_image_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_project_mapper")
    @mock.patch.object(AwsClientPlugin, 'describe_images')
    @mock.patch.object(AwsClientPlugin, 'create_instance')
    @mock.patch.object(AwsClientPlugin, 'create_tags',
                       mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'delete_instances')
    def test_spawn_with_bdms_error_on_aws(self, delete_instances_mock,
                                          create_instance_mock,
                                          describe_images_mock,
                                          get_project_mapper_mock):
        connection_info = self._make_connection_info()
        bdm = {'mount_device': None,
               'disk_bus': 'fake-bus',
               'device_type': 'fake-type',
               'boot_index': 1,
               'source_type': 'volume',
               'connection_info': connection_info}
        block_device_info = {}
        block_device_info['block_device_mapping'] = [bdm]
        create_instance_mock.return_value = ['fake']
        bdms = [{'DeviceName': '/dev/sda1', 'Ebs': {'VolumeSize': 1}}]
        describe_images_mock.return_value = [{'ImageId': 'fake',
                                              'BlockDeviceMappings': bdms}]
        get_project_mapper_mock.return_value = self._make_project_mapper()
        instance = self._create_instance(image_ref='fake')
        self.assertRaises(exception_ex.ProviderCreateInstanceFailed,
                          self.driver.spawn, self.context,
                          instance, None, None, None,
                          block_device_info=block_device_info)
        delete_instances_mock.assert_called_once_with(InstanceIds=['fake'])

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_base_image_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags',
                       mock.MagicMock())
    @mock.patch.object(AwsComputeDriver, "_get_project_mapper")
    @mock.patch.object(AwsClientPlugin, 'describe_images')
    @mock.patch.object(AwsClientPlugin, 'create_instance')
    @mock.patch.object(AwsClientPlugin, 'attach_volume')
    @mock.patch.object(AwsClientPlugin, 'delete_instances')
    def test_spawn_with_bdms_attach_error_on_aws(self, delete_instances_mock,
                                                 attach_volume_mock,
                                                 create_instance_mock,
                                                 describe_images_mock,
                                                 get_project_mapper_mock):
        connection_info = self._make_connection_info()
        bdm = {'mount_device': 'dev/vdb',
               'disk_bus': 'fake-bus',
               'device_type': 'fake-type',
               'boot_index': 1,
               'source_type': 'volume',
               'connection_info': connection_info}
        block_device_info = {}
        block_device_info['block_device_mapping'] = [bdm]
        error_response = {'Error': {'Message': "fake",
                                    'Code': 'fake'}}
        operation_name = 'AttachVolume'
        attach_volume_mock.side_effect = ClientError(error_response,
                                                     operation_name)
        create_instance_mock.return_value = ['fake']
        bdms = [{'DeviceName': '/dev/sda1', 'Ebs': {'VolumeSize': 1}}]
        describe_images_mock.return_value = [{'ImageId': 'fake',
                                              'BlockDeviceMappings': bdms}]
        get_project_mapper_mock.return_value = self._make_project_mapper()
        instance = self._create_instance(image_ref='fake')
        self.assertRaises(ClientError,
                          self.driver.spawn, self.context,
                          instance, None, None, None,
                          block_device_info=block_device_info)
        delete_instances_mock.assert_called_once_with(InstanceIds=['fake'])

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_base_image_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_project_mapper")
    @mock.patch.object(AwsClientPlugin, 'describe_images')
    @mock.patch.object(AwsClientPlugin, 'create_instance')
    @mock.patch.object(AwsClientPlugin, 'create_tags',
                       mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    @mock.patch.object(jacket.db.extend.api, 'instance_mapper_create')
    @mock.patch.object(AwsClientPlugin, 'delete_instances')
    def test_spawn_save_error(self, delete_instances_mock,
                              instance_mapper_create_mock,
                              describe_instances_mock,
                              create_instance_mock,
                              describe_images_mock,
                              get_project_mapper_mock):
        instance_mapper_create_mock.side_effect = NoCredentialsError
        netWorkInterface = {'SubnetId': 'subnet2',
                            'PrivateIpAddresses':
                            [{'PrivateIpAddress': '192.168.3.5'}]}
        describe_instances_mock.return_value = [{'NetworkInterfaces':
                                                 [netWorkInterface],
                                                 'InstanceId': 'fake'}]
        create_instance_mock.return_value = ['fake']
        bdms = [{'DeviceName': '/dev/sda1', 'Ebs': {'VolumeSize': 1}}]
        describe_images_mock.return_value = [{'ImageId': 'fake',
                                              'BlockDeviceMappings': bdms}]
        get_project_mapper_mock.return_value = self._make_project_mapper()
        instance = self._create_instance(image_ref='fake', system_metadata={},
                                         expected_attrs=['system_metadata'])

        def _save():
            pass
        setattr(instance, 'save', _save)
        self.assertRaises(exception_ex.ProviderCreateInstanceFailed,
                          self.driver.spawn, self.context,
                          instance, None, None, None)
        delete_instances_mock.assert_called_once_with(InstanceIds=['fake'])

    @mock.patch.object(AwsComputeDriver, "_get_provider_flavor_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsComputeDriver, "_get_provider_base_image_id",
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags',
                       mock.MagicMock())
    @mock.patch.object(jacket.db.extend.api, 'instance_mapper_create',
                       mock.MagicMock())
    @mock.patch.object(AwsComputeDriver, "_get_project_mapper")
    @mock.patch.object(AwsClientPlugin, 'describe_images')
    @mock.patch.object(AwsClientPlugin, 'create_instance')
    @mock.patch.object(AwsClientPlugin, 'describe_instances')
    def test_spawn(self,
                   describe_instances_mock,
                   create_instance_mock,
                   describe_images_mock,
                   get_project_mapper_mock):
        netWorkInterface = {'SubnetId': 'subnet2',
                            'PrivateIpAddresses':
                            [{'PrivateIpAddress': '192.168.3.5'}]}
        describe_instances_mock.return_value = [{'NetworkInterfaces':
                                                 [netWorkInterface],
                                                 'InstanceId': 'fake'}]
        create_instance_mock.return_value = ['fake']
        bdms = [{'DeviceName': '/dev/sda1', 'Ebs': {'VolumeSize': 1}}]
        describe_images_mock.return_value = [{'ImageId': 'fake',
                                              'BlockDeviceMappings': bdms}]
        get_project_mapper_mock.return_value = self._make_project_mapper()
        instance = self._create_instance(image_ref='fake', system_metadata={},
                                         expected_attrs=['system_metadata'])

        def _save():
            pass
        setattr(instance, 'save', _save)
        self.driver.spawn(self.context, instance, None, None, None)
