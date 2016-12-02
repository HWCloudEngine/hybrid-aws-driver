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
import mock
import testtools

from jacket import context
from jacket.drivers.aws.client import AwsClientPlugin
from jacket.drivers.aws import exception_ex
from jacket.drivers.aws.volume_driver import AwsVolumeDriver
from jacket.drivers.aws.volume_driver import BaseDriver
from jacket.i18n import _
from jacket.storage import exception as cinder_ex
from jacket.tests.storage.unit import fake_snapshot
from jacket.tests.storage.unit import fake_volume


class TestAwsVolumeDriver(testtools.TestCase):
    """Generic class for the Aws volume driver test case."""

    def _get_driver(self, context=None):
        driver = AwsVolumeDriver()

        def _get_aws_client(context):
            kwargs = {}
            kwargs['aws_access_key_id'] = 'fake'
            kwargs['aws_secret_access_key'] = 'fake'
            kwargs['region_name'] = 'ap-northeast-1'
            ec2_client = boto3.client('ec2', **kwargs)
            return AwsClientPlugin(ec2_client)
        setattr(driver._aws_client, 'get_aws_client', _get_aws_client)
        return driver

    def setUp(self):
        """Initialise variable common to all the test cases."""
        super(TestAwsVolumeDriver, self).setUp()
        self.ctx = context.RequestContext('fake', 'fake', is_admin=False)
        self.driver = self._get_driver(self.ctx)
        self.volume = fake_volume.fake_volume_obj(self.ctx)
        self.snapshot = fake_snapshot.fake_snapshot_obj(self.ctx)
        setattr(self.snapshot, 'volume', self.volume)
        self._fake_ebs = {'VolumeId': 'fake'}
        self._fake_snap = {'SnapshotId': 'fake'}

    @mock.patch.object(BaseDriver, '_get_provider_type_name',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(BaseDriver, '_get_provider_az',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags', mock.MagicMock())
    @mock.patch('jacket.db.extend.api.volume_mapper_create', mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'create_volume')
    def test_create_volume(self, mock_create):
        mock_create.return_value = self._fake_ebs
        self.driver.create_volume(self.volume)
        create_args = {'AvailabilityZone': 'fake',
                       'VolumeType': 'fake',
                       'Size': self.volume.size}
        mock_create.assert_called_once_with(**create_args)

    @mock.patch.object(BaseDriver, '_create_volume')
    def test_create_volume_failed(self, mock_create):
        msg = (_("create provider volume failed vol:%s") % self.volume.id)
        mock_create.side_effect = cinder_ex.VolumeBackendAPIException(data=msg)
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.create_volume,
                          self.volume)

    @mock.patch.object(BaseDriver, '_get_provider_type_name')
    @mock.patch.object(AwsClientPlugin, 'create_volume')
    @mock.patch.object(BaseDriver, '_get_provider_az',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags', mock.MagicMock())
    @mock.patch('jacket.db.extend.api.volume_mapper_create', mock.MagicMock())
    def test_create_volume_no_type(self, mock_create, mock_get_type):
        mock_get_type.return_value = None
        mock_create.return_value = self._fake_ebs
        self.driver.create_volume(self.volume)
        create_args = {'AvailabilityZone': 'fake',
                       'VolumeType': 'standard',
                       'Size': self.volume.size}
        mock_create.assert_called_once_with(**create_args)

    @mock.patch.object(BaseDriver, '_get_provider_az')
    @mock.patch.object(BaseDriver, '_get_provider_type_name',
                       mock.MagicMock(return_value='fake'))
    def test_create_volume_az_error(self, mock_get_az):
        msg = (_("create provider volume failed,no provider_az vol:%s") %
               self.volume.id)
        mock_get_az.side_effect = cinder_ex.VolumeBackendAPIException(data=msg)
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.create_volume,
                          self.volume)

    @mock.patch.object(AwsClientPlugin, 'create_tags')
    @mock.patch.object(AwsClientPlugin, 'create_volume')
    @mock.patch.object(BaseDriver, '_get_provider_az',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(BaseDriver, '_get_provider_type_name',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_mapper_create', mock.MagicMock())
    def test_create_volume_tag_error(self, mock_create_vol, mock_tag):
        mock_create_vol.return_value = self._fake_ebs
        msg = (_("create provider volume failed vol:%s") % self.volume.id)
        mock_tag.side_effect = cinder_ex.VolumeBackendAPIException(data=msg)
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.create_volume,
                          self.volume)

    @mock.patch('jacket.db.extend.api.volume_mapper_create')
    @mock.patch.object(AwsClientPlugin, 'create_volume')
    @mock.patch.object(AwsClientPlugin, 'delete_volume',
                       mock.MagicMock())
    @mock.patch.object(BaseDriver, '_get_provider_az',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(BaseDriver, '_get_provider_type_name',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags', mock.MagicMock())
    def test_create_volume_mapper_error(self, mock_create, mock_mapper):
        mock_create.return_value = self._fake_ebs
        mock_mapper.side_effect = cinder_ex.VolumeDriverException(message='')
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.create_volume,
                          self.volume)

    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_mapper_delete', mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    def test_delete_volume(self, mock_delete):
        vol_id = 'fake'
        self.driver.delete_volume(self.volume)
        mock_delete.assert_called_with(VolumeId=vol_id)

    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_mapper_delete', mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    def test_delete_volume_failed(self, mock_delete):
        mock_delete.side_effect = exception_ex.ProviderDeleteVolumeFailed(
            reason='fake'
        )
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.delete_volume,
                          self.volume)

    @mock.patch.object(BaseDriver, '_get_provider_volume_id')
    @mock.patch.object(BaseDriver, '_get_provider_volume')
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    @mock.patch('jacket.db.extend.api.volume_mapper_delete', mock.MagicMock())
    def test_delete_volume_multi_vol_id(self, mock_delete, mock_get_vol,
                                        mock_get_id):
        mock_get_id.return_value = None
        mock_get_vol.return_value = ['fake1', 'fake2']
        self.driver.delete_volume(self.volume)
        calls = [mock.call(VolumeId='fake1'), mock.call(VolumeId='fake2')]
        mock_delete.assert_has_calls(calls)

    @mock.patch('jacket.db.extend.api.volume_mapper_delete')
    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'delete_volume', mock.MagicMock())
    def test_delete_volume_mapper_error(self, mock_mapper):
        mock_mapper.side_effect = cinder_ex.VolumeDriverException(message='')
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.delete_volume,
                          self.volume)

    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags', mock.MagicMock())
    @mock.patch('jacket.db.extend.api.volume_snapshot_mapper_create',
                mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'create_snapshot')
    def test_create_snapshot(self, mock_create):
        mock_create.return_value = self._fake_snap
        self.driver.create_snapshot(self.snapshot)
        create_args = {'VolumeId': 'fake'}
        mock_create.assert_called_once_with(**create_args)

    @mock.patch.object(BaseDriver, '_create_snapshot')
    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    def test_create_snapshot_failed(self, mock_create):
        msg = (_("create provider snapshot failed vol:%s") % self.snapshot.id)
        mock_create.side_effect = cinder_ex.VolumeBackendAPIException(data=msg)
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.create_snapshot,
                          self.snapshot)

    @mock.patch('jacket.db.extend.api.volume_snapshot_mapper_create')
    @mock.patch.object(BaseDriver, '_create_snapshot')
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags', mock.MagicMock())
    def test_create_snapshot_mapper_error(self, mock_delete, mock_create,
                                          mock_mapper):
        mock_create.return_value = self._fake_snap
        mock_mapper.side_effect = cinder_ex.VolumeDriverException(message='')
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.create_snapshot,
                          self.snapshot)
        calls = [mock.call(SnapshotId='fake')]
        mock_delete.assert_has_calls(calls)

    @mock.patch.object(BaseDriver, '_get_provider_snapshot_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_snapshot_mapper_delete',
                mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    def test_delete_snapshot(self, mock_delete):
        snapshot_id = 'fake'
        self.driver.delete_snapshot(self.snapshot)
        mock_delete.assert_called_with(SnapshotId=snapshot_id)

    @mock.patch.object(BaseDriver, '_get_provider_snapshot_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_snapshot_mapper_delete',
                mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    def test_delete_snapshot_failed(self, mock_delete):
        mock_delete.side_effect = exception_ex.ProviderDeleteSnapshotFailed(
            reason='fake'
        )
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.delete_snapshot,
                          self.snapshot)

    @mock.patch.object(BaseDriver, '_get_provider_snapshot_id')
    @mock.patch.object(BaseDriver, '_get_provider_snapshot')
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    @mock.patch('jacket.db.extend.api.volume_snapshot_mapper_delete',
                mock.MagicMock())
    def test_delete_snapshot_multi_id(self, mock_delete, mock_get_snapshot,
                                      mock_get_id):
        mock_get_id.return_value = None
        mock_get_snapshot.return_value = ['fake1', 'fake2']
        self.driver.delete_snapshot(self.snapshot)
        calls = [mock.call(SnapshotId='fake1'), mock.call(SnapshotId='fake2')]
        mock_delete.assert_has_calls(calls)

    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_mapper_create',
                mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    @mock.patch.object(AwsClientPlugin, 'create_snapshot')
    @mock.patch.object(BaseDriver, '_create_volume')
    def test_create_cloned_volume(self, mock_create_vol, mock_create_snap,
                                  mock_delete):
        mock_create_vol.return_value = self._fake_ebs
        mock_create_snap.return_value = self._fake_snap
        self.driver.create_cloned_volume(self.volume, 'fake')
        mock_delete.assert_called_once_with(
            SnapshotId=self._fake_snap['SnapshotId']
        )

    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_mapper_create')
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    @mock.patch.object(AwsClientPlugin, 'create_snapshot')
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    @mock.patch.object(BaseDriver, '_create_volume')
    def test_create_cloned_volume_failed(self, mock_create_vol,
                                         mock_delete_vol,
                                         mock_create_snap,
                                         mock_delete_snap,
                                         mock_mapper):
        mock_create_vol.return_value = self._fake_ebs
        mock_create_snap.return_value = self._fake_snap
        mock_mapper.side_effect = cinder_ex.VolumeBackendAPIException(data='')
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.create_cloned_volume,
                          self.volume,
                          'fake')
        mock_delete_snap.assert_called_once_with(
            SnapshotId=self._fake_snap['SnapshotId']
        )
        mock_delete_vol.assert_called_once_with(
            VolumeId=self._fake_ebs['VolumeId']
        )

    @mock.patch.object(BaseDriver, '_get_provider_snapshot_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_mapper_create',
                mock.MagicMock())
    @mock.patch.object(BaseDriver, '_create_volume')
    def test_create_volume_from_snapshot(self, mock_create_vol):
        mock_create_vol.return_value = self._fake_ebs
        self.driver.create_volume_from_snapshot(self.volume, self.snapshot)

    @mock.patch.object(BaseDriver, '_get_provider_snapshot_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_mapper_create')
    @mock.patch.object(BaseDriver, '_create_volume')
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    def test_create_volume_from_snapshot_failed(self, mock_delete,
                                                mock_create_vol, mock_mapper):
        mock_create_vol.return_value = self._fake_ebs
        mock_mapper.side_effect = cinder_ex.VolumeDriverException(message='')
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.create_volume_from_snapshot,
                          self.volume,
                          self.snapshot)
        mock_delete.assert_called_once_with(
            VolumeId=self._fake_ebs['VolumeId'])

    @mock.patch.object(BaseDriver, '_get_provider_type_name',
                       mock.MagicMock(return_value='standard'))
    @mock.patch.object(BaseDriver, '_get_provider_az',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags', mock.MagicMock())
    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake_old'))
    @mock.patch('jacket.db.extend.api.volume_mapper_update', mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    @mock.patch.object(AwsClientPlugin, 'create_snapshot')
    @mock.patch.object(AwsClientPlugin, 'create_volume')
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    def test_extend_volume(self, mock_delete_vol, mock_create_vol,
                           mock_create_snap, mock_delete_snap):
        mock_create_vol.return_value = self._fake_ebs
        mock_create_snap.return_value = self._fake_snap
        self.driver.extend_volume(self.volume, 10)
        volume_args = {'AvailabilityZone': 'fake',
                       'VolumeType': 'standard',
                       'Size': 10,
                       'SnapshotId': 'fake'}
        mock_create_vol.assert_called_once_with(**volume_args)
        mock_delete_snap.assert_called_once_with(
            SnapshotId=self._fake_snap['SnapshotId']
        )
        mock_delete_vol.assert_called_once_with(VolumeId='fake_old')

    @mock.patch.object(BaseDriver, '_get_provider_type_name',
                       mock.MagicMock(return_value='standard'))
    @mock.patch.object(BaseDriver, '_get_provider_az',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags', mock.MagicMock())
    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake_old'))
    @mock.patch('jacket.db.extend.api.volume_mapper_update')
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    @mock.patch.object(AwsClientPlugin, 'create_snapshot')
    @mock.patch.object(AwsClientPlugin, 'create_volume')
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    def test_extend_volume_failed(self, mock_delete_vol, mock_create_vol,
                                  mock_create_snap, mock_delete_snap, mapper):
        mock_create_vol.return_value = self._fake_ebs
        mock_create_snap.return_value = self._fake_snap
        mapper.side_effect = cinder_ex.VolumeDriverException(message='')
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.extend_volume,
                          self.volume,
                          10)
        volume_args = {'AvailabilityZone': 'fake',
                       'VolumeType': 'standard',
                       'Size': 10,
                       'SnapshotId': 'fake'}
        mock_create_vol.assert_called_once_with(**volume_args)
        mock_delete_snap.assert_called_once_with(
            SnapshotId=self._fake_snap['SnapshotId']
        )
        mock_delete_vol.assert_called_once_with(VolumeId='fake')

    @mock.patch.object(BaseDriver, '_get_provider_type_name',
                       mock.MagicMock(return_value='new_type'))
    @mock.patch.object(BaseDriver, '_get_provider_az',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags', mock.MagicMock())
    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake_old'))
    @mock.patch('jacket.db.extend.api.volume_mapper_update', mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    @mock.patch.object(AwsClientPlugin, 'create_snapshot')
    @mock.patch.object(AwsClientPlugin, 'create_volume')
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    def test_retype(self, mock_delete_vol, mock_create_vol,
                    mock_create_snap, mock_delete_snap):
        mock_create_vol.return_value = self._fake_ebs
        mock_create_snap.return_value = self._fake_snap
        self.driver.retype(self.ctx, self.volume, 'new_type', 'diff', 'local')
        volume_args = {'AvailabilityZone': 'fake',
                       'VolumeType': 'new_type',
                       'Size': self.volume.size,
                       'SnapshotId': 'fake'}
        mock_create_vol.assert_called_once_with(**volume_args)
        mock_delete_snap.assert_called_once_with(
            SnapshotId=self._fake_snap['SnapshotId']
        )
        mock_delete_vol.assert_called_once_with(VolumeId='fake_old')

    @mock.patch.object(BaseDriver, '_get_provider_type_name',
                       mock.MagicMock(return_value='new_type'))
    @mock.patch.object(BaseDriver, '_get_provider_az',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags', mock.MagicMock())
    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake_old'))
    @mock.patch('jacket.db.extend.api.volume_mapper_update')
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    @mock.patch.object(AwsClientPlugin, 'create_snapshot')
    @mock.patch.object(AwsClientPlugin, 'create_volume')
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    def test_retype_failed(self, mock_delete_vol, mock_create_vol,
                           mock_create_snap, mock_delete_snap, mapper):
        mock_create_vol.return_value = self._fake_ebs
        mock_create_snap.return_value = self._fake_snap
        mapper.side_effect = cinder_ex.VolumeDriverException(message='')
        self.assertRaises(cinder_ex.VolumeBackendAPIException,
                          self.driver.retype,
                          self.ctx,
                          self.volume,
                          'new_type',
                          'diff',
                          'local')
        volume_args = {'AvailabilityZone': 'fake',
                       'VolumeType': 'new_type',
                       'Size': self.volume.size,
                       'SnapshotId': 'fake'}
        mock_create_vol.assert_called_once_with(**volume_args)
        mock_delete_snap.assert_called_once_with(
            SnapshotId=self._fake_snap['SnapshotId']
        )
        mock_delete_vol.assert_called_once_with(VolumeId='fake')
