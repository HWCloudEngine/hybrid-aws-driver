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
from jacket.drivers.aws.volume_driver import AwsBackupDriver
from jacket.drivers.aws.volume_driver import BaseDriver
from jacket.i18n import _
from jacket.storage import exception as cinder_ex
from jacket.tests.storage.unit import fake_backup
from jacket.tests.storage.unit import fake_volume
from jacket.tests.storage.unit.volume.drivers.aws import fake_db


class TestAwsBackupDriver(testtools.TestCase):
    """Generic class for the Aws Backup driver test case."""

    def _get_driver(self, context=None):
        fake_db_path = 'jacket.tests.storage.unit.volume.drivers.aws.fake_db'
        driver = AwsBackupDriver(context, fake_db_path)

        def _get_aws_client(context):
            kwargs = {}
            kwargs['aws_access_key_id'] = 'fake'
            kwargs['aws_secret_access_key'] = 'fake'
            kwargs['region_name'] = 'ap-northeast-1'
            ec2_client = boto3.client('ec2', **kwargs)
            return AwsClientPlugin(ec2_client)
        setattr(driver._aws_client, 'get_aws_client', _get_aws_client)
        if not hasattr(driver.caa_db_api, 'volume_backup_mapper_create'):
            setattr(driver.caa_db_api, 'volume_backup_mapper_create',
                    fake_db.volume_backup_mapper_create)
            self.set_create_flag = True
        if not hasattr(driver.caa_db_api, 'volume_backup_mapper_delete'):
            setattr(driver.caa_db_api, 'volume_backup_mapper_delete',
                    fake_db.volume_backup_mapper_delete)
            self.set_delete_flag = True
        return driver

    def setUp(self):
        """Initialise variable common to all the test cases."""
        super(TestAwsBackupDriver, self).setUp()
        self.ctx = context.RequestContext('fake', 'fake', is_admin=False)
        self.volume = fake_volume.fake_volume_obj(self.ctx)
        self.backup = fake_backup.fake_backup_obj(self.ctx)
        self.set_delete_flag = False
        self.set_create_flag = False
        self.driver = self._get_driver(self.ctx)
        self.fake_snap = {'SnapshotId': 'fake'}
        self.fake_ebs = {'VolumeId': 'fake'}

    def tearDown(self):
        super(TestAwsBackupDriver, self).tearDown()
        if self.set_delete_flag:
            delattr(self.driver.caa_db_api, 'volume_backup_mapper_delete')
        if self.set_create_flag:
            delattr(self.driver.caa_db_api, 'volume_backup_mapper_create')

    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'create_tags', mock.MagicMock())
    @mock.patch('jacket.db.extend.api.volume_backup_mapper_create',
                mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'create_snapshot')
    def test_create_backup(self, mock_create):
        mock_create.return_value = self.fake_snap
        self.driver.backup(self.backup, 'fake')
        create_args = {'VolumeId': 'fake'}
        mock_create.assert_called_once_with(**create_args)

    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_backup_mapper_create',
                mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'create_snapshot')
    def test_create_backup_failed(self, mock_create):
        msg = (_("create provider snapshot failed vol:%s") % 'fake')
        mock_create.side_effect = cinder_ex.VolumeBackendAPIException(data=msg)
        self.assertRaises(cinder_ex.BackupOperationError,
                          self.driver.backup,
                          self.backup,
                          'fake')

    @mock.patch.object(AwsBackupDriver, '_get_provider_backup_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch('jacket.db.extend.api.volume_backup_mapper_delete',
                mock.MagicMock())
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    def test_delete_backup(self, mock_delete):
        self.driver.delete(self.backup)
        delete_args = {'SnapshotId': 'fake'}
        mock_delete.assert_called_once_with(**delete_args)

    @mock.patch.object(AwsBackupDriver, '_get_provider_backup_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    def test_delete_backup_failed(self, mock_delete):
        mock_delete.side_effect = \
            exception_ex.ProviderDeleteSnapshotFailed(reason='')
        self.assertRaises(cinder_ex.BackupOperationError,
                          self.driver.delete,
                          self.backup)

    @mock.patch.object(AwsBackupDriver, '_get_provider_backup_id')
    @mock.patch.object(BaseDriver, '_get_provider_snapshot')
    @mock.patch.object(AwsClientPlugin, 'delete_snapshot')
    @mock.patch('jacket.db.extend.api.volume_backup_mapper_delete',
                mock.MagicMock())
    def test_delete_backup_multi_backup(self, mock_delete, mock_get_snap,
                                        mock_get_id):
        mock_get_id.return_value = None
        mock_get_snap.return_value = ['fake1', 'fake2']
        self.driver.delete(self.backup)
        calls = [mock.call(SnapshotId='fake1'), mock.call(SnapshotId='fake2')]
        mock_delete.assert_has_calls(calls)

    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='old_fake'))
    @mock.patch.object(AwsBackupDriver, '_get_provider_backup_id',
                       mock.MagicMock(return_value='fake_backup'))
    @mock.patch('jacket.db.extend.api.volume_mapper_update',
                mock.MagicMock())
    @mock.patch.object(BaseDriver, '_create_volume')
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    def test_restore(self, mock_delete, mock_create_volume):
        mock_create_volume.return_value = self.fake_ebs
        self.driver.restore(self.backup, self.volume.id, '')
        mock_delete.assert_called_once_with(VolumeId='old_fake')

    @mock.patch.object(BaseDriver, '_get_provider_volume_id',
                       mock.MagicMock(return_value='fake'))
    @mock.patch.object(AwsBackupDriver, '_get_provider_backup_id',
                       mock.MagicMock(return_value='fake_backup'))
    @mock.patch('jacket.db.extend.api.volume_mapper_update')
    @mock.patch.object(BaseDriver, '_create_volume')
    @mock.patch.object(AwsClientPlugin, 'delete_volume')
    def test_restore_failed(self, mock_delete,
                            mock_create_volume,
                            mock_update):
        mock_create_volume.return_value = self.fake_ebs
        mock_update.side_effect = Exception()
        self.assertRaises(cinder_ex.BackupOperationError,
                          self.driver.restore,
                          self.backup,
                          self.volume.id,
                          '')
        mock_delete.assert_called_once_with(VolumeId='fake')
