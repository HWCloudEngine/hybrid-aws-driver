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

"""
Volume Drivers for Amazon EC2 Block Storage
"""

from oslo_log import log as logging

from jacket import conf
from jacket import context as req_context
from jacket.db.extend import api as caa_db_api
from jacket.drivers.aws import client
from jacket import exception
from jacket.i18n import _LE, _LI, _
from jacket.storage.backup.driver import BackupDriver
from jacket.storage import exception as cinder_ex
from jacket.storage.volume import driver

LOG = logging.getLogger(__name__)

CONF = conf.CONF


class BaseDriver(object):

    def __init__(self, *args, **kwargs):
        super(BaseDriver, self).__init__(*args, **kwargs)
        self._aws_client = client.AwsClient()
        self.caa_db_api = caa_db_api

    def _get_provider_type_name(self, context, type_id):
        provider_type = None
        try:
            project_mapper = self.caa_db_api.project_mapper_get(
                context,
                context.project_id)
            provider_type = project_mapper.get('provider_type', None)
        except exception.EntityNotFound as ex:
            LOG.error(_LE("type_mapper not found! ex = %s"), ex)
        return provider_type

    def _get_provider_az(self, context, availability_zone):
        provider_az = None
        try:
            project_mapper = self.caa_db_api.project_mapper_get(
                context,
                context.project_id
            )
            provider_az = project_mapper.get('provider_az', None)
        except exception.EntityNotFound as ex:
            LOG.error(_LE("az_mapper not found! ex = %s"), ex)
        return provider_az

    def _get_provider_volume_id(self, context, volume):
        provider_volume_id = None
        try:
            volume_mapper = self.caa_db_api.volume_mapper_get(
                context,
                volume.id,
                context.project_id
            )
            provider_volume_id = volume_mapper.get('provider_volume_id', None)
        except exception.EntityNotFound as ex:
            LOG.error(_LE("volume_mapper not found! ex = %s"), ex)
        return provider_volume_id

    def _get_provider_volume(self, context, volume_id):
        volumes = []
        filters = [{'Name': 'tag:caa_volume_id',
                    'Values': [volume_id]}]
        aws_volumes = self._aws_client.get_aws_client(context).\
            describe_volumes(Filters=filters)
        for aws_volume in aws_volumes:
            volumes.append(aws_volume.get('VolumeId'))
        return volumes

    def _get_provider_snapshot(self, context, os_id):
        snapshots = []
        filters = [{'Name': 'tag:caa_snapshot_id',
                    'Values': [os_id]}]
        aws_snapshots = self._aws_client.get_aws_client(context).\
            describe_snapshots(Filters=filters)
        for aws_snapshot in aws_snapshots:
            snapshots.append(aws_snapshot.get('SnapshotId'))
        return snapshots

    def _get_provider_snapshot_id(self, context, snapshot_id):
        provider_snapshot_id = None
        try:
            snapshot_mapper = self.caa_db_api.volume_snapshot_mapper_get(
                context, snapshot_id
            )
            provider_snapshot_id = snapshot_mapper.get(
                'provider_snapshot_id', None
            )
        except exception.EntityNotFound as ex:
            LOG.error(_LE("snapshot_mapper not found! ex = %s"), ex)
        return provider_snapshot_id

    def _create_volume(self, volume, context, new_size=None,
                       new_type=None, snapshot=None):
        provider_type = self._get_provider_type_name(
            req_context.get_admin_context(),
            new_type or volume.volume_type_id
        )
        provider_az = self._get_provider_az(
            req_context.get_admin_context(),
            volume.availability_zone
        )
        if not provider_az:
            msg = (_("create provider volume failed,no provider_az vol:%s") %
                   volume.id)
            LOG.error(msg)
            raise cinder_ex.VolumeBackendAPIException(data=msg)
        volume_args = {'AvailabilityZone': provider_az,
                       'VolumeType': provider_type or 'standard',
                       'Size': new_size or volume.size}
        if snapshot:
            volume_args['SnapshotId'] = snapshot

        try:
            provider_vol = self._aws_client.get_aws_client(context).\
                create_volume(**volume_args)
            # create remote volume mapper
            tags = [{'Key': 'caa_volume_id', 'Value': volume.id}]
            self._aws_client.get_aws_client(context).\
                create_tags(Resources=[provider_vol['VolumeId']],
                            Tags=tags)
        except Exception as ex:
            LOG.error(_LE("create provider volume failed! vol:%(id)s,"
                          " ex = %(ex)s"), {'id': volume.id, 'ex': ex})
            msg = (_("create provider volume failed vol:%s") % volume.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        return provider_vol

    def _create_snapshot(self, context, provider_vol, os_id):
        try:
            snapshot_args = {'VolumeId': provider_vol}
            provider_snap = self._aws_client.get_aws_client(context).\
                create_snapshot(**snapshot_args)
            # create remote volume mapper
            tags = [{'Key': 'caa_snapshot_id', 'Value': os_id}]
            self._aws_client.get_aws_client(context).\
                create_tags(
                Resources=[provider_snap['SnapshotId']],
                Tags=tags
            )
        except Exception as ex:
            LOG.error(_LE("create provider snapshot failed! os_id:%(os_id)s,"
                          " ex = %(ex)s"), {'os_id': os_id, 'ex': ex})
            msg = (_("create provider snapshot failed os_id:%s") % os_id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)
        return provider_snap


class AwsVolumeDriver(BaseDriver, driver.VolumeDriver):
    CLOUD_DRIVER = True

    def __init__(self, *args, **kwargs):
        super(AwsVolumeDriver, self).__init__(*args, **kwargs)

    def _modify_volume(self, volume, new_size=None, new_type=None):
        context = req_context.RequestContext(is_admin=True,
                                             project_id=volume.project_id)
        snapshot = None
        try:
            old_vol = self._get_provider_volume_id(context, volume)
            snapshot = self._aws_client.get_aws_client(context).\
                create_snapshot(VolumeId=old_vol)
            provider_vol = self._create_volume(volume,
                                               context,
                                               snapshot=snapshot['SnapshotId'],
                                               new_type=new_type,
                                               new_size=new_size)
        except Exception as ex:
            msg = _("Modify Volume failed! Result: %s.") % ex
            raise cinder_ex.VolumeBackendAPIException(data=msg)
        finally:
            if snapshot:
                self._aws_client.get_aws_client(context).\
                    delete_snapshot(
                    SnapshotId=snapshot['SnapshotId']
                )

        # update local volume mapper
        try:
            values = {'provider_volume_id': provider_vol['VolumeId']}
            self.caa_db_api.volume_mapper_update(context, volume.id,
                                                 context.project_id, values)
        except Exception as ex:
            LOG.error(_LE("volume_mapper_create failed! ex = %s"), ex)
            self._aws_client.get_aws_client(context).\
                delete_volume(VolumeId=provider_vol['VolumeId'])
            raise
        self._aws_client.get_aws_client(context).\
            delete_volume(VolumeId=old_vol)
        LOG.debug('create volume %s success.' % volume.id)

    def check_for_setup_error(self):
        return

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        LOG.debug('dir volume: %s' % dir(volume))

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        pass

    def create_cloned_volume(self, volume, src_vref):
        """Create a clone of the specified volume."""
        context = req_context.RequestContext(is_admin=True,
                                             project_id=volume.project_id)
        snapshot = None
        try:
            src_vol = self._get_provider_volume_id(context, src_vref)
            snapshot = self._aws_client.get_aws_client(context).\
                create_snapshot(VolumeId=src_vol)
            provider_vol = self._create_volume(volume,
                                               context,
                                               snapshot=snapshot['SnapshotId'])
        except Exception as ex:
            LOG.error(_LE("create_cloned_volume failed! volume:%(id)s,"
                          "ex: %(ex)s"), {'id': volume.id, 'ex': ex})
            msg = (_("create_cloned_volume failed! volume:%s") % volume.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)
        finally:
            if snapshot:
                self._aws_client.get_aws_client(context).\
                    delete_snapshot(
                    SnapshotId=snapshot['SnapshotId']
                )
        # create local volume mapper
        try:
            values = {'provider_volume_id': provider_vol['VolumeId']}
            self.caa_db_api.volume_mapper_create(context, volume.id,
                                                 context.project_id, values)
        except Exception as ex:
            LOG.error(_LE("volume_mapper_create failed! vol:%(id)s,"
                          " ex = %(ex)s"), {'id': volume.id, 'ex': ex})
            self._aws_client.get_aws_client(context).\
                delete_volume(VolumeId=provider_vol['VolumeId'])
            msg = (_("volume_mapper_create failed! volume:%s") % volume.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        LOG.debug('create volume %s success.' % volume.id)

    def create_export(self, context, volume, connector):
        """Export the volume."""
        pass

    def create_volume(self, volume):
        context = req_context.RequestContext(is_admin=True,
                                             project_id=volume.project_id)
        try:
            provider_vol = self._create_volume(volume, context)
        except Exception:
            msg = (_("create_volume failed! volume:%s") % volume.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        # create local volume mapper
        try:
            values = {'provider_volume_id': provider_vol['VolumeId']}
            self.caa_db_api.volume_mapper_create(context, volume.id,
                                                 context.project_id, values)
        except Exception as ex:
            msg = (_("volume_mapper_create failed! vol: %(id)s,ex: %(ex)s"),
                   {'id': volume.id, 'ex': ex})
            self._aws_client.get_aws_client(context).\
                delete_volume(VolumeId=provider_vol['VolumeId'])
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        LOG.debug('create volume %s success.' % volume.id)

    def delete_volume(self, volume):
        context = req_context.RequestContext(is_admin=True,
                                             project_id=volume.project_id)
        try:
            vol_id = self._get_provider_volume_id(context, volume)
            if not vol_id:
                volumes = self._get_provider_volume(context, volume.id)
                # if len(volumes) > 1,there must have been an error,we should
                # delete all volumes
                for vol in volumes:
                    self._aws_client.get_aws_client(context).\
                        delete_volume(VolumeId=vol)
            else:
                self._aws_client.get_aws_client(context).\
                    delete_volume(VolumeId=vol_id)
        except Exception as ex:
            LOG.error(_LE("delete volume failed! vol:%(id)s,ex = %(ex)s"),
                      {'id': volume.id, 'ex': ex})
            msg = (_("delete_volume failed! volume:%s") % volume.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        # delete volume mapper
        try:
            self.caa_db_api.volume_mapper_delete(context, volume.id,
                                                 context.project_id)
        except Exception as ex:
            LOG.error(_LE("delete volume mapper failed! vol: %(id)s,"
                          "ex = %(ex)s"), {'id': volume.id, 'ex': ex})
            msg = (_("volume_mapper_delete failed! vol: %(id)s,ex: %(ex)s"),
                   {'id': volume.id, 'ex': ex})
            raise cinder_ex.VolumeBackendAPIException(data=msg)

    def extend_volume(self, volume, new_size):
        """Extend a volume."""
        try:
            self._modify_volume(volume, new_size=new_size)
        except Exception as e:
            LOG.error(_LE("extend failed! volume:%(id)s,ex: %(ex)s"),
                      {'id': volume.id, 'ex': e})
            msg = (_("extend_volume failed! volume:%s") % volume.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create a volume from a snapshot."""
        context = req_context.RequestContext(is_admin=True,
                                             project_id=volume.project_id)
        try:
            provider_snap = self._get_provider_snapshot_id(context,
                                                           snapshot.id)
            vol = self._create_volume(volume, context, snapshot=provider_snap)
        except Exception as ex:
            LOG.error(_LE('create_volume_from_snapshot failed,'
                          'snapshot:%(id)s,ex:%(ex)s'),
                      {'id': snapshot.id, 'ex': ex})
            msg = (_("create_volume_from_snapshot failed! volume:%s") %
                   volume.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        # create local volume mapper
        try:
            values = {'provider_volume_id': vol['VolumeId']}
            self.caa_db_api.volume_mapper_create(context, volume.id,
                                                 context.project_id, values)
        except Exception as ex:
            LOG.error(_LE("volume_mapper_create failed! ex = %s"), ex)
            self._aws_client.get_aws_client(context).\
                delete_volume(VolumeId=vol['VolumeId'])
            msg = (_("create_volume_from_snapshot failed! volume:%s") %
                   volume.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        LOG.debug('create volume %s success.' % volume.id)

    def create_snapshot(self, snapshot):
        context = req_context.RequestContext(is_admin=True,
                                             project_id=snapshot.project_id)
        volume = snapshot.volume
        try:
            provider_vol = self._get_provider_volume_id(context, volume)
            provider_snap = self._create_snapshot(context,
                                                  provider_vol,
                                                  snapshot.id)
        except Exception as ex:
            LOG.error(_LE("create snapshot %(id)s failed! ex = %(ex)s"),
                      {'id': snapshot.id, 'ex': ex})
            msg = (_("create_snapshot failed! volume:%s") % snapshot.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        # create volume snapshot mapper
        try:
            values = {"provider_snapshot_id": provider_snap['SnapshotId']}
            self.caa_db_api.volume_snapshot_mapper_create(context, snapshot.id,
                                                          context.project_id,
                                                          values)
        except Exception as ex:
            LOG.error(_LE("create snapshot mapper failed! snapshot:%(id)s,"
                          "ex = %(ex)s"),
                      {'id': snapshot.id, 'ex': ex})
            self._aws_client.get_aws_client(context).\
                delete_snapshot(
                SnapshotId=provider_snap['SnapshotId']
            )
            msg = (_("create_snapshot failed! snapshot:%s") % snapshot.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        LOG.info(_LI("create snapshot:%s success!"), snapshot.id)

    def delete_snapshot(self, snapshot):
        """Delete a snapshot."""
        context = req_context.RequestContext(is_admin=True,
                                             project_id=snapshot.project_id)

        try:
            provider_snap = self._get_provider_snapshot_id(context,
                                                           snapshot.id)
            if provider_snap:
                self._aws_client.get_aws_client(context).\
                    delete_snapshot(SnapshotId=provider_snap)
            else:
                snapshots = self._get_provider_snapshot(context, snapshot.id)
                for snap in snapshots:
                    self._aws_client.get_aws_client(context).\
                        delete_snapshot(SnapshotId=snap)
        except Exception as ex:
            LOG.error(_LE("delete snapshot failed! snapshot:%(id)s,"
                          "ex = %(ex)s"), {'id': snapshot.id, 'ex': ex})
            msg = (_("delete_snapshot failed! snapshot:%s") % snapshot.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        # delete snapshot mapper
        try:
            self.caa_db_api.volume_snapshot_mapper_delete(context, snapshot.id,
                                                          context.project_id)
        except Exception as ex:
            LOG.error(_LE("delete snapshot mapper failed! snapshot:%(id)s,"
                          "ex = %(ex)s"), {'id': snapshot.id, 'ex': ex})
            msg = (_("delete_snapshot failed! snapshot:%s") % snapshot.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)

        LOG.info(_LI("delete snapshot(%s) success!"), snapshot.id)

    def do_setup(self, context):
        """Instantiate common class and log in storage system."""
        pass

    def ensure_export(self, context, volume):
        """Synchronously recreate an export for a volume."""
        pass

    def _update_volume_pool_info(self):
        return {'total_capacity_gb': 1000,
                'free_capacity_gb': 1000}

    def _update_volume_stats(self):
        backend_name = self.configuration.safe_get('volume_backend_name')
        pool_info = self._update_volume_pool_info()
        data = {'volume_backend_name': backend_name or 'AWS',
                'vendor_name': 'Open Source',
                'driver_version': self.VERSION,
                'storage_protocol': 'EBS',
                'reserved_percentage': 0,
                'total_capacity_gb': pool_info.get('total_capacity_gb', 1000),
                'free_capacity_gb': pool_info.get('free_capacity_gb', 1000)}
        self._stats = data

    def get_volume_stats(self, refresh=False):
        """Get volume stats."""
        if refresh:
            self._update_volume_stats()
        return self._stats

    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info."""
        LOG.debug('AWS Driver: initialize_connection')

        driver_volume_type = 'EBS'
        data = {'backend': 'AWS',
                'volume_id': volume['id'],
                'display_name': volume['display_name']}

        return {'driver_volume_type': driver_volume_type,
                'data': data}

    def remove_export(self, context, volume):
        """Remove an export for a volume."""
        pass

    def terminate_connection(self, volume, connector, **kwargs):
        """Disallow connection from connector"""
        LOG.debug('AWS Driver: terminate_connection')
        pass

    def validate_connector(self, connector):
        """Fail if connector doesn't contain all the data needed by driver."""
        LOG.debug('AWS Driver: validate_connector')
        pass

    def attach_volume(self, context, volume, instance_uuid, host_name,
                      mountpoint):
        """Callback for volume attached to instance or host."""
        LOG.debug('start to attach volume.')

    def detach_volume(self, context, volume, mountpoint):
        """Callback for volume detached."""
        LOG.debug('start to detach volume.')

    def retype(self, context, volume, new_type, diff, host):
        try:
            self._modify_volume(volume, new_type=new_type)
        except Exception as e:
            LOG.error(_LE("retype failed! volume:%(id)s,ex: %(ex)s"),
                      {'id': volume.id, 'ex': e})
            msg = (_("retype failed! volume:%s") % volume.id)
            raise cinder_ex.VolumeBackendAPIException(data=msg)
        return True, None


class AwsBackupDriver(BackupDriver, BaseDriver):

    def __init__(self, context, db_driver=None):
        super(AwsBackupDriver, self).__init__(context, db_driver)

    def _get_provider_backup_id(self, context, backup):
        backup_mapper = self.caa_db_api.backup_mapper_get(context,
                                                          backup.id)
        provider_backup_id = backup_mapper.get('provider_backup_id', None)
        return provider_backup_id

    def backup(self, backup, volume_file, backup_metadata=False):
        """Start a backup of a specified volume."""
        context = req_context.RequestContext(is_admin=True,
                                             project_id=backup.project_id)
        volume = self.db.volume_get(context, backup.volume_id)
        try:
            provider_vol = self._get_provider_volume_id(context, volume)
            provider_snap = self._create_snapshot(context,
                                                  provider_vol,
                                                  backup.id)
        except Exception as ex:
            msg = (_("Backup failed,backup_id:%(id)s,ex:%(ex)s") %
                   {'id': backup.id, 'ex': ex})
            LOG.error(msg)
            raise cinder_ex.BackupOperationError(msg)

        # create volume backup mapper
        try:
            values = {"provider_backup_id": provider_snap['SnapshotId']}
            self.caa_db_api.volume_backup_mapper_create(
                context, backup.id,
                context.project_id, values
            )
        except Exception as ex:
            msg = (_("create backup mapper failed! backup:%(id)s,ex = %(ex)s"),
                   {'id': backup.id, 'ex': ex})
            LOG.error(msg)
            self._aws_client.get_aws_client(context).\
                delete_snapshot(
                SnapshotId=provider_snap['SnapshotId']
            )
            raise cinder_ex.BackupOperationError(msg)

        LOG.info(_LI("create backup(%(id)s) success!"), backup.id)

    def restore(self, backup, volume_id, volume_file):
        """Restore a saved backup."""
        context = req_context.RequestContext(is_admin=True,
                                             project_id=backup.project_id)
        volume = self.db.volume_get(context, volume_id)
        try:
            old_vol = self._get_provider_volume_id(context, volume)
            provider_snap = self._get_provider_backup_id(context, backup)
            vol = self._create_volume(volume, context, snapshot=provider_snap)
        except Exception as e:
            msg = _LE("Restore failed,backup_id:%(id)s, "
                      "ex:%(e)s") % {'id': backup.id, 'e': e}
            LOG.error(msg)
            raise cinder_ex.BackupOperationError(msg)

        # update local volume mapper
        try:
            values = {'provider_volume_id': vol['VolumeId']}
            self.caa_db_api.volume_mapper_update(context, volume.id,
                                                 context.project_id,
                                                 values)
        except Exception as ex:
            msg = (_("backup mapper delete failed,backup_id:%(id)s,ex:%(ex)s")
                   % {'id': backup.id, 'ex': ex})
            LOG.error(msg)
            self._aws_client.get_aws_client(context).\
                delete_volume(VolumeId=vol['VolumeId'])
            raise cinder_ex.BackupOperationError(msg)
        else:
            self._aws_client.get_aws_client(context).\
                delete_volume(VolumeId=old_vol)

        LOG.debug('restore volume %s success.' % volume.id)

    def delete(self, backup):
        """Delete a saved backup."""
        context = req_context.RequestContext(is_admin=True,
                                             project_id=backup.project_id)
        try:
            provider_snap = self._get_provider_backup_id(context, backup)
            if not provider_snap:
                snapshots = self._get_provider_snapshot(backup.id)
                # if len(volumes) > 1,there must have been an error,we should
                # delete all volumes
                for snapshot in snapshots:
                    self._aws_client.get_aws_client(context).\
                        delete_snapshot(SnapshotId=snapshot)
            else:
                self._aws_client.get_aws_client(context).\
                    delete_snapshot(SnapshotId=provider_snap)
        except Exception as ex:
            msg = (_LE("backup delete failed,backup_id:%(id)s, ex:%(ex)s") %
                   {'id': backup.id, 'ex': ex})
            LOG.error(msg)
            raise cinder_ex.BackupOperationError(msg)

        # delete backup mapper
        try:
            self.caa_db_api.volume_backup_mapper_delete(
                context,
                backup.id, context.project_id
            )
        except Exception as ex:
            msg = (_LE("backup mapper delete failed, backup_id: %(id)s,"
                       "ex: %(ex)s") % {'id': backup.id, 'ex': ex})
            LOG.error(msg)
            raise cinder_ex.BackupOperationError(msg)

        LOG.info(_LI("delete backup(%s) success!"), backup.id)


def get_backup_driver(context):
    return AwsBackupDriver(context)
