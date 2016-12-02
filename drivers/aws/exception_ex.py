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

from jacket.exception import JacketException
from jacket.i18n import _


class MultiInstanceConfusion(JacketException):
    msg_fmt = _("More than one instance are found")


class MultiVolumeConfusion(JacketException):
    msg_fmt = _("More than one volume are found")


class ProviderCreateInstanceFailed(JacketException):
    msg_fmt = _("Provider create instance failed,error msg: %(reason)s")


class ProviderCreateVolumeFailed(JacketException):
    msg_fmt = _("Provider create volume failed,error msg: %(reason)s")


class ProviderDeleteVolumeFailed(JacketException):
    msg_fmt = _("Provider delete volume failed,error msg: %(reason)s")


class ProviderCreateSnapshotFailed(JacketException):
    msg_fmt = _("Provider create volume failed,error msg: %(reason)s")


class ProviderDeleteSnapshotFailed(JacketException):
    msg_fmt = _("Provider delete volume failed,error msg: %(reason)s")


class AccountNotConfig(JacketException):
    msg_fmt = _('os account info not config')


class OsAwsConnectFailed(JacketException):
    msg_fmt = _("connect aws failed!")


class AttachVolumeFailed(JacketException):
    msg_fmt = _("Attach volume on provider cloud failed")
