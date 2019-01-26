#!/usr/bin/env python

from os.path import exists
from subprocess import call

from xapi.storage import log

from xapi.storage.libs.xcpng.meta import MetadataHandler
from xapi.storage.libs.xcpng.meta import NON_PERSISTENT_TAG, ACTIVE_ON_TAG, UUID_TAG, QEMU_PID_TAG, QEMU_QMP_SOCK_TAG, \
                                         QEMU_NBD_SOCK_TAG, QEMU_QMP_LOG_TAG, PARENT_URI_TAG, REF_COUNT_TAG
from xapi.storage.libs.xcpng.qemudisk import introduce, create, Qemudisk, ROOT_NODE_NAME

from xapi.storage.libs.xcpng.utils import SR_PATH_PREFIX, get_current_host_uuid, get_sr_uuid_by_uri, \
                                          get_vdi_type_by_uri, get_vdi_uuid_by_uri, get_vdi_datapath_by_uri

import platform
if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.datapath import Datapath_skeleton
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.datapath import Datapath_skeleton

class Datapath(object):

    def __init__(self):
        self.MetadataHandler = MetadataHandler()
        self.blkdev = None

    def gen_vol_path(self, dbg, uri):
        return "%s/%s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri), get_vdi_uuid_by_uri(dbg, uri))

    def gen_vol_uri(self, dbg, uri):
        return self.gen_vol_path(dbg, uri)

    def map_vol(self, dbg, uri, chained=False):
        if self.blkdev:
            _blkdev_ = self.blkdev
            image_meta = self.MetadataHandler.load(dbg, uri)

            if chained:
                if PARENT_URI_TAG in image_meta:
                    self.map_vol(dbg, image_meta[PARENT_URI_TAG][0], chained)

            if REF_COUNT_TAG in image_meta:
                new_meta = {}
                new_meta[REF_COUNT_TAG] = image_meta[REF_COUNT_TAG] + 1
                self.MetadataHandler.update(dbg, uri, new_meta)
            else:
                new_meta = {}
                new_meta[REF_COUNT_TAG] = 1
                call(['ln', '-s', _blkdev_, self.gen_vol_path(dbg, uri)])
                self.MetadataHandler.update(dbg, uri, new_meta)

    def unmap_vol(self, dbg, uri, chained=False):
        image_meta = self.MetadataHandler.load(dbg, uri)
        path = self.gen_vol_path(dbg, uri)

        if REF_COUNT_TAG in image_meta:
            new_meta = {}
            if image_meta[REF_COUNT_TAG] == 1:
                new_meta[REF_COUNT_TAG] = None
                if exists(path):
                    call(['unlink', path])
            else:
                new_meta[REF_COUNT_TAG] = image_meta[REF_COUNT_TAG] - 1
            self.MetadataHandler.update(dbg, uri, new_meta)

        if chained:
            if PARENT_URI_TAG in image_meta:
                self.unmap_vol(dbg, image_meta[PARENT_URI_TAG][0], chained)


    def _open(self, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    def open(self, dbg, uri, persistent):
        log.debug("%s: xcpng.Datapath.open: uri: %s persistent: %s"
                  % (dbg, uri, persistent))

        image_meta = self.MetadataHandler.load(dbg, uri)

        if NON_PERSISTENT_TAG in image_meta:
            vdi_non_persistent = image_meta[NON_PERSISTENT_TAG]
        else:
            vdi_non_persistent = False

        if persistent:
            log.debug("%s: xcpng.Datapath.open: uri: %s will be marked as persistent" % (dbg, uri))
            if vdi_non_persistent:
                # unmark as non-peristent
                image_meta = {
                    NON_PERSISTENT_TAG: None,
                }
                self.MetadataHandler.update(dbg, uri, image_meta)
                # on detach remove special snapshot to rollback to
        elif vdi_non_persistent:
            log.debug("%s: xcpng.Datapath.open: uri: %s already marked as non-persistent" % (dbg, uri))
        else:
            log.debug("%s: xcpng.Datapath.open: uri: %s will be marked as non-persistent" % (dbg, uri))
            # mark as non-peristent
            image_meta = {
                NON_PERSISTENT_TAG: True,
            }
            self.MetadataHandler.update(dbg, uri, image_meta)
            # on attach create special snapshot to rollback to on detach

        self._open(dbg, uri, persistent)

    def _close(self, dbg, uri):
        raise NotImplementedError('Override in Datapath specifc class')

    def close(self, dbg, uri):
        log.debug("%s: xcpng.Datapath.close: uri: %s"
                  % (dbg, uri))

        image_meta = self.MetadataHandler.load(dbg, uri)

        if NON_PERSISTENT_TAG in image_meta:
            vdi_non_persistent = image_meta[NON_PERSISTENT_TAG]
        else:
            vdi_non_persistent = False

        log.debug("%s: xcpng.Datapath.close: uri: %s will be marked as persistent" % (dbg, uri))
        if vdi_non_persistent:
            # unmark as non-peristent
            image_meta = {
                NON_PERSISTENT_TAG: None,
            }
            self.MetadataHandler.update(dbg, uri, image_meta)

        self._close(dbg, uri)

    def _attach(self, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    def attach(self, dbg, uri, domain):
        log.debug("%s: xcpng.Datapath.attach: uri: %s domain: %s"
                  % (dbg, uri, domain))

        self.map_vol(dbg, uri, chained=True)

        if platform.linux_distribution()[1] == '7.5.0':
            protocol, params = self._attach(dbg, uri, domain)
            return {
                'domain_uuid': '0',
                'implementation': [protocol, params]
            }
        elif platform.linux_distribution()[1] == '7.6.0':
            return {
                'implementations': self._attach(dbg, uri, domain)
            }

    def _detach(self, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    def detach(self, dbg, uri, domain):
        log.debug("%s: xcpng.Datapath.detach: uri: %s domain: %s"
                  % (dbg, uri, domain))

        self.unmap_vol(dbg, uri, chained=True)

        self._detach(dbg, uri, domain)

    def _activate(self, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    def activate(self, dbg, uri, domain):
        log.debug("%s: xcpng.Datapath.activate: uri: %s domain: %s"
                  % (dbg, uri, domain))

        # TODO: Check that VDI is not active on other host

        self._activate(dbg, uri, domain)

        image_meta = {
            ACTIVE_ON_TAG: get_current_host_uuid()
        }

        self.MetadataHandler.update(dbg, uri, image_meta)

    def _deactivate(self, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    def deactivate(self, dbg, uri, domain):
        log.debug("%s: xcpng.Datapath.deactivate: uri: %s domain: %s"
                  % (dbg, uri, domain))

        self._deactivate(dbg, uri, domain)

        image_meta = {
            ACTIVE_ON_TAG: None
        }

        self.MetadataHandler.update(dbg, uri, image_meta)

    def _suspend(self, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    def suspend(self, dbg, uri, domain):
        log.debug("%s: xcpng.Datapath.suspend: uri: %s domain: %s"
                  % (dbg, uri, domain))

        self._suspend(dbg, uri, domain)

    def _resume(self, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    def resume(self, dbg, uri, domain):
        log.debug("%s: xcpng.Datapath.resume: uri: %s domain: %s"
                  % (dbg, uri, domain))

        self._resume(dbg, uri, domain)

    def _snapshot(self, dbg, base_uri, snap_uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    def snapshot(self, dbg, base_uri, snap_uri, domain):
        log.debug("%s: xcpng.Datapath.snapshot: base_uri: %s snap_uri: %s domain: %s"
                  % (dbg, base_uri, snap_uri, domain))

        self._snapshot(dbg, base_uri, snap_uri, domain)


class QdiskDatapath(Datapath):

    def __init__(self):
        super(QdiskDatapath, self).__init__()
        self.qemudisk = Qemudisk

    def _load_qemu_dp(self, dbg, uri, domain):
        log.debug("%s: xcpng.QdiskDatapath._load_qemu_dp: uri: %s domain: %s"
                  % (dbg, uri, domain))

        image_meta = self.MetadataHandler.load(dbg, uri)

        return introduce(dbg,
                         self.qemudisk,
                         get_sr_uuid_by_uri(dbg, uri),
                         image_meta[UUID_TAG],
                         get_vdi_type_by_uri(dbg, uri),
                         image_meta[QEMU_PID_TAG],
                         image_meta[QEMU_QMP_SOCK_TAG],
                         image_meta[QEMU_NBD_SOCK_TAG],
                         image_meta[QEMU_QMP_LOG_TAG])

    def _open(self, dbg, uri, persistent):
        log.debug("%s: xcpng.QdiskDatapath._open: uri: %s persistent: %s"
                  % (dbg, uri, persistent))

    def _close(self, dbg, uri):
        log.debug("%s: xcpng.QdiskDatapath._close: uri: %s"
                  % (dbg, uri))

    def _attach(self, dbg, uri, domain):
        log.debug("%s: xcpng.QdiskDatapath._attach: uri: %s domain: %s"
                  % (dbg, uri, domain))

        protocol = 'Qdisk'

        qemu_dp = create(dbg, self.qemudisk, uri)

        image_meta = {
            QEMU_PID_TAG: qemu_dp.pid,
            QEMU_QMP_SOCK_TAG: qemu_dp.qmp_sock,
            QEMU_NBD_SOCK_TAG: qemu_dp.nbd_sock,
            QEMU_QMP_LOG_TAG: qemu_dp.qmp_log
        }

        self.MetadataHandler.update(dbg, uri, image_meta)

        if platform.linux_distribution()[1] == '7.5.0':
            return (protocol, qemu_dp.params)
        elif platform.linux_distribution()[1] == '7.6.0':
            implementations = [
                [
                    'XenDisk',
                    {
                        'backend_type': 'qdisk',
                        'params': "vdi:{}".format(qemu_dp.vdi_uuid),
                        'extra': {}
                    }
                ],
                [
                    'Nbd',
                    {
                        'uri': 'nbd:unix:{}:exportname={}'
                            .format(qemu_dp.nbd_sock, ROOT_NODE_NAME)
                    }
                ]
            ]
            return (implementations)

    def _detach(self, dbg, uri, domain):
        log.debug("%s: xcpng.QdiskDatapath._detach: uri: %s domain: %s"
                  % (dbg, uri, domain))

        qemu_dp = self._load_qemu_dp(dbg, uri, domain)

        qemu_dp.quit(dbg)

        image_meta = {
            QEMU_PID_TAG: None,
            QEMU_QMP_SOCK_TAG: None,
            QEMU_NBD_SOCK_TAG: None,
            QEMU_QMP_LOG_TAG: None
        }

        self.MetadataHandler.update(dbg, uri, image_meta)

    def _activate(self, dbg, uri, domain):
        log.debug("%s: xcpng.QdiskDatapath._activate: uri: %s domain: %s"
                  % (dbg, uri, domain))

        qemu_dp = self._load_qemu_dp(dbg, uri, domain)

        qemu_dp.open(dbg)

    def _deactivate(self, dbg, uri, domain):
        log.debug("%s: libzfs.QdiskDatapath._deactivate: uri: %s domain: %s"
                  % (dbg, uri, domain))

        qemu_dp = self._load_qemu_dp(dbg, uri, domain)

        qemu_dp.close(dbg)

    def _suspend(self, dbg, uri, domain):
        log.debug("%s: xcpng.QdiskDatapath._suspend: uri: %s domain: %s"
                  % (dbg, uri, domain))

        qemu_dp = self._load_qemu_dp(dbg, uri, domain)

        qemu_dp.suspend(dbg)

    def _resume(self, dbg, uri, domain):
        log.debug("%s: xcpng.QdiskDatapath._resume: uri: %s domain: %s"
                  % (dbg, uri, domain))

        qemu_dp = self._load_qemu_dp(dbg, uri, domain)

        qemu_dp.resume(dbg)

    def _snapshot(self, dbg, base_uri, snap_uri, domain):
        log.debug("%s: xcpng.QdiskDatapath._snapshot: base_uri: %s snap_uri: %s domain: %s"
                  % (dbg, base_uri, snap_uri, domain))

        qemu_dp = self._load_qemu_dp(dbg, base_uri, domain)

        qemu_dp.snap(dbg, snap_uri)


DATAPATHES = {'qdisk': QdiskDatapath()}


class Implementation(Datapath_skeleton):
    """
    Datapath implementation
    """
    def __init__(self, datapathes):
        super(Implementation, self).__init__()
        self.Datapathes = datapathes

    def open(self, dbg, uri, persistent):
        log.debug("%s: Datapath.open: uri: %s persistent: %s" % (dbg, uri, persistent))

        self.Datapathes[get_vdi_datapath_by_uri(dbg, uri)].open(dbg, uri, persistent)

    def close(self, dbg, uri):
        log.debug("%s: Datapath.close: uri: %s" % (dbg, uri))

        self.Datapathes[get_vdi_datapath_by_uri(dbg, uri)].close(dbg, uri)

    def attach(self, dbg, uri, domain):
        log.debug("%s: Datapath.attach: uri: %s domain: %s" % (dbg, uri, domain))

        return self.Datapathes[get_vdi_datapath_by_uri(dbg, uri)].attach(dbg, uri, domain)

    def detach(self, dbg, uri, domain):
        log.debug("%s: Datapath.detach: uri: %s domain: %s" % (dbg, uri, domain))

        self.Datapathes[get_vdi_datapath_by_uri(dbg, uri)].detach(dbg, uri, domain)

    def activate(self, dbg, uri, domain):
        log.debug("%s: Datapath.activate: uri: %s domain: %s" % (dbg, uri, domain))

        self.Datapathes[get_vdi_datapath_by_uri(dbg, uri)].activate(dbg, uri, domain)

    def deactivate(self, dbg, uri, domain):
        log.debug("%s: Datapath.deactivate: uri: %s domain: %s" % (dbg, uri, domain))

        self.Datapathes[get_vdi_datapath_by_uri(dbg, uri)].deactivate(dbg, uri, domain)
