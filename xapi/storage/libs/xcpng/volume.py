#!/usr/bin/env python

import os
import sys
import uuid
from copy import deepcopy
from subprocess import call

from xapi.storage.libs.xcpng.utils import get_vdi_type_by_uri, get_vdi_datapath_by_uri, \
                                          validate_and_round_vhd_size, fullSizeVHD, get_current_host_uuid
from xapi.storage.libs.xcpng.meta import KEY_TAG, UUID_TAG, NAME_TAG, PARENT_URI_TAG, DESCRIPTION_TAG, READ_WRITE_TAG, \
                                         VIRTUAL_SIZE_TAG, PHYSICAL_UTILISATION_TAG, URI_TAG, SHARABLE_TAG, \
                                         CUSTOM_KEYS_TAG, TYPE_TAG, SNAPSHOT_OF_TAG, ACTIVE_ON_TAG, \
                                         QEMU_PID_TAG, QEMU_NBD_SOCK_TAG, QEMU_QMP_SOCK_TAG, QEMU_QMP_LOG_TAG, \
                                         IS_A_SNAPSHOT_TAG
from xapi.storage.libs.xcpng.meta import MetadataHandler
from xapi.storage.libs.xcpng.datapath import DATAPATHES
from xapi.storage import log

import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import Volume_does_not_exist
    from xapi.storage.api.v4.volume import Volume_skeleton
    from xapi.storage.api.v4.volume import Activated_on_another_host
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.volume import Volume_does_not_exist
    from xapi.storage.api.v5.volume import Volume_skeleton
    from xapi.storage.api.v5.volume import Activated_on_another_host


class VolumeOperations(object):

    def __init__(self):
        pass

    def create(self, dbg, uri, size):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def destroy(self, dbg, uri):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def resize(self, dbg, uri, size):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def swap(self, dbg, uri1, uri2):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def get_phisical_utilization(self, dbg, uri):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def roundup_size(self, dbg, size):
        raise NotImplementedError('Override in VolumeOperations specifc class')


class Volume(object):

    def __init__(self):
        self.MetadataHandler = MetadataHandler()
        self.VolOpsHendler = VolumeOperations()
        self.Datapathes = DATAPATHES

    def _create(self, dbg, sr, name, description, size, sharable, image_meta):
        # Override in Volume specifc class
        return image_meta

    def create(self, dbg, sr, name, description, size, sharable):
        log.debug("%s: xcpng.Volume.create: SR: %s Name: %s Description: %s Size: %s, Sharable: %s"
                  % (dbg, sr, name, description, size, sharable))

        vdi_uuid = str(uuid.uuid4())
        vdi_uri = "%s/%s" % (sr, vdi_uuid)
        vsize = size
        psize = size

        image_meta = {
            KEY_TAG: vdi_uuid,
            UUID_TAG: vdi_uuid,
            TYPE_TAG: get_vdi_type_by_uri(dbg, vdi_uri),
            NAME_TAG: name,
            DESCRIPTION_TAG: description,
            READ_WRITE_TAG: True,
            VIRTUAL_SIZE_TAG: vsize,
            PHYSICAL_UTILISATION_TAG: psize,
            URI_TAG: [vdi_uri],
            SHARABLE_TAG: sharable,  # False,
            CUSTOM_KEYS_TAG: {}
        }

        self.MetadataHandler.update(dbg, vdi_uri, image_meta)

        try:
            image_meta = self._create(dbg, sr, name, description, size, sharable, image_meta)
        except Exception:
            self.MetadataHandler.remove(dbg, vdi_uri)
            raise Volume_does_not_exist(vdi_uuid)

        return image_meta

    def _set(self, dbg, sr, key, k, v, image_meta):
        # Override in Volume specifc class
        pass

    def set(self, dbg, sr, key, k, v):
        log.debug("%s: xcpng.Volume.set: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sr, key, k, v))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = self.MetadataHandler.load(dbg, uri)
            image_meta['keys'][k] = v
            self.MetadataHandler.update(dbg, uri, image_meta)
            self._set(dbg, sr, key, k, v, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    def _unset(self, dbg, sr, key, k, image_meta):
        # Override in Volume specifc class
        pass

    def unset(self, dbg, sr, key, k):
        log.debug("%s: xcpng.Volume.unset: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sr, key, k))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = self.MetadataHandler.load(dbg, uri)
            image_meta['keys'].pop(k, None)
            self.MetadataHandler.update(dbg, uri, image_meta)
            self._unset(dbg, sr, key, k, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    def _stat(self, dbg, sr, key, image_meta):
        # Override in Volume specific class
        return image_meta

    def stat(self, dbg, sr, key):
        log.debug("%s: xcpng.Volume.stat: SR: %s Key: %s"
                  % (dbg, sr, key))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = self.MetadataHandler.load(dbg, uri)
            image_meta[PHYSICAL_UTILISATION_TAG] = self.VolOpsHendler.get_phisical_utilization(dbg, uri)
            log.debug("%s: xcpng.Volume.stat: SR: %s Key: %s Metadata: %s"
                      % (dbg, sr, key, image_meta))
            return self._stat(dbg, sr, key, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    def _destroy(self, dbg, sr, key, image_meta):
        # Override in Volume specifc class
        pass

    def destroy(self, dbg, sr, key):
        log.debug("%s: xcpng.Volume.destroy: SR: %s Key: %s"
                  % (dbg, sr, key))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = self.MetadataHandler.load(dbg, uri)
            self._destroy(dbg, sr, key, image_meta)
            self.VolOpsHendler.destroy(dbg, uri)
            self.MetadataHandler.remove(dbg, uri)
        except Exception:
            raise Volume_does_not_exist(key)

    def _set_description(self, dbg, sr, key, new_description, image_meta):
        # Override in Volume specifc class
        pass

    def set_description(self, dbg, sr, key, new_description):
        log.debug("%s: xcpng.Volume.set_description: SR: %s Key: %s New_description: %s"
                  % (dbg, sr, key, new_description))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'description': new_description,
        }

        try:
            self.MetadataHandler.update(dbg, uri, image_meta)
            self._set_description(dbg, sr, key, new_description, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    def _set_name(self, dbg, sr, key, new_name, image_meta):
        # Override in Volume specifc class
        pass

    def set_name(self, dbg, sr, key, new_name):
        log.debug("%s: xcpng.Volume.set_name: SR: %s Key: %s New_name: %s"
                  % (dbg, sr, key, new_name))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'name': new_name,
        }

        try:
            self.MetadataHandler.update(dbg, uri, image_meta)
            self._set_name(dbg, sr, key, new_name, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    def _resize(self, dbg, sr, key, new_size, image_meta):
        # Override in Volume specifc class
        pass

    def resize(self, dbg, sr, key, new_size):
        log.debug("%s: xcpng.Volume.resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'virtual_size': new_size,
        }

        try:
            self._resize(dbg, sr, key, new_size, image_meta)
            self.MetadataHandler.update(dbg, uri, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    def _clone(self, dbg, sr, key, mode, base_meta):
        raise NotImplementedError('Override in Volume specifc class')

    def clone(self, dbg, sr, key, mode):
        log.debug("%s: xcpng.Volume.clone: SR: %s Key: %s Mode: %s"
                  % (dbg, sr, key, mode))

        orig_uri = "%s/%s" % (sr, key)

        try:
            orig_meta = self.MetadataHandler.load(dbg, orig_uri)
        except Exception:
            raise Volume_does_not_exist(key)

        if IS_A_SNAPSHOT_TAG in orig_meta[CUSTOM_KEYS_TAG]:
            base_uri = orig_meta[PARENT_URI_TAG][0]
            try:
                base_meta = self.MetadataHandler.load(dbg, base_uri)
            except Exception:
                raise Volume_does_not_exist(key)
        else:
            base_meta = deepcopy(orig_meta)

        if ACTIVE_ON_TAG in base_meta:
            current_host = get_current_host_uuid()
            if base_meta[ACTIVE_ON_TAG] != current_host:
                log.debug("%s: librbd.Volume.clone: SR: %s Key: %s Can not snapshot on %s as VDI already active on %s"
                          % (dbg, sr, base_meta[UUID_TAG],
                             current_host, base_meta[ACTIVE_ON_TAG]))
                raise Activated_on_another_host(base_meta[ACTIVE_ON_TAG])

        return self._clone(dbg, sr, key, mode, base_meta)


class RAWVolume(Volume):

    def _get_full_vol_size(self, dbg, size):
        return self.VolOpsHendler.roundup_size(dbg, size)

    def _create(self, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: xcpng.RAWVolume.create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        uri = image_meta[URI_TAG][0]

        try:
            self.VolOpsHendler.create(dbg, uri, self._get_full_vol_size(dbg, size))
        except Exception:
            try:
                self.VolOpsHendler.destroy(dbg, uri)
            except Exception:
                pass
            finally:
                raise Volume_does_not_exist(image_meta[UUID_TAG])

        return image_meta

    def _resize(self, dbg, sr, key, new_size, image_meta):
        log.debug("%s: xcpng.RAWVolume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        uri = image_meta[URI_TAG][0]

        try:
            self.VolOpsHendler.resize(dbg, uri, self._get_full_vol_size(dbg, new_size))
        except Exception:
            raise Volume_does_not_exist(key)


class QCOW2Volume(RAWVolume):

    def _get_full_vol_size(self, dbg, size):
        # TODO: Implement overhead calculation for QCOW2 format
        return self.VolOpsHendler.roundup_size(dbg, fullSizeVHD(validate_and_round_vhd_size(size)))

    def _clone(self, dbg, sr, key, mode, base_meta):
        log.debug("%s: xcpng.QCOW2Volume._clone: SR: %s Key: %s Mode: %s"
                  % (dbg, sr, key, mode))

        datapath = get_vdi_datapath_by_uri(dbg, sr)
        devnull = open(os.devnull, 'wb')

        #try:
        if base_meta[KEY_TAG] == key:
            # create clone
            clone_meta = self.create(dbg,
                                     sr,
                                     base_meta[NAME_TAG],
                                     base_meta[DESCRIPTION_TAG],
                                     base_meta[VIRTUAL_SIZE_TAG],
                                     base_meta[SHARABLE_TAG])

            # create new base
            new_base_meta = self.create(dbg,
                                        sr,
                                        base_meta[NAME_TAG],
                                        base_meta[DESCRIPTION_TAG],
                                        base_meta[VIRTUAL_SIZE_TAG],
                                        base_meta[SHARABLE_TAG])

            # swap base and new base
            self.VolOpsHendler.swap(dbg, base_meta[URI_TAG][0], new_base_meta[URI_TAG][0])

            self.Datapathes[datapath].map_vol(dbg, clone_meta[URI_TAG][0])
            self.Datapathes[datapath].map_vol(dbg, base_meta[URI_TAG][0])

            call(["/usr/lib64/qemu-dp/bin/qemu-img",
                  "rebase",
                  "-u",
                  "-f", base_meta[TYPE_TAG],
                  "-b", self.Datapathes[datapath].gen_vol_uri(dbg, new_base_meta[URI_TAG][0]),
                  self.Datapathes[datapath].gen_vol_path(dbg, base_meta[URI_TAG][0])],
                  stdout=devnull, stderr=devnull)

            call(["/usr/lib64/qemu-dp/bin/qemu-img",
                  "rebase",
                  "-u",
                  "-f", clone_meta[TYPE_TAG],
                  "-b", self.Datapathes[datapath].gen_vol_uri(dbg, new_base_meta[URI_TAG][0]),
                  self.Datapathes[datapath].gen_vol_path(dbg, clone_meta[URI_TAG][0])],
                  stdout=devnull, stderr=devnull)

            self.Datapathes[datapath].unmap_vol(dbg, clone_meta[URI_TAG][0])

            new_base_uuid = new_base_meta[UUID_TAG]
            new_base_meta = deepcopy(base_meta)
            new_base_meta[NAME_TAG] = "(base) %s" % new_base_meta[NAME_TAG]
            new_base_meta[KEY_TAG] = new_base_uuid
            new_base_meta[UUID_TAG] = new_base_uuid
            new_base_meta[URI_TAG] = ["%s/%s" % (sr, new_base_uuid)]
            new_base_meta[READ_WRITE_TAG] = False

            if ACTIVE_ON_TAG in new_base_meta:
                self.Datapathes[datapath].snapshot(dbg, new_base_meta[URI_TAG][0], base_meta[URI_TAG][0], 0)
            else:
                self.Datapathes[datapath].unmap_vol(dbg, base_meta[URI_TAG][0])

            if ACTIVE_ON_TAG in new_base_meta:
                new_base_meta[ACTIVE_ON_TAG] = None
                new_base_meta[QEMU_PID_TAG] = None
                new_base_meta[QEMU_NBD_SOCK_TAG] = None
                new_base_meta[QEMU_QMP_SOCK_TAG] = None
                new_base_meta[QEMU_QMP_LOG_TAG] = None

            base_meta[PARENT_URI_TAG] = new_base_meta[URI_TAG]
            clone_parent = new_base_meta[URI_TAG]

            self.MetadataHandler.update(dbg, new_base_meta[URI_TAG][0], new_base_meta)
            self.MetadataHandler.update(dbg, base_meta[URI_TAG][0], base_meta)

        else:
            # create clone
            clone_meta = self.create(dbg,
                                     sr,
                                     base_meta[NAME_TAG],
                                     base_meta[DESCRIPTION_TAG],
                                     base_meta[VIRTUAL_SIZE_TAG],
                                     base_meta[SHARABLE_TAG])

            self.Datapathes[datapath].map_vol(dbg, clone_meta[URI_TAG][0])

            call(["/usr/lib64/qemu-dp/bin/qemu-img",
                  "rebase",
                  "-u",
                  "-f", base_meta[TYPE_TAG],
                  "-b", self.Datapathes[datapath].gen_vol_uri(dbg, base_meta[URI_TAG][0]), # TODO: Fix it to datapath like rbd://cluster/pool/image. For ZFSSR it is the same as path
                  self.Datapathes[datapath].gen_vol_path(dbg, clone_meta[URI_TAG][0])],
                  stdout = devnull, stderr = devnull)

            self.Datapathes[datapath].unmap_vol(dbg, clone_meta[URI_TAG][0])

            clone_parent = base_meta[URI_TAG]

        clone_uuid = clone_meta[UUID_TAG]
        clone_meta = deepcopy(base_meta)
        clone_meta[KEY_TAG] = clone_uuid
        clone_meta[UUID_TAG] = clone_uuid
        clone_meta[URI_TAG] = ["%s/%s" % (sr, clone_uuid)]
        clone_meta[PARENT_URI_TAG] =  clone_parent

        if ACTIVE_ON_TAG in clone_meta:
            clone_meta.pop(ACTIVE_ON_TAG, None)
            clone_meta.pop(QEMU_PID_TAG, None)
            clone_meta.pop(QEMU_NBD_SOCK_TAG, None)
            clone_meta.pop(QEMU_QMP_SOCK_TAG, None)
            clone_meta.pop(QEMU_QMP_LOG_TAG, None)

        if mode is 'snapshot':
            clone_meta[READ_WRITE_TAG] = False
        elif mode is 'clone':
            clone_meta[READ_WRITE_TAG] = True

        self.MetadataHandler.update(dbg, clone_meta[URI_TAG][0], clone_meta)

        return clone_meta
        #except Exception:
        #    raise Volume_does_not_exist(key)
        #finally:
        #    devnull.close()

    def _create(self, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: xcpng.QCOW2Volume._create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        super(QCOW2Volume, self)._create(dbg, sr, name, description, size, sharable, image_meta)

        uri = image_meta[URI_TAG][0]
        datapath = get_vdi_datapath_by_uri(dbg, uri)

        self.Datapathes[datapath].map_vol(dbg, uri)

        devnull = open(os.devnull, 'wb')
        call(["/usr/lib64/qemu-dp/bin/qemu-img",
              "create",
              "-f", image_meta[TYPE_TAG],
              self.Datapathes[datapath].gen_vol_uri(dbg, uri),
              str(size)],
              stdout=devnull, stderr=devnull)
        devnull.close()

        self.Datapathes[datapath].unmap_vol(dbg, uri)

        return image_meta

    def _resize(self, dbg, sr, key, new_size, image_meta):
        log.debug("%s: xcpng.QCOW2Volume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        super(RAWVolume, self)._resize(dbg, sr, key, new_size, image_meta)

        uri = image_meta[URI_TAG][0]
        datapath = get_vdi_datapath_by_uri(dbg, uri)

        self.Datapathes[datapath].map_vol(dbg, uri)

        devnull = open(os.devnull, 'wb')
        call(["/usr/lib64/qemu-dp/bin/qemu-img",
              "resize",
              self.Datapathes[datapath].gen_vol_uri(dbg, uri),
              str(new_size)],
              stdout=devnull, stderr=devnull)
        devnull.close()

        self.Datapathes[datapath].unmap_vol(dbg, uri)


VOLUME_TYPES = {'raw': RAWVolume, 'qcow2': QCOW2Volume}


class Implementation(Volume_skeleton):

    def __init__(self, vol_types):
        super(Implementation, self).__init__()
        self.VolumeTypes = vol_types

    def create(self, dbg, sr, name, description, size, sharable):
        log.debug("%s: Volume.%s: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, name, description, size))

        return self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().create(dbg, sr, name, description, size, sharable)

    def clone(self, dbg, sr, key, mode='clone'):
        log.debug("%s: Volume.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))

        return self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().clone(dbg, sr, key, mode)

    def snapshot(self, dbg, sr, key):

        return self.clone(dbg, sr, key, mode='snapshot')

    def destroy(self, dbg, sr, key):
        log.debug("%s: Volume.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().destroy(dbg, sr, key)

    def set_name(self, dbg, sr, key, new_name):
        log.debug("%s: Volume.%s: SR: %s Key: %s New_name: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, new_name))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().set_name(dbg, sr, key, new_name)

    def set_description(self, dbg, sr, key, new_description):
        log.debug("%s: Volume.%s: SR: %s Key: %s New_description: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, new_description))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().set_description(dbg, sr, key, new_description)

    def set(self, dbg, sr, key, k, v):
        log.debug("%s: Volume.%s: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, k, v))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().set(dbg, sr, key, k, v)

    def unset(self, dbg, sr, key, k):
        log.debug("%s: Volume.%s: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, k))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().unset(dbg, sr, key, k)

    def resize(self, dbg, sr, key, new_size):
        log.debug("%s: Volume.%s: SR: %s Key: %s New_size: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, new_size))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().resize(dbg, sr, key, new_size)

    def stat(self, dbg, sr, key):
        log.debug("%s: Volume.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))

        return self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().stat(dbg, sr, key)

#    def compare(self, dbg, sr, key, key2):

#    def similar_content(self, dbg, sr, key):

#    def enable_cbt(self, dbg, sr, key):

#    def disable_cbt(self, dbg, sr, key):

#    def data_destroy(self, dbg, sr, key):

#    def list_changed_blocks(self, dbg, sr, key, key2, offset, length):
