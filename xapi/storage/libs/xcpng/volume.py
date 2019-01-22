#!/usr/bin/env python

import os

import sys

import uuid

from subprocess import call, Popen, PIPE, check_output

from xapi.storage.libs.xcpng.utils import SR_PATH_PREFIX, get_sr_uuid_by_uri, get_vdi_type_by_uri, \
                                          validate_and_round_vhd_size, fullSizeVHD
from xapi.storage.libs.xcpng.meta import KEY_TAG, UUID_TAG, NAME_TAG, PATH_TAG, DESCRIPTION_TAG, READ_WRITE_TAG, \
                                         VIRTUAL_SIZE_TAG, PHYSICAL_UTILISATION_TAG, URI_TAG, SHARABLE_TAG, \
                                         CUSTOM_KEYS_TAG, TYPE_TAG
from xapi.storage.libs.xcpng.meta import MetadataHandler

from xapi.storage import log

import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import Volume_does_not_exist
    from xapi.storage.api.v4.volume import Volume_skeleton
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.volume import Volume_does_not_exist
    from xapi.storage.api.v5.volume import Volume_skeleton


class VolumeOperations(object):

    def __init__(self):
        pass

    def create(self, dbg, uri, size, path):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def destroy(self, dbg, uri, path):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def resize(self, dbg, uri, size):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def map(self, dbg, uri, path):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def unmap(self, dbg, uri, path):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def get_phisical_utilization(self, dbg, uri):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def roundup_size(self, dbg, size):
        raise NotImplementedError('Override in VolumeOperations specifc class')


class Volume(object):

    def __init__(self):
        self.MetadataHandler = MetadataHandler
        self.VolOpsHendler = VolumeOperations()

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
            PATH_TAG: "%s/%s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, vdi_uri), vdi_uuid),
            DESCRIPTION_TAG: description,
            READ_WRITE_TAG: True,
            VIRTUAL_SIZE_TAG: vsize,
            PHYSICAL_UTILISATION_TAG: psize,
            URI_TAG: [vdi_uri],
            SHARABLE_TAG: sharable,  # False,
            CUSTOM_KEYS_TAG: {}
        }

        image_meta = self._create(dbg, sr, name, description, size, sharable, image_meta)

        self.MetadataHandler.update(dbg, vdi_uri, image_meta)

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
            self.VolOpsHendler.destroy(dbg, uri, image_meta[PATH_TAG])
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


class RAWVolume(Volume):

    def _get_full_vol_size(self, dbg, size):
        return self.VolOpsHendler.roundup_size(dbg, size)

    def _create(self, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: xcpng.RAWVolume.create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        uri = image_meta[URI_TAG][0]

        try:
            self.VolOpsHendler.create(dbg, uri, self._get_full_vol_size(dbg, size), image_meta[PATH_TAG])
        except Exception:
            try:
                self.VolOpsHendler.destroy(dbg, uri, image_meta[PATH_TAG])
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

    def _create(self, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: xcpng.QCOW2Volume._create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        super(QCOW2Volume, self)._create(dbg, sr, name, description, size, sharable, image_meta)

        uri = image_meta[URI_TAG][0]

        self.VolOpsHendler.map(dbg, uri, image_meta[PATH_TAG])

        devnull = open(os.devnull, 'wb')
        call(["/usr/lib64/qemu-dp/bin/qemu-img",
              "create",
              "-f", image_meta[TYPE_TAG],
              image_meta[PATH_TAG],
              str(size)], stdout=devnull, stderr=devnull)

        self.VolOpsHendler.unmap(dbg, uri, image_meta[PATH_TAG])

        return image_meta

    def _resize(self, dbg, sr, key, new_size, image_meta):
        log.debug("%s: xcpng.QCOW2Volume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        super(RAWVolume, self)._resize(dbg, sr, key, new_size, image_meta)

        uri = image_meta[URI_TAG][0]

        self.VolOpsHendler.map(dbg, uri, image_meta[PATH_TAG])

        devnull = open(os.devnull, 'wb')
        call(["/usr/lib64/qemu-dp/bin/qemu-img",
              "resize",
              image_meta[PATH_TAG],
              str(new_size)], stdout=devnull, stderr=devnull)

        self.VolOpsHendler.unmap(dbg, uri, image_meta[PATH_TAG])


class Implementation(Volume_skeleton):

    def __init__(self):
        super(Implementation, self).__init__()
        self.RAWVolume = RAWVolume()
        self.QCOW2Volume = QCOW2Volume()

    def create(self, dbg, sr, name, description, size, sharable):
        log.debug("%s: Volume.%s: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, name, description, size))
        if get_vdi_type_by_uri(dbg, sr) == 'raw':
            return self.RAWVolume.create(dbg, sr, name, description, size, sharable)
        elif get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return self.QCOW2Volume.create(dbg, sr, name, description, size, sharable)

#    def clone(self, dbg, sr, key, mode='clone'):
#        log.debug("%s: Volume.%s: SR: %s Key: %s"
#                  % (dbg, sys._getframe().f_code.co_name, sr, key))
#        if get_vdi_type_by_uri(dbg, sr) == 'raw':
#            return self.RAWVolume.clone(dbg, sr, key, mode)
#        elif get_vdi_type_by_uri(dbg, sr) == 'qcow2':
#            return self.QCOW2Volume.clone(dbg, sr, key, mode)

    def snapshot(self, dbg, sr, key):
        return self.clone(dbg, sr, key, mode='snapshot')

    def destroy(self, dbg, sr, key):
        log.debug("%s: Volume.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))
        if get_vdi_type_by_uri(dbg, sr) == 'raw':
            return self.RAWVolume.destroy(dbg, sr, key)
        elif get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return self.QCOW2Volume.destroy(dbg, sr, key)

    def set_name(self, dbg, sr, key, new_name):
        log.debug("%s: Volume.%s: SR: %s Key: %s New_name: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, new_name))
        if get_vdi_type_by_uri(dbg, sr) == 'raw':
            return self.RAWVolume.set_name(dbg, sr, key, new_name)
        elif get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return self.QCOW2Volume.set_name(dbg, sr, key, new_name)

    def set_description(self, dbg, sr, key, new_description):
        log.debug("%s: Volume.%s: SR: %s Key: %s New_description: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, new_description))
        if get_vdi_type_by_uri(dbg, sr) == 'raw':
            return self.RAWVolume.set_description(dbg, sr, key, new_description)
        elif get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return self.QCOW2Volume.set_description(dbg, sr, key, new_description)

    def set(self, dbg, sr, key, k, v):
        log.debug("%s: Volume.%s: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, k, v))
        if get_vdi_type_by_uri(dbg, sr) == 'raw':
            return self.RAWVolume.set(dbg, sr, key, k, v)
        elif get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return self.QCOW2Volume.set(dbg, sr, key, k, v)

    def unset(self, dbg, sr, key, k):
        log.debug("%s: Volume.%s: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, k))
        if get_vdi_type_by_uri(dbg, sr) == 'raw':
            return self.RAWVolume.unset(dbg, sr, key, k)
        elif get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return self.QCOW2Volume.unset(dbg, sr, key, k)

    def resize(self, dbg, sr, key, new_size):
        log.debug("%s: Volume.%s: SR: %s Key: %s New_size: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, new_size))
        if get_vdi_type_by_uri(dbg, sr) == 'raw':
            return self.RAWVolume.resize(dbg, sr, key, new_size)
        elif get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return self.QCOW2Volume.resize(dbg, sr, key, new_size)

    def stat(self, dbg, sr, key):
        log.debug("%s: Volume.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))
        if get_vdi_type_by_uri(dbg, sr) == 'raw':
            return self.RAWVolume.stat(dbg, sr, key)
        elif get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return self.QCOW2Volume.stat(dbg, sr, key)

#   def compare(self, dbg, sr, key, key2):

#    def similar_content(self, dbg, sr, key):
#        log.debug("%s: Volume.%s: SR: %s Key: %s"
#                  % (dbg, sys._getframe().f_code.co_name, sr, key))
#
#        result = {}
#
#        return result

#    def enable_cbt(self, dbg, sr, key):

#    def disable_cbt(self, dbg, sr, key):

#    def data_destroy(self, dbg, sr, key):

#   def list_changed_blocks(self, dbg, sr, key, key2, offset, length):
