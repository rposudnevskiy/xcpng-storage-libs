#!/usr/bin/env python

import os
import sys
import uuid
from copy import deepcopy

from xapi.storage.libs.xcpng.utils import call, get_vdi_type_by_uri, get_vdi_datapath_by_uri, \
                                          validate_and_round_vhd_size, fullSizeVHD, get_current_host_uuid
from xapi.storage.libs.xcpng.meta import KEY_TAG, UUID_TAG, NAME_TAG, PARENT_URI_TAG, DESCRIPTION_TAG, READ_WRITE_TAG, \
                                         VIRTUAL_SIZE_TAG, PHYSICAL_UTILISATION_TAG, URI_TAG, SHARABLE_TAG, \
                                         CUSTOM_KEYS_TAG, TYPE_TAG, ACTIVE_ON_TAG, IS_A_SNAPSHOT_TAG
from xapi.storage.libs.xcpng.meta import MetadataHandler, merge, snap_merge_pattern
from xapi.storage.libs.xcpng.datapath import DATAPATHES
from xapi.storage import log

import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import Volume_skeleton
    from xapi.storage.api.v4.volume import Activated_on_another_host
elif platform.linux_distribution()[1] == '7.6.0':
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
        self.Datapathes = {}
        for k, v in DATAPATHES.iteritems():
            self.Datapathes[k] = v()

    def _create(self, dbg, sr, name, description, size, sharable, image_meta):
        raise NotImplementedError('Override in Volume specifc class')

    def create(self, dbg, sr, name, description, size, sharable):
        log.debug("%s: xcpng.volume.Volume.create: SR: %s Name: %s Description: %s Size: %s, Sharable: %s"
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
            PHYSICAL_UTILISATION_TAG: 0,
            URI_TAG: [vdi_uri],
            SHARABLE_TAG: sharable,  # False,
            CUSTOM_KEYS_TAG: {}
        }

        try:
            self.MetadataHandler.update(dbg, vdi_uri, image_meta)
            image_meta = self._create(dbg, sr, name, description, size, sharable, image_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.create: Failed to create volume: key %s: SR: %s" % (dbg, vdi_uuid, sr))
            try:
                self.MetadataHandler.remove(dbg, vdi_uri)
            except:
                pass
            raise Exception(e)

        return image_meta

    def _set(self, dbg, sr, key, k, v, image_meta):
        # Override in Volume specifc class
        pass

    def set(self, dbg, sr, key, k, v):
        log.debug("%s: xcpng.volume.Volume.set: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sr, key, k, v))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = self.MetadataHandler.load(dbg, uri)
            image_meta['keys'][k] = v
            self.MetadataHandler.update(dbg, uri, image_meta)
            self._set(dbg, sr, key, k, v, image_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to set volume param: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _unset(self, dbg, sr, key, k, image_meta):
        # Override in Volume specifc class
        pass

    def unset(self, dbg, sr, key, k):
        log.debug("%s: xcpng.volume.Volume.unset: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sr, key, k))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = self.MetadataHandler.load(dbg, uri)
            image_meta['keys'].pop(k, None)
            self.MetadataHandler.update(dbg, uri, image_meta)
            self._unset(dbg, sr, key, k, image_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to unset volume param: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _stat(self, dbg, sr, key, image_meta):
        # Override in Volume specific class
        return image_meta

    def stat(self, dbg, sr, key):
        log.debug("%s: xcpng.volume.Volume.stat: SR: %s Key: %s"
                  % (dbg, sr, key))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = self.MetadataHandler.load(dbg, uri)
            image_meta[PHYSICAL_UTILISATION_TAG] = self.VolOpsHendler.get_phisical_utilization(dbg, uri)
            log.debug("%s: xcpng.volume.Volume.stat: SR: %s Key: %s Metadata: %s"
                      % (dbg, sr, key, image_meta))
            return self._stat(dbg, sr, key, image_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.stat: Failed to get volume stat: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _destroy(self, dbg, sr, key, image_meta):
        # Override in Volume specifc class
        pass

    def destroy(self, dbg, sr, key):
        log.debug("%s: xcpng.volume.Volume.destroy: SR: %s Key: %s"
                  % (dbg, sr, key))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = self.MetadataHandler.load(dbg, uri)
            self._destroy(dbg, sr, key, image_meta)
            self.VolOpsHendler.destroy(dbg, uri)
            self.MetadataHandler.remove(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.destroy: Failed to destroy volume: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _set_description(self, dbg, sr, key, new_description, image_meta):
        # Override in Volume specifc class
        pass

    def set_description(self, dbg, sr, key, new_description):
        log.debug("%s: xcpng.volume.Volume.set_description: SR: %s Key: %s New_description: %s"
                  % (dbg, sr, key, new_description))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'description': new_description,
        }

        try:
            self.MetadataHandler.update(dbg, uri, image_meta)
            self._set_description(dbg, sr, key, new_description, image_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to set volume description: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _set_name(self, dbg, sr, key, new_name, image_meta):
        # Override in Volume specifc class
        pass

    def set_name(self, dbg, sr, key, new_name):
        log.debug("%s: xcpng.volume.Volume.set_name: SR: %s Key: %s New_name: %s"
                  % (dbg, sr, key, new_name))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'name': new_name,
        }

        try:
            self.MetadataHandler.update(dbg, uri, image_meta)
            self._set_name(dbg, sr, key, new_name, image_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to set volume name: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _resize(self, dbg, sr, key, new_size, image_meta):
        raise NotImplementedError('Override in Volume specifc class')

    def resize(self, dbg, sr, key, new_size):
        log.debug("%s: xcpng.volume.Volume.resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'virtual_size': new_size,
        }

        try:
            self._resize(dbg, sr, key, new_size, image_meta)
            self.MetadataHandler.update(dbg, uri, image_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to resize volume: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _clone(self, dbg, sr, key, mode, base_meta):
        raise NotImplementedError('Override in Volume specifc class')

    def clone(self, dbg, sr, key, mode):
        log.debug("%s: xcpng.volume.Volume.clone: SR: %s Key: %s Mode: %s"
                  % (dbg, sr, key, mode))

        orig_uri = "%s/%s" % (sr, key)

        try:
            orig_meta = self.MetadataHandler.load(dbg, orig_uri)

            if IS_A_SNAPSHOT_TAG in orig_meta[CUSTOM_KEYS_TAG]:
                base_uri = orig_meta[PARENT_URI_TAG][0]
                base_meta = self.MetadataHandler.load(dbg, base_uri)
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
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to clone volume: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)


class RAWVolume(Volume):

    def _get_full_vol_size(self, dbg, size):
        return self.VolOpsHendler.roundup_size(dbg, size)

    def _create(self, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: xcpng.volume.RAWVolume._create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        uri = image_meta[URI_TAG][0]

        try:
            self.VolOpsHendler.create(dbg, uri, self._get_full_vol_size(dbg, size))
        except Exception as e:
            log.error("%s: xcpng.volume.RAWVolume._create: Failed to create volume: key %s: SR: %s" %
                      (dbg, image_meta[UUID_TAG], sr))
            try:
                self.VolOpsHendler.destroy(dbg, uri)
            except:
                pass
            raise Exception(e)

        return image_meta

    def _resize(self, dbg, sr, key, new_size, image_meta):
        log.debug("%s: xcpng.volume.RAWVolume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        uri = image_meta[URI_TAG][0]

        try:
            self.VolOpsHendler.resize(dbg, uri, self._get_full_vol_size(dbg, new_size))
        except Exception as e:
            log.error("%s: xcpng.volume.RAWVolume._resize: Failed to create volume: key %s: SR: %s" %
                      (dbg, image_meta[UUID_TAG], sr))
            raise Exception(e)

    def _clone(self, dbg, sr, key, mode, base_meta):
        raise NotImplementedError('Not implemented in RAWVolume class')


class QCOW2Volume(RAWVolume):

    def _get_full_vol_size(self, dbg, size):
        # TODO: Implement overhead calculation for QCOW2 format
        return self.VolOpsHendler.roundup_size(dbg, fullSizeVHD(validate_and_round_vhd_size(size)))

    def _clone(self, dbg, sr, key, mode, base_meta):
        log.debug("%s: xcpng.volume.QCOW2Volume._clone: SR: %s Key: %s Mode: %s"
                  % (dbg, sr, key, mode))

        datapath = get_vdi_datapath_by_uri(dbg, sr)

        try:
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

                merge(base_meta, new_base_meta, snap_merge_pattern)
                new_base_meta[NAME_TAG] = "(base) %s" % new_base_meta[NAME_TAG]
                new_base_meta[READ_WRITE_TAG] = False
                base_meta[PARENT_URI_TAG] = new_base_meta[URI_TAG]
                clone_parent = new_base_meta[URI_TAG]

                self.MetadataHandler.update(dbg, new_base_meta[URI_TAG][0], new_base_meta)
                self.MetadataHandler.update(dbg, base_meta[URI_TAG][0], base_meta)

                # swap base and new base
                self.VolOpsHendler.swap(dbg, base_meta[URI_TAG][0], new_base_meta[URI_TAG][0])

                self.Datapathes[datapath].map_vol(dbg, clone_meta[URI_TAG][0], chained=None)
                self.Datapathes[datapath].map_vol(dbg, base_meta[URI_TAG][0], chained=None)

                call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                           "rebase",
                           "-u",
                           "-f", base_meta[TYPE_TAG],
                           "-b", self.Datapathes[datapath].gen_vol_uri(dbg, new_base_meta[URI_TAG][0]),
                           self.Datapathes[datapath].gen_vol_uri(dbg, base_meta[URI_TAG][0])])

                call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                           "rebase",
                           "-u",
                           "-f", clone_meta[TYPE_TAG],
                           "-b", self.Datapathes[datapath].gen_vol_uri(dbg, new_base_meta[URI_TAG][0]),
                           self.Datapathes[datapath].gen_vol_uri(dbg, clone_meta[URI_TAG][0])])

                self.Datapathes[datapath].unmap_vol(dbg, clone_meta[URI_TAG][0], chained=None)

                if ACTIVE_ON_TAG in base_meta:
                    self.Datapathes[datapath].snapshot(dbg, new_base_meta[URI_TAG][0], base_meta[URI_TAG][0], 0)
                else:
                    self.Datapathes[datapath].unmap_vol(dbg, base_meta[URI_TAG][0], chained=None)

            else:
                # create clone
                clone_meta = self.create(dbg,
                                         sr,
                                         base_meta[NAME_TAG],
                                         base_meta[DESCRIPTION_TAG],
                                         base_meta[VIRTUAL_SIZE_TAG],
                                         base_meta[SHARABLE_TAG])

                self.Datapathes[datapath].map_vol(dbg, clone_meta[URI_TAG][0], chained=None)

                call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                           "rebase",
                           "-u",
                           "-f", base_meta[TYPE_TAG],
                           "-b", self.Datapathes[datapath].gen_vol_uri(dbg, base_meta[URI_TAG][0]),
                           self.Datapathes[datapath].gen_vol_uri(dbg, clone_meta[URI_TAG][0])])

                self.Datapathes[datapath].unmap_vol(dbg, clone_meta[URI_TAG][0], chained=None)

                clone_parent = base_meta[URI_TAG]

            merge(base_meta, clone_meta, snap_merge_pattern)
            clone_meta[PARENT_URI_TAG] = clone_parent

            if mode is 'snapshot':
                clone_meta[READ_WRITE_TAG] = False
            elif mode is 'clone':
                clone_meta[READ_WRITE_TAG] = True

            self.MetadataHandler.update(dbg, clone_meta[URI_TAG][0], clone_meta)

            return clone_meta
        except Exception as e:
            log.error("%s: xcpng.volume.QCOW2Volume._clone: Failed to clone/snapshot volume: key %s: SR: %s" %
                      (dbg, key, sr))
            try:
                self.Datapathes[datapath].unmap_vol(dbg, clone_meta[URI_TAG][0])
                self.Datapathes[datapath].unmap_vol(dbg, base_meta[URI_TAG][0])
            except:
                pass
            raise Exception(e)

    def _create(self, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: xcpng.volume.QCOW2Volume._create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        uri = image_meta[URI_TAG][0]
        datapath = get_vdi_datapath_by_uri(dbg, uri)

        try:
            super(QCOW2Volume, self)._create(dbg, sr, name, description, size, sharable, image_meta)

            self.Datapathes[datapath].map_vol(dbg, uri)

            call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                       "create",
                       "-f", image_meta[TYPE_TAG],
                       self.Datapathes[datapath].gen_vol_path(dbg, uri),
                       str(size)])

            self.Datapathes[datapath].unmap_vol(dbg, uri)

            return image_meta
        except Exception as e:
            log.error("%s: xcpng.volume.QCOW2Volume._create: Failed to create volume: key %s: SR: %s" %
                      (dbg, image_meta[UUID_TAG], sr))
            try:
                self.Datapathes[datapath].unmap_vol(dbg, uri)
            except:
                pass
            raise Exception(e)

    def _resize(self, dbg, sr, key, new_size, image_meta):
        log.debug("%s: xcpng.volume.QCOW2Volume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        uri = image_meta[URI_TAG][0]
        datapath = get_vdi_datapath_by_uri(dbg, uri)

        try:
            super(QCOW2Volume, self)._resize(dbg, sr, key, new_size, image_meta)

            self.Datapathes[datapath].map_vol(dbg, uri)

            call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                       "resize",
                       self.Datapathes[datapath].gen_vol_path(dbg, uri),
                       str(new_size)])

            self.Datapathes[datapath].unmap_vol(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.volume.QCOW2Volume._resize: Failed to resize volume: key %s: SR: %s" %
                      (dbg, image_meta[UUID_TAG], sr))
            try:
                self.Datapathes[datapath].unmap_vol(dbg, uri)
            except:
                pass
            raise Exception(e)


VOLUME_TYPES = {'raw': RAWVolume, 'qcow2': QCOW2Volume}


class Implementation(Volume_skeleton):

    def __init__(self, vol_types):
        super(Implementation, self).__init__()
        self.VolumeTypes = vol_types

    def create(self, dbg, sr, name, description, size, sharable):
        log.debug("%s: xcpng.volume.Implementation.%s: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, name, description, size))

        return self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().create(dbg, sr, name, description, size, sharable)

    def clone(self, dbg, sr, key, mode='clone'):
        log.debug("%s: xcpng.volume.Implementation.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))

        return self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().clone(dbg, sr, key, mode)

    def snapshot(self, dbg, sr, key):

        return self.clone(dbg, sr, key, mode='snapshot')

    def destroy(self, dbg, sr, key):
        log.debug("%s: xcpng.volume.Implementation.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().destroy(dbg, sr, key)

    def set_name(self, dbg, sr, key, new_name):
        log.debug("%s: xcpng.volume.Implementation.%s: SR: %s Key: %s New_name: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, new_name))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().set_name(dbg, sr, key, new_name)

    def set_description(self, dbg, sr, key, new_description):
        log.debug("%s: xcpng.volume.Implementation.%s: SR: %s Key: %s New_description: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, new_description))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().set_description(dbg, sr, key, new_description)

    def set(self, dbg, sr, key, k, v):
        log.debug("%s: xcpng.volume.Implementation.%s: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, k, v))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().set(dbg, sr, key, k, v)

    def unset(self, dbg, sr, key, k):
        log.debug("%s: xcpng.volume.Implementation.%s: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, k))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().unset(dbg, sr, key, k)

    def resize(self, dbg, sr, key, new_size):
        log.debug("%s: xcpng.volume.Implementation.%s: SR: %s Key: %s New_size: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key, new_size))

        self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().resize(dbg, sr, key, new_size)

    def stat(self, dbg, sr, key):
        log.debug("%s: xcpng.volume.Implementation.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))

        return self.VolumeTypes[get_vdi_type_by_uri(dbg, sr)]().stat(dbg, sr, key)

#    def compare(self, dbg, sr, key, key2):

#    def similar_content(self, dbg, sr, key):

#    def enable_cbt(self, dbg, sr, key):

#    def disable_cbt(self, dbg, sr, key):

#    def data_destroy(self, dbg, sr, key):

#    def list_changed_blocks(self, dbg, sr, key, key2, offset, length):
