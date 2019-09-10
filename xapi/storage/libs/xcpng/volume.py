#!/usr/bin/env python

import xapi.storage.libs.xcpng.globalvars
import sys
import uuid
from copy import deepcopy

from xapi.storage.libs.xcpng.utils import call, get_vdi_type_by_uri, get_vdi_datapath_by_uri, module_exists, \
                                          validate_and_round_vhd_size, fullSizeVHD, get_current_host_uuid, \
                                          get_vdi_uuid_by_uri
from xapi.storage.libs.xcpng.meta import KEY_TAG, VDI_UUID_TAG, IMAGE_UUID_TAG, NAME_TAG, PARENT_URI_TAG, \
                                         DESCRIPTION_TAG, READ_WRITE_TAG, VIRTUAL_SIZE_TAG, PHYSICAL_UTILISATION_TAG, \
                                         URI_TAG, SHARABLE_TAG, CUSTOM_KEYS_TAG, TYPE_TAG, ACTIVE_ON_TAG, \
                                         SNAPSHOT_OF_TAG, QEMU_IMAGE_URI_TAG, REF_COUNT_TAG
from xapi.storage.libs.xcpng.meta import MetadataHandler, merge, snap_merge_pattern, clone_merge_pattern
from xapi.storage.libs.xcpng.datapath import DATAPATHES
from xapi.storage import log

import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import Volume_skeleton
    from xapi.storage.api.v4.volume import Activated_on_another_host
elif platform.linux_distribution()[1] == '7.6.0' or platform.linux_distribution()[1] == '8.0.0':
    from xapi.storage.api.v5.volume import Volume_skeleton
    from xapi.storage.api.v5.volume import Activated_on_another_host


class VolumeOperations(object):

    def __init__(self):
        self.MetadataHandler = MetadataHandler()

    def create(self, dbg, uri, size):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def destroy(self, dbg, uri):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def resize(self, dbg, uri, size):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def swap(self, dbg, uri1, uri2):
        log.debug("%s: xcpng.volume.VolumeOperations.swap: uri1: %s uri2: %s" % (dbg, uri1, uri2))
        volume1_meta = self.MetadataHandler.get_vdi_meta(dbg, uri1)
        volume2_meta = self.MetadataHandler.get_vdi_meta(dbg, uri2)
        log.debug("%s: xcpng.volume.VolumeOperations.swap: before image_uuid1: %s image_uudi2: %s" %
                  (dbg, volume1_meta[IMAGE_UUID_TAG], volume2_meta[IMAGE_UUID_TAG]))
        image1_uuid = volume1_meta[IMAGE_UUID_TAG]
        image2_uuid = volume2_meta[IMAGE_UUID_TAG]
        volume1_meta = {IMAGE_UUID_TAG: image2_uuid}
        volume2_meta = {IMAGE_UUID_TAG: image1_uuid}
        log.debug("%s: xcpng.volume.VolumeOperations.swap: after image_uuid1: %s image_uudi2: %s" %
                  (dbg, volume1_meta[IMAGE_UUID_TAG], volume2_meta[IMAGE_UUID_TAG]))
        self.MetadataHandler.update_vdi_meta(dbg, uri1, volume1_meta)
        self.MetadataHandler.update_vdi_meta(dbg, uri2, volume2_meta)

    def get_phisical_utilization(self, dbg, uri):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def roundup_size(self, dbg, size):
        raise NotImplementedError('Override in VolumeOperations specifc class')


plugin_specific_volume = module_exists("xapi.storage.libs.xcpng.lib%s.volume" % xapi.storage.libs.xcpng.globalvars.plugin_type)
if plugin_specific_volume:
    _VolumeOperations_ = getattr(plugin_specific_volume, 'VolumeOperations')
else:
    _VolumeOperations_ = VolumeOperations


class Volume(object):

    def __init__(self):
        self.MetadataHandler = MetadataHandler()
        self.VolOpsHendler = _VolumeOperations_()
        self.Datapathes = DATAPATHES

        for k, v in DATAPATHES.iteritems():
            self.Datapathes[k] = v()

    def _create(self, dbg, sr, name, description, size, sharable, volume_meta):
        raise NotImplementedError('Override in Volume specifc class')

    def create(self, dbg, sr, name, description, size, sharable):
        log.debug("%s: xcpng.volume.Volume.create: SR: %s Name: %s Description: %s Size: %s, Sharable: %s"
                  % (dbg, sr, name, description, size, sharable))

        vdi_uuid = str(uuid.uuid4())
        image_uuid = str(uuid.uuid4())
        vdi_uri = "%s/%s" % (sr, vdi_uuid)

        volume_meta = {
            KEY_TAG: vdi_uuid,
            VDI_UUID_TAG: vdi_uuid,
            IMAGE_UUID_TAG: image_uuid,
            TYPE_TAG: get_vdi_type_by_uri(dbg, vdi_uri),
            NAME_TAG: name,
            DESCRIPTION_TAG: description,
            READ_WRITE_TAG: True,
            VIRTUAL_SIZE_TAG: size,
            PHYSICAL_UTILISATION_TAG: 0,
            URI_TAG: [vdi_uri],
            SHARABLE_TAG: sharable,
            CUSTOM_KEYS_TAG: {}
        }

        try:
            self.MetadataHandler.update_vdi_meta(dbg, vdi_uri, volume_meta)
            volume_meta = self._create(dbg, sr, name, description, size, sharable, volume_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.create: Failed to create volume: key %s: SR: %s" % (dbg, vdi_uuid, sr))
            try:
                self.destroy(dbg, sr, vdi_uuid)
                self.MetadataHandler.remove_vdi_meta(dbg, vdi_uri)
            except:
                pass
            raise Exception(e)

        return volume_meta

    def set(self, dbg, sr, key, k, v):
        log.debug("%s: xcpng.volume.Volume.set: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sr, key, k, v))

        uri = "%s/%s" % (sr, key)

        volume_meta = {CUSTOM_KEYS_TAG: {}}

        try:
            volume_meta[CUSTOM_KEYS_TAG][k] = v
            self.MetadataHandler.update_vdi_meta(dbg, uri, volume_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to set volume param: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def unset(self, dbg, sr, key, k):
        log.debug("%s: xcpng.volume.Volume.unset: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sr, key, k))

        uri = "%s/%s" % (sr, key)

        volume_meta = {CUSTOM_KEYS_TAG: {}}

        try:
            volume_meta[CUSTOM_KEYS_TAG][k] = None
            self.MetadataHandler.update_vdi_meta(dbg, uri, volume_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to unset volume param: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _stat(self, dbg, sr, key, volume_meta):
        # Override in Volume specific class
        return volume_meta

    def stat(self, dbg, sr, key):
        log.debug("%s: xcpng.volume.Volume.stat: SR: %s Key: %s"
                  % (dbg, sr, key))

        uri = "%s/%s" % (sr, key)

        try:
            volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
            volume_meta[PHYSICAL_UTILISATION_TAG] = self.VolOpsHendler.get_phisical_utilization(dbg, uri)
            log.debug("%s: xcpng.volume.Volume.stat: SR: %s Key: %s Metadata: %s"
                      % (dbg, sr, key, volume_meta))
            return self._stat(dbg, sr, key, volume_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.stat: Failed to get volume stat: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _destroy(self, dbg, sr, key):
        # Override in Volume specifc class
        pass

    def destroy(self, dbg, sr, key):
        log.debug("%s: xcpng.volume.Volume.destroy: SR: %s Key: %s"
                  % (dbg, sr, key))

        uri = "%s/%s" % (sr, key)

        try:
            self._destroy(dbg, sr, key)
            self.VolOpsHendler.destroy(dbg, uri)
            self.MetadataHandler.remove_vdi_meta(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.destroy: Failed to destroy volume: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def set_description(self, dbg, sr, key, new_description):
        log.debug("%s: xcpng.volume.Volume.set_description: SR: %s Key: %s New_description: %s"
                  % (dbg, sr, key, new_description))

        uri = "%s/%s" % (sr, key)

        volume_meta = {
            'description': new_description,
        }

        try:
            self.MetadataHandler.update_vdi_meta(dbg, uri, volume_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to set volume description: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def set_name(self, dbg, sr, key, new_name):
        log.debug("%s: xcpng.volume.Volume.set_name: SR: %s Key: %s New_name: %s"
                  % (dbg, sr, key, new_name))

        uri = "%s/%s" % (sr, key)

        volume_meta = {
            'name': new_name,
        }

        try:
            self.MetadataHandler.update_vdi_meta(dbg, uri, volume_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to set volume name: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _resize(self, dbg, sr, key, new_size):
        raise NotImplementedError('Override in Volume specifc class')

    def resize(self, dbg, sr, key, new_size):
        log.debug("%s: xcpng.volume.Volume.resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        uri = "%s/%s" % (sr, key)

        volume_meta = {
            'virtual_size': new_size,
        }

        try:
            self._resize(dbg, sr, key, new_size)
            self.MetadataHandler.update_vdi_meta(dbg, uri, volume_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to resize volume: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _clone(self, dbg, sr, key, mode, volume_meta):
        raise NotImplementedError('Override in Volume specifc class')

    def clone(self, dbg, sr, key, mode):
        log.debug("%s: xcpng.volume.Volume.clone: SR: %s Key: %s Mode: %s"
                  % (dbg, sr, key, mode))

        orig_uri = "%s/%s" % (sr, key)

        try:
            orig_meta = self.MetadataHandler.get_vdi_meta(dbg, orig_uri)

            if SNAPSHOT_OF_TAG in orig_meta[CUSTOM_KEYS_TAG]:
                base_uri = orig_meta[PARENT_URI_TAG][0]
                base_meta = self.MetadataHandler.get_vdi_meta(dbg, base_uri)
            else:
                base_meta = deepcopy(orig_meta)

            if ACTIVE_ON_TAG in base_meta:
                current_host = get_current_host_uuid()
                if base_meta[ACTIVE_ON_TAG] != current_host:
                    log.debug("%s: librbd.Volume.clone: SR: %s Key: %s Can not snapshot on %s as VDI already active on %s"
                              % (dbg, sr, base_meta[VDI_UUID_TAG],
                                 current_host, base_meta[ACTIVE_ON_TAG]))
                    raise Activated_on_another_host(base_meta[ACTIVE_ON_TAG])

            return self._clone(dbg, sr, key, mode, base_meta)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to clone volume: key %s: SR: %s" % (dbg, key, sr))
            raise Exception(e)

    def _commit(self, dbg, sr, child, parent):
        raise NotImplementedError('Coalesce is not supported')

    def _set_parent(self, dbg, sr, child, parent):
        raise NotImplementedError('Coalesce is not supported')

    def commit(self, dbg, sr, child, parent):
        self._commit(dbg, sr, child, parent)

    def set_parent(self, dbg, sr, child, parent):
        self._set_parent(dbg, sr, child, parent)

    def coalesce(self, dbg, sr, key):

        uri = "%s/%s" % (sr, key)

        try:
            volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
            children = self.MetadataHandler.find_vdi_children(dbg, uri)

            self._commit(dbg, sr, uri, volume_meta[PARENT_URI_TAG])

            for child in children:
                self._set_parent(dbg, sr, child[URI_TAG], volume_meta[PARENT_URI_TAG])
                meta = {PARENT_URI_TAG: volume_meta[PARENT_URI_TAG]}
                self.MetadataHandler.update_vdi_meta(dbg, child[URI_TAG], meta)

            self.destroy(dbg, sr, key)
            self.MetadataHandler.remove_vdi_meta(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.volume.Volume.set: Failed to coalesce volume with parent: key %s: SR: %s"
                      % (dbg, key, sr))
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
                      (dbg, image_meta[VDI_UUID_TAG], sr))
            try:
                self.VolOpsHendler.destroy(dbg, uri)
            except:
                pass
            raise Exception(e)

        return image_meta

    def _resize(self, dbg, sr, key, new_size):
        log.debug("%s: xcpng.volume.RAWVolume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        uri = "%s/%s" % (sr, key)

        try:
            self.VolOpsHendler.resize(dbg, uri, self._get_full_vol_size(dbg, new_size))
        except Exception as e:
            log.error("%s: xcpng.volume.RAWVolume._resize: Failed to resize volume: key %s: SR: %s" %
                      (dbg, key, sr))
            raise Exception(e)

    def _clone(self, dbg, sr, key, mode, base_meta):
        raise NotImplementedError('Not implemented in RAWVolume class')


class QCOW2Volume(RAWVolume):

    def _get_full_vol_size(self, dbg, size):
        # TODO: Implement overhead calculation for QCOW2 format
        return self.VolOpsHendler.roundup_size(dbg, fullSizeVHD(validate_and_round_vhd_size(size)))

    def _commit_offline(self, dbg, sr, child, parent):
        datapath = get_vdi_datapath_by_uri(dbg, sr)

        self.Datapathes[datapath].DatapathOpsHandler.map_vol(dbg, child, chained=None)
        self.Datapathes[datapath].DatapathOpsHandler.map_vol(dbg, parent, chained=None)

        call(dbg, ['/usr/lib64/qemu-dp/bin/qemu-img',
                   'commit',
                   '-t', 'none'
                   '-b', self.Datapathes[datapath].DatapathOpsHandler.gen_vol_uri(dbg, parent),
                   '-d',
                   self.Datapathes[datapath].DatapathOpsHandler.gen_vol_uri(dbg, child)])

        self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, child, chained=None)
        self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, parent, chained=None)

    def _commit_online(self, dbg, uri, child, parent):
        datapath = get_vdi_datapath_by_uri(dbg, uri)
        self.Datapathes[datapath].commit(dbg, uri, child, parent, 0)


    def _set_parent_offline(self, dbg, sr, child, parent):
        raise NotImplementedError('Coalesce is not supported')

    def _relink_online(self, dbg, sr, child, parent):
        raise NotImplementedError('Coalesce is not supported')

    def _clone(self, dbg, sr, key, mode, base_meta):
        log.debug("%s: xcpng.volume.QCOW2Volume._clone: SR: %s Key: %s Mode: %s"
                  % (dbg, sr, key, mode))

        swapped = False
        clone_uri_for_exception = None
        new_base_uri_for_exception = None
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

                clone_uri_for_exception = clone_meta[URI_TAG][0]

                # create new base
                new_base_meta = self.create(dbg,
                                            sr,
                                            base_meta[NAME_TAG],
                                            base_meta[DESCRIPTION_TAG],
                                            base_meta[VIRTUAL_SIZE_TAG],
                                            base_meta[SHARABLE_TAG])

                new_base_uri_for_exception = new_base_meta[URI_TAG][0]

                self.Datapathes[datapath].DatapathOpsHandler.map_vol(dbg, clone_meta[URI_TAG][0], chained=None)
                self.Datapathes[datapath].DatapathOpsHandler.map_vol(dbg, new_base_meta[URI_TAG][0], chained=None)

                call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                           "rebase",
                           "-u",
                           "-f", base_meta[TYPE_TAG],
                           "-b", self.Datapathes[datapath].DatapathOpsHandler.gen_vol_uri(dbg, base_meta[URI_TAG][0]),
                           self.Datapathes[datapath].DatapathOpsHandler.gen_vol_uri(dbg, new_base_meta[URI_TAG][0])])

                call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                           "rebase",
                           "-u",
                           "-f", clone_meta[TYPE_TAG],
                           "-b", self.Datapathes[datapath].DatapathOpsHandler.gen_vol_uri(dbg, base_meta[URI_TAG][0]),
                           self.Datapathes[datapath].DatapathOpsHandler.gen_vol_uri(dbg, clone_meta[URI_TAG][0])])

                self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, clone_meta[URI_TAG][0], chained=None)

                # swap backing images uuids for base vdi and new base vdi in metadata
                self.VolOpsHendler.swap(dbg, base_meta[URI_TAG][0], new_base_meta[URI_TAG][0])
                swapped = True
                base_meta = self.MetadataHandler.get_vdi_meta(dbg, base_meta[URI_TAG][0])
                new_base_meta = self.MetadataHandler.get_vdi_meta(dbg, new_base_meta[URI_TAG][0])
                clone_meta = self.MetadataHandler.get_vdi_meta(dbg, clone_meta[URI_TAG][0])

                merge(base_meta, new_base_meta, snap_merge_pattern)
                new_base_meta[NAME_TAG] = "(base) %s" % new_base_meta[NAME_TAG]
                new_base_meta[READ_WRITE_TAG] = False
                base_meta[PARENT_URI_TAG] = new_base_meta[URI_TAG]
                base_meta[REF_COUNT_TAG] = 1
                clone_parent = new_base_meta[URI_TAG]

                self.MetadataHandler.update_vdi_meta(dbg, new_base_meta[URI_TAG][0], new_base_meta)
                self.MetadataHandler.update_vdi_meta(dbg, base_meta[URI_TAG][0], base_meta)

                if ACTIVE_ON_TAG in base_meta:
                    base_meta[QEMU_IMAGE_URI_TAG] = self.Datapathes[datapath].DatapathOpsHandler.gen_vol_uri(dbg, base_meta[URI_TAG][0])
                    self.MetadataHandler.update_vdi_meta(dbg, base_meta[URI_TAG][0], base_meta)
                    self.Datapathes[datapath].snapshot(dbg, base_meta[URI_TAG][0], base_meta[URI_TAG][0], 0)
                else:
                    self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, base_meta[URI_TAG][0], chained=None)

            else:
                # create clone
                clone_meta = self.create(dbg,
                                         sr,
                                         base_meta[NAME_TAG],
                                         base_meta[DESCRIPTION_TAG],
                                         base_meta[VIRTUAL_SIZE_TAG],
                                         base_meta[SHARABLE_TAG])

                clone_uri_for_exception = clone_meta[URI_TAG][0]

                self.Datapathes[datapath].DatapathOpsHandler.map_vol(dbg, clone_meta[URI_TAG][0], chained=None)

                call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                           "rebase",
                           "-u",
                           "-f", base_meta[TYPE_TAG],
                           "-b", self.Datapathes[datapath].DatapathOpsHandler.gen_vol_uri(dbg, base_meta[URI_TAG][0]),
                           self.Datapathes[datapath].DatapathOpsHandler.gen_vol_uri(dbg, clone_meta[URI_TAG][0])])

                self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, clone_meta[URI_TAG][0], chained=None)

                clone_parent = base_meta[URI_TAG]

            merge(base_meta, clone_meta, clone_merge_pattern)
            clone_meta[PARENT_URI_TAG] = clone_parent

            if mode is 'snapshot':
                clone_meta[READ_WRITE_TAG] = False
            elif mode is 'clone':
                clone_meta[READ_WRITE_TAG] = True

            self.MetadataHandler.update_vdi_meta(dbg, clone_meta[URI_TAG][0], clone_meta)

            return clone_meta
        except Exception as e:
            log.error("%s: xcpng.volume.QCOW2Volume._clone: Failed to clone/snapshot volume: key %s: SR: %s" %
                      (dbg, key, sr))
            try:
                if clone_uri_for_exception is not None:
                    self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, clone_uri_for_exception)
            except:
                pass

            if clone_uri_for_exception is not None:
                self.destroy(dbg, sr, get_vdi_uuid_by_uri(dbg, clone_uri_for_exception))

            if swapped is True:
                self.VolOpsHendler.swap(dbg, new_base_uri_for_exception, base_meta[URI_TAG][0])

            try:
                if new_base_uri_for_exception is not None:
                    self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, new_base_uri_for_exception)
            except:
                pass

            if new_base_uri_for_exception is not None:
                self.destroy(dbg, sr, get_vdi_uuid_by_uri(dbg, new_base_uri_for_exception))

            raise Exception(e)

    def _create(self, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: xcpng.volume.QCOW2Volume._create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        uri = image_meta[URI_TAG][0]
        datapath = get_vdi_datapath_by_uri(dbg, uri)

        try:
            super(QCOW2Volume, self)._create(dbg, sr, name, description, size, sharable, image_meta)

            self.Datapathes[datapath].DatapathOpsHandler.map_vol(dbg, uri)

            call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                       "create",
                       "-f", image_meta[TYPE_TAG],
                       self.Datapathes[datapath].DatapathOpsHandler.gen_vol_path(dbg, uri),
                       str(size)])

            self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, uri)

            return image_meta
        except Exception as e:
            log.error("%s: xcpng.volume.QCOW2Volume._create: Failed to create volume: key %s: SR: %s" %
                      (dbg, image_meta[VDI_UUID_TAG], sr))
            try:
                self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, uri)
            except:
                pass
            raise Exception(e)

    def _resize(self, dbg, sr, key, new_size):
        log.debug("%s: xcpng.volume.QCOW2Volume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        uri = "%s/%s" % (sr, key)
        datapath = get_vdi_datapath_by_uri(dbg, uri)

        try:
            super(QCOW2Volume, self)._resize(dbg, sr, key, new_size)

            self.Datapathes[datapath].DatapathOpsHandler.map_vol(dbg, uri)

            call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                       "resize",
                       self.Datapathes[datapath].DatapathOpsHandler.gen_vol_path(dbg, uri),
                       str(new_size)])

            self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.volume.QCOW2Volume._resize: Failed to resize volume: key %s: SR: %s" %
                      (dbg, key, sr))
            try:
                self.Datapathes[datapath].DatapathOpsHandler.unmap_vol(dbg, uri)
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
