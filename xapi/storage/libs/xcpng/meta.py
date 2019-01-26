#!/usr/bin/env python

from xapi.storage import log


# define tags for metadata
UUID_TAG = 'uuid'
SR_UUID_TAG = 'sr_uuid'
TYPE_TAG = 'vdi_type'
KEY_TAG = 'key'
NAME_TAG = 'name'
PATH_TAG = 'path'
QEMU_URI_TAG = 'qemu_uri'
DESCRIPTION_TAG = 'description'
CONFIGURATION_TAG = 'configuration'
READ_WRITE_TAG = 'read_write'
VIRTUAL_SIZE_TAG = 'virtual_size'
PHYSICAL_UTILISATION_TAG = 'physical_utilisation'
URI_TAG = 'uri'
CUSTOM_KEYS_TAG = 'keys'
SHARABLE_TAG = 'sharable'
NON_PERSISTENT_TAG = 'nonpersistent'
QEMU_PID_TAG = 'qemu_pid'
QEMU_QMP_SOCK_TAG = 'qemu_qmp_sock'
QEMU_NBD_SOCK_TAG = 'qemu_nbd_sock'
QEMU_QMP_LOG_TAG = 'qemu_qmp_log'
ACTIVE_ON_TAG = 'active_on'
SNAPSHOT_OF_TAG = 'snapshot_of'
IS_A_SNAPSHOT_TAG = 'is_a_snapshot'
IMAGE_FORMAT_TAG = 'image-format'
DATAPATH_TAG = 'datapath'
CEPH_CLUSTER_TAG = 'cluster'
PARENT_URI_TAG = 'parent'
REF_COUNT_TAG = 'ref_count'

# define tag types
TAG_TYPES = {
    UUID_TAG: str,
    SR_UUID_TAG: str,
    TYPE_TAG: str,
    KEY_TAG: str,
    NAME_TAG: str,
    PATH_TAG: str,
    QEMU_URI_TAG: str,
    DESCRIPTION_TAG: str,
    CONFIGURATION_TAG: eval,  # dict
    READ_WRITE_TAG: eval,  # boolean
    VIRTUAL_SIZE_TAG: int,
    PHYSICAL_UTILISATION_TAG: int,
    URI_TAG: eval,  # string list
    CUSTOM_KEYS_TAG: eval,  # dict
    SHARABLE_TAG: eval,  # boolean
    NON_PERSISTENT_TAG: eval,
    QEMU_PID_TAG: int,
    QEMU_QMP_SOCK_TAG: str,
    QEMU_NBD_SOCK_TAG: str,
    QEMU_QMP_LOG_TAG: str,
    ACTIVE_ON_TAG: str,
    SNAPSHOT_OF_TAG: str,
    IMAGE_FORMAT_TAG: str,
    DATAPATH_TAG: str,
    CEPH_CLUSTER_TAG: str,
    PARENT_URI_TAG: str,
    REF_COUNT_TAG: int
}


class MetadataHandler(object):

    def _create(self, dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specific class')

    def _destroy(self, dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specific class')

    def _remove(self, dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specific class')

    def _load(self, dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specific class')

    def _update(self, dbg, uri, image_meta):
        raise NotImplementedError('Override in MetadataHandler specific class')

    def _get_vdi_chain(self, dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specific class')

    def create(self, dbg, uri):
        log.debug("%s: meta.MetadataHandler.create: uri: %s "
                  % (dbg, uri))

        return self._create(dbg, uri)

    def destroy(self, dbg, uri):
        log.debug("%s: meta.MetadataHandler.create: uri: %s "
                  % (dbg, uri))

        return self._destroy(dbg, uri)

    def remove(self, dbg, uri):
        log.debug("%s: meta.MetadataHandler.remove: uri: %s "
                  % (dbg, uri))

        return self._remove(dbg, uri)

    def load(self, dbg, uri):
        log.debug("%s: meta.MetadataHandler.load: uri: %s "
                  % (dbg, uri))

        return self._load(dbg, uri)

    def update(self, dbg, uri, image_meta):
        log.debug("%s: meta.MetadataHandler.update: uri: %s "
                  % (dbg, uri))

        self._update(dbg, uri, image_meta)

    def get_vdi_chain(self, dbg, uri):
        log.debug("%s: meta.MetadataHandler.get_vdi_chain: uri: %s "
                  % (dbg, uri))

        return self._get_vdi_chain(dbg, uri)
