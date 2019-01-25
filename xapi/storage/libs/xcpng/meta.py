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
IMAGE_FORMAT_TAG = 'image-format'
DATAPATH_TAG = 'datapath'
CEPH_CLUSTER_TAG = 'cluster'

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
    CEPH_CLUSTER_TAG: str
}


class MetadataHandler(object):

    @staticmethod
    def _create(dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @staticmethod
    def _destroy(dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @staticmethod
    def _remove(dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @staticmethod
    def _load(dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @staticmethod
    def _update(dbg, uri, image_meta):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @classmethod
    def create(cls, dbg, uri):
        log.debug("%s: meta.MetadataHandler.create: uri: %s "
                  % (dbg, uri))

        return cls._create(dbg, uri)

    @classmethod
    def destroy(cls, dbg, uri):
        log.debug("%s: meta.MetadataHandler.create: uri: %s "
                  % (dbg, uri))

        return cls._destroy(dbg, uri)

    @classmethod
    def remove(cls, dbg, uri):
        log.debug("%s: meta.MetadataHandler.remove: uri: %s "
                  % (dbg, uri))

        return cls._remove(dbg, uri)

    @classmethod
    def load(cls, dbg, uri):
        log.debug("%s: meta.MetadataHandler.load: uri: %s "
                  % (dbg, uri))

        return cls._load(dbg, uri)

    @classmethod
    def update(cls, dbg, uri, image_meta):
        log.debug("%s: meta.MetadataHandler.update: uri: %s "
                  % (dbg, uri))

        cls._update(dbg, uri, image_meta)
