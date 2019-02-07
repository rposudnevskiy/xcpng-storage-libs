#!/usr/bin/env python

from tinydb import TinyDB, Query, where
from tinydb.operations import delete

from xapi.storage import log
from xapi.storage.libs.xcpng.utils import get_sr_uuid_by_uri, get_vdi_uuid_by_uri

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
    PARENT_URI_TAG: str,
    REF_COUNT_TAG: int
}


snap_merge_pattern = (CUSTOM_KEYS_TAG,
                      PHYSICAL_UTILISATION_TAG)


def merge(src, dst, pattern):
    for key in pattern:
        if key in src:
            dst[key] = src[key]


class MetaDBOperations(object):

    def create(self, dbg, uri):
        raise NotImplementedError('Override in MetaDBOperations specific class')

    def destroy(self, dbg, uri):
        raise NotImplementedError('Override in MetaDBOperations specific class')

    def load(self, dbg, uri):
        raise NotImplementedError('Override in MetaDBOperations specific class')

    def dump(self, dbg, uri, db):
        raise NotImplementedError('Override in MetaDBOperations specific class')

    def lock(self, dbg, uri):
        raise NotImplementedError('Override in MetaDBOperations specific class')

    def unlock(self, dbg, uri):
        raise NotImplementedError('Override in MetaDBOperations specific class')


class MetadataHandler(object):

    def __init__(self):
        self.MetaDBOpsHendler = MetaDBOperations()

    def create(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.create: uri: %s " % (dbg, uri))
        try:
            self.MetaDBOpsHendler.create(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.create: Failed to create metadata database: uri: %s " % (dbg, uri))
            raise Exception(e)

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.destroy: uri: %s " % (dbg, uri))
        try:
            self.MetaDBOpsHendler.destroy(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.destroy: Failed to destroy metadata database: uri: %s " % (dbg, uri))
            raise Exception(e)

    def remove(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.remove: uri: %s " % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        if vdi_uuid != '':
            table_name = 'vdis'
            uuid_tag = UUID_TAG
            uuid = vdi_uuid
        else:
            table_name = 'sr'
            uuid_tag = SR_UUID_TAG
            uuid = sr_uuid

        try:
            self.MetaDBOpsHendler.lock(dbg, uri)
            db = self.MetaDBOpsHendler.load(dbg, uri)
            table = db.table(table_name)
            table.remove(where(uuid_tag) == uuid)
            self.MetaDBOpsHendler.dump(dbg, uri, db)
            self.MetaDBOpsHendler.unlock(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.remove: Failed to remove metadata for uri: %s " % (dbg, uri))
            raise Exception(e)

    def load(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.load: uri: %s " % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        if vdi_uuid != '':
            table_name = 'vdis'
            uuid_tag = UUID_TAG
            uuid = vdi_uuid
        else:
            table_name = 'sr'
            uuid_tag = SR_UUID_TAG
            uuid = sr_uuid

        try:
            db = self.MetaDBOpsHendler.load(dbg, uri)
            table = db.table(table_name)

            if uuid_tag == SR_UUID_TAG and uuid == '12345678-1234-1234-1234-123456789012':
                meta = table.all()[0]
            else:
                meta = table.search(where(uuid_tag) == uuid)[0]

            return meta
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.load: Failed to load metadata for uri: %s " % (dbg, uri))
            raise Exception(e)

    def update(self, dbg, uri, image_meta):
        log.debug("%s: xcpng.meta.MetadataHandler.update: uri: %s" % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        if vdi_uuid != '':
            table_name = 'vdis'
            uuid_tag = UUID_TAG
            uuid = vdi_uuid
        else:
            table_name = 'sr'
            uuid_tag = SR_UUID_TAG
            uuid = sr_uuid

        try:
            self.MetaDBOpsHendler.lock(dbg, uri)
            db = self.MetaDBOpsHendler.load(dbg, uri)
            table = db.table(table_name)

            if table.search(Query()[uuid_tag] == uuid):
                for tag, value in image_meta.iteritems():
                    if value is None:
                        log.debug("%s: xcpng.meta.MetadataHandler._update_meta: tag: %s remove value" % (dbg, tag))
                        table.update(delete(tag), Query()[uuid_tag] == uuid)
                    else:
                        log.debug("%s: xcpng.meta.MetadataHandler._update_meta: tag: %s set value: %s" % (dbg, tag, value))
                        table.update({tag: value}, Query()[uuid_tag] == uuid)
            else:
                table.insert(image_meta)

            self.MetaDBOpsHendler.dump(dbg, uri, db)
            self.MetaDBOpsHendler.unlock(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.update: Failed to update metadata for uri: %s " % (dbg, uri))
            raise Exception(e)

    def get_vdi_chain(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.get_vdi_chain: uri: %s" % (dbg, uri))

        vdi_chain = []
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        try:
            db = self.MetaDBOpsHendler.load(dbg, uri)
            table = db.table('vdis')

            while True:
                image_meta = table.search(where(UUID_TAG) == vdi_uuid)[0]

                if PARENT_URI_TAG in image_meta:
                    vdi_chain.append(image_meta[PARENT_URI_TAG])
                    vdi_uuid = get_vdi_uuid_by_uri(dbg, image_meta[PARENT_URI_TAG])
                else:
                    break

            return vdi_chain
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.get_vdi_chain: Failed to get vdi chain for uri: %s " % (dbg, uri))
            raise Exception(e)
