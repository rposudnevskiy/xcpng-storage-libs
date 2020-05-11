#!/usr/bin/env python

import xapi.storage.libs.xcpng.globalvars
import atexit

from tinydb import TinyDB, Query, where
from tinydb.operations import delete
from tinydb.storages import MemoryStorage as Storage
from tinydb.database import StorageProxy
from json import dumps, loads

from xapi.storage import log
from xapi.storage.libs.xcpng.utils import get_sr_uuid_by_uri, get_vdi_uuid_by_uri, module_exists

# define tags for metadata
VDI_UUID_TAG = 'uuid'
IMAGE_UUID_TAG = 'image_uuid'
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
QEMU_IMAGE_URI_TAG = 'qemu_img_uri'
ACTIVE_ON_TAG = 'active_on'
SNAPSHOT_OF_TAG = 'snapshot_of'
IS_A_SNAPSHOT_TAG = 'is_a_snapshot'
IMAGE_FORMAT_TAG = 'image-format'
DATAPATH_TAG = 'datapath'
PARENT_URI_TAG = 'parent'
REF_COUNT_TAG = 'ref_count'

# define tag types
TAG_TYPES = {
    VDI_UUID_TAG: str,
    IMAGE_UUID_TAG: str,
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
    QEMU_IMAGE_URI_TAG: str,
    ACTIVE_ON_TAG: str,
    SNAPSHOT_OF_TAG: str,
    IMAGE_FORMAT_TAG: str,
    DATAPATH_TAG: str,
    PARENT_URI_TAG: str,
    REF_COUNT_TAG: int
}


snap_merge_pattern = (CUSTOM_KEYS_TAG,
                      PHYSICAL_UTILISATION_TAG,
                      PARENT_URI_TAG,
                      REF_COUNT_TAG)

clone_merge_pattern = (CUSTOM_KEYS_TAG,
                      PHYSICAL_UTILISATION_TAG,
                      PARENT_URI_TAG)

def merge(src, dst, pattern):
    for key in pattern:
        if key in src:
            dst[key] = src[key]
        else:
            if key in dst:
                dst[key] = None


class MetaDBOperations(object):

    def create(self, dbg, uri, db, size=0):
        raise NotImplementedError('Override in MetaDBOperations specific class')

    def destroy(self, dbg, uri):
        raise NotImplementedError('Override in MetaDBOperations specific class')

    def load(self, dbg, uri):
        raise NotImplementedError('Override in MetaDBOperations specific class')

    def dump(self, dbg, uri, db):
        raise NotImplementedError('Override in MetaDBOperations specific class')


class LocksOpsMgr(object):

    def __init__(self):
        self.__lhs = {}

    def lock(self, dbg, uri, timeout=10):
        raise NotImplementedError('Override in MetaDBOperations specific class')

    def unlock(self, dbg, uri):
        raise NotImplementedError('Override in MetaDBOperations specific class')


plugin_specific_metadb_ops = module_exists("xapi.storage.libs.xcpng.lib%s.meta" %
                                           xapi.storage.libs.xcpng.globalvars.plugin_type)
if plugin_specific_metadb_ops:
    _MetaDBOperations_ = getattr(plugin_specific_metadb_ops, 'MetaDBOperations')
else:
    _MetaDBOperations_ = MetaDBOperations

plugin_specific_metadb_cs = module_exists("xapi.storage.libs.xcpng.cluster_stack.%s.tinydb_storage"
                                          % xapi.storage.libs.xcpng.globalvars.cluster_stack)
if plugin_specific_metadb_cs:
    _Storage_ = getattr(plugin_specific_metadb_cs, 'Storage')
    _StorageProxy_ = getattr(plugin_specific_metadb_cs, 'StorageProxy')
else:
    _Storage_ = Storage
    _StorageProxy_ = StorageProxy

plugin_specific_metadb_lk = module_exists("xapi.storage.libs.xcpng.cluster_stack.%s.locks"
                                          % xapi.storage.libs.xcpng.globalvars.cluster_stack)
if plugin_specific_metadb_lk:
    _LocksOpsMgr_ = getattr(plugin_specific_metadb_lk, 'LocksOpsMgr')
else:
    plugin_specific_metadb_lk = module_exists("xapi.storage.libs.xcpng.lib%s.locks" %
                                              xapi.storage.libs.xcpng.globalvars.plugin_type)
    if plugin_specific_metadb_lk:
        _LocksOpsMgr_ = getattr(plugin_specific_metadb_lk, 'LocksOpsMgr')
    else:
        _LocksOpsMgr_ = LocksOpsMgr


class MetadataHandler(object):

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls,'_inst'):
            cls._inst = super(MetadataHandler, cls).__new__(cls, *args, **kwargs)
        else:
            def init_pass(self, *dt, **mp): pass
            cls.__init__ = init_pass

        log.debug("xcpng.meta.MetadataHandler.__new___: %s : %s " % (cls, cls._inst))
        return cls._inst

    def __init__(self):
        log.debug("xcpng.meta.MetadataHandler.__init___")
        self.db = TinyDB('srs_meta', storage=_Storage_, storage_proxy_class=_StorageProxy_, default_table='sr')
        self.__loaded = False
        self.__locked = False
        self.__updated = False
        self.__uri = None
        self.__dbg = None

        self.MetaDBOpsHandler = _MetaDBOperations_()
        self.LocksOpshandler = _LocksOpsMgr_()

        atexit.register(self.__on_exit)

    def __on_exit(self):
        log.debug("xcpng.meta.MetadataHandler.__on_exit")
        if self.__updated:
            self.__dump(self.__dbg, self.__uri)
        if self.__locked:
            self.unlock(self.__dbg, self.__uri)

    def create(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.create: uri: %s " % (dbg, uri))
        try:
            self.MetaDBOpsHandler.create(dbg, uri, '{"sr": {}, "vdis": {}}')
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.create: Failed to create metadata database: uri: %s " % (dbg, uri))
            raise Exception(e)

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.destroy: uri: %s " % (dbg, uri))
        try:
            self.MetaDBOpsHandler.destroy(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.destroy: Failed to destroy metadata database: uri: %s " % (dbg, uri))
            raise Exception(e)

    def __load(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.__load: uri: %s " % (dbg, uri))
        self.__uri = uri
        self.__dbg = dbg
        try:
            self.db._storage.set_db_name(get_sr_uuid_by_uri(dbg, uri))
            if self.db._storage.is_loaded:
                self.__loaded = True
            else:
                self.db._storage.load(loads(self.MetaDBOpsHandler.load(dbg, uri)))
                self.__loaded = True
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.load: Failed to load metadata" % dbg)
            raise Exception(e)

    def load(self, dbg, uri):
        self.__load(dbg, uri)

    def __dump(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.__dump: uri: %s " % (dbg, uri))
        try:
            self.db._storage.set_db_name(get_sr_uuid_by_uri(dbg, uri))
            self.MetaDBOpsHandler.dump(dbg, uri, dumps(self.db._storage.read(), default=dict))
            self.__updated = False
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.__dump: Failed to dump metadata" % dbg)
            raise Exception(e)

    def dump(self, dbg, uri):
        self.__dump(dbg, uri)

    def lock(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.lock: uri: %s " % (dbg, uri))
        try:
            self.LocksOpshandler.lock(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.lock: Failed to lock metadata DB" % dbg)
            raise Exception(e)

    def unlock(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.unlock: uri: %s " % (dbg, uri))
        try:
            self.LocksOpshandler.unlock(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.unlock: Failed to unlock metadata DB" % dbg)
            raise Exception(e)

    def update_vdi_meta(self, dbg, uri, meta):
        log.debug("%s: xcpng.meta.MetadataHandler.update_vdi_meta: uri: %s " % (dbg, uri))

        if self.__loaded is False:
            self.__load(dbg, uri)

        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)
        sr_uuid = get_sr_uuid_by_uri(dbg, uri)

        if vdi_uuid == '':
            raise('Incorrect VDI uri')

        self.db._storage.set_db_name(sr_uuid)
        self.__update(dbg, vdi_uuid, 'vdis', meta)

    def update_sr_meta(self, dbg, uri, meta):
        log.debug("%s: xcpng.meta.MetadataHandler.update_sr_meta: uri: %s " % (dbg, uri))

        if self.__loaded is False:
            self.__load(dbg, uri)

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)

        if sr_uuid == '':
            raise Exception('Incorrect SR uri')

        self.db._storage.set_db_name(sr_uuid)
        self.__update(dbg, sr_uuid, 'sr', meta)

    def __update(self, dbg, uuid, table_name, meta):
        log.debug("%s: xcpng.meta.MetadataHandler.__update: uuid: %s table_name: %s meta: %s"
                  % (dbg, uuid, table_name, meta))

        if table_name == 'sr':
            uuid_tag = SR_UUID_TAG
        elif table_name == 'vdis':
            uuid_tag = VDI_UUID_TAG
        else:
            raise Exception('Incorrect table name')

        table = self.db.table(table_name)

        try:
            if table.contains(Query()[uuid_tag] == uuid):
                for tag, value in meta.iteritems():
                    if value is None:
                        log.debug("%s: xcpng.meta.MetadataHandler.__update: tag: %s remove value" % (dbg, tag))
                        table.update(delete(tag), Query()[uuid_tag] == uuid)
                    else:
                        log.debug("%s: xcpng.meta.MetadataHandler.__update: tag: %s set value: %s" % (dbg, tag, value))
                        table.update({tag: value}, Query()[uuid_tag] == uuid)
            else:
                table.insert(meta)
            self.__updated = True
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler._update: Failed to update metadata" % dbg)
            raise Exception(e)

    def remove_vdi_meta(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.remove_vdi_meta: uri: %s " % (dbg, uri))

        if self.__loaded is False:
            self.__load(dbg, uri)

        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        if vdi_uuid == '':
            raise('Incorrect VDI uri')

        self.db._storage.set_db_name(get_sr_uuid_by_uri(dbg, uri))
        self.__remove(dbg, vdi_uuid, 'vdis')

    def __remove(self, dbg, uuid, table_name):
        log.debug("%s: xcpng.meta.MetadataHandler.__remove: uuid: %s table_name: %s" % (dbg, uuid, table_name))

        if table_name == 'sr':
            uuid_tag = SR_UUID_TAG
        elif table_name == 'vdis':
            uuid_tag = VDI_UUID_TAG
        else:
            raise Exception('Incorrect table name')

        table = self.db.table(table_name)

        try:
            table.remove(where(uuid_tag) == uuid)
            self.__updated = True
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler._remove: Failed to remove metadata" % dbg)
            raise Exception(e)

    def get_vdi_meta(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.get_vdi_meta: uri: %s " % (dbg, uri))

        if self.__loaded is False:
            self.__load(dbg, uri)

        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)
        sr_uuid = get_sr_uuid_by_uri(dbg, uri)

        if vdi_uuid == '':
            raise('Incorrect VDI uri')

        self.db._storage.set_db_name(sr_uuid)
        return self.__get_meta(dbg, vdi_uuid, 'vdis')

    def get_sr_meta(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.get_sr_meta: uri: %s " % (dbg, uri))

        if self.__loaded is False:
            self.__load(dbg, uri)

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)

        if sr_uuid == '':
            raise Exception('Incorrect SR uri')

        self.db._storage.set_db_name(sr_uuid)
        return self.__get_meta(dbg, sr_uuid, 'sr')

    def __get_meta(self, dbg, uuid, table_name):
        log.debug("%s: xcpng.meta.MetadataHandler.__get_meta: uuid: %s table_name: %s" % (dbg, uuid, table_name))

        if table_name == 'sr':
            uuid_tag = SR_UUID_TAG
        elif table_name == 'vdis':
            uuid_tag = VDI_UUID_TAG
        else:
            raise Exception('Incorrect table name')

        table = self.db.table(table_name)
        try:
            if uuid_tag == SR_UUID_TAG and uuid == '12345678-1234-1234-1234-123456789012':
                meta = table.all()[0]
            else:
                meta = table.search(where(uuid_tag) == uuid)[0]
            return meta
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.__get_meta: Failed to get metadata" % dbg)
            raise Exception(e)

    def find_vdi_children(self, dbg, uri):
        log.debug("%s: xcpng.meta.MetadataHandler.find_vdi_children: uri: %s" % (dbg, uri))

        if self.__loaded is False:
            self.__load(dbg, uri)

        self.db._storage.set_db_name(get_sr_uuid_by_uri(dbg, uri))

        try:
            table = self.db.table('vdis')
            return table.search(where(PARENT_URI_TAG) == get_vdi_uuid_by_uri(dbg, uri))
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.find_vdi_children: Failed to find "
                      "children for uri: %s " % (dbg, uri))
            raise Exception(e)

    def find_coalesceable_pairs(self, dbg, sr):
        log.debug("%s: xcpng.meta.MetadataHandler.find_coalesceable pairs: sr: %s" % (dbg, sr))

        if self.__loaded is False:
            self.__load(dbg, sr)

        pairs = []

        self.db._storage.set_db_name(get_sr_uuid_by_uri(dbg, sr))

        table = self.db.table('vdis')

        try:
            roots = table.search(~ (where(PARENT_URI_TAG).exists()))
            while len(roots) != 0:
                _roots_ = []
                for root in roots:
                    children = table.search(where(PARENT_URI_TAG) == root[KEY_TAG])
                    if len(children) == 1:
                        pairs.append((root, children[0]))
                    elif len(children) > 1:
                        _roots_.extend(children)
                roots = _roots_
            return pairs
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.find_coalesceable_pairs: Failed to find "
                      "coalesceable pairs for sr: %s " % (dbg, sr))
            raise Exception(e)
