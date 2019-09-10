#!/usr/bin/env python

import consul
from time import time, sleep
from xapi.storage import log
from xapi.storage.libs.xcpng.utils import get_sr_uuid_by_uri, get_vdi_uuid_by_uri
from xapi.storage.libs.xcpng.meta import LocksOpsMgr as _LocksOpsMgr_

CONSUL_KV_LOCKS_PREFIX = 'locks/'

class LocksOpsMgr(_LocksOpsMgr_):

    def __init__(self):
        super(LocksOpsMgr, self).__init__()
        self.__consul = consul.Consul()

    def lock(self, dbg, uri, timeout=10):
        log.debug("%s: xcpng.cluster-stack.locks.lock: uri: %s timeout %s" % (dbg, uri, timeout))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        if vdi_uuid is not None:
            lock_uuid = vdi_uuid
        else:
            lock_uuid = sr_uuid

        start_time = time()
        lh = [None, None, None]

        if self.__consul.kv.get(key="%s%s" % (CONSUL_KV_LOCKS_PREFIX, sr_uuid))[1] is not None:
            # SR is locked
            raise Exception('SR is locked')

        try:
            while True:
                try:
                    if lock_uuid in self.__lhs:
                        raise Exception('Already locked')

                    session_id = self.__consul.session.create()
                    if not self.__consul.kv.put(key="%s%s" % (CONSUL_KV_LOCKS_PREFIX, lock_uuid), acquire=session_id):
                        raise Exception('Already locked')

                    self.__lhs[lock_uuid] = session_id
                    break
                except Exception as e:
                    if time() - start_time >= timeout:
                        log.error("%s: xcpng.cluster-stack.locks.lock: Failed to lock: uri: %s" % (dbg, uri))
                        raise Exception(e)
                    sleep(1)
                    pass
        except Exception as e:
            log.error("%s: xcpng.librbd.meta.MetaDBOpeations.lock: Failed to lock: uri: %s"
                      % (dbg, uri))
            raise Exception(e)

    def unlock(self, dbg, uri):
        log.debug("%s: xcpng.cluster-stack.locks.unlock: uri: %s" % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        if vdi_uuid is not None:
            lock_uuid = vdi_uuid
        else:
            lock_uuid = sr_uuid

        if lock_uuid in self.__lhs:
            session_id = self.__lhs[lock_uuid]
            self.__consul.session.destroy(session_id)
            del self.__lhs[lock_uuid]