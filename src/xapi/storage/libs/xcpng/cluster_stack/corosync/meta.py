
from xapi.storage.libs.xcpng.meta import MetadataHandler as _MetadataHandler_
from xapi.storage import log
from xapi.storage.libs.xcpng.corosync.cpg import CPG
from json import dumps, loads

#-- CorosyncMetadataHandler Modes --#
PRIMARY   = 1
SECONDARY = 2
BOOTSTRAP = 3

#TODO: Определить single server или pool
#TODO: Загрузить тип clusterstack из json файла плагина
#TODO: Если single server - то используем стандартный MetadataHandler, если pool - то MetadataHandler соответствующий clusterstack
#TODO: Splitbrain https://serverfault.com/questions/908434/corosync-ha-preventing-split-brain-scenario


class MetadataHandler(_MetadataHandler_):

    def __init__(self):
        log.debug('xcpng.corosync.meta.MetadataHandler.__init___')
        super(MetadataHandler, self).__init__()
        self.bootstrap_cpg = CPG('xcpng.meta.bootstrap')
        self.members_cpg = CPG('xcpng.meta.members')
        self.bootstrap_cpg.message_delivered = self.__on_message_bootstrap_cpg
        self.bootstrap_cpg.configuration_changed = self.__on_cpg_change_bootstrap_cpg
        self.members_cpg.message_delivered = self.__on_message_members_cpg
        self.members_cpg.configuration_changed = self.__on_cpg_change_members_cpg
        self.bootstrap_cpg.start()
        self._mode = BOOTSTRAP
        self._master = None

    def __on_exit(self):
        log.debug('xcpng.corosync.meta.MetadataHandler.__on_exit')
        self.members_cpg.stop()
        self.bootstrap_cpg.stop()
        super(MetadataHandler, self).__on_exit()

    def __bootstrap(self):
        log.debug('xcpng.corosync.meta.MetadataHandler.__bootstrap')
        message = dumps({'bootstrap': self.db})
        self.bootstrap_cpg.send_message(message)

    def __on_message_bootstrap_cpg(self, addr, message):
        log.debug('xcpng.corosync.meta.MetadataHandler.__on_message_bootstrap_cpg')
        _dict_message = loads(message)
        if 'bootstrap' in _dict_message.keys():
            if self._mode == BOOTSTRAP:
                self.db = _dict_message['bootstrap']
                self._master = addr[0]
                self.members_cpg.start()
                self._mode = SECONDARY

    def __on_cpg_change_members_cpg(self, members, left, joined):
        log.debug('xcpng.corosync.meta.MetadataHandler.__on_cpg_change_members_cpg')
        if len(left) > 0:
            if self.members_cpg.local_nodeid not in set(node[0] for node in left):
                if self._master in set(node[0] for node in left):
                    if self._mode == SECONDARY:
                        least_id = None
                        for member in set(node[0] for node in members):  # select member with the least id
                            if least_id is None:
                                least_id = member
                            elif member < least_id:
                                least_id = member
                        if self.members_cpg.local_nodeid == least_id: # Current instance has the least node id and become the primary
                            self._mode = PRIMARY
                        else:
                            self._master = least_id

    def __on_cpg_change_bootstrap_cpg(self, members, left, joined):
        log.debug('xcpng.corosync.meta.MetadataHandler.__on_cpg_change_bootstrap_cpg')
        if len(joined) > 0:
            if self.bootstrap_cpg.local_nodeid in set(node[0] for node in joined):
                if len(members) == len(joined) : # there is(are) not members yet
                    if len(joined) > 1: # Two or more first members joined
                        least_id = None
                        for member in set(node[0] for node in joined): # select joined member with the least id
                            if least_id is None:
                                least_id = member
                            elif member[0] < least_id:
                                least_id = member
                        if self.bootstrap_cpg.local_nodeid == least_id: # Current instance has the least node id and become the primary
                            self._mode = PRIMARY
                            self.load()
                            self.members_cpg.start()
                            self.__bootstrap()
                        else:
                            self._mode = BOOTSTRAP
                    elif len(joined) == 1: # One member joined
                        self._mode = PRIMARY
                        self.load()
                        self.members_cpg.start()
                elif len(members) > len(joined): # there is(are) members yet
                    self._mode = BOOTSTRAP
            elif  self.bootstrap_cpg.local_nodeid not in set(node[0] for node in joined):
                if self._mode is PRIMARY:
                    self.__bootstrap()

    def __update(self, dbg, uuid, table_name, meta):
        log.debug("%s: xcpng.corosync.meta.MetadataHandler.__update: uuid: %s table_name: %s meta: %s"
                  % (dbg, uuid, table_name, meta))

        message = dumps({'update': {'dbg': dbg, 'uuid': uuid, 'table_name': table_name, 'meta': meta}})
        self.members_cpg.send_message(message)

    def __on_message_members_cpg(self, addr, message):
        log.debug('xcpng.corosync.meta.MetadataHandler.__on_cpg_change_bootstrap_cpg')
        _dict_message = loads(message)
        if 'update' in _dict_message.keys():
            message = _dict_message['update']
            super(MetadataHandler, self).__update(message['dbg'],
                                                          message['uuid'],
                                                          message['table_name'],
                                                          message['meta'])
