#!/usr/bin/env python

import xapi.storage.libs.xcpng.globalvars
from xapi.storage import log

from xapi.storage.libs.xcpng.meta import MetadataHandler
from xapi.storage.libs.xcpng.qemudisk import Qemudisk
from xapi.storage.libs.xcpng.utils import module_exists

from xapi.storage.api.v5.datapath import Data_skeleton


class DataOperations(object):

    def __init__(self):
        self.MetadataHandler = MetadataHandler()


plugin_specific_data = module_exists("xapi.storage.libs.xcpng.lib%s.data" % xapi.storage.libs.xcpng.globalvars.plugin_type)
if plugin_specific_data:
    _DataOperations_ = getattr(plugin_specific_data, 'VolumeOperations')
else:
    _DataOperations_ = DataOperations


class Data(object):

    def __init__(self):
        self.MetadataHandler = MetadataHandler()
        self.DataOpsHandler = _DataOperations_()

    def _copy(self, dbg, uri, domain, remote, blocklist):
        raise NotImplementedError('Override in Data specifc class')

    def copy(self, dbg, uri, domain, remote, blocklist):
        self._copy(dbg, uri, domain, remote, blocklist)

    def _mirror(self, dbg, uri, domain, remote):
        raise NotImplementedError('Override in Data specifc class')

    def mirror(self, dbg, uri, domain, remote):
        self._mirror(dbg, uri, domain, remote)

    def _stat(self, dbg, operation):
        raise NotImplementedError('Override in Data specifc class')

    def stat(self, dbg, operation):
        self._stat(dbg, operation)

    def _cancel(self, dbg, operation):
        raise NotImplementedError('Override in Data specifc class')

    def cancel(self, dbg, operation):
        self._cancel(dbg, operation)

    def _destroy(self, dbg, operation):
        raise NotImplementedError('Override in Data specifc class')

    def destroy(self, dbg, operation):
        self._destroy(dbg, operation)

    def _ls(self, dbg):
        raise NotImplementedError('Override in Data specifc class')

    def ls(self, dbg):
        self._ls(dbg)


class QdiskData(Data):

    def __init__(self):
        super(QdiskData, self).__init__()
        self.qemudisk = Qemudisk

    def _copy(self, dbg, uri, domain, remote, blocklist):
        raise NotImplementedError('QdiskData._copy')

    def _mirror(self, dbg, uri, domain, remote):
        raise NotImplementedError('QdiskData._mirror')

    def _stat(self, dbg, operation):
        raise NotImplementedError('QdiskData._stat')

    def _cancel(self, dbg, operation):
        raise NotImplementedError('QdiskData._cancel')

    def _destroy(self, dbg, operation):
        raise NotImplementedError('QdiskData._destroy')

    def _ls(self, dbg):
        raise NotImplementedError('QdiskData._ls')


class Implementation(Data_skeleton):
    """
    Data implementation
    """
    def __init__(self, data):
        super(Implementation, self).__init__()
        self.Data = data()

    def copy(self, dbg, uri, domain, remote, blocklist):
        log.debug("%s: xcpng.data.Implementation.copy: uri: %s domain: %s remote: %s blocklist: %s" % (dbg, uri, domain, remote, blocklist))
        return self.Data.copy(dbg, uri, domain, remote, blocklist)

    def mirror(self, dbg, uri, domain, remote):
        log.debug("%s: xcpng.data.Implementation.mirror: uri: %s domain: %s remote: %s" % (dbg, uri, domain, remote))
        return self.Data.mirror(dbg, uri, domain, remote)

    def stat(self, dbg, operation):
        log.debug("%s: xcpng.data.Implementation.stat: operation: %s" % (dbg, operation))
        return self.Data.stat(dbg, operation)

    def cancel(self, dbg, operation):
        log.debug("%s: xcpng.data.Implementation.cancel: operation: %s" % (dbg, operation))
        return self.Data.cancel(dbg, operation)

    def destroy(self, dbg, operation):
        log.debug("%s: xcpng.data.Implementation.destroy: operation: %s" % (dbg, operation))
        return self.Data.destroy(dbg, operation)

    def ls(self, dbg):
        log.debug("%s: xcpng.data.Implementation.ls" % dbg)
        return self.Data.ls(dbg)