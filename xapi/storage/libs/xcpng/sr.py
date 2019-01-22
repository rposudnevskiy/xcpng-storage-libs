#!/usr/bin/env python

import json
from copy import deepcopy
import platform

from xapi.storage import log

from xapi.storage.libs.xcpng.meta import IMAGE_FORMAT_TAG, SR_UUID_TAG, CONFIGURATION_TAG, NAME_TAG, DESCRIPTION_TAG, \
                                         DATAPATH_TAG, UUID_TAG, KEY_TAG, READ_WRITE_TAG, VIRTUAL_SIZE_TAG, \
                                         PHYSICAL_UTILISATION_TAG, URI_TAG, CUSTOM_KEYS_TAG, SHARABLE_TAG, \
                                         MetadataHandler
from xapi.storage.libs.xcpng.utils import get_sr_uuid_by_name, get_sr_uuid_by_uri, get_vdi_uuid_by_name

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import SR_skeleton
    from xapi.storage.api.v4.volume import Sr_not_attached
    from xapi.storage.api.v4.volume import Volume_does_not_exist
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.volume import SR_skeleton
    from xapi.storage.api.v5.volume import Sr_not_attached
    from xapi.storage.api.v5.volume import Volume_does_not_exist


class SROperations(object):

    def __init__(self):
        self.DEFAULT_SR_NAME = ''
        self.DEFAULT_SR_DESCRIPTION = ''

    def create(self, dbg, uri, configuration):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def destroy(self, dbg, uri):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def get_sr_list(self, dbg, configuration):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def get_vdi_list(self, dbg, uri):
        # zvol.startswith(utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)]):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def sr_import(self, dbg, uri, configuration):
        # Override in VolumeOperations specifc class if required
        pass

    def sr_export(self, dbg, uri):
        # Override in VolumeOperations specifc class if required
        pass

    def extend_uri(self, dbg, uri, configuration):
        # Override in VolumeOperations specifc class if required
        return uri

    def get_clustered(self, dbg, uri):
        # Override in VolumeOperations specifc class if required
        return False

    def get_health(self, dbg, uri):
        # Override in VolumeOperations specifc class if required
        return ['Healthy', '']

    def get_datasources(self, dbg, uri):
        # Override in VolumeOperations specifc class if required
        return []

    def get_free_space(self, dbg, uri):
        raise NotImplementedError('Override in VolumeOperations specifc class')

    def get_size(self, dbg, uri):
        raise NotImplementedError('Override in VolumeOperations specifc class')


class SR(object):

    def __init__(self):
        self.sr_type = ''
        # self.sr_name_prefix = ''
        self.MetadataHandler = MetadataHandler
        self.SROpsHendler = SROperations()

    def probe(self, dbg, configuration):
        log.debug("{}: xcpng.sr.SR.probe: configuration={}".format(dbg, configuration))

        if IMAGE_FORMAT_TAG in configuration:
            _uri_ = "%s+%s" % (self.sr_type, configuration[IMAGE_FORMAT_TAG])
            if DATAPATH_TAG in configuration:
                _uri_ = "%s+%s://" % (_uri_, configuration[DATAPATH_TAG])
        else:
            _uri_ = "%s://" % self.sr_type

        _uri_ = self.SROpsHendler.extend_uri(dbg, _uri_, configuration)

        uri = "%s%s" % (_uri_, configuration[SR_UUID_TAG]) if SR_UUID_TAG in configuration else _uri_

        log.debug("{}: xcpng.sr.SR.probe: uri to probe: {}".format(dbg, uri))

        result = []

        log.debug("%s: xcpng.sr.SR.probe: Available Pools" % dbg)
        log.debug("%s: xcpng.sr.SR.probe: ---------------------------------------------------" % dbg)

        srs = self.SROpsHendler.get_sr_list(dbg, configuration)

        for sr_name in srs:
            log.debug("%s: xcpng.sr.SR.probe: %s" % (dbg, sr_name))

            sr_uuid = get_sr_uuid_by_name(dbg, sr_name)

            self.SROpsHendler.sr_import(dbg, "%s%s" % (_uri_, sr_uuid), configuration)
            sr_meta = self.MetadataHandler.load(dbg, "%s%s" % (_uri_, sr_uuid))

            if (IMAGE_FORMAT_TAG in configuration and
                ((CONFIGURATION_TAG in sr_meta and
                  IMAGE_FORMAT_TAG in sr_meta[CONFIGURATION_TAG] and
                  configuration[IMAGE_FORMAT_TAG] != sr_meta[CONFIGURATION_TAG][IMAGE_FORMAT_TAG]) or
                 (CONFIGURATION_TAG in sr_meta and
                  IMAGE_FORMAT_TAG not in sr_meta[CONFIGURATION_TAG]) or
                 CONFIGURATION_TAG not in sr_meta)):
                sr_name = None

            if (DATAPATH_TAG in configuration and
                ((CONFIGURATION_TAG in sr_meta and
                  DATAPATH_TAG in sr_meta[CONFIGURATION_TAG] and
                  configuration[DATAPATH_TAG] != sr_meta[CONFIGURATION_TAG][DATAPATH_TAG]) or
                 (CONFIGURATION_TAG in sr_meta and
                  DATAPATH_TAG not in sr_meta[CONFIGURATION_TAG]) or
                 CONFIGURATION_TAG not in sr_meta)):
                sr_name = None

            if (SR_UUID_TAG in configuration and
                ((CONFIGURATION_TAG in sr_meta and
                  SR_UUID_TAG in sr_meta[CONFIGURATION_TAG] and
                  configuration[SR_UUID_TAG] != sr_meta[CONFIGURATION_TAG][SR_UUID_TAG]) or
                 (CONFIGURATION_TAG in sr_meta and
                  SR_UUID_TAG not in sr_meta[CONFIGURATION_TAG] and
                  SR_UUID_TAG in sr_meta and
                  configuration[SR_UUID_TAG] != sr_meta[SR_UUID_TAG]) or
                 (CONFIGURATION_TAG not in sr_meta and
                  SR_UUID_TAG in sr_meta and
                  configuration[SR_UUID_TAG] != sr_meta[SR_UUID_TAG]) or
                 SR_UUID_TAG not in sr_meta)):
                sr_name = None

            if SR_UUID_TAG not in sr_meta:
                sr_name = None

            if sr_name is not None:
                _result_ = {}
                _result_['complete'] = True
                _result_['configuration'] = {}
                _result_['configuration'] = deepcopy(configuration)
                _result_['extra_info'] = {}

                sr = {}
                sr['sr'] = "%s%s" % (_uri_, sr_meta[SR_UUID_TAG])
                sr['name'] = sr_meta[NAME_TAG] if NAME_TAG in sr_meta \
                                               else self.SROpsHendler.DEFAULT_SR_NAME
                sr['description'] = sr_meta[DESCRIPTION_TAG] if DESCRIPTION_TAG in sr_meta \
                                                             else self.SROpsHendler.DEFAULT_SR_DESCRIPTION
                sr['free_space'] = self.SROpsHendler.get_free_space(dbg, "%s%s" % (_uri_, sr_uuid))
                sr['total_space'] = self.SROpsHendler.get_size(dbg, "%s%s" % (_uri_, sr_uuid))
                sr['datasources'] = self.SROpsHendler.get_datasources(dbg, "%s%s" % (_uri_, sr_uuid))
                sr['clustered'] = self.SROpsHendler.get_clustered(dbg, "%s%s" % (_uri_, sr_uuid))
                sr['health'] = self.SROpsHendler.get_health(dbg, "%s%s" % (_uri_, sr_uuid))

                _result_['sr'] = sr
                _result_['configuration']['sr_uuid'] = sr_meta[SR_UUID_TAG]

                result.append(_result_)

            self.SROpsHendler.sr_export(dbg, "%s%s" % (_uri_, sr_uuid))

        return result

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug("%s: xcpng.sr.SR.create: sr_uuid %s configuration %s name '%s' description: '%s'" %
                  (dbg, sr_uuid, configuration, name, description))

        if IMAGE_FORMAT_TAG in configuration:
            uri = "%s+%s" % (self.sr_type, configuration[IMAGE_FORMAT_TAG])
            if DATAPATH_TAG in configuration:
                uri = "%s+%s://" % (uri, configuration[DATAPATH_TAG])
        else:
            uri = "%s://" % self.sr_type

        uri = self.SROpsHendler.extend_uri(dbg, uri, configuration)

        uri = "%s%s" % (uri, sr_uuid)

        try:
            self.SROpsHendler.create(dbg, uri, configuration)
        except Exception:
            log.debug("%s: xcpng.sr.SR.create: Failed to create SR - sr_uuid: %s" % (dbg, sr_uuid))
            raise Exception

        try:
            self.MetadataHandler.create(dbg, uri)
        except Exception:
            try:
                self.SROpsHendler.sr_export(dbg, uri)
                self.SROpsHendler.destroy(dbg, uri)
            except Exception:
                raise Exception
            log.debug("%s: xcpng.sr.SR.create: Failed to create SR metadata - sr_uuid: %s" % (dbg, sr_uuid))
            raise Exception

        configuration['sr_uuid'] = sr_uuid

        pool_meta = {
            SR_UUID_TAG: sr_uuid,
            NAME_TAG: name,
            DESCRIPTION_TAG: description,
            CONFIGURATION_TAG: json.dumps(configuration)
        }

        try:
            self.MetadataHandler.update(dbg, uri, pool_meta)
        except Exception:
            log.debug("%s: xcpng.sr.SR.create: Failed to update pool metadata - sr_uuid: %s" % (dbg, sr_uuid))
            raise Exception

        self.SROpsHendler.sr_export(dbg, uri)

        return configuration

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.sr.SR.destroy: uri: %s" % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)

        try:
            self.SROpsHendler.destroy(dbg, uri)
        except Exception:
            log.debug("%s: xcpng.sr.SR.destroy: Failed to destroy SR - sr_uuid: %s" % (dbg, sr_uuid))
            raise Exception

    def attach(self, dbg, configuration):
        log.debug("%s: xcpng.sr.SR.attach: configuration: %s" % (dbg, configuration))

        if IMAGE_FORMAT_TAG in configuration:
            uri = "%s+%s" % (self.sr_type, configuration[IMAGE_FORMAT_TAG])
            if DATAPATH_TAG in configuration:
                uri = "%s+%s://" % (uri, configuration[DATAPATH_TAG])
        else:
            uri = "%s://" % self.sr_type

        uri = self.SROpsHendler.extend_uri(dbg, uri, configuration)

        uri="%s%s" % (uri, configuration[SR_UUID_TAG]) if SR_UUID_TAG in configuration else uri

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)

        log.debug("%s: xcpng.sr.SR.attach: uri %s sr_uuid: %s" % (dbg, uri, sr_uuid))

        try:
            self.SROpsHendler.sr_import(dbg, uri, configuration)
        except Exception:
            log.debug("%s: xcpng.sr.SR.attach: Failed to attach SR - sr_uuid: %s" % (dbg, sr_uuid))
            raise Sr_not_attached(configuration['sr_uuid'])

        return uri

    def detach(self, dbg, uri):
        log.debug("%s: xcpng.sr.SR.detach: uri: %s" % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)

        try:
            self.SROpsHendler.sr_export(dbg, uri)
        except Exception:
            log.debug("%s: xcpng.sr.SR.detach: Failed to detach SR - sr_uuid: %s" % (dbg, sr_uuid))
            raise Exception

    def stat(self, dbg, uri):
        log.debug("%s: xcpng.sr.SR.stat: uri: %s" % (dbg, uri))

        sr_meta = self.MetadataHandler.load(dbg, uri)

        log.debug("%s: xcpng.sr.SR.stat: pool_meta: %s" % (dbg, sr_meta))

        # Get the sizes
        tsize = self.SROpsHendler.get_size(dbg, uri)
        fsize = self.SROpsHendler.get_free_space(dbg, uri)
        log.debug("%s: xcpng.sr.SR.stat total_space = %Ld free_space = %Ld" % (dbg, tsize, fsize))

        overprovision = 0

        return {
            'sr': uri,
            'uuid': get_sr_uuid_by_uri(dbg, uri),
            'name': sr_meta[NAME_TAG] if NAME_TAG in sr_meta
                                      else self.SROpsHendler.DEFAULT_SR_NAME,
            'description': sr_meta[DESCRIPTION_TAG] if DESCRIPTION_TAG in sr_meta
                                                    else self.SROpsHendler.DEFAULT_SR_DESCRIPTION,
            'total_space': tsize,
            'free_space': fsize,
            'overprovision': overprovision,
            'datasources': self.SROpsHendler.get_datasources(dbg, uri),
            'clustered': self.SROpsHendler.get_clustered(dbg, uri),
            'health': self.SROpsHendler.get_health(dbg, uri)
        }

    def set_name(self, dbg, uri, new_name):
        log.debug("%s: xcpng.sr.SR.set_name: SR: %s New_name: %s"
                  % (dbg, uri, new_name))

        sr_meta = {
            NAME_TAG: new_name,
        }

        try:
            self.MetadataHandler.update(dbg, uri, sr_meta)
        except Exception:
            raise Volume_does_not_exist(uri)

    def set_description(self, dbg, uri, new_description):
        log.debug("%s: xcpng.sr.SR.set_description: SR: %s New_description: %s"
                  % (dbg, uri, new_description))

        sr_meta = {
            DESCRIPTION_TAG: new_description,
        }

        try:
            self.MetadataHandler.update(dbg, uri, sr_meta)
        except Exception:
            raise Volume_does_not_exist(uri)

    def ls(self, dbg, uri):
        log.debug("%s: xcpng.sr.SR.ls: uri: %s" % (dbg, uri))

        results = []
        key = ''

        try:
            for volume in self.SROpsHendler.get_vdi_list(dbg, uri):
                log.debug("%s: xcpng.sr.SR.ls: SR: %s Volume: %s" % (dbg, uri, volume))

                key = get_vdi_uuid_by_name(dbg, volume)

                log.debug("%s: xcpng.sr.SR.ls: SR: %s vdi : %s" % (dbg, uri, key))

                volume_meta = self.MetadataHandler.load(dbg, "%s/%s" % (uri, key))
                # log.debug("%s: xcpng.SR.ls: SR: %s Volume: %s Metadata: %s" % (dbg, uri, Volume, volume_meta))

                results.append({UUID_TAG: volume_meta[UUID_TAG],
                                KEY_TAG: volume_meta[KEY_TAG],
                                NAME_TAG: volume_meta[NAME_TAG],
                                DESCRIPTION_TAG: volume_meta[DESCRIPTION_TAG],
                                READ_WRITE_TAG: volume_meta[READ_WRITE_TAG],
                                VIRTUAL_SIZE_TAG: volume_meta[VIRTUAL_SIZE_TAG],
                                PHYSICAL_UTILISATION_TAG: volume_meta[PHYSICAL_UTILISATION_TAG],
                                URI_TAG: volume_meta[URI_TAG],
                                CUSTOM_KEYS_TAG: volume_meta[CUSTOM_KEYS_TAG],
                                SHARABLE_TAG: volume_meta[SHARABLE_TAG]})
                # log.debug("%s: xcpng.SR.ls: Result: %s" % (dbg, results))
            return results
        except Exception:
            raise Volume_does_not_exist(key)


class Implementation(SR_skeleton):

    def __init__(self):
        super(Implementation, self).__init__()
        self.SR = SR()

    def probe(self, dbg, configuration):
        log.debug("%s: xcpng.sr.Implementation.probe: configuration=%s" % (dbg, configuration))
        return self.SR.probe(dbg, configuration)

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug("%s: xcpng.sr.Implementation.create: sr_uuid %s configuration %s name '%s' description: '%s'" %
                  (dbg, sr_uuid, configuration, name, description))

        return self.SR.create(dbg, sr_uuid, configuration, name, description)

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.sr.Implementation.destroy: uri: %s" % (dbg, uri))
        self.SR.destroy(dbg, uri)

    def attach(self, dbg, configuration):
        log.debug("%s: xcpng.sr.Implementation.attach: configuration: %s" % (dbg, configuration))
        return self.SR.attach(dbg, configuration)

    def detach(self, dbg, uri):
        log.debug("%s: xcpng.sr.Implementation.detach: uri: %s" % (dbg, uri))
        self.SR.detach(dbg, uri)

    def stat(self, dbg, uri):
        log.debug("%s: xcpng.sr.Implementation.stat: uri: %s" % (dbg, uri))
        return self.SR.stat(dbg, uri)

    def set_name(self, dbg, uri, new_name):
        log.debug("%s: xcpng.sr.Implementation.set_name: SR: %s New_name: %s"
                  % (dbg, uri, new_name))
        self.SR.set_name(dbg, uri, new_name)

    def set_description(self, dbg, uri, new_description):
        log.debug("%s: xcpng.sr.SR.set_description: SR: %s New_description: %s"
                  % (dbg, uri, new_description))
        self.SR.set_description(dbg, uri, new_description)

    def ls(self, dbg, uri):
        log.debug("%s: xcpng.sr.Implementation.ls: uri: %s" % (dbg, uri))
        return self.SR.ls(dbg, uri)
