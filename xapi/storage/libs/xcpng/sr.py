#!/usr/bin/env python

import xapi.storage.libs.xcpng.globalvars
import platform
from copy import deepcopy
from xapi.storage import log
from xapi.storage.libs.xcpng.meta import IMAGE_FORMAT_TAG, SR_UUID_TAG, CONFIGURATION_TAG, NAME_TAG, DESCRIPTION_TAG, \
                                         DATAPATH_TAG, VDI_UUID_TAG, KEY_TAG, READ_WRITE_TAG, VIRTUAL_SIZE_TAG, \
                                         PHYSICAL_UTILISATION_TAG, URI_TAG, CUSTOM_KEYS_TAG, SHARABLE_TAG, \
                                         MetadataHandler
from xapi.storage.libs.xcpng.utils import SR_PATH_PREFIX, get_known_srs, get_sr_uuid_by_uri, get_vdi_uuid_by_name, \
                                          call, module_exists

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import SR_skeleton
elif platform.linux_distribution()[1] == '7.6.0' or platform.linux_distribution()[1] == '8.0.0':
    from xapi.storage.api.v5.volume import SR_skeleton

class SROperations(object):

    def __init__(self):
        self.MetadataHandler = MetadataHandler()
        self.DEFAULT_SR_NAME = ''
        self.DEFAULT_SR_DESCRIPTION = ''

    def create(self, dbg, uri, configuration):
        raise NotImplementedError('Override in SROperations specific class')

    def destroy(self, dbg, uri):
        raise NotImplementedError('Override in SROperations specific class')

    def get_sr_list(self, dbg, uri, configuration):
        raise NotImplementedError('Override in SROperations specific class')

    def get_vdi_list(self, dbg, uri):
        raise NotImplementedError('Override in SROperations specific class')

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
        raise NotImplementedError('Override in SROperations specific class')

    def get_size(self, dbg, uri):
        raise NotImplementedError('Override in SROperations specific class')


plugin_specific_sr = module_exists("xapi.storage.libs.xcpng.lib%s.sr" % xapi.storage.libs.xcpng.globalvars.plugin_type)
if plugin_specific_sr:
    _SROperations_ = getattr(plugin_specific_sr, 'SROperations')
else:
    _SROperations_ = SROperations


class SR(object):

    def __init__(self):
        self.MetadataHandler = MetadataHandler()
        self.SROpsHendler = _SROperations_()
        self.sr_type = xapi.storage.libs.xcpng.globalvars.plugin_type

    def probe(self, dbg, configuration):
        log.debug("{}: xcpng.sr.SR.probe: configuration={}".format(dbg, configuration))

        if IMAGE_FORMAT_TAG in configuration:
            _uri_ = "%s+%s" % (self.sr_type, configuration[IMAGE_FORMAT_TAG])
            if DATAPATH_TAG in configuration:
                _uri_ = "%s+%s://" % (_uri_, configuration[DATAPATH_TAG])
        else:
            _uri_ = "%s://" % self.sr_type

        _uri_ = self.SROpsHendler.extend_uri(dbg, _uri_, configuration)

        uri = "%s/%s" % (_uri_, configuration[SR_UUID_TAG]) if SR_UUID_TAG in configuration else _uri_

        log.debug("{}: xcpng.sr.SR.probe: uri to probe: {}".format(dbg, uri))

        result = []
        known_srs = get_known_srs()

        try:
            srs = self.SROpsHendler.get_sr_list(dbg, uri, configuration)

            log.debug("%s: xcpng.sr.SR.probe: Available Pools" % dbg)
            log.debug("%s: xcpng.sr.SR.probe: ---------------------------------------------------" % dbg)

            for _sr_ in srs:
                sr_uuid = get_sr_uuid_by_uri(dbg, _sr_)
                if sr_uuid not in known_srs:
                    sr_found = True

                    log.debug("%s: xcpng.sr.SR.probe: %s" % (dbg, _sr_))

                    configuration['mountpoint'] = "%s/%s" % (SR_PATH_PREFIX, sr_uuid)

                    try:
                        self.SROpsHendler.sr_import(dbg, _sr_, configuration)
                        sr_meta = self.MetadataHandler.get_sr_meta(dbg, _sr_)
                    except Exception:
                        try:
                            self.SROpsHendler.sr_export(dbg, _sr_)
                        except:
                            pass
                        break

                    if (IMAGE_FORMAT_TAG in configuration and
                            ((CONFIGURATION_TAG in sr_meta and
                              IMAGE_FORMAT_TAG in sr_meta[CONFIGURATION_TAG] and
                              configuration[IMAGE_FORMAT_TAG] != sr_meta[CONFIGURATION_TAG][IMAGE_FORMAT_TAG]) or
                             (CONFIGURATION_TAG in sr_meta and
                              IMAGE_FORMAT_TAG not in sr_meta[CONFIGURATION_TAG]) or
                             CONFIGURATION_TAG not in sr_meta)):
                        sr_found = False

                    if (DATAPATH_TAG in configuration and
                            ((CONFIGURATION_TAG in sr_meta and
                              DATAPATH_TAG in sr_meta[CONFIGURATION_TAG] and
                              configuration[DATAPATH_TAG] != sr_meta[CONFIGURATION_TAG][DATAPATH_TAG]) or
                             (CONFIGURATION_TAG in sr_meta and
                              DATAPATH_TAG not in sr_meta[CONFIGURATION_TAG]) or
                             CONFIGURATION_TAG not in sr_meta)):
                        sr_found = False

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
                        sr_found = False

                    if SR_UUID_TAG in sr_meta and sr_meta[SR_UUID_TAG] in known_srs:
                        sr_found = False

                    if SR_UUID_TAG not in sr_meta:
                        sr_found = False

                    if sr_found:
                        _result_ = {}
                        _result_['complete'] = True
                        _result_['configuration'] = {}
                        _result_['configuration'] = deepcopy(sr_meta[CONFIGURATION_TAG])
                        # _result_['configuration'] = deepcopy(configuration)
                        _result_['extra_info'] = {}

                        sr = {}
                        sr['sr'] = _sr_
                        sr['name'] = sr_meta[NAME_TAG] if NAME_TAG in sr_meta \
                            else self.SROpsHendler.DEFAULT_SR_NAME
                        sr['description'] = sr_meta[DESCRIPTION_TAG] if DESCRIPTION_TAG in sr_meta \
                            else self.SROpsHendler.DEFAULT_SR_DESCRIPTION
                        sr['free_space'] = self.SROpsHendler.get_free_space(dbg, _sr_)
                        sr['total_space'] = self.SROpsHendler.get_size(dbg, _sr_)
                        sr['datasources'] = self.SROpsHendler.get_datasources(dbg, _sr_)
                        sr['clustered'] = self.SROpsHendler.get_clustered(dbg, _sr_)
                        sr['health'] = self.SROpsHendler.get_health(dbg, _sr_)

                        _result_['sr'] = sr
                        # _result_['configuration']['sr_uuid'] = sr_meta[SR_UUID_TAG]

                        result.append(_result_)

                        self.SROpsHendler.sr_export(dbg, _sr_)
        except Exception as e:
            log.error("%s: xcpng.sr.SR.probe: Failed to probe SRs for configuration: %s" % (dbg, configuration))
            raise Exception(e)

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
        uri = "%s/%s" % (uri, sr_uuid)

        log.debug("%s: xcpng.sr.SR.create: uri %s" % (dbg, uri))

        configuration['mountpoint'] = "%s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri))

        try:
            call(dbg, ['mkdir', '-p', configuration['mountpoint']])

            self.SROpsHendler.create(dbg, uri, configuration)
            self.MetadataHandler.create(dbg, uri)

            configuration['sr_uuid'] = sr_uuid
            sr_meta = {
                SR_UUID_TAG: sr_uuid,
                NAME_TAG: name,
                DESCRIPTION_TAG: description,
                #CONFIGURATION_TAG: json.dumps(configuration)
                CONFIGURATION_TAG: configuration
            }

            self.MetadataHandler.update_sr_meta(dbg, uri, sr_meta)
            self.MetadataHandler.dump(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.sr.SR.create: Failed to create SR - sr_uuid: %s" % (dbg, sr_uuid))
            try:
                self.SROpsHendler.destroy(dbg, uri)
            except:
                pass
            raise Exception(e)

        try:
            self.SROpsHendler.sr_export(dbg, uri)
        except Exception as e:
            log.error("%s: xcpng.sr.SR.create: Created but failed to export SR after creation - sr_uuid: %s"
                      "Please check and export SR manually before attaching the SR" % (dbg, sr_uuid))

        return configuration

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.sr.SR.destroy: uri: %s" % (dbg, uri))
        try:
            self.MetadataHandler.destroy(dbg, uri)
            self.SROpsHendler.destroy(dbg, uri)
            call(dbg, ['rm', '-rf', "%s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri))])
        except Exception as e:
            log.error("%s: xcpng.sr.SR.destroy: Failed to destroy SR - sr_uuid: %s" % (dbg, get_sr_uuid_by_uri(dbg, uri)))
            raise Exception(e)

    def attach(self, dbg, configuration):
        log.debug("%s: xcpng.sr.SR.attach: configuration: %s" % (dbg, configuration))

        if IMAGE_FORMAT_TAG in configuration:
            uri = "%s+%s" % (self.sr_type, configuration[IMAGE_FORMAT_TAG])
            if DATAPATH_TAG in configuration:
                uri = "%s+%s://" % (uri, configuration[DATAPATH_TAG])
        else:
            uri = "%s://" % self.sr_type

        uri = self.SROpsHendler.extend_uri(dbg, uri, configuration)
        uri = "%s/%s" % (uri, configuration[SR_UUID_TAG]) if SR_UUID_TAG in configuration else uri

        log.debug("%s: xcpng.sr.SR.attach: uri: %s" % (dbg, uri))

        configuration['mountpoint'] = "%s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri))

        try:
            call(dbg, ['mkdir', '-p', configuration['mountpoint']])
            self.SROpsHendler.sr_import(dbg, uri, configuration)
        except Exception as e:
            log.error("%s: xcpng.sr.SR.attach: Failed to attach SR - sr_uuid: %s" % (dbg, get_sr_uuid_by_uri(dbg, uri)))
            try:
                self.SROpsHendler.sr_export(dbg, uri)
                call(dbg, ['rm', '-rf', configuration['mountpoint']])
            except:
                pass
            raise Exception(e)

        return uri

    def detach(self, dbg, uri):
        log.debug("%s: xcpng.sr.SR.detach: uri: %s" % (dbg, uri))
        try:
            self.SROpsHendler.sr_export(dbg, uri)
            call(dbg, ['rm', '-rf', "%s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri))])
        except Exception as e:
            log.error("%s: xcpng.sr.SR.detach: Failed to detach SR - sr_uuid: %s" % (dbg, get_sr_uuid_by_uri(dbg, uri)))
            raise Exception(e)

    def stat(self, dbg, uri):
        log.debug("%s: xcpng.sr.SR.stat: uri: %s" % (dbg, uri))

        try:
            sr_meta = self.MetadataHandler.get_sr_meta(dbg, uri)
            log.debug("%s: xcpng.sr.SR.stat: pool_meta: %s" % (dbg, sr_meta))

            # Get the sizes
            tsize = self.SROpsHendler.get_size(dbg, uri)
            fsize = self.SROpsHendler.get_free_space(dbg, uri)
            log.debug("%s: xcpng.sr.SR.stat total_space = %Ld free_space = %Ld" % (dbg, tsize, fsize))
        except Exception as e:
            log.error("%s: xcpng.sr.SR.stat: Failed to get stat for SR: %s" % (dbg, uri))
            raise Exception(e)

        overprovision = 0

        return {
            'sr': uri,
            'uuid': get_sr_uuid_by_uri(dbg, uri),
            'name': sr_meta[NAME_TAG] if sr_meta[NAME_TAG] is not None else ''
                                      if NAME_TAG in sr_meta
                                      else self.SROpsHendler.DEFAULT_SR_NAME,
            'description': sr_meta[DESCRIPTION_TAG] if sr_meta[DESCRIPTION_TAG] is not None
                                                    else '' if DESCRIPTION_TAG in sr_meta
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
            self.MetadataHandler.update_sr_meta(dbg, uri, sr_meta)
        except Exception as e:
            log.error("%s: xcpng.sr.SR.set_name: Failed to set name for SR: %s" % (dbg, uri))
            raise Exception(e)

    def set_description(self, dbg, uri, new_description):
        log.debug("%s: xcpng.sr.SR.set_description: SR: %s New_description: %s"
                  % (dbg, uri, new_description))

        sr_meta = {
            DESCRIPTION_TAG: new_description,
        }

        try:
            self.MetadataHandler.update_sr_meta(dbg, uri, sr_meta)
        except Exception as e:
            log.error("%s: xcpng.sr.SR.set_description: Failed to set description for SR: %s" % (dbg, uri))
            raise Exception(e)

    def ls(self, dbg, uri):
        log.debug("%s: xcpng.sr.SR.ls: uri: %s" % (dbg, uri))

        results = []
        key = ''

        try:
            for volume in self.SROpsHendler.get_vdi_list(dbg, uri):
                log.debug("%s: xcpng.sr.SR.ls: SR: %s Volume: %s" % (dbg, uri, volume))

                key = get_vdi_uuid_by_name(dbg, volume)

                log.debug("%s: xcpng.sr.SR.ls: SR: %s vdi : %s" % (dbg, uri, key))

                volume_meta = self.MetadataHandler.get_vdi_meta(dbg, "%s/%s" % (uri, key))

                results.append({VDI_UUID_TAG: volume_meta[VDI_UUID_TAG],
                                KEY_TAG: volume_meta[KEY_TAG],
                                NAME_TAG: volume_meta[NAME_TAG],
                                DESCRIPTION_TAG: volume_meta[DESCRIPTION_TAG],
                                READ_WRITE_TAG: volume_meta[READ_WRITE_TAG],
                                VIRTUAL_SIZE_TAG: volume_meta[VIRTUAL_SIZE_TAG],
                                PHYSICAL_UTILISATION_TAG: volume_meta[PHYSICAL_UTILISATION_TAG],
                                URI_TAG: volume_meta[URI_TAG],
                                CUSTOM_KEYS_TAG: volume_meta[CUSTOM_KEYS_TAG],
                                SHARABLE_TAG: volume_meta[SHARABLE_TAG]})
            return results
        except Exception as e:
            log.error("%s: xcpng.sr.SR.ls: Failed to list of vdis for SR: %s" % (dbg, uri))
            raise Exception(e)


class Implementation(SR_skeleton):

    def __init__(self, sr):
        super(Implementation, self).__init__()
        self.SR = sr()

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
        log.debug("%s: xcpng.sr.Implementation.set_description: SR: %s New_description: %s"
                  % (dbg, uri, new_description))
        self.SR.set_description(dbg, uri, new_description)

    def ls(self, dbg, uri):
        log.debug("%s: xcpng.sr.Implementation.ls: uri: %s" % (dbg, uri))
        return self.SR.ls(dbg, uri)
