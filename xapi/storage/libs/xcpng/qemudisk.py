#!/usr/bin/env python

import subprocess
import os
import re
import platform
import time

from xapi.storage import log
from xapi.storage.libs.xcpng import utils, qmp

QEMU_DP = "/usr/lib64/qemu-dp/bin/qemu-dp"
NBD_CLIENT = "/usr/sbin/nbd-client"
QEMU_DP_SOCKET_DIR = utils.VAR_RUN_PREFIX + "/qemu-dp"

IMAGE_TYPES = ['qcow2', 'qcow', 'vhdx', 'vpc', 'raw']
LEAF_NODE_NAME = 'qemu_node'
SNAP_NODE_NAME = 'snap_node'
RBD_NODE_NAME = 'rbd_node'


def create(dbg, qemudisk, uri, img_qemu_uri):
    log.debug("%s: xcpng.qemudisk.create: uri: %s " % (dbg, uri))

    vdi_uuid = utils.get_vdi_uuid_by_uri(dbg, uri)
    sr_uuid = utils.get_sr_uuid_by_uri(dbg, uri)
    vdi_type = utils.get_vdi_type_by_uri(dbg, uri)
    if vdi_type not in IMAGE_TYPES:
        raise Exception('Incorrect VDI type')

    utils.mkdir_p(QEMU_DP_SOCKET_DIR, 0o0700)

    nbd_sock = QEMU_DP_SOCKET_DIR + "/qemu-nbd.{}".format(vdi_uuid)
    qmp_sock = QEMU_DP_SOCKET_DIR + "/qmp_sock.{}".format(vdi_uuid)
    qmp_log = QEMU_DP_SOCKET_DIR + "/qmp_log.{}".format(vdi_uuid)
    log.debug("%s: xcpng.qemudisk.create: Spawning qemu process for VDI %s with qmp socket at %s"
              % (dbg, vdi_uuid, qmp_sock))

    cmd = [QEMU_DP, qmp_sock]

    try:
        log_fd = open(qmp_log, 'w+')
        p = subprocess.Popen(cmd, stdout=log_fd, stderr=log_fd)
    except Exception as e:
        log.error("%s: xcpng.qemudisk.create: Failed to create qemu_dp instance: uri %s" %
                  (dbg, uri))
        try:
            log_fd.close()
        except:
            pass
        raise Exception(e)

    log.debug("%s: xcpng.qemudisk.create: New qemu process has pid %d" % (dbg, p.pid))

    return qemudisk(dbg, sr_uuid, vdi_uuid, vdi_type, img_qemu_uri, p.pid, qmp_sock, nbd_sock, qmp_log)


def introduce(dbg, qemudisk, sr_uuid, vdi_uuid, vdi_type, img_qemu_uri, pid, qmp_sock, nbd_sock, qmp_log):
    log.debug("%s: xcpng.qemudisk.introduce: sr_uuid: %s vdi_uuid: %s vdi_type: %s image_uri: %s pid: %d qmp_sock: %s "
              "nbd_sock: %s qmp_log: %s" % (dbg, sr_uuid, vdi_uuid, vdi_type, img_qemu_uri, pid, qmp_sock,
                                            nbd_sock, qmp_log))

    return qemudisk(dbg, sr_uuid, vdi_uuid, vdi_type, img_qemu_uri, pid, qmp_sock, nbd_sock, qmp_log)


class Qemudisk(object):
    def __init__(self, dbg, sr_uuid, vdi_uuid, vdi_type, img_qemu_uri, pid, qmp_sock, nbd_sock, qmp_log):
        log.debug("%s: xcpng.qemudisk.Qemudisk.__init__: sr_uuid: %s vdi_uuid: %s vdi_type: %s image_uri: %s pid: %d "
                  "qmp_sock: %s nbd_sock: %s qmp_log: %s"
                  % (dbg, sr_uuid, vdi_uuid, vdi_type, img_qemu_uri, pid, qmp_sock, nbd_sock, qmp_log))

        self.vdi_uuid = vdi_uuid
        self.sr_uuid = sr_uuid
        self.vdi_type = vdi_type
        self.pid = pid
        self.qmp_sock = qmp_sock
        self.nbd_sock = nbd_sock
        self.qmp_log = qmp_log
        self.img_uri = img_qemu_uri

        self.params = 'nbd:unix:%s' % self.nbd_sock
        qemu_params = '%s:%s:%s' % (self.vdi_uuid, LEAF_NODE_NAME, self.qmp_sock)

        self.params = "hack|%s|%s" % (self.params, qemu_params)

        self.open_args = {'driver': self.vdi_type,
                          'cache': {'direct': True, 'no-flush': True},
                          # 'discard': 'unmap',
                          'file': self._parse_image_uri(dbg),
                          'node-name': LEAF_NODE_NAME}

    def _parse_image_uri(self, dbg):
        log.debug("%s: xcpng.qemudisk.Qemudisk.parse_image_uri: vdi_uuid %s uri %s"
                  % (dbg, self.vdi_uuid, self.img_uri))
        regex = re.compile('^([A-Za-z+]*):(.*)$')
        result = regex.match(self.img_uri)
        driver = result.group(1)
        path = result.group(2)
        if driver == 'file':
            # file:/tmp/test.qcow2
            file = {'driver': 'file', 'filename':  path}
        elif driver == 'rbd':
            # rbd:pool/image:conf=/etc/ceph/ceph.conf
            regex = re.compile('^([A-Za-z0-9+_-]*)/([A-Za-z0-9+_-]*):conf=([A-Za-z0-9/.]*)$')
            result = regex.match(path)
            pool = result.group(1)
            image = result.group(2)
            conf = result.group(3)
            file = {'driver': 'rbd', 'pool': pool, 'image': image, 'conf': conf}
        elif driver == 'sheepdog+unix':
            # sheepdog+unix:///vdi?socket=socket_path
            regex = re.compile('^///([A-Za-z0-9_-]*)\\?socket=([A-Za-z0-9/.-]*)$')
            result = regex.match(path)
            vdi = result.group(1)
            socket = result.group(2)
            file = {'driver': 'sheepdog', 'server': {'type': 'unix', 'path': socket}, 'vdi': vdi}
        else:
            log.error("%s: xcpng.qemudisk.Qemudisk.parse_uri: Driver %s is not supported" % (dbg, driver))
            raise Exception("Qemu-dp driver %s is not supported" % driver)
        return file

    def quit(self, dbg):
        log.debug("%s: xcpng.qemudisk.Qemudisk.quit: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))
        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)
        try:
            _qmp_.connect()
            _qmp_.command('quit')
            _qmp_.close()
        except Exception as e:
            log.error("%s: xcpng.qemudisk.Qemudisk.quit: Failed to destroy qemu_dp instance: pid %s" % (dbg, self.pid))
            try:
                _qmp_.close()
            except:
                pass
            raise Exception(e)

    def open(self, dbg):
        log.debug("%s: xcpng.qemudisk.Qemudisk.open: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))

        log.debug("%s: xcpng.qemudisk.Qemudisk.open: args: %s" % (dbg, self.open_args))

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)

        try:
            _qmp_.connect()

            _qmp_.command("blockdev-add", **self.open_args)

            # Start an NBD server exposing this blockdev
            _qmp_.command("nbd-server-start",
                          addr={'type': 'unix',
                                'data': {'path': self.nbd_sock}})
            _qmp_.command("nbd-server-add",
                          device=LEAF_NODE_NAME, writable=True)
            log.debug("%s: xcpng.qemudisk.Qemudisk.open: Image opened: %s" % (dbg, self.open_args))
        except Exception as e:
            log.error("%s: xcpng.qemudisk.Qemudisk.open: Failed to open image in qemu_dp instance: uuid: %s pid %s" %
                      (dbg, self.vdi_uuid, self.pid))
            try:
                _qmp_.close()
            except:
                pass
            raise Exception(e)

    def close(self, dbg):
        log.debug("%s: xcpng.qemudisk.Qemudisk.close: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)

        try:
            _qmp_.connect()

            if platform.linux_distribution()[1] == '7.5.0':
                try:
                    path = "{}/{}".format(utils.VAR_RUN_PREFIX, self.vdi_uuid)
                    with open(path, 'r') as f:
                        line = f.readline().strip()
                    utils.call(dbg, ["/usr/bin/xenstore-write", line, "5"])
                    os.unlink(path)
                except Exception:
                    log.debug("%s: xcpng.qemudisk.Qemudisk.close: There was no xenstore setup" % dbg)
            elif platform.linux_distribution()[1] == '7.6.0' or platform.linux_distribution()[1] == '8.0.0':
                path = "{}/{}".format(utils.VAR_RUN_PREFIX, self.vdi_uuid)
                try:
                    with open(path, 'r') as f:
                        line = f.readline().strip()
                    os.unlink(path)
                    args = {'type': 'qdisk',
                            'domid': int(re.search('domain/(\d+)/',
                                                   line).group(1)),
                            'devid': int(re.search('vbd/(\d+)/',
                                                   line).group(1))}
                    _qmp_.command(dbg, "xen-unwatch-device", **args)
                except Exception:
                    log.debug("%s: xcpng.qemudisk.Qemudisk.close: There was no xenstore setup" % dbg)

            # Stop the NBD server
            _qmp_.command("nbd-server-stop")
            # Remove the block device
            args = {"node-name": LEAF_NODE_NAME}
            _qmp_.command("blockdev-del", **args)
        except Exception as e:
            log.error("%s: xcpng.qemudisk.Qemudisk.close: Failed to close image in qemu_dp instance: uuid: %s pid %s" %
                      (dbg, self.vdi_uuid, self.pid))
            try:
                _qmp_.close()
            except:
                pass
            raise Exception(e)

    def snap(self, dbg, snap_uri):
        log.debug("%s: xcpng.qemudisk.Qemudisk.snap: vdi_uuid %s pid %d qmp_sock %s snap_uri %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock, snap_uri))

        if self.vdi_type != 'qcow2':
            raise Exception('Incorrect VDI type')

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)

        try:
            _qmp_.connect()

            args = {'driver': 'qcow2',
                    'cache': {'direct': True, 'no-flush': True},
                    # 'discard': 'unmap',
                    'file': self._parse_image_uri(dbg),
                    'node-name': SNAP_NODE_NAME,
                    'backing': ''}

            _qmp_.command('blockdev-add', **args)

            args = {'node': LEAF_NODE_NAME,
                    'overlay': SNAP_NODE_NAME}

            _qmp_.command('blockdev-snapshot', **args)

            _qmp_.close()
        except Exception as e:
            log.error("%s: xcpng.qemudisk.Qemudisk.snap: Failed to set backing file for image in qemu_dp instance: "
                      "uuid: %s pid %s" % (dbg, self.vdi_uuid, self.pid))
            try:
                _qmp_.close()
            except:
                pass
            raise Exception(e)

    def suspend(self, dbg):
        log.debug("%s: xcpng.qemudisk.Qemudisk.suspend: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)

        try:
            _qmp_.connect()
            # Suspend IO on blockdev
            args = {"device": LEAF_NODE_NAME}
            _qmp_.command("x-blockdev-suspend", **args)
        except Exception as e:
            log.error("%s: xcpng.qemudisk.Qemudisk.suspend: Failed to suspend IO for image in qemu_dp instance: "
                      "uuid: %s pid %s" % (dbg, self.vdi_uuid, self.pid))
            try:
                _qmp_.close()
            except:
                pass
            raise Exception(e)

    def resume(self, dbg):
        log.debug("%s: xcpng.qemudisk.Qemudisk.resume: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)

        try:
            _qmp_.connect()
            # Resume IO on blockdev
            args = {"device": LEAF_NODE_NAME}
            _qmp_.command("x-blockdev-resume", **args)
        except Exception as e:
            log.error("%s: xcpng.qemudisk.Qemudisk.resume: Failed to resume IO for image in qemu_dp instance: "
                      "uuid: %s pid %s" % (dbg, self.vdi_uuid, self.pid))
            try:
                _qmp_.close()
            except:
                pass
            raise Exception(e)

    def relink(self, dbg, top, base):
        log.debug("%s: xcpng.qemudisk.Qemudisk.relink: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)

        try:
            _qmp_.connect()
            # Commit
            args = {"job-id": "relink-{}".format(self.vdi_uuid),
                    "device": LEAF_NODE_NAME,
                    "top": top,
                    "base": base,
                    "backing-file": base}

            _qmp_.command('relink-chain', **args)

            for i in range(50):
                res = _qmp_.command(dbg, "query-block-jobs")
                if len(res) == 0:
                    break
                time.sleep(0.1)
            _qmp_.close()
        except Exception as e:
            log.error("%s: xcpng.qemudisk.Qemudisk.relink: Failed to relink chain for image in qemu_dp instance: "
                      "uuid: %s pid %s" % (dbg, self.vdi_uuid, self.pid))
            try:
                _qmp_.close()
            except:
                pass
            raise Exception(e)

    def commit(self, dbg, top, base):
        log.debug("%s: xcpng.qemudisk.Qemudisk.commit: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)

        try:
            _qmp_.connect()
            # Commit
            args = {"job-id": "commit-{}".format(self.vdi_uuid),
                    "device": LEAF_NODE_NAME,
                    "top": top,
                    "base": base}
            
            _qmp_.command('block-commit', **args)

            for i in range(50):
                res = _qmp_.command(dbg, "query-block-jobs")
                if len(res) == 0:
                    if self.img_uri == top:
                        args = {"device": "commit-{}".format(self.vdi_uuid)}
                        _qmp_.command('block-job-complete', **args)
                    else:
                        break
                time.sleep(0.1)
            _qmp_.close()
        except Exception as e:
            log.error("%s: xcpng.qemudisk.Qemudisk.commit: Failed to commit changes for image in qemu_dp instance: "
                      "uuid: %s pid %s" % (dbg, self.vdi_uuid, self.pid))
            try:
                _qmp_.close()
            except:
                pass
            raise Exception(e)
