#!/usr/bin/env python
import re
import os
import math
import urlparse
import xapi
import errno
import shutil
from uuid import UUID
from pkgutil import find_loader
from importlib import import_module
from xapi.storage import log
from subprocess import Popen, PIPE

SR_PATH_PREFIX = "/run/sr-mount"
POOL_PREFIX = '_XenStorage-'
VDI_PREFIXES = {'raw': 'RAW-', 'vpc': 'VHD-', 'vhdx': 'VHDX-', 'qcow2': 'QCOW2-', 'qcow': 'QCOW-'}
SR_METADATA_IMAGE_NAME = '__meta__'
VAR_RUN_PREFIX = '/var/run'

MIN_VHD_SIZE = 2 * 1024 * 1024
MAX_VHD_SIZE = 2093050 * 1024 * 1024
VHD_BLOCK_SIZE = 2 * 1024 * 1024
RBD_BLOCK_SIZE = 2 * 1024 * 1024
RBD_BLOCK_ORDER = int(math.log(RBD_BLOCK_SIZE, 2))

def is_valid_uuid(val):
    try:
        UUID(str(val))
        return True
    except ValueError:
        return False

def module_exists(module_name):
    try:
        find_loader(module_name)
        return import_module(module_name)
    except ImportError:
        return None

def get_vdi_type_by_uri(dbg, uri):
    scheme = urlparse.urlparse(uri).scheme
    regex = re.compile('(.*)\+(.*)\+(.*)')
    result = regex.match(scheme)
    return result.group(2)


def get_vdi_datapath_by_uri(dbg, uri):
    scheme = urlparse.urlparse(uri).scheme
    regex = re.compile('(.*)\+(.*)\+(.*)')
    result = regex.match(scheme)
    return result.group(3)


def get_cluster_name_by_uri(dbg, uri):
    return urlparse.urlparse(uri).netloc


def get_sr_uuid_by_uri(dbg, uri):
    regex = re.compile('/([A-Za-z0-9\-]*)/{0,1}([A-Za-z0-9\-]*)')
    result = regex.match(urlparse.urlparse(uri).path)
    if result is not None:
        return result.group(1)
    else:
        return None


def get_sr_uuid_by_name(dbg, name):
    regex = re.compile(".*%s" % POOL_PREFIX)
    return regex.sub('', name)


def get_vdi_uuid_by_uri(dbg, uri):
    regex = re.compile('/([A-Za-z0-9\-]*)/{0,1}([A-Za-z0-9\-]*)')
    result = regex.match(urlparse.urlparse(uri).path)
    if result is not None:
        return result.group(2)
    else:
        return None


def get_vdi_uuid_by_name(dbg, name):
    regex = re.compile('.*-(.{8}-.{4}-.{4}-.{4}-.{12})')
    result = regex.match(name)
    return result.group(1)


def get_sr_name_by_uri(dbg, uri):
    return "%s%s%s" % (get_sr_type_by_uri(dbg, uri), POOL_PREFIX, get_sr_uuid_by_uri(dbg, uri))


def get_vdi_name_by_uri(dbg, uri):
    return "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], get_vdi_uuid_by_uri(dbg, uri))


def get_sr_type_by_uri(dbg, uri):
    scheme = urlparse.urlparse(uri).scheme
    regex = re.compile('^(\w*)\+{0,1}(\w*)\+{0,1}(\w*)$')
    result = regex.match(scheme)
    return result.group(1).upper()


def get_current_host_uuid():
    fd = open("/etc/xensource-inventory")
    for line in fd:
        if line.strip().startswith("INSTALLATION_UUID"):
            return line.split("'")[1]
    fd.close()

def get_host_uuid_by_name(name):
    args = ['xe', 'host-list', "name-label=%s" % name]
    proc = Popen(args, stdout=PIPE, close_fds=True)
    stdout = proc.communicate()[0]
    uuid = None
    if proc.returncode == 0:
        for line in stdout.split('\n'):
            if 'uuid' in line:
                uuid=line.split(':')[1].strip()
    return uuid

def get_known_srs():
    srs = []
    args = ['xe', 'sr-list']
    proc = Popen(args, stdout=PIPE, close_fds=True)
    stdout = proc.communicate()[0]
    if proc.returncode == 0:
        for line in stdout.split('\n'):
            if 'uuid' in line:
                srs.append(line.split(':')[1].strip())
    return srs


def call(dbg, args):
    log.debug("%s: Running cmd: %s" % (dbg, args))
    proc = Popen(args, stdout=PIPE, stderr=PIPE, close_fds=True)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        log.debug("%s: Exec of cmd: %s failed, error: %s, reason: %s"
                  % (dbg, args, os.strerror(proc.returncode), stderr.strip()))
        raise Exception(os.strerror(proc.returncode))
    return stdout


def _call(dbg, args):
    log.debug("%s: Running cmd: %s" % (dbg, args))
    proc = Popen(args, stdout=PIPE, stderr=PIPE, close_fds=True)
    stdout, stderr = proc.communicate()
    log.debug("%s: Cmd return: %s, error: %s, reason: %s"
              % (dbg, proc.returncode, os.strerror(proc.returncode), stderr.strip()))
    return proc.returncode


def mkdir_p(path, mode=None):
    if not mode:
        mode = 0o777
    if os.path.exists(path):
        if mode:
            os.chmod(path, mode)
    else:
        try:
            os.makedirs(path, mode)
        except OSError:
            raise

def remove_path(path, force=False):
    try:
        os.unlink(path)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            if not force:
                raise
        elif exc.errno == errno.EISDIR:
            shutil.rmtree(path)
        else:
            raise

# Functions from /opt/xensource/sm/vhdutil.py

def calcOverheadEmpty(virtual_size):
    """Calculate the VHD space overhead (metadata size) for an empty VDI of
    size virtual_size"""

    size_mb = virtual_size / (1024 * 1024)

    # Footer + footer copy + header + possible CoW parent locator fields
    overhead = 3 * 1024

    # BAT 4 Bytes per block segment
    overhead += (size_mb / 2) * 4
    overhead = roundup(512, overhead)

    # BATMAP 1 bit per block segment
    overhead += (size_mb / 2) / 8
    overhead = roundup(4096, overhead)

    return overhead


def calcOverheadBitmap(virtual_size):
    num_blocks = virtual_size / VHD_BLOCK_SIZE
    if virtual_size % VHD_BLOCK_SIZE:
        num_blocks += 1
    return num_blocks * 4096


def calcOverheadFull(virtual_size):
    """Calculate the VHD space overhead for a full VDI of size virtual_size
    (this includes bitmaps, which constitute the bulk of the overhead)"""
    return calcOverheadEmpty(virtual_size) + calcOverheadBitmap(virtual_size)


def fullSizeVHD(virtual_size):
    return virtual_size + calcOverheadFull(virtual_size)


def roundup(divisor, value):
    """Retruns the rounded up value so it is divisible by divisor."""
    if value == 0:
        value = 1
    if value % divisor != 0:
        return ((int(value) / divisor) + 1) * divisor
    return value


def validate_and_round_vhd_size(size):
    """ Take the supplied vhd size, in bytes, and check it is positive and less
    that the maximum supported size, rounding up to the next block boundary
    """
    if size < 0 or size > MAX_VHD_SIZE:
        raise xapi.XenAPIException('VDISize',
                                   ['VDI size must be between %d MB and %d MB' %
                                    ((MIN_VHD_SIZE / 1024 / 1024), (MAX_VHD_SIZE / 1024 / 1024))])

    if size < MIN_VHD_SIZE:
        size = MIN_VHD_SIZE

    size = roundup(VHD_BLOCK_SIZE, size)

    return size
