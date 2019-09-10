#!/usr/bin/env python
import os
import mmap
import pickle
import sys
import errno
import consul
from xapi.storage import log
from xapi.storage.libs.xcpng.utils import remove_path, mkdir_p, get_current_host_uuid

BLK_ALIGN = 4096
BLK_SIZE = 512
MAX_HOSTS = 64
HA_ENABLE_PATH = '/var/run/cluster'
HA_ENABLE_FILE = 'ha_enabled'
XAPI_HA_LOCKSPACE = 'xapi-ha-lockspace'

c = consul.Consul()
agent = c.agent.self()
consul_node_id = agent['Config']['NodeID']
consul_node_name = agent['Config']['NodeName']
xapi_host_uuid = get_current_host_uuid()

class Host:
    def __init__(self, uuid, online):
        self.uuid = uuid
        self.online = online


class Master:
    def __init__(self, uuid, pid):
        self.uuid = uuid
        self.pid = pid


class Statefile:
    def __init__(self):
        with open("/etc/xensource/xhad.conf") as fd:
            for line in fd:
                line = line.strip()
                if line.startswith("<StateFile>"):
                    self.state_file_path = line[11:-12]
                    try:
                        self.open_state_file()
                    except Exception:
                        log.error("HA Statefile error", exc_info=True)
                        # statefile might be unplugged already, e.g. during
                        # shutdown
                        sys.exit(14)
                    break

    def open_state_file(self):
        self.fd = os.open(self.state_file_path, os.O_RDWR | os.O_DIRECT)
        self.fo = os.fdopen(self.fd, 'rw')
        self.mm = mmap.mmap(-1, 1024 * 4)

    def read(self, offset):
        self.mm.seek(0)
        self.fo.seek(offset * BLK_ALIGN)
        self.fo.readinto(self.mm)
        return pickle.loads(self.mm.read(BLK_SIZE))

    def write(self, offset, val):
        s = pickle.dumps(val)
        if len(s) < BLK_SIZE:
            self.mm.seek(0)
            self.mm.write(s)
        else:
            raise ValueError(
                'Tried to write more than {} bytes'.format(BLK_SIZE))
        os.lseek(self.fd, offset * BLK_ALIGN, os.SEEK_SET)
        os.write(self.fd, self.mm)

    def read_all_hosts(self):
        hosts = {}
        for i in range(1, MAX_HOSTS + 1):
            hosts[i] = self.read(i)
        return hosts

    def format_all_hosts(self):
        for i in range(1, MAX_HOSTS + 1):
            self.write(i, Host(None, False))

    def set_host(self, offset, host):
        self.write(offset, host)

    def get_host(self, offset):
        return self.read(offset)

    def set_master(self, master):
        self.write(0, master)

    def get_master(self):
        return self.read(0)

    def set_invalid(self, invalid):
        self.write(MAX_HOSTS + 1, invalid)

    def get_invalid(self):
        return self.read(MAX_HOSTS + 1)


def get_current_host_node_id():
    return consul_node_id


def touch_signal_file():
    """
    Create the file that signals that we want to monitor Xapi health
    """
    try:
        mkdir_p(HA_ENABLE_PATH)
        f = open(os.path.join(HA_ENABLE_PATH, HA_ENABLE_FILE), 'ab')
        f.close()
    except OSError as ose:
        if ose.errno != errno.EEXIST:
            raise


def remove_signal_file():
    """
    Remove the file that signals that we want to monitor Xapi health
    """
    remove_path(os.path.join(HA_ENABLE_PATH, HA_ENABLE_FILE), force=True)


def join_xapi_ha_lockspace():
    """
    Join the xapi-ha-lockspace to indicate that xapi is alive on this host.
    This should be called after touch_signal_file. The lockspace membership of
    each node will be an indicator of xapi's liveness on that host. If xapi
    dies, or the host fails in another way, it will leave this lockspace.
    This should not be called multiple times, because in that case we'll join
    the lockspace multiple times, and we'll also have to leave the lockspace
    multiple times.
    """
    session_id = c.session.create(name="xapi-ha-%s" % consul_node_name)
    c.kv.put(key="%s/%s" % (XAPI_HA_LOCKSPACE, xapi_host_uuid),
             value=consul_node_id,
             acquire=session_id)


def leave_xapi_ha_lockspace():
    """
    Leave the xapi-ha-lockspace after having joined it with
    join_xapi_ha_lockspace. This should be called after remove_path when xapi's
    health is not monitored any more. It must be called before shutdown,
    because there must be no active lockspaces when dlm is stopped.
    """
    c.kv.delete(key="%s/%s" % (XAPI_HA_LOCKSPACE, xapi_host_uuid))


def get_xapi_ha_lockspace_members():
    """
    Returns the members of the xapi-ha-lockspace, indicating on which hosts
    xapi is alive.
    """
    members = []
    for entries in c.kv.get(key=XAPI_HA_LOCKSPACE, recurse=True):
        members.append(entries("Value"))
    return members

def is_consul_leader():
    if agent['Stats']['raft']['state'] == 'Leader':
        return True
    else:
        return False

