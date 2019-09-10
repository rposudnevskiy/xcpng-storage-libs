#!/usr/bin/env python
"""
Datapath for ZFS ZVol using QEMU qdisk
"""
import xapi.storage.libs.xcpng.globalvars
import os
import sys
import platform
import json

from xapi.storage import log

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.datapath import Datapath_commandline, Unimplemented
elif platform.linux_distribution()[1] == '7.6.0' or platform.linux_distribution()[1] == '8.0.0':
    from xapi.storage.api.v5.datapath import Datapath_commandline, Unimplemented

xapi.storage.libs.xcpng.globalvars.plugin_type = \
    str(os.path.dirname(os.path.abspath(__file__)).split('/')[-1:][0]).split('.')[-1:][0][0:3]

with open("%s/plugin.json" % os.path.dirname(os.path.abspath(__file__))) as fd:
    plugin = json.load(fd)
    xapi.storage.libs.xcpng.globalvars.cluster_stack = plugin['required_cluster_stack'][0]

from xapi.storage.libs.xcpng.datapath import Implementation, DATAPATHES

if __name__ == "__main__":
    log.log_call_argv()
    CMD = Datapath_commandline(Implementation(DATAPATHES))
    CMD_BASE = os.path.basename(sys.argv[0])
    if CMD_BASE == "Datapath.activate":
        CMD.activate()
    elif CMD_BASE == "Datapath.attach":
        CMD.attach()
    elif CMD_BASE == "Datapath.close":
        CMD.close()
    elif CMD_BASE == "Datapath.deactivate":
        CMD.deactivate()
    elif CMD_BASE == "Datapath.detach":
        CMD.detach()
    elif CMD_BASE == "Datapath.open":
        CMD.open()
    else:
        raise Unimplemented(CMD_BASE)