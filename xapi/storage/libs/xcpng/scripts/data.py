#!/usr/bin/env python
"""
Data interface for RBD using QEMU qdisk
"""
import xapi.storage.libs.xcpng.globalvars
import os
import sys
import platform
import json

from xapi.storage import log

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.datapath import Unimplemented
    raise Unimplemented(os.path.basename(sys.argv[0]))
elif platform.linux_distribution()[1] == '7.6.0' or platform.linux_distribution()[1] == '8.0.0':
    from xapi.storage.api.v5.datapath import Data_commandline, Unimplemented

xapi.storage.libs.xcpng.globalvars.plugin_type = \
    str(os.path.dirname(os.path.abspath(__file__)).split('/')[-1:][0]).split('.')[-1:][0][0:3]

with open("%s/plugin.json" % os.path.dirname(os.path.abspath(__file__))) as fd:
    plugin = json.load(fd)
    xapi.storage.libs.xcpng.globalvars.cluster_stack = plugin['required_cluster_stack'][0]


from xapi.storage.libs.xcpng.data import Implementation, QdiskData


if __name__ == "__main__":
    log.log_call_argv()
    CMD = Data_commandline(Implementation(QdiskData))
    CMD_BASE = os.path.basename(sys.argv[0])
    if CMD_BASE == "Data.copy":
        CMD.copy()
    elif CMD_BASE == "Data.mirror":
        CMD.mirror()
    elif CMD_BASE == "Data.stat":
        CMD.stat()
    elif CMD_BASE == "Data.cancel":
        CMD.cancel()
    elif CMD_BASE == "Data.destroy":
        CMD.destroy()
    elif CMD_BASE == "Data.ls":
        CMD.ls()
    else:
        raise Unimplemented(CMD_BASE)