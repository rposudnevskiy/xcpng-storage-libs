#!/usr/bin/env python

import xapi.storage.libs.xcpng.globalvars
import os
import sys
import platform
import json

from xapi.storage import log

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import Volume_commandline, Unimplemented
elif platform.linux_distribution()[1] == '7.6.0' or \
     platform.linux_distribution()[1] == '8.0.0' or \
     platform.linux_distribution()[1] == '8.1.0' or \
     platform.linux_distribution()[1] == '8.2.0' or \
     platform.linux_distribution()[1] == '8.2.1':
    from xapi.storage.api.v5.volume import Volume_commandline, Unimplemented

xapi.storage.libs.xcpng.globalvars.plugin_type = \
    str(os.path.dirname(os.path.abspath(__file__)).split('/')[-1:][0]).split('.')[-1:][0][0:3]

with open("%s/plugin.json" % os.path.dirname(os.path.abspath(__file__))) as fd:
    plugin = json.load(fd)
    if len(plugin['required_cluster_stack']) > 0:
        xapi.storage.libs.xcpng.globalvars.cluster_stack = plugin['required_cluster_stack'][0]

from xapi.storage.libs.xcpng.volume import Implementation, VOLUME_TYPES


if __name__ == "__main__":
    """Parse the arguments and call the required command"""
    log.log_call_argv()
    cmd = Volume_commandline(Implementation(VOLUME_TYPES))
    base = os.path.basename(sys.argv[0])
    if base == "Volume.create":
        cmd.create()
    elif base == "Volume.snapshot":
        cmd.snapshot()
    elif base == "Volume.clone":
        cmd.clone()
    elif base == "Volume.destroy":
        cmd.destroy()
    elif base == "Volume.set_name":
        cmd.set_name()
    elif base == "Volume.set_description":
        cmd.set_description()
    elif base == "Volume.set":
        cmd.set()
    elif base == "Volume.unset":
        cmd.unset()
    elif base == "Volume.resize":
        cmd.resize()
    elif base == "Volume.stat":
        cmd.stat()
#    elif base == "Volume.compare":
#        cmd.compare()
#    elif base == "Volume.similar_content":
#        cmd.similar_content()
#    elif base == "Volume.enable_cbt":
#        cmd.enable_cbt()
#    elif base == "Volume.disable_cbt":
#        cmd.disable_cbt()
#    elif base == "Volume.data_destroy":
#        cmd.data_destroy()
#    elif base == "Volume.list_changed_blocks":
#        cmd.list_changed_blocks()
    else:
        raise Unimplemented(base)