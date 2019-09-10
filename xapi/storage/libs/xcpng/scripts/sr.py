#!/usr/bin/env python

import xapi.storage.libs.xcpng.globalvars
import os
import sys
import platform
import json

from xapi.storage import log

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import SR_commandline, Unimplemented
elif platform.linux_distribution()[1] == '7.6.0' or platform.linux_distribution()[1] == '8.0.0':
    from xapi.storage.api.v5.volume import SR_commandline, Unimplemented

xapi.storage.libs.xcpng.globalvars.plugin_type = \
    str(os.path.dirname(os.path.abspath(__file__)).split('/')[-1:][0]).split('.')[-1:][0][0:3]

with open("%s/plugin.json" % os.path.dirname(os.path.abspath(__file__))) as fd:
    plugin = json.load(fd)
    xapi.storage.libs.xcpng.globalvars.cluster_stack = plugin['required_cluster_stack'][0]

from xapi.storage.libs.xcpng.sr import Implementation, SR


if __name__ == "__main__":
    log.log_call_argv()
    cmd = SR_commandline(Implementation(SR))
    base = os.path.basename(sys.argv[0])
    if base == 'SR.probe':
        cmd.probe()
    elif base == 'SR.attach':
        cmd.attach()
    elif base == 'SR.create':
        cmd.create()
    elif base == 'SR.destroy':
        cmd.destroy()
    elif base == 'SR.detach':
        cmd.detach()
    elif base == 'SR.ls':
        cmd.ls()
    elif base == 'SR.stat':
        cmd.stat()
    elif base == 'SR.set_name':
        cmd.set_name()
    elif base == 'SR.set_description':
        cmd.set_description()
    else:
        raise Unimplemented(base)

