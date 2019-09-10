#!/usr/bin/env python

import os
import sys
import platform
import json
from xapi.storage import log

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.plugin import Plugin_skeleton, Plugin_commandline, Unimplemented
elif platform.linux_distribution()[1] == '7.6.0' or platform.linux_distribution()[1] == '8.0.0':
    from xapi.storage.api.v5.plugin import Plugin_skeleton, Plugin_commandline, Unimplemented


class Implementation(Plugin_skeleton):

    def diagnostics(self, dbg):
        return "No diagnostic data to report"

    def query(self, dbg):
        with open("%s/plugin.json" % os.path.abspath(os.path.dirname(sys.argv[0]))) as fd:
            plugin = json.load(fd)
        return plugin


if __name__ == "__main__":
    log.log_call_argv()
    cmd = Plugin_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == 'Plugin.diagnostics':
        cmd.diagnostics()
    elif base == 'Plugin.Query':
        cmd.query()
    else:
        raise Unimplemented(base)
