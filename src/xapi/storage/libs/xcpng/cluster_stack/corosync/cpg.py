#!/usr/bin/env python

import select
import errno
from threading import Thread
from xapi.storage import log
from xapi.storage.libs.xcpng.corosync import _cpg

# Import CPG_* constant
for symbol in dir(_cpg):
    if symbol.isupper():
        globals()[symbol] = getattr(_cpg, symbol)


class CPGError(Exception):
    """Base class for exceptions."""


class CPG(Thread):
    def __init__(self, name):
        log.debug('xcpng.corosync.cpg.CPG.__init___: name: %s' % name)
        self._handle = None
        self._name = name
        self._fd = None
        self._is_stopped = False
        super(CPG, self).__init__()

    def get_name(self, dbg):
        """Return the process group name."""
        log.debug('xcpng.corosync.cpg.CPG.get_name')
        return self.name

    def start(self):
        """Activate the process group. This will start the delivery of messages."""
        log.debug('xcpng.corosync.cpg.CPG.start')

        if self._handle is not None:
            log.error('xcpng.corosync.cpg.CPG.start: Failed. Service already started')
            raise CPGError('Service already started')

        self._handle = _cpg.initialize(self)
        _cpg.join(self._handle, self._name)

        self.local_nodeid = self.get_nodeid()
        self._fd = _cpg.fd_get(self._handle)

        super(CPG, self).start()

    def stop(self):
        """Deactivate the process group. This will stop the delivery of messages."""
        log.debug('xcpng.corosync.cpg.CPG.stop')

        if self._handle is not None:
            _cpg.leave(self._handle, self._name)

    def wait(self):
        """Wait until all services are stopped and then stop the dispatcher thread."""
        log.debug('xcpng.corosync.cpg.CPG.wait')
        self.join()

    def run(self):
        """Dispatch events."""
        log.debug('xcpng.corosync.cpg.CPG.run')
        timeout = 1.0

        if self._handle is None:
            log.error('xcpng.corosync.cpg.CPG.run: Failed. Service is not started')
            raise CPGError('Service is not started')

        while not self._is_stopped:
            try:
                ret = select.select([self._fd], [], [], timeout)
            except select.error, err:
                error = err.args[0]
                if error == errno.EINTR:
                    continue  # interrupted by signal
                else:
                    raise CPGError(str(err))  # not recoverable

            if not ret[0]:
                continue  # timeout

            _cpg.dispatch(self._handle, _cpg.DISPATCH_ALL)

        _cpg.finalize(self._handle)
        self._handle = None
        self._fd = None

    def is_active(self):
        """Return True if this service is active."""
        log.debug('xcpng.corosync.cpg.CPG.is_active')
        return True if self._handle is not None else False

    def get_nodeid(self):
        """Return a node id."""
        log.debug('xcpng.corosync.cpg.CPG.get_nodeid')

        if self._handle is None:
            log.error('xcpng.corosync.cpg.CPG.get_nodeid: Failed. Service is not started')
            raise CPGError('Service is not started')

        return _cpg.local_get(self._handle)

    def send_message(self, message):
        """Send a message to all group members."""
        log.debug('xcpng.corosync.cpg.CPG.send_message: message: %s' % message)

        if self._handle is None:
            log.error('xcpng.corosync.cpg.CPG.get_nodeid: Failed. Service is not started')
            raise CPGError('Service is not started')

        _cpg.mcast_joined(self._handle, _cpg.TYPE_AGREED, message)

    def _deliver_fn(self, name, addr, message):
        """INTERNAL: message delivery callback."""
        self.message_delivered(addr, message)

    def _confchg_fn(self, name, members, left, joined):
        """INTERNAL: configuration change callback."""
        self.configuration_changed(members, left, joined)
        if len(left) > 0:
            if self.local_nodeid in set(node[0] for node in left):
                self._is_stopped = True

    def message_delivered(self, addr, message):
        """Callback that is raised when a message is delivered."""
        raise NotImplementedError('Override in MetadataHandler specific class')

    def configuration_changed(self, members, left, joined):
        """Callback that is raised when a configuration change happens."""
        raise NotImplementedError('Override in MetadataHandler specific class')
