#!/usr/bin/env python

import os
import sys
import time
import atexit
import select
#from watchdog.observers import Observer
#from watchdog.events import FileSystemEventHandler, FileCreatedEvent
from threading import Thread, Event
from signal import SIGTERM
from xapi.storage.libs.xcpng.volume import VOLUME_TYPES
from xapi.storage.libs.xcpng.meta import MetadataHandler
from xapi.storage.libs.xcpng.utils import get_vdi_type_by_uri, is_valid_uuid


class GarbageCollector(Thread):

    def __init__(self, sr_uri, event):
        self.sr_uri = sr_uri
        self.MetadataHandler = MetadataHandler()
        self.VolumeTypes = VOLUME_TYPES
        super(GarbageCollector, self).__init__()

    def run(self):
        # ........
        # self.VolumeHandler = self.VolumeTypes[get_vdi_type_by_uri('GarbageCollector', self.sr)]
        # for pair in self.MetadataHandler.find_coalesceable_pairs('GarbageCollector', self.sr):
        #    self.VolumeHandler.commit('GarbageCollector', self.sr, pair[0], pair[1])
        # ........
        pass


class SRMonitor(Thread):
    def __init__(self, fifo, event):
        self.fifo = open(fifo)
        self.event = event
        super(SRMonitor, self).__init__()

    def run(self):
        while True:
            select.select([self.fifo], [], [self.fifo])
            data = self.fifo.read()
            if data.startswith('shutdown'):
                self.event.set()
                break


class Dispatcher(object):

    def __init__(self):
        self.srs = {}
        self.gcs = {}
        self.observer = Observer()
        self.handler = FileSystemEventHandler()
        self.handler.on_created = self.on_sr_attached
        self.observer.schedule(self.handler, XCPNG_SM_SRS_DIR)
        self.observer.start()

    def on_sr_attached(self, event):
        if isinstance(event, FileCreatedEvent):
            (sr_type, sr_uuid) = os.path.basename(event.src_path).split(':')
            if is_valid_uuid(sr_uuid):
                deatch_event = Event()
                self.srs[event.src_path] = (GarbageCollector("%s://%s" % (sr_type, sr_uuid), deatch_event),
                                            SRMonitor(event.src_path, deatch_event))

class Daemon:
    """
    https://gist.github.com/Jd007/5573672
    Base Daemon class that takes care of the start, stop, and forking of the processes.
    Used to daemonize any Python script.
    """
    def __init__(self, pidfile, worker_id, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.pidfile = pidfile
        self.worker_id = worker_id
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.init_time = int(time.time())
        self.successful_job_count = 0
        self.failed_job_count = 0
        self.avg_job_time = 0


    def delpid(self):
        """
        Removes the PID file
        :return:
        """
        os.remove(self.pidfile)

    def daemonize(self):
        """
        Double-forks the process to daemonize the script.
        """
        try:
            pid = os.fork()
            if pid > 0:
                # Exit from first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("Fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # Decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # Second fork
        try:
            pid = os.fork()
            if pid > 0:
                # Exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("Fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # Redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # Write the PID file
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write("%s\n" % pid)

    def start(self):
        """
        Start the daemon.
        :return:
        """
        # Check for a PID file to see if the Daemon is already running
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "PID file %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

        # No PID file, start the daemon
        self.daemonize()
        self.run()

    def stop(self):
        """
        Stop the daemon.
        :return:
        """
        # Get the PID from the PID file
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "PID file %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try to kill the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon.
        :return:
        """
        self.stop()
        self.start()

    def run(self):
        """
        The script to execute by the daemon after it starts.
        :return:
        """
        pass
