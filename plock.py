#!/usr/bin/env python

''' module plock: simple process locking.
        this may be needed when more than one copy of a process is trying
        to access the same files, such as during ingestion'''

import os
class PlockError(Exception): pass
class PlockPresent(PlockError): pass

class Plock:
    def __init__(self,filename):
        self.filename = filename
        pid = self._haslock()
        print ">>>>",pid, filename
        if pid !=0 :
            raise PlockPresent("locked by process: %d" % pid)
        else:
            self.lock()
            
    def _haslock(self):
        ''' _haslock check for existence of file and check process id'''
        if os.path.islink(self.filename):
            print "is link", os.readlink(self.filename)
            pid = int(os.readlink(self.filename))
            print pid
            # test to see if process is running            
            try:
                os.kill(pid, 0)
                return pid
            except OSError:
                # stale lock
                self.release()
        return 0
    
    def lock(self):
        ''' lock create lock file and write current process id '''
        pid = os.getpid()
        os.symlink("%d" % pid,
                   self.filename)
        
    def release(self):
        ''' release remove lock file to release lock'''
        os.unlink(self.filename)

