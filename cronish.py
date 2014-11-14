#!/usr/local/bin/python
#
# python wrapper script to run a job repeatedly

# The script launches jobs and tells other instances of the script that it is looking after 
# the job. The script can be started repeatedly from a cron job to make sure it does not die.
# Only one instance of each job will be allowed to run.

# communication between jobs is done via a lock file

#  1. Startup 
#       Makes the com file. 
#       Starts a watcher script.
#       creates first monitor that launches job runs.
#       regularly touches the com file to show its active.
#  2. Watcher 
#       queues another watcher. 
#       Takes over as monitor if it thinks the monitor has failed.

# There are time parameters that are needed for script operation
#  freq - Job frequency. This is the time between jobs it is measured from the 
#           start of the job to the start of the next job. No job should start until
#           the last job it has been freq since the last job start.
#  wait - Minimum wait time between jobs. There should be at least wait between the 
#           end of the last job and the start of the next.
#  script - the script to run.
#  jobname - a unique name for the series of script runs.
#
# Types of behaviour 
# Regular job of predictable length, freq=6
# ....XX....XX....XX....XX....
# Irreg job length freq=6 wait=0
# ....XX....X.....XXXX..XX....
# Irreg freq=6 wait=3
# ....xx....xxxxxxx...x.....x.....xxxxxxxxxxxxxxxxx...xx....x....
# Irreg job  freq=0 wait=5
# ...X.....XXXXX.....X.....XXXXXXX.....XX.....
#
#
# Author: Sam Pepler
# 2014 Aug - bcron
# 2014 Nov - revised to work with cron as starter - cronish


import time, os, subprocess, signal, sys, ConfigParser

#-------------------------------------
''' module plock: simple process locking.
        this may be needed when more than one copy of a process is trying
        to access the same files, such as during ingestion'''

class PlockPresent(Exception): pass

class Plock:
    def __init__(self,filename):
        self.filename = filename
        pid = self._haslock()
        if pid !=0 : raise PlockPresent("locked by process: %d" % pid)
        else: self.lock()
            
    def _haslock(self):
        ''' _haslock check for existence of file and check process id'''
        if os.path.islink(self.filename):
            pid = int(os.readlink(self.filename))
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
        os.symlink("%d" % pid, self.filename)
        
    def release(self):
        ''' release remove lock file to release lock'''
        print "release"
        os.unlink(self.filename)

  
#----------------------------------------------
class Monitor:
    '''Class to repeated run jobs then wait to run next job'''

    def __init__(self,jobname, freq, script, verbose, timeout, wait):
        self.polls=0
        self.verbose = verbose
        self.timeout = timeout
        self.wait = wait
        self.freq=freq
	self.script=script
        self.jobname = jobname
        self.started = time.time()
        self.jobs=0

    def start(self):
        
        while 1:
            job = Job(self.script, self.timeout)
            self.jobs +=1 
            if self.verbose: print "MONITOR: Start job %s: %s" % (self.jobs, self.script)
            job.do()
            if self.verbose: print "\nMONITOR: End job %s" % self.jobs

            # time of next job
            nextjobstart = max(job.start_time+self.freq, job.end_time+self.wait)
            if self.verbose: print "MONITOR: Waiting %s seconds before starting new run." % (nextjobstart - time.time(),) 
            time.sleep(nextjobstart - time.time())  
                                    


#-------------------------------
class Job: 

    def __init__(self,script, timeout):
        self.start_time=time.time()
        self.polls = 0
        self.timeout=timeout
	self.process = subprocess.Popen(script, shell=True, bufsize=4096)
	self.script=script
	self.killed = 0
	self.returncode = None
	self.cwd = os.getcwd()

    def runtime(self): return time.time()-self.start_time

    def pid(self):
        if os.name == "nt": return "WINDOWS no pid"
        else: return self.process.pid

    def kill(self):
        os.kill(self.process.pid,signal.SIGKILL)     
	time.sleep(1)
	self.killed=1

    def do(self):
        #  loop until process stops.  If time out is reached kill process 
        poll = 1.0
        while 1: 
	    self.returncode = self.process.poll()
	    self.polls +=1		
	    if self.returncode == None and self.runtime() < self.timeout: 
	        time.sleep(poll)
	        poll = 1.1*poll
	    elif self.returncode == None: self.kill()
	    else: break		
	self.end_time=time.time()

#-----------------------------------
from optparse import OptionParser

def main():
    usage = """usage: %prog [options] start|stop <jobname1>
  start        Starts job sequence. If the job is already going it will not start a new one"
  stop         Stop a job sequence.     
  -l, --list   list jobs in config file.
  -v           verbose          """
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", action="count", dest="verbose")
    parser.add_option("-l", "--list", action="store_true", dest="list", help="list jobs")
    (options, args) = parser.parse_args()

    cronish_dir = os.path.join(os.environ['HOME'], '.cronish')
    if not os.path.exists(cronish_dir): os.mkdir(cronish_dir)       
    configfile = os.path.join(cronish_dir,'cronish.cfg')
    if not os.path.exists(configfile): raise Exception("No cronish file: %s" % configfile)
   
    
    if options.list:
        print 'job files: ', 
        config = ConfigParser.ConfigParser()
        config.read(configfile)
        jobs = config.sections()
        print jobs
        sys.exit()
    
    if len(args) == 0: raise Exception("Need a start or stop option.") 
    if args[0] not in ('start', 'stop'): raise Exception("need start or stop as the command.")
    if len(args) == 1: 
        # launch all jobs
        raise Exception("Not implimented yet. Should launch all jobs.")
    if len(args) > 2: 
        # launch some jobs
        raise Exception("Not implimented yet. Should launch list of named jobs.")
    operation = args[0]
    jobname = args[1]    

    lockfile=os.path.join(cronish_dir, "%s.lock" % jobname)
	           
    if operation == 'stop':
        if options.verbose: print "Stopping monitor."     
        if os.path.islink(lockfile):
            pid = int(os.readlink(lockfile))
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                # stale lock
                pass
            os.unlink(lockfile)

    if options.verbose:
        print "verbose on..." 

    # start the monitor
    if operation == 'start':
        if options.verbose: print "Starting monitor: Reading options..." 
        # get options from conf file
        config = ConfigParser.ConfigParser()
        config.read(configfile)
        if not config.has_section(jobname): raise Exception("No job in config file.")
        if config.has_option(jobname, 'freq'):
            freq = config.getfloat(jobname, 'freq')
        else: raise Exception ("Need a freq option in config")
        if config.has_option(jobname, 'script'):
            script = config.get(jobname, 'script')
        else: raise Exception ("Need a script option in config")
        if config.has_option(jobname, 'timeout'):
            timeout = config.getfloat(jobname, 'timeout')
        else: timeout=3600
        if config.has_option(jobname, 'wait'):
            wait = config.getfloat(jobname, 'wait')
        else: wait=0
    
        if options.verbose: print "Starting monitor: creating lockfile..."     
        try:
            plock=Plock(lockfile)
            if options.verbose: print "Create lockfile (%s)" % lockfile
        except PlockPresent, err:
            raise Exception ("Already running. Lockfile present (%s)" % lockfile)
            
        if options.verbose: print "Starting monitor: makeing monitor and starting job..."     
        try:
            m = Monitor(jobname,freq, script, options.verbose, timeout, wait)
            m.start()
        finally: 
            print "finally!"
            plock.release()   

        
if __name__ == "__main__":
    main()	
	    


