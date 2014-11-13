#!/usr/local/bin/python
#
#
# python wrapper script to run a job repeatedly
# Uses bsub to run jobs to make sure they continue

# The script launches jobs and tells other intances of the script that it is looking after 
# the job. If the script dies then another instance of the script (queued via bsub) will take 
# over. 
 

# communication between jobs is done via a file
# if the file is untouched in last x mins the assume job is inactive and start more jobs.
# 

# 2 types of script use
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
#  poll - The monitor polls the job and updates the com file at these intervals. 
#           The watcher waits to to poll intervals to check the monitor is dead.
#  watch - A watcher is started at these intervals. 
#  script - the script to run.
#  jobname - a unique name for the series of script runs.
#
#Types of behaviour 
#Regular predictable freq=6
#....XX....XX....XX....XX....
#irreg job length freq=6 wait=0
#....XX....X.....XXXX..XX....
#irreg freq=6 wait=3
#....xx....xxxxxxx...x.....x.....xxxxxxxxxxxxxxxxx...xx....x....
#irreg job  freq=0 wait=5
# ...X.....XXXXX.....X.....XXXXXXX.....XX.....
#
# Job, Monitor and Watcher sequence
# J  J  J  J  J  J  J  J  J  J           J  J  J  J  J  J  J  J  J   ...  
# M                            X (dies)  M  
# W             W            W           W (restart M)  W      
# 
#
# Author: Sam Pepler
# 2014 Aug


import time, os, subprocess, signal, sys, ConfigParser
from plock import Plock, PlockPresent

class Watcher:

    def __init__(self, jobname, freq, script, options):
        bcrondir = os.path.join(os.environ['HOME'], '.bcron')
        jobfile = os.path.join(bcrondir, jobname)
        v = options.verbose
        poll = options.poll
        wait = options.wait
        watch = options.watch
        timeout = options.timeout
        
        # die if no job file
        if v: print "WATCHER: Starting watcher process to check if the monitor is running..."
        if not os.path.exists(jobfile): 
            print "WATCHER: no job file (%s). Stopping." % jobfile
            sys.exit()
        
        # schedule next watcher job
        if v: print "WATCHER: scheduling next watcher run with command: "
        cmd = 'sleep %s && python bcron.py --restarter --watch=%s %s -p%s -w%s -t%s %s %s \'%s\'  &' % (watch,
            watch, '-v '*v, poll, wait, timeout, jobname, freq, script)
        if v: print "WATCHER:       %s" % cmd    
        subprocess.call(cmd, shell=True)
        if v: print "WATCHER: Next watcher job queued."
           
        # if the monitor has failed then continue as the monitor 
        if os.path.getmtime(jobfile) < time.time() - poll - freq - wait:
            print "WATCHER: Take over as new monitor."
            m = Monitor(jobname, freq, script, options)
            m.start()   
        else: 
            if v: print "WATCHER: Monitor ok."
        

    
#----------------------------------------------
class Monitor:

    def __init__(self,jobname, freq, script, verbose, poll, timeout, wait):
        self.beat=0
        self.verbose = verbose
        self.poll = poll
        self.timeout = timeout
        self.wait = wait
        self.freq=freq
	self.script=script
        self.jobname = jobname

    def heartbeat(self): 
        self.beat += 1
        print "MONITOR: Beat %s\r" %self.beat,
        sys.stdout.flush()

    def start(self):
        self.heartbeat()
        while 1:
            job = Job(self.script, self.poll, self.timeout, self.heartbeat) 
            if self.verbose: print "MONITOR: Start job"
            job.do()
            if self.verbose: print "\nMONITOR: End job"
            nextjobstart = max(job.start_time+self.freq, job.end_time+self.wait)
            if self.verbose: print "MONITOR: Waiting %s seconds before starting new run." % (nextjobstart - time.time(),) 
            while time.time() < nextjobstart:
                time.sleep(self.poll)
                self.heartbeat()
                                    


#-------------------------------
class Job: 

    def __init__(self,script, poll, timeout, heartbeat):
        self.start_time=time.time()
        self.poll = poll
        self.timeout=timeout
	self.process = subprocess.Popen(script, shell=True, bufsize=4096)
	self.script=script
	self.killed = 0
	self.returncode = None
	self.cwd = os.getcwd()
        self.heartbeat = heartbeat

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
        while 1: 
	    self.returncode = self.process.poll()
	    if self.returncode == None and self.runtime() < self.timeout: 
                self.heartbeat()
	        time.sleep(self.poll)		
	    elif self.returncode == None: self.kill()
	    else: break
		
	self.end_time=time.time()
	self.heartbeat()


from optparse import OptionParser

def main():
    usage = "usage: %prog [options] start|stop [<jobname1>, <jobname2>...]"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", action="count", dest="verbose")
    parser.add_option("-l", "--list", dest="list")
    (options, args) = parser.parse_args()

    cronish_dir = os.path.join(os.environ['HOME'], '.cronish')
    if not os.path.exists(cronish_dir): os.mkdir(cronish_dir)       
    configfile = os.path.join(cronish_dir,'cronish.cfg')
    
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
            if os.path.exists("/proc/%d" % pid):
                os.kill(pid, signal.SIGKILL)
            os.unlink(lockfile)

    if options.verbose:
        print "verbose on..." 

    # start the monitor
    if operation == 'start':
        if options.verbose: print "Starting monitor: Reading options..." 
        # get options from conf file
        if not os.path.exists(configfile): raise Exception("No cronish file: %s" % configfile)
        config = ConfigParser.ConfigParser()
        config.read(configfile)
        if not config.has_section(jobname): raise Exception("No job in config file.")
        if config.has_option(jobname, 'freq'):
            freq = config.getfloat(jobname, 'freq')
        else: raise Exception ("Need a freq option in config")
        if config.has_option(jobname, 'script'):
            script = config.get(jobname, 'script')
        else: raise Exception ("Need a script option in config")
        if config.has_option(jobname, 'poll'):
            poll = config.getfloat(jobname, 'poll')
        else: poll=30
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
            raise Exception ("Could not create lockfile (%s)" % lockfile)
            
        if options.verbose: print "Starting monitor: makeing monitor and starting job..."     
        try:
            m = Monitor(jobname,freq, script, options.verbose, poll, timeout, wait)
            m.start()
        except: 
            plock.release()   

if __name__ == "__main__":
    main()	
	    


