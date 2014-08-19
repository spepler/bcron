#!/usr/local/bin/python
#
#
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


import time, os, subprocess, signal, sys

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
        

    

class Monitor:

    def __init__(self,jobname, freq, script, options):
        self.beat=0
        self.options = options
        self.freq=freq
	self.script=script
        self.jobname = jobname
        bcrondir = os.path.join(os.environ['HOME'], '.bcron')
        self.jobfile = os.path.join(bcrondir, jobname)            

    def heartbeat(self): 
        self.beat += 1
        # the monitor goes as long as the jobfile exists.
        if not os.path.exists(self.jobfile): 
            sys.exit()
        else:
            os.utime(self.jobfile, None)     
            if self.options.verbose >1 :print "MONITOR: Beat %s\r" %self.beat,
            sys.stdout.flush()

        
    def start(self):
        self.heartbeat()
        while 1:
            job = Job(self.script, self.options.poll, self.options.timeout, self.heartbeat) 
            if self.options.verbose: print "MONITOR: Start job"
            job.do()
            if self.options.verbose: print "\nMONITOR: End job"
            nextjobstart = max(job.start_time+self.freq, job.end_time+self.options.wait)
            if self.options.verbose: print "MONITOR: Waiting %s seconds before starting new run." % (nextjobstart - time.time(),) 
            while time.time() < nextjobstart:
                time.sleep(self.options.poll)
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
    usage = "usage: %prog [options] jobname frequency script"
    parser = OptionParser(usage)
    parser.add_option("-t", "--timeout", dest="timeout", type='int', default=3600,
                      help="set a timeout in seconds on the script [default: %default]")
    parser.add_option("-w", "--wait", dest="wait", type='int', default=0,
                      help="set a minimum wait time after the script has finished until the next script run. [default: %default]")
    parser.add_option("-p", "--poll", dest="poll", type='int', default=30, 
                      help="set a poll interval to check on the script. [default: %default]")
    parser.add_option("--watch", dest="watch", type='int', default=3600,
                      help="set time for the watch process check for failed monitor processes.")
    parser.add_option("--restarter", action="store_true", dest="restarter", help="start a watcher process")
    parser.add_option("-l", "--list", action="store_true", dest="list", help="list jobs")
    parser.add_option("-s", "--stop", action="store_true", dest="stop", help="stop new job runs")
    parser.add_option("-v", "--verbose", action="count", dest="verbose")
    
    (options, args) = parser.parse_args()
    
    # make bcron directory
    bcrondir = os.path.join(os.environ['HOME'], '.bcron')
    if not os.path.exists(bcrondir): os.mkdir(bcrondir)

    if options.list:
        print 'job files: ', 
        print os.listdir(bcrondir)
        sys.exit()

    jobname = args[0]
    if options.stop:
        print 'removing job %s' % jobname
        os.unlink(os.path.join(bcrondir, jobname))
        sys.exit()

    if len(args) != 3:
        parser.error("incorrect number of arguments. Need jobname, frequency and script")

    if options.verbose:
        print "verbose on..." 

    freq = int(args[1])
    script = args[2]

    if not options.restarter:
        # make the initial job file 
        jobfile = os.path.join(bcrondir, jobname)
        if os.path.exists(jobfile): 
            raise Exception("Job file already exists.")
        else:
            open(jobfile,'a').close()

    # start first watcher - this will queue the next watcher and exit.
    Watcher(jobname, freq, script, options)

    # start the monitor if not a watcher 
    if not options.restarter:  
        m = Monitor(jobname,freq, script, options)
        m.start()   

if __name__ == "__main__":
    main()	
	    

