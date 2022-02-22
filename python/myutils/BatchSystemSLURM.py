from __future__ import print_function
import xml.etree.ElementTree
import subprocess
import fnmatch
import hashlib
import json
from BatchSystem import BatchSystem
from time import sleep
import shlex

class BatchSystemSLURM(BatchSystem):

    def __init__(self, config=None, interactive=False, local=False, configFile=None):
        super(BatchSystemSLURM, self).__init__(interactive=interactive, local=local, configFile=configFile)
        self.name = 'SLURM'
        self.config = config
        self.submitScriptTemplate = "sbatch --job-name={jobName} --mem={memory} --time={time} --output={output} --open-mode=append {extraOptions} {runscript}"
        self.cancelJobTemplate = "scancel {jobId}"
        self.submissionDelay = eval(self.config.get('SLURM', 'submissionDelay')) if self.config.has_section('SLURM') and self.config.has_option('SLURM', 'submissionDelay') else 0.2
        self.resubmitToDifferentNode = eval(self.config.get('SLURM', 'resubmitToDifferentNode')) if self.config.has_option('SLURM', 'resubmitToDifferentNode') else True


    def getJobNames(self, includeDeleted=True):
        jobs = self.getJobs(includeDeleted=includeDeleted)
        jobNames = [x['name'] for x in jobs]
        return jobNames
    
    def getJobNamesRunning(self):
        jobs = self.getJobs(includeDeleted=False)
        jobNames = [x['name'] for x in jobs if x['is_running']]
        return jobNames

    def getJobs(self, includeDeleted=True):
        lines = subprocess.Popen(["squeue -u $USER --format='%.18i %.9P %.256j %.8u %.8T %.10M %.9l %.6D %R'"], shell=True, stdout=subprocess.PIPE).stdout.read().split("\n")
        headerParts = [x.strip() for x in lines[0].strip().split(' ') if len(x.strip()) > 0]
        result = []
        for line in lines[1:]:
            if len(line.strip()) > 0:
                lineParts = [x.strip() for x in line.strip().split(' ') if len(x.strip()) > 0]
                jobDict = {
                        'name':       lineParts[headerParts.index('NAME')], 
                        'id':         lineParts[headerParts.index('JOBID')],
                        'state':      lineParts[headerParts.index('STATE')],
                        'slots':      lineParts[headerParts.index('NODES')],
                        'is_deleted': False, 
                        'is_running': lineParts[headerParts.index('STATE')].strip() == 'RUNNING', 
                        'is_pending': lineParts[headerParts.index('STATE')].strip() == 'PENDING', 
                        'is_waiting_for_dependency': lineParts[headerParts.index('STATE')].strip() == 'PENDING' and "(Dependency)" in lineParts[headerParts.index('NODELIST(REASON)')],
                        }
                result.append(jobDict)
        return result

    def getJobIDfromOutput(self, stdOutput):
        jobId = -1
        if stdOutput.startswith('Submitted batch job'):
            jobId = int(stdOutput.split('Submitted batch job')[1].strip())
        else:
            print("WARNING: can't interpret:", stdOutput)
        print("JOB ID was", jobId)
        return jobId

    def getOption(self, command, option, value=None):
        commandParts = shlex.split(command, posix=False)
        matches = [i for i,x in enumerate(commandParts) if x.startswith('--'+option+'=')]
        if len(matches) == 1:
            return commandParts[matches[0]].split('=')[1].strip()
        else:
            return value

    def changeOption(self, command, option, value):
        commandParts = shlex.split(command, posix=False)
        matches = [i for i,x in enumerate(commandParts) if x.startswith('--'+option+'=')]
        if len(matches) == 1:
            commandParts[matches[0]] = '--'+option+'='+value
        elif len(matches) < 1:
            commandParts = [commandParts[0]] + ['--'+option+'='+value] + commandParts[1:]
        else:
            print("ERROR: multiple match:", command, option, value)
            raise Exception("BatchSystemSLURMError")
        return " ".join(commandParts)

    def resubmit(self, job):
        if self.resubmitToDifferentNode and 'host' in job and job['host'] is not None:
            # add WN of failed job to exclude list
            excludeList = (",".join([job['host']] + self.getOption(job['submitCommand'], "exclude", "").split(','))).strip(',')
            job['submitCommand'] = self.changeOption(job['submitCommand'], "exclude", excludeList)
           
            timeLimit = self.getOption(job['submitCommand'], "time")
            if timeLimit.startswith("0-10:"):
                job['submitCommand'] = self.changeOption(job['submitCommand'], "time", "0-20:00") 
        print("RESUBMIT:", job['submitCommand'])
        stdOutput = subprocess.check_output([job['submitCommand']], shell=True)
        job['id'] = self.getJobIDfromOutput(stdOutput)
        job['nResubmits'] = (job['nResubmits']+1) if 'nResubmits' in job else 1
        return job

    def submit(self, job, repDict):
        self.nJobsProcessed += 1
        self.submitPreprocess(job, repDict)


        runscript = self.getRunScriptCommand(repDict)
        logPaths = self.getLogPaths(repDict)

        memoryLimit = '3000M'
        partitionAuto = None
        if 'queue' in repDict:
            if repDict['queue'] == 'all.q':
                timeLimit = '0-10:00'
            elif repDict['queue'] == 'long.q':
		timeLimit = '1-00:00'
        	partitionAuto = "long"     
	    elif repDict['queue'] == 'twodays.q':
                timeLimit = '2-00:00'
            elif repDict['queue'] == 'threedays.q':
                timeLimit = '3-00:00'
            elif repDict['queue'] == 'short.q':
                timeLimit = '0-03:00'
            elif repDict['queue'] == 'veryshort.q':
                timeLimit = '0-00:59'
                partitionAuto = "quick"
            elif repDict['queue'] == 'espresso.q':
                timeLimit = '0-00:05'
                partitionAuto = "quick"
            elif repDict['queue'] == 'bigmem.q':
                timeLimit = '1-00:00'
                memoryLimit = '12000M'

        extraOptions = self.config.get('SLURM', 'options') if self.config.has_section('SLURM') and self.config.has_option('SLURM', 'options') else ''
        if partitionAuto is not None and "--partition" not in extraOptions:
            extraOptions += " --partition=%s "%partitionAuto

        if 'dependency' in repDict:
            extraOptions += " --dependency=afterany:{jobId} ".format(jobId=repDict['dependency'])

        command = self.submitScriptTemplate.format(jobName=repDict['name'], memory=memoryLimit, time=timeLimit, runscript=runscript, output=logPaths['out'], extraOptions=extraOptions)

        if self.submissionDelay > 0:
            sleep(self.submissionDelay)

        repDict['batchSystem'] = self.name
        return self.run(command, runscript, repDict, getJobIdFn=self.getJobIDfromOutput)

    def cancelJob(self, job):
        jobId = int(job['id'])
        command = self.cancelJobTemplate.format(jobId=jobId)
        commandOutput = self.runShell([command])
        return commandOutput is not None and 'has deleted job' in commandOutput
    
    def supportsDependencies(self):
        return True 
