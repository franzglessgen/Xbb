import ROOT,sys,os,subprocess,random,string,hashlib
from printcolor import printc
from FileLocator import FileLocator

class CopyTreePSI(object):
    
    def __init__(self, config):
        self.config = config
        self.debug = 'XBBDEBUG' in os.environ
        self.fileLocator = FileLocator(config=self.config)

    def copySingleFile(self, whereToLaunch,inputFile,outputFile,skimmingCut,remove_branches):

        if self.debug:
            print("INPUT:", inputFile)
        input = ROOT.TFile.Open(inputFile,'read')
        if not input:
          print 'input file NOT EXISTING:',inputFile
          #input.Close()
          return
        try:
            __tmpPath = os.environ["TMPDIR"]
        except:
            __tmpPath = self.config.get('Directories', 'scratch')
        try:
            if not os.path.isdir(__tmpPath):
                os.makedirs(__tmpPath)
        except:
            pass
        outputFileName = outputFile.split('/')[-1]
        print 'outputFileName',__tmpPath+'/'+outputFileName
        output = ROOT.TFile.Open(__tmpPath+'/'+outputFileName,'recreate')

        inputTree = input.Get("tree")
        if not inputTree:
            inputTree = input.Get("Events")
        nEntries = inputTree.GetEntries()
        for branch in remove_branches:
          if branch and not branch.isspace():
            # print 'DROPPING BRANCHES LIKE',str(branch)
            inputTree.SetBranchStatus(str(branch), ROOT.kFALSE);

        output.cd()
        print '\n\t copy file: %s with cut: %s' %(inputFile,skimmingCut)
        outputTree = inputTree.CopyTree(skimmingCut)
        kEntries = outputTree.GetEntries()
        printc('blue','',"\t before cuts\t %s" %nEntries)
        printc('green','',"\t survived\t %s" %kEntries)
        outputTree.AutoSave()
        input.cd()
        obj = ROOT.TObject
        for key in ROOT.gDirectory.GetListOfKeys():
            input.cd()
            obj = key.ReadObj()
            # this contains the event tree, which will be copied skimmed only
            if obj.GetName() in  ['tree', 'Events']:
                continue
            if self.debug:
                print "DEBUG: clone object ", obj.GetName()
            # all other objects are just cloned
            output.cd()
            if obj.IsA().InheritsFrom(ROOT.TTree.Class()):
                objClone = obj.CloneTree(-1)
            else:
                objClone = obj
            objClone.Write(key.GetName())
        output.Write()
        output.Close()
        input.Close()
        tmpFile = __tmpPath+'/'+outputFileName
        print 'copy to final location:\x1b[34m', outputFile, '\x1b[0m'
        self.fileLocator.cp(source=tmpFile, target=outputFile)
        print 'checking if the copy worked'
        # check root file existence
        if self.fileLocator.exists(outputFile, attempts=2):
            if self.fileLocator.isValidRootFile(outputFile):
                if self.debug:
                    print('DEBUG: file exists and is good!')
            else:
                raise Exception('ERROR: file DOES NOT exist OR is corrupted!')
        else:
            raise Exception("self.fileLocator.exists(outputFile, attempts=2) failed: file DOES NOT exist")
              
        self.fileLocator.rm(tmpFile)

    def copySingleFileOneInput(self, inputs):
        return self.copySingleFile(*inputs)

    def getRedirector(self):
        # default redirector
        redirector = 'root://xrootd-cms.infn.it/'
        try:
            if 'XBBXRD' in os.environ:
                redirector = os.environ['XBBXRD']
            elif self.config.has_option('Configuration', 'xrootdRedirectorGlobal'):
                redirector = self.config.get('Configuration', 'xrootdRedirectorGlobal')
        except:
            print "could not get xrootd redirector, using default one:", redirector
            print "specify redirector in config [Directories] xrootdRedirectorGlobal=.."
        # add base path where storage is located on fs (if sample txt files don't contain absolute path)
        if self.config.has_option('Configuration', 'inputStoragePath'):
            redirector += self.config.get('Configuration', 'inputStoragePath') + '/'
        return redirector

    def copytreePSI(self, pathIN, pathOUT, folderName, skimmingCut, fileList=None):
        config = self.config
        fileLocator = self.fileLocator

        print 'start copytreePSI.py'
        fileNames = open(pathIN+'/'+folderName+'.txt').readlines() if not fileList else fileList
        print 'len(filenames)', len(fileNames), fileNames[0], skimmingCut

        ## search the folder containing the input files
        inputFiles = []
        print "##### COPY TREE - BEGIN ######"
        whereToLaunch = config.get('Configuration','whereToLaunch')
        remove_branches = config.get('General','remove_branches').replace("[","").replace("]","").replace("'","").split(',')
        print 'remove_branches:',remove_branches,'len(remove_branches):',len(remove_branches)

        redirector = self.getRedirector()
        for fileName in fileNames:
            fileName = fileName.strip()
            if fileName.lower().endswith('.root'):
                inputFiles.append(redirector + fileName)

        if len(inputFiles) == 0 :
            print "No .root files found in ", pathIN+'/'+folderName
            return

        ## prepare output folder
        outputFolder = "%s/%s/" %(pathOUT, folderName)
        fileLocator.makedirs(outputFolder)
        
        ## prepare a list of input(inputFile,outputFile,skimmingCut) for the files to be processed
        inputs=[]
        filenames=[]
        for inputFile in inputFiles:
            fileName = fileLocator.getFilenameAfterPrep(inputFile)
            outputFile = "%s/%s/%s" %(pathOUT,folderName,fileName)
            
            if fileLocator.exists(outputFile):
                if not fileLocator.isValidRootFile(outputFile):
                    fileLocator.rm(outputFile)
                    inputs.append((whereToLaunch,inputFile,outputFile,skimmingCut,remove_branches))
                else:
                    if self.debug:
                        print("SKIP INPUT:", inputFile)
            else:
                inputs.append((whereToLaunch,inputFile,outputFile,skimmingCut,remove_branches))

        # print 'inputs',inputs
        outputs = []
        multiprocess=int(config.get('Configuration','nprocesses'))
        if multiprocess>1:
            ## process the input list (using multiprocess)
            from multiprocessing import Pool
            p = Pool(multiprocess)
            outputs = p.map(copySingleFileOneInput,inputs)
        else:
            for input_ in inputs:
                    output = self.copySingleFileOneInput(input_)
                    outputs.append(output)
        
        print "##### COPY TREE - END ######"

def filelist(pathIN,folderName):

    filenames = open(pathIN+'/'+folderName+'.txt').readlines()
    
    ## search the folder containing the input files
    inputFiles = []

    for filename_ in filenames:
        if '.root' in filename_ :
            inputFiles.append(filename_.rstrip('\n'))

    return inputFiles

