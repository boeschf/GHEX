#!/usr/bin/python

# https://github.com/BlueBrain/git-cmake-format

from __future__ import print_function
import os
import subprocess
import sys
import time

Git='git'
Diff='diff'
Sed='sed'
ClangFormat='clang-format'
Style='-style=file'
IgnoreList=[]
ExtensionList=['.h', '.cpp', '.hpp', '.c', '.cc', '.hh', '.cxx', '.hxx', '.cu', '.m']

def getGitHead():
    RevParse = subprocess.Popen([Git, 'rev-parse', '--verify', 'HEAD'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    RevParse.communicate()
    if RevParse.returncode:
        return '4b825dc642cb6eb9a060e54bf8d69288fbee4904'
    else:
        return 'HEAD'

def getGitRoot():
    RevParse = subprocess.Popen([Git, 'rev-parse', '--show-toplevel'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return RevParse.stdout.read().strip()

def getEditedFiles(InPlace):
    Head = getGitHead()
    GitArgs = [Git, 'diff-index']
    if not InPlace:
        GitArgs.append('--cached')
    GitArgs.extend(['--diff-filter=ACMR', '--name-only', Head])
    DiffIndex = subprocess.Popen(GitArgs, stdout=subprocess.PIPE)
    DiffIndexRet = DiffIndex.stdout.read().strip()
    DiffIndexRet = DiffIndexRet.decode()

    return DiffIndexRet.split('\n')

def isFormattable(File):
    for Dir in IgnoreList:
        if '' != Dir and '' != os.path.commonprefix([os.path.relpath(File), os.path.relpath(Dir)]):
            return False
    Extension = os.path.splitext(File)[1]
    for Ext in ExtensionList:
        if Ext == Extension:
            return True
    return False

def formatFile(FileName, GitRoot):
    subprocess.Popen([ClangFormat, Style, '-i', os.path.join(GitRoot,FileName)])
    return

def patchFile(FileName,PatchFile):
    GitShowRet = subprocess.Popen([Git, "show", ":" + FileName], stdout=subprocess.PIPE)
    ClangFormatRet = subprocess.Popen([ClangFormat, Style], stdin=GitShowRet.stdout, stdout=subprocess.PIPE)
    DiffRet = subprocess.Popen([Diff, "-u", FileName, "-"], stdin=ClangFormatRet.stdout, stdout=subprocess.PIPE)
    SedRet = subprocess.Popen([Sed, "-e", "1s|--- |--- a/|", "-e", "2s|+++ -|+++ b/" + FileName + "|"], stdin=DiffRet.stdout, stdout=PatchFile)
    SedRet.communicate()

def printUsageAndExit():
    print("Usage: " + sys.argv[0] + " [--pre-commit|--cmake] " +
          "[<path/to/git>] [<path/to/clang-format]")
    sys.exit(1)

if __name__ == "__main__":
    if 2 > len(sys.argv):
        printUsageAndExit()

    if "--pre-commit" == sys.argv[1]:
        InPlace = False
    elif "--cmake" == sys.argv[1]:
        InPlace = True
    else:
        printUsageAndExit()

    for arg in sys.argv[2:]:
        if "git" in arg:
            Git = arg
        elif "clang-format" in arg:
            ClangFormat = arg
        elif "-style=" in arg:
            Style = arg
        elif "-ignore=" in arg:
            IgnoreList = arg.strip("-ignore=").split(";")
        else:
            printUsageAndExit()

    EditedFiles = getEditedFiles(InPlace)

    ReturnCode = 0

    if InPlace:
        GitRoot = getGitRoot()
        for FileName in EditedFiles:
            if isFormattable(FileName):
                formatFile(FileName,GitRoot)
        sys.exit(ReturnCode)

    Prefix = "pre-commit-clang-format"
    Suffix = time.strftime("%Y%m%d-%H%M%S")
    PatchName = "/tmp/" + Prefix + "-" + Suffix + ".patch"
    f = open(PatchName, "w+")

    for FileName in EditedFiles:
        if not isFormattable(FileName):
            continue
        patchFile(FileName,f)

    f.seek(0)
    if not f.read(1):
        f.close()
        print("Files in this commit comply with the clang-format rules.")
        os.remove(PatchName)
    else:
        f.seek(0)
        ReturnCode = 1
        print("The following differences wre found between the code to commit")
        print("and the clang-format rules:")
        print()
        print(f.read())
        f.close()
        print()
        print("You can apply these changes with:")
        print("git apply --index ", PatchName)
        print("(may need to be called from the root directory of your repository)")

    sys.exit(ReturnCode)
