#!/bin/bash

BASEDIR=`pwd`
STUDYDIR="studies/`date -Iseconds`"
echo $STUDYDIR
mkdir -p "$STUDYDIR"
git diff --submodule=diff > $STUDYDIR/git.diff
git status > $STUDYDIR/git.status
git describe --always > $STUDYDIR/git.describe

cd $STUDYDIR
echo "Running geth"
$BASEDIR/build/bin/geth "$@" 2>&1 | tee -i geth.log
