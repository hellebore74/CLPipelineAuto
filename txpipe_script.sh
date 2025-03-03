#!/bin/sh

condaDir=$1
extra_setup=$2
yamlFile=$3
shift 3

# CC-IN2P3 specific package 
$extra_setup

echo "txpipe_script => setup conda"
cd $condaDir
conda activate txpipe_cpl

# Launch ceci
echo "txpipe_script => start ceci : $yamlfile"
echo "ceci $yamlFile --yamlId TXPipe"
ceci $yamlFile --yamlId TXPipe



