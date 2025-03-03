#!/usr/bin/bash

condaDir=$1
extra_setup=$2
yamlFile=$3
shift 3

# CC-IN2P3 specific package 
echo "SETUP $extra_setup"
$extra_setup
module list

echo "txpipe_script => setup conda  $condaDir"
cd $condaDir
echo "pwd : `pwd`"
export CONDA_ENVS_PATH="${PWD}/.conda/envs/"
conda info --envs

conda activate txpipe_clp

# Launch ceci
echo "txpipe_script => start ceci : $yamlfile"
echo "ceci $yamlFile --yamlId TXPipe"
ceci $yamlFile --yamlId TXPipe



