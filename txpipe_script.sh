#!/usr/bin/bash

setup_script=$1
shift 1

source $setup_script

# CC-IN2P3 specific package 
cmd_extra_setup="$EXTRA_MPI_SETUP"
echo "SETUP $cmd_extra_setup"
$cmd_extra_setup
module list

echo "txpipe_script => setup conda  $CONDA_PIPELINE_DIR"
cd ${CONDA_PIPELINE_DIR}
echo "pwd : `pwd`"
export CONDA_ENVS_PATH="${PWD}/.conda/envs/"
conda info --envs

conda activate txpipe_clp
export PYTHONPATH=${TXPIPE_INSTALL_DIR}:${PYTHONPATH}

# Launch ceci
echo "txpipe_script => start ceci : $yamlfile"
echo "ceci $CLPIPELINE_YAML --yamlId TXPipe"
ceci $CLPIPELINE_YAML --yamlId TXPipe





