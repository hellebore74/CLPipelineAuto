#!/bin/sh
condaDir=$1
outputDir=$2
txpipeDir=$3
dataDir=$4
shift 4

cd $condaDir
conda activate txpipe_clp 
export DATA_DIR=$dataDir
export HDF5_DO_MPI_FILE_SYNC=0
export PYTHONPATH=$txpipeDir:$PYTHONPATH
ceci tests/CL_test_txpipe_concat.yml --yamlId TJPCov
conda deactivate

