#!/bin/sh
condaDir=$1
outputDir=$2
txpipeDir=$3
dataDir=$4
shift 4

cd $condaDir

conda activate firecrown_clp
export DATA_DIR=$dataDir
export HDF5_DO_MPI_FILE_SYNC=0
export PYTHONPATH=$txpipeDir:$PYTHONPATH
ceci tests/CL_test_txpipe_concat.yml --yamlId Firecrown
cd tests/outputs
cosmosis cluster_counts_mean_mass_redshift_richness.ini

