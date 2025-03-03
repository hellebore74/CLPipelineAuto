
import parsl
import glob
import os
import sys
import yaml

import configparser
import argparse

from parsl.app.app import join_app, python_app, bash_app

from parsl.config import Config
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_interface
from parsl.monitoring.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname

# Uncomment this to see logging info
#import logging
#logging.basicConfig(level=logging.DEBUG)

# Confid defines the different backends that might be used (batch, local, ...)
config = Config(
    executors=[
        HighThroughputExecutor(
            label='txpipe',
            provider=SlurmProvider(
                partition="hpc",
                exclusive=False,
                nodes_per_block=1,
                # string to prepend to #SBATCH blocks in the submit
                scheduler_options='#SBATCH --time=05:00:00\n#SBATCH --partition=hpc\n#SBATCH --ntasks=30\n#SBATCH --cpus-per-task=1\n#SBATCH --mem=128000\n',
                # Command to be run before starting a worker
                worker_init='module load python; source activate parsl',
                init_blocks=1,
                max_blocks=10,
            ),
        ),

        HighThroughputExecutor(
            label='tjpcov_batch',
            provider=SlurmProvider(
                partition="hpc,lsst",
                exclusive=False,
                nodes_per_block=1,
                # string to prepend to #SBATCH blocks in the submit
                scheduler_options='#SBATCH --time=05:00:00\n#SBATCH --partition=hpc,lsst\n#SBATCH --cpus-per-task=1\n#SBATCH --mem=128000',
                # Command to be run before starting a worker
                worker_init='module load python; source activate parsl',
                init_blocks=1,
                max_blocks=10,
            ),
        ),

        # HighThroughputExecutor(
        #     label='tjpcov_local',
        #     provider=SlurmProvider(
        #         partition="htc",
        #         exclusive=False,
        #         nodes_per_block=1,
        #         # string to prepend to #SBATCH blocks in the submit
        #         scheduler_options='#SBATCH --ntasks=1\n#SBATCH --mem=9G\n#SBATCH --time=0-05:00:00\n',
        #         # Command to be run before starting a worker
        #         worker_init='module load python; source activate parsl',
        #         init_blocks=1,
        #         max_blocks=10,
        #     ),
        # ),

        HighThroughputExecutor(
            label='firecrown_batch',
            provider=SlurmProvider(
                partition="hpc,lsst",
                exclusive=False,
                nodes_per_block=1,
                # string to prepend to #SBATCH blocks in the submit
                scheduler_options='#SBATCH --time=05:00:00\n#SBATCH --partition=hpc,lsst\n#SBATCH --cpus-per-task=1\n#SBATCH --mem=128000',
                # Command to be run before starting a worker
                worker_init='module load python; source activate parsl',
                init_blocks=1,
                max_blocks=20,
            ),
        ),

        # HighThroughputExecutor(
        #     label='firecrown_local',
        #     provider=SlurmProvider(
        #         partition="htc",
        #         exclusive=False,
        #         nodes_per_block=1,
        #         # string to prepend to #SBATCH blocks in the submit
        #         scheduler_options='#SBATCH --ntasks=1\n#SBATCH --mem=54G\n#SBATCH --time=0-05:00:00\n',
        #         # Command to be run before starting a worker
        #         worker_init='module load python; source activate parsl',
        #         init_blocks=1,
        #         max_blocks=10,
        #     ),
        # ),
    ],

    monitoring=MonitoringHub(
        hub_address=address_by_hostname(),
        hub_port=55055,
        monitoring_debug=False,
        resource_monitoring_interval=10,
    ),
)

# load the Parsl config
parsl.load(config)


from parsl import python_app, bash_app

    
# Definiton of the different types of jobs that are going to be launched
#   if a job has to be launched once another is done, it has to be defined as first paareter
@bash_app(executors=["txpipe"])
def run_txpipe(condadir, extra_setup, yamlfile, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    return 'sh txpipe_script.sh {} {} {}'.format(condadir, extra_setup, yamlfile)

# @bash_app(executors=["tjpcov_local"])
# def run_tjpcov_local(txpipe_job, condadir, outputdir, txpipedir, datadir, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
#     return 'sh tjpcov_script.sh {} {} {} {}'.format(condadir, outputdir, txpipedir, datadir)

@bash_app(executors=["tjpcov_batch"])
def run_tjpcov_batch(tyamlfile, condadir, outputdir, txpipedir, datadir, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    return 'sh tjpcov_script.sh {} {} {} {}'.format(condadir, outputdir, txpipedir, datadir)

@bash_app(executors=["firecrown_batch"])
def run_firecrown_batch(tjpcov_job,condadir, outputdir, txpipedir, datadir, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    return 'sh firecrown_script.sh {} {} {} {}'.format(condadir, outputdir, txpipedir, datadir,)

# @bash_app(executors=["firecrown_local"])
# def run_firecrown_local(tjpcov_job,condadir, outputdir, txpipedir, datadir,, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
#     return 'sh firecrown_script.sh {} {} {} {}'.format(condadir, outputdir, txpipedir, datadir)



def main():

    # --- Option parser
    parser = argparse.ArgumentParser()

    # define task/data/catalog/level IDs
    parser.add_argument("--create", type=str, help="pipeline configfile name")
    parser.add_argument("--run", type=str, help="pipeline id")
    args = parser.parse_args()
    print(args)

    
    # --------------------------------------
    #  Create pipeline configuration files
    # --------------------------------------

    if args.create:
        from toolbox import createPipelineSetup
        createPipelineSetup(args.create)

        sys.exit()


    if not args.run: sys.exit()

    bashing = []

    # ------------------------------
    #  TxPipe
    # ------------------------------

    # Name of the CLPipeline yaml file

    from toolbox import read_yaml_file_general, get_batch_data
    clpipe_config,_ = read_yaml_file_general(args.run)
    for key in clpipe_config:
        print(clpipe_config[key])

    general_CLpipeline_yaml = clpipe_config["pipeline"]["CLpipeline_yaml"]
    extra_script_setup=";".join([clpipe_config["setup"]["extra_conda_setup"],clpipe_config["setup"]["extra_mpi_setup"]])

    site_data, maxProcess  = get_batch_data(general_CLpipeline_yaml, "TXPipe")

    print(site_data)
    print(maxProcess)

    sys.exit()
        
    # todo : check if job is batch/mpi or local by reading yaml file site//name
    #        check if txpipe is done

    bTxPipeJobDone = False
    jobTxPipe = None

    if not bTxPipeJobDone:
        print(">>>> TXPIPE JOB")
    

    
    
        jobTxPipe = run_txpipe( clpipe_config["txpipe"]["conda_dir"],
                                extra_script_setup,
                                general_CLpipeline_yaml
                                )
        bashing.append(jobTxPipe)

    sys.exit()

    # ------------------------------
    #  CLpipeline
    # ------------------------------

    # if jobTxPipe is None => jobTjpCov will start immediately
    #                         otherwise, TjpCov wiil start automatically when txpipe is done

    jobTjpCov = run_tjpcov(jobTxPipe,
                            clpipe_config["tjpcov"]["conda_dir"],
                            clpipe_config["tjpcov"]["output_dir"],
                            clpipe_config["txpipe"]["conda_dir"],  
                            clpipe_config["txpipe"]["data_dir"],
                            )                        


    # if jobTjpCov is None => jobTjpCov will start immediately
    #                         otherwise, jobFireCrown will start automatically when jobTjpCov is done
    jobFireCrown = run_firecrown(jobTjpCov,
                            clpipe_config["firecrown"]["conda_dir"],
                            clpipe_config["firecrown"]["output_dir"],
                            clpipe_config["txpipe"]["conda_dir"],  
                            clpipe_config["txpipe"]["data_dir"],
                            )   


    # ------------------------------------------
    # Parsl loop - process the collected jobs 
    # ------------------------------------------
    ## Bash app futures are files,
    for i,r in enumerate(bashing):
        try:
            # block for execution
            r.result()
            with open(r.stdout, 'r') as f:
                print(f.read())
        except:
            print("No result for job ",i)


main()

#if __name__=="main":
#
#    main()

