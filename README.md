# CLpipelineAuto

Create and configure input yaml files for a given survey :

- configure the clpipeline_generic.yml file  (pipeline_id and local_dir)
- define the survey  ( survey data directory and survey data file list)

- a copy of pipeline and config yaml files are availebale in inputs_config_generic
    pathes should be defined by their absolute value
    environmenet variables as defined in clpipeline_generic.yml file (setup:env_variables)
        can be used to define the pathes, they will be replaced automaticallly by their real values
    
- a local directory with a named based on the pipeline_id will be created in local_dir
    a copy of all the pipeline and config yaml files are  available in this directory
    the files are fully configured


To create the pipeline files :

    python parsl_jobs --create clpipeline_generic.yml


To launch the jobs -- currently in development

