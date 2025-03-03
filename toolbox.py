
import os
import sys
import shutil
import yaml

import ruamel.yaml
yaml_ruamel = ruamel.yaml.YAML()
yaml_ruamel.indent(mapping=2, sequence=4, offset=2)


def setup_environment_parameter(pipe_config,jobId, bVerbose=False):
    """ Extract the list of environment variables from setup:env_variables 
        and set them using os.environ if they are define in the
        pipe_config data """

    if not "setup" in pipe_config: return
    if not "env_variables" in pipe_config["setup"]: return

    # Get the variable list"
    envParam = [x.strip() for x in pipe_config["setup"]["env_variables"].split(" ") if x.strip()!=""]
    newParamList=[]
    for pName in envParam:
        pEnvName = pName.upper()
        # define new env parameter for the defined jobId (txpipe, tjpcov, ...)
        if pName in pipe_config[jobId]: newParamList.append((pEnvName,pipe_config[jobId][pName]))
            
    print(f"Env parameter to be set : {newParamList}")
    for p in newParamList:
        name,value = p
        os.environ[name] = value
    if not bVerbose: return

    for p in envParam:
        pName = p.upper()
        if pName in os.environ: print(f"{pName} : {os.environ[pName]}")
        else : print(f"{pName} : unset")


def reset_environment_parameter(pipe_config):
    """ Unset the environement variables defined in setup::env_variables """

    if not "setup" in pipe_config: return
    if not "env_variables" in pipe_config["setup"]: return

    envParam = [x.strip() for x in pipe_config["setup"]["env_variables"].split(" ") if x.strip()!=""]
    for pName in envParam:
        pEnvName = pName.upper()
        # delete env parameter if it is already defined
        if pEnvName in os.environ: os.environ.pop(pEnvName)

    return

#def read_yaml_file(filename):
#    """ Read a yaml file content, return raw_text and yaml configured object"""
    
#    # YAML input file.
#    # Load the text and then expand any environment variables
#    with open(filename) as config_file:
#        raw_config_text = config_file.read()
#    config_text_init = os.path.expandvars(raw_config_text)
#    # Then parse with YAML
#    pipe_config = yaml.safe_load(config_text_init)
    
#    return pipe_config, raw_config_text


def merge_yaml_files(filename1, filename2, output_filename):
    """ Merge two yaml files, object from second file are added to first file
        ruamel is ised in order to keep line ranking and commented out lines """

    #Load the yaml files
    with open(filename1) as fp:
        data1 = yaml_ruamel.load(fp)
    with open(filename2) as fp:
        data2 = yaml_ruamel.load(fp)

    for key in list(data2):
        if not key in data1 or data1[key]==None:
            if isinstance(data2[key],dict):
                data1[key]=data2[key].copy()
                continue
            elif isinstance(data2[key],list):
                data1[key]=data2[key][:]
                continue
            else:
                print("-- undefined type / new object ", type(data2[key]))
                sys.exit()
        else:
            if isinstance(data1[key],dict):
                for i in data2[key]:
                    print(i,data2[key][i])
                    data1[key].update({i:data2[key][i]})
            elif isinstance(data1[key],list):
                data1[key]=data1[key]+data2[key]
            else:
                print("-- undefined type / concatenate")
                sys.exit()
            
    #create a new file with merged yaml
    yaml_ruamel.dump(data1,open(output_filename, 'w'))

    return

def yaml_get_value(filename, key):
    """ Get values from a yaml file based on a key
         - if yaml is a simple file : return the value
         - if yaml is a multiple dicument file : return a list of values """
    
    #Load the yaml files
    with open(filename) as config_file:
        raw_config_text = config_file.read()
    data_config = yaml_ruamel.load_all(raw_config_text)

    subKey=None
    if ":" in key: key,subKey=key.split(":")
    
    dataRes=[]
    for data in data_config:
        if data==None: continue
        if not key in data: dataRes.append(None)
        else:
            if subKey: dataRes.append(data[key][subKey])
            else: dataRes.append(data[key]) 
        
    if len(dataRes)==1: return dataRes[0]
    return dataRes


def yaml_set_value(filename, key, value, index=0):
    """ Set value associated to a given key and ind
          - if yaml is a simple fiel : set the value associated to the key
          - if yaml is a multiple document file : set the value of the document No index 
    """

    #Load the yaml files
    with open(filename) as config_file:
        raw_config_text = config_file.read()
    data_config = yaml_ruamel.load_all(raw_config_text)

    subKey=None
    if ":" in key: key,subKey=key.split(":")
    
    dataList=[]
    for i,data in enumerate(data_config):

        if i!=index: continue
    
        if isinstance(value,dict):
            if not key in data: data[key]={}
            if subKey: data[key][subKey]=value.copy()
            else: data[key]=value.copy()
        elif isinstance(value,list):
            if not key in data: data[key]=[]
            if subKey: data[key][subKey]=value[:]
            else: data[key]=value[:]
        else:
            if subKey: data[key][subKey]=value
            else: data[key]=value

        dataList.append(data)

    #update yaml file
    with open(filename,"w") as stream:
        yaml_ruamel.dump_all(dataList, stream)

    return
        
# def update_yaml_file_document(filename, filedoc, patternList):

#     import ruamel.yaml
#     yaml = ruamel.yaml.YAML()

#     print(f"YAML : add {filedoc} into {filename} / pattern {patternList}")

#     #Load the yaml files
#     docList=[]
#     with open(filedoc) as config_file2:
#         raw_config_text2 = config_file2.read()
#     doc_config = yaml.load_all(raw_config_text2)
#     for doc in doc_config: 
#         docList.append(doc)


#     #Load the yaml files
#     with open(filename) as config_file:
#         raw_config_text = config_file.read()
#     data_config = yaml.load_all(raw_config_text)

#     dataList=[]
#     for data in data_config:
#         if data==None: continue
#         bPatternFound = True
#         for (key,value) in patternList:
#             if not key in data or data[key]!=value: bPatternFound=False

#         if bPatternFound:
#             for doc in docList:
#                 doc["id"]="TXpipe"
#                 dataList.append(doc)
#         else:
#             dataList.append(data)
            

#     #update yaml file
#     with open(filename,"w") as stream:
#         yaml.dump_all(dataList, stream)

#     return


def get_document_from_yaml_file(filename, patternList):

    #Load the yaml files
    with open(filename) as config_file:
        raw_config_text = config_file.read()
    data_config = yaml_ruamel.load_all(raw_config_text)

    for data in data_config:
        if data==None: continue
        bPatternFound = True
        for (key,value) in patternList:
            if not key in data or data[key]!=value: bPatternFound=False
        if bPatternFound: return data

    return None
            

def update_yaml_file_document(filename, filedoc, pattern):
    """ Insert content of a yaml file (filedoc) in an initial file (filename)]
          starting from the position given by the pattern
        ( used to insert TXpipe yaml file in concatenated yaml file)   
     """
    f=open(filename,"r")
    text=f.readlines()
    f.close()

    pattern = pattern.replace(" ","")

    index=-1
    for i,l in enumerate(text):
        if l.strip().replace(" ","")==pattern: 
            index=i
            break

    if index<0: return

    f=open(filedoc,"r")
    text_doc=f.readlines()
    f.close()

    text_res=text[0:index+1]
    text_res=text_res+text_doc
    text_res=text_res+text[index+1:]

    f=open(filename,"w")
    f.writelines(text_res)
    f.close()

    return


def read_yaml_file_general(filename, index=0, bVerbose=False):
    """ Used to check if a yaml file is well formatted """

    #Load the yaml files
    with open(filename) as config_file:
        raw_config_text = config_file.read()
    data_config = yaml_ruamel.load_all(raw_config_text)

    dataList=[]
    for data in data_config:
        dataList.append(data)

    return dataList[0], raw_config_text


#    # YAML input file.
#    # Load the text and then expand any environment variables
#    with open(filename) as config_file:
#        raw_config_text = config_file.read()
#    config_text_init = os.path.expandvars(raw_config_text)
#    # Then parse with YAML
#    pipe_config = yaml.safe_load(config_text_init)
    
#    return pipe_config, raw_config_text



def expand_variable_yaml_file(fileinit,fileres,inputList):
    """ Replaces all the environement variables by their values as defined in os.environ
        ex: ${LOCAL_DIR} becomes /sps/lsst/....
    """
    f=open(fileinit,"r")
    text = f.readlines()
    f.close()
    textLine = "".join(text)

    variableList = [x.strip() for x in inputList.split(" ") if x.strip()!=""]
    print("Expand environment variable : ",variableList)
    bVariableFound = True
    while(bVariableFound):
        bVariableFound = False
        for v in variableList:
            vName = v.upper()
            if not vName in os.environ: continue
            varName = "${"+vName+"}"
            if varName in textLine:
                bVariableFound = True
                textLine = textLine.replace(varName,os.environ[vName])

    if fileres==None: fileres=fileinit
    f=open(fileres,"w")
    f.writelines(textLine)
    f.close()

    return


def get_batch_data(filename, dataId):

    data = get_document_from_yaml_file(filename,[("id",dataId)])

    nprocessList=[]
    for d in data["stages"]:
        if "nprocess" in d: nprocessList.append(d["nprocess"])

    return data["site"], max(nprocessList)
    

def read_and_decode_general_pipeline(filename,output_filename):
    """ Read the pipeline yaml file and expand all the environement variables 
        Save the result in the pipeline directory
    """

    pipe_config, raw_text = read_yaml_file_general(filename)
    
    # Create environment variables for the pipeline
    setup_environment_parameter(pipe_config, "pipeline", True)
    setup_environment_parameter(pipe_config, "survey", True)
    setup_environment_parameter(pipe_config, "txpipe", True)
    config_text_init = os.path.expandvars(raw_text)
    pipe_config_expand = yaml_ruamel.load(config_text_init)

    setup_environment_parameter(pipe_config, "tjpcov_firecrown", True)
    config_text_init = os.path.expandvars(raw_text)
    pipe_config_expand2 = yaml_ruamel.load(config_text_init)

    pipe_config_expand["tjpcov_firecrown"]=pipe_config_expand2["tjpcov_firecrown"].copy()

    #update yaml file
    yaml_ruamel.dump(pipe_config_expand,open(output_filename, 'w'))
    
    return


def createPipelineSetup(filename):
    """ Create the global pipeline yaml files  (TXpipe, TJPCov and FireCrown)
        - create a directory named ${LOCAL_DIR}/pipeline_id
        - copy the yaml config and pipeline files (TxPipe and CLPipeline ) in the directory
        - configure all the files by replacing environement parameters by their values
        - concatenate CLPipeline and TX pipe pipeline yaml files 

        all the files needed to launch the CLPipeline are stored in the directory with
          fully defined pathes for all the references to output and log dirs, data files, etc...
          ==> the CLPipeline can be launched from any filesystem directory 
    """

    pipe_config, raw_text = read_yaml_file_general(filename)
    
    # Create environment variables for the pipeline
    setup_environment_parameter(pipe_config, "pipeline", True)

    # Create directory based on pipeline Id and save a copy of the current clpipeline yml file
    pipeDir = os.environ["LOCAL_DIR"]+"/"+pipe_config["pipeline"]["pipeline_id"]
    print(f"Pipeline directory : {pipeDir}")
    if not os.path.isdir(pipeDir): os.makedirs(pipeDir)

    final_global_pipeline=pipeDir+"/clpipeline.yml"
    read_and_decode_general_pipeline(filename,final_global_pipeline)

    # ---------------------------------------------------------
    # Create TxPipe yaml file
    # ---------------------------------------------------------

    # Set the environement variables as defined in yaml file
    setup_environment_parameter(pipe_config, "txpipe", True)
    setup_environment_parameter(pipe_config, "survey", True)

    # Read the clpipeline yaml file
    config_text_init = os.path.expandvars(raw_text)
    pipe_config_env = yaml.safe_load(config_text_init)

    # Merge txpipe yaml and survey data files
    #   copy the txpipe and survey yaml files and merge them (inputs key)
    txpipe_tmp_yaml=pipeDir+"/txpipe_tmp.yml"
    print("Temporary txpipe yaml file : ", txpipe_tmp_yaml)
    merge_yaml_files(pipe_config_env["txpipe"]["pipeline_yaml"],  
                        pipe_config_env["survey"]["survey_data_files"],     
                        txpipe_tmp_yaml)    
    
    # Finalize the txpipe yaml file  (replace env variables by real names)
    #     a txpipe_standalone.ymal is stored in the pipeline directory
    txpipe_final_yaml=pipeDir+"/txpipe_standalone.yml"
    expand_variable_yaml_file(txpipe_tmp_yaml, txpipe_final_yaml,pipe_config["setup"]["env_variables"])
    yaml_set_value(final_global_pipeline,"txpipe:pipeline_yaml",txpipe_final_yaml)
    
    # Copy the txpipe config yaml file to pipeline directory
    configYamlFile = yaml_get_value(txpipe_final_yaml, "config")
    local_configYamlFile = pipeDir+"/"+configYamlFile.split("/")[-1]
    shutil.copy(configYamlFile,local_configYamlFile)
    # Modify the config key in the txpipe yaml in order to point to the pipedir config file
    yaml_set_value(txpipe_final_yaml,"config",local_configYamlFile)
    yaml_set_value(final_global_pipeline,"txpipe:config_yaml",local_configYamlFile)
    
    # Finalize the config yaml file (replace env variables by real names)
    expand_variable_yaml_file(local_configYamlFile, None, pipe_config["setup"]["env_variables"])
    

    # ---------------------------------------------------------
    # Create TJPCov and FireCrown yaml file
    # ---------------------------------------------------------

    # Set the environement variables as defined in clpipeline yaml file
    reset_environment_parameter(pipe_config)
    setup_environment_parameter(pipe_config, "pipeline", True)
    setup_environment_parameter(pipe_config, "tjpcov_firecrown", True)
    setup_environment_parameter(pipe_config, "survey", True)

    # Read the tjpcov_firecrown section from yaml file
    config_text_init = os.path.expandvars(raw_text)
    pipe_config_env = yaml.safe_load(config_text_init)

    # Copy the tjpcov_firecron pipeline yaml file to the pipeline directory
    pipelineYamlFile = pipe_config_env["tjpcov_firecrown"]["pipeline_yaml"]
    local_pipelineYamlFile = pipeDir+"/"+pipelineYamlFile.split("/")[-1]
    shutil.copy(pipelineYamlFile,local_pipelineYamlFile)

    # Finalize the pipeline yaml file (replace env variables by real names)
    expand_variable_yaml_file(local_pipelineYamlFile, None, pipe_config["setup"]["env_variables"])
    yaml_set_value(final_global_pipeline,"tjpcov_firecrown:pipeline_yaml",local_pipelineYamlFile)
    yaml_set_value(final_global_pipeline,"pipeline:CLpipeline_yaml",local_pipelineYamlFile)
    
    # Copy the config.yaml file to the pipeline directory
    #    ( taking into account that config files are defined in different sections of the yaml file )
    configYamlFile = yaml_get_value(local_pipelineYamlFile, "config")
    print(">>>>>>>> " ,configYamlFile)
    copiedYamlFiles=[]
    for nameConfig in configYamlFile:
        if nameConfig==None:
            copiedYamlFiles.append(None)
            continue
        local_configYamlFile = pipeDir+"/"+nameConfig.split("/")[-1]
        if local_configYamlFile in copiedYamlFiles:
            copiedYamlFiles.append(local_configYamlFile)
            continue
        # copy yaml file to pipeline directory
        shutil.copy(nameConfig,local_configYamlFile)
        # Finalize config yaml file (replace env variables by real names)
        expand_variable_yaml_file(local_configYamlFile, None, pipe_config["setup"]["env_variables"])

    # Update config yaml file names in pipeline yaml file
    for i,v in enumerate(copiedYamlFiles):
        if v==None: continue
        yaml_set_value(local_pipelineYamlFile,"config",v,i)

    yaml_set_value(final_global_pipeline,"tjpcov_firecrown:config_yaml",local_configYamlFile)


    # ---------------------------------------------------------
    # Update TxPipe document nested in concatenated yaml file
    # ---------------------------------------------------------
    update_yaml_file_document(local_pipelineYamlFile,txpipe_final_yaml,"id:TXPipe")


    


    
