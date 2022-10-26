############################################################################################################
## Purpose: Main modeling pipeline script to be run after 03_collapse_contra.R
###########################################################################################################


## SET-UP --------------------------------------------------------------------

# clear memory
rm(list=ls())

# load packages
pacman::p_load(data.table,tidyverse,readxl,openxlsx)

## create function for the opposite of %in%
'%ni%' <- Negate('%in%')

# global settings
global.indicator_group <- "main_pipeline"
global.use_method_mix <- TRUE  # T/F whether to use mod_prop produced from method mix instead of direct estimates from data
global.date <- format(Sys.Date(), format = '%b%d%Y')
global.code.dir <- "FILEPATH"

# load parameter map and obtain list of modelable_entity_name's
param_map <- read_excel("FILEPATH", sheet = global.indicator_group) %>% as.data.table()
global.models <- param_map$modelable_entity_name

# set up new column with today's date to log new run_ids
if (global.date %ni% names(param_map)) param_map[, global.date] <- as.integer(0)

# load workbook and write data to the specified sheet
wb <- loadWorkbook("FILEPATH")
writeData(wb, global.indicator_group, param_map)
saveWorkbook(wb, "FILEPATH", overwrite = T)


## global.launch.model FUNCTION ------------------------------------------------------

# create helper function to run prep scripts, launch ST-GPR and track run_ids
global.launch_model <- function(global.me_name) {
  print(paste('Beginning data preparation for', global.me_name))
  
  # source prep script to prepare the data for ST-GPR
  source(paste0(global.code.dir,'04b_prep_input_data.R'))
  
  # source necessary functions for ST-GPR registration and sendoff
  central_root <- "FILEPATH"
  setwd(central_root)
  source("FILEPATH")
  source("FILEPATH")
  
  # source ST-GPR shared functions 
  source("FILEPATH")
  
  # obtain ST-GPR run_id and sendoff to be run
  run_id <- register_stgpr_model("FILEPATH", 
                                 model_index_id = param_map[modelable_entity_name == global.me_name]$model_index_id)
  stgpr_sendoff(run_id, "ADDRESS", nparallel = 100)
  
  # wait for ST-GPR to finish running before proceeding 
  Sys.sleep(800)
  while(check_run(run_id) == 2){Sys.sleep(60)}
  
  # check that run actually finished and didn't break 
  if (check_run(run_id) != 1) stop(paste0('Run for ', global.me_name, ' broke! Find error before continuing!'))
  print('Run finished!')
  
  # record successful run
  param_map[modelable_entity_name == global.me_name, latest := run_id]
  param_map[modelable_entity_name == global.me_name, (global.date) := run_id]
  
  # save parameter map with new run_id 
  # load workbook and write data to the specified sheet
  global.indicator_group <- "main_pipeline"
  wb <- loadWorkbook("FILEPATH")
  writeData(wb, global.indicator_group, param_map)
  saveWorkbook(wb, "FILEPATH", overwrite = T)
}


## LAUNCH MAIN PIPELINE #####################################################################

## 1. MERGE REPORT DATA AND MICRODATA FOR MARITAL STATUS --------------------------------------

# script-specific settings
global.marital_only <- T

# source script to prepare marital status data files 
source(paste0(global.code.dir, '04a_mergeon_reports.R'))


## 2. MARITAL STATUS ESTIMATES ---------------------------------------------------------

for (global.me_name in global.models[1:2]) global.launch_model(global.me_name)


## 3. MERGE REPORT DATA AND MICRODATA FOR CONTRA VARS -----------------------------------

# script-specific settings
global.marital_only <- F

# source script to prepare contraceptive use data files 
source(paste0(global.code.dir, '04a_mergeon_reports.R'))


## 4. MARRIED AND UNMARRIED CONTRA ESTIMATES --------------------------------------------

# any_contra
for (global.me_name in global.models[3:6]) global.launch_model(global.me_name)

# mod_prop

# general settings
jdrive_flag <- "-C archive"
partition_flag <- "-p long.q"
error_filepath <- "FILEPATH"
output_filepath <- "FILEPATH"
account_flag <- "-A ADDRESS"

# run method mix models

# script-specific settings
job_name <- "launch_method_mix"
thread_flag <- "-c 4"
mem_flag <- "--mem=60G"
runtime_flag <- "-t 48:00:00"
next_script <- paste0("FILEPATH/04c_launch_method_mix.R")

# add jdrive_flag if needed
sbatch_command <- paste("sbatch", thread_flag,
                        "-J", job_name, account_flag, mem_flag, runtime_flag, partition_flag,
                        "-e", error_filepath, "-o", output_filepath,
                        "FILEPATH -s", next_script)

# submit sbatch
system(sbatch_command)


# compile method mix draws; need method mix mod_prop results for 05_launch_compile_draws.R

# script-specific settings
job_name <- "launch_compile_draws_method_mix"
thread_flag <- "-c 18"
mem_flag <- "--mem=200G"
runtime_flag <- "-t 10:00:00"
prev_job <- "launch_pipeline"
next_script <- paste0("FILEPATH/04d_launch_compile_draws_method_mix.R")

# add jdrive_flag if needed
sbatch_command <- paste("sbatch", thread_flag,
                        "-N", job_name, account_flag, mem_flag, runtime_flag, partition_flag,
                        "-e", error_filepath, "-o", output_filepath,
                        "FILEPATH -s", next_script)

# submit sbatch
system(sbatch_command)


# unmet_prop
for (global.me_name in global.models[11:14]) global.launch_model(global.me_name)
