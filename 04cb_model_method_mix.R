############################################################################################################
## Purpose: Modeling pipeline for method mix
###########################################################################################################


## CLUSTER ARRAY JOB SETTINGS ------------------------------------------------

library(data.table)

# Getting sbatch arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]

# Retrieving array task_id
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
param_map <- fread(param_map_filepath, header = T)

# variable from the param_map that would have been in your loop
# only edit this line!!!
global.me_root <- param_map[task_id, var]
global.indicator_group <- param_map[task_id, group]


## SET-UP --------------------------------------------------------------------

print(global.me_root)
print(global.indicator_group)

# pause script for random amount of time up to 2 minutes to stagger calling/writing of param_map
Sys.sleep(runif(n = 1, max = 120))

# load packages
pacman::p_load(data.table,tidyverse,openxlsx,readxl)

## create function for the opposite of %in%
'%ni%' <- Negate('%in%')

# global settings
global.date <- format(Sys.Date(), format = '%b%d%Y')
global.code.dir <- "FILEPATH"

# load parameter map and set up new column with today's date to log new run_ids
param_map <- read_excel("FILEPATH", sheet = global.indicator_group) %>% as.data.table()
if (global.date %ni% names(param_map)) param_map[, global.date] <- as.integer(0)

# load entire contraception_ids workbook and write data to the specified sheet
wb <- loadWorkbook("FILEPATH")
writeData(wb = wb, sheet = global.indicator_group, x = param_map)
saveWorkbook(wb, "FILEPATH", overwrite = T)


## global.launch.model FUNCTION ------------------------------------------------------

# merge reports and microdata
source(paste0(global.code.dir, 'FILEPATH/04ca_mergeon_reports_method_mix.R'))

# create helper function to run prep scripts, launch ST-GPR and track run_ids
for (submodel in c("_mar_prelim", "_unmar_prelim", "_mar_final", "_unmar_final")) {
  
  global.me_name <- paste0(global.me_root, submodel)
  print(paste('Beginning data preparation for', global.me_name))
  
  # source prep script to prepare the data for ST-GPR
  source(paste0(global.code.dir,'04b_prep_input_data.R'), local = T)
  
  # source necessary functions for registration and sendoff
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
  while(check_run(run_id) == 2){Sys.sleep(30)}
  
  # check that run actually finished and didn't break 
  if (check_run(run_id) != 1) stop(paste0('Run for ', global.me_name, ' broke! Find error before continuing!'))
  print('Run finished!')
  
  # record successful run
  param_map <- read_excel("FILEPATH", sheet = global.indicator_group) %>% as.data.table()
  param_map[modelable_entity_name == global.me_name, latest := run_id]
  param_map[modelable_entity_name == global.me_name, (global.date) := run_id]
  
  # save parameter map with new run_id 
  # load entire contraception_ids workbook and write data to the specified sheet
  wb <- loadWorkbook("FILEPATH")
  writeData(wb = wb, sheet = global.indicator_group, x = param_map)
  saveWorkbook(wb, "FILEPATH", overwrite = T)
}