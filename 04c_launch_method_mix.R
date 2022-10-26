############################################################################################################
## Purpose: Launch an array job of method mix models
############################################################################################################

# load packages
pacman::p_load(data.table,tidyverse,readxl,openxlsx)

## ARRAY JOB SETTINGS -----------------------------------------------------------------

# create param_map
indicator_groups <- c("method_mix")

param <- data.table()
for (group in indicator_groups) {
  # read in tracking workbook and obtain list of modelable_entity_name's to model
  param_map <- read_excel("FILEPATH", sheet = group) %>% as.data.table()
  global.models <- param_map$modelable_entity_name
  
  # add indicator_group and modelable_entity_name to param
  x <- data.table(var = gsub("_mar_prelim", "", global.models[grepl("_mar_prelim", global.models)]),
                  group = rep(group, length(gsub("_mar_prelim", "", global.models[grepl("_mar_prelim", global.models)]))))
  param <- rbind(param, x)
}

param_map_filepath <- "FILEPATH"
write.csv(param, param_map_filepath)

# sbatch command
job_name <- "model_gv_vars"
thread_flag <- "-c 4"
mem_flag <- "--mem=60G"
runtime_flag <- "-t 48:00:00"
jdrive_flag <- "-C archive"
partition_flag <- "-p all.q"
throttle_flag <- "%200"
n_jobs <- paste0("1-", nrow(param), throttle_flag)
next_script <- paste0("FILEPATH/model_decomp_vars.R")
error_filepath <- "FILEPATH"
output_filepath <- "FILEPATH"
account_flag <- "-A ADDRESS"


## LAUNCH ARRAY JOB -----------------------------------------------------------------

# make sbatch
sbatch_command <- paste("sbatch", "-J", job_name,
                        thread_flag, account_flag, mem_flag, runtime_flag, partition_flag, 
                        "-a", n_jobs, "-e", error_filepath, "-o", output_filepath,
                        "FILEPATH -s", next_script, param_map_filepath)

# launch job
jid <- system(sbatch_command, intern = TRUE)
