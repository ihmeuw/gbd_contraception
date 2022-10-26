############################################################################################################
## Purpose: Launch an array job to process every UbCov survey through our indicator construction code (non-counterfactual)
############################################################################################################

library(data.table)

## ARRAY JOB SETTINGS -----------------------------------------------------------------

# obtain lists of surveys
in.dir_j <- "FILEPATH"
in.dir_l <- "FILEPATH"

surveys_j <- list.files(in.dir_j)
surveys_l <- list.files(in.dir_l)

# create param_map with all surveys and their corresponding input directory
param <- data.table(survey = c(surveys_j, surveys_l),
                    input_dir = c(rep(in.dir_j, length(surveys_j)), rep(in.dir_l, length(surveys_l))),
                    variables = rep(vars, length(c(surveys_j, surveys_l))))

param_map_filepath <- "FILEPATH"
write.csv(param, param_map_filepath)

# sbatch command
job_name <- "01a_prep_ubcov_data"
thread_flag <- "-c 2"
mem_flag <- "--mem=20G"
runtime_flag <- "-t 02:40:00"
partition_flag <- "-p long.q"
jdrive_flag <- "-C archive"
throttle_flag <- "%400"
n_jobs <- paste0("1-", nrow(param), throttle_flag)
next_script <- paste0("FILEPATH/01a_prep_ubcov_data.R")
error_filepath <- paste0("FILEPATH")
output_filepath <- paste0("FILEPATH")
account_flag <- "-A ADDRESS"


## LAUNCH ARRAY JOB -----------------------------------------------------------------

sbatch_command <- paste("sbatch", thread_flag, jdrive_flag,
                        "-J", job_name, account_flag, mem_flag, runtime_flag, partition_flag, 
                        "-a", n_jobs, "-e", error_filepath, "-o", output_filepath, 
                        "FILEPATH -s", next_script, param_map_filepath)
if (nrow(param) > 0) system(sbatch_command, intern = TRUE)
