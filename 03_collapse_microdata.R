#############################################################################################
### Purpose: Collapse ubcov extraction output for contraception
#############################################################################################


## SET-UP -----------------------------------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
  pacman::p_load(data.table,haven,dplyr,survey,Hmisc,parallel,RMySQL,ini,readstata13,knitr,tidyverse,readr)
}

# in/out
dir.root <- "FILEPATH"

# settings
config.path <- file.path(dir.root,"FILEPATH") ## Note: Parallel runs will not work if there are spaces in this file path.
parallel <- F ## Run in parallel?
cluster_project <- "ADDRESS"  ## You must enter a cluster project in order to run in parallel
fthreads <- 2 ## How many threads per job (used in mclapply) | Set to 1 if running on desktop
m_mem_free <- 20 ## How many GBs of RAM to use per job
h_rt <- "00:30:00"  ## How much run time to request per job | format is "HH:MM:SS"
logs <- "FILEPATH" ## Path to logs

# big_job settings
big_fthreads <- 10
big_m_mem_free <- 100 ## How many GBs of RAM to use per job
big_h_rt <- "28:00:00"

# load functions
ubcov_central <- "FILEPATH"
setwd(ubcov_central)
source("FILEPATH")

# packages
library(parallel)
library(stringr)
library(purrr)


## COLLAPSE MICRODATA -------------------------------------------------------------------

# List all extractions (regular and counterfactuals) that need to be collapsed
counterfacs <- c("desire","desire_later","desire_timing","fecund","no_preg","no_ppa")
counterfactuals <- c(counterfacs, lapply(seq_along(counterfacs)[-1L], function(y) combn(counterfacs, y, paste0, collapse = "_")), recursive = TRUE) %>% keep(~ str_count(.x, 'desire') <= 1)

total_topics <- c("contraception", "contra_method_mix", counterfactuals)
total_types <- c("nats", "subnats")

# create list of all topics to be collapsed
topics <- apply(expand.grid(total_topics, total_types), 1, paste, collapse = "_")

# subset to topics which actually have data to be collapsed 
for (type in total_types) {
  for (topic in total_topics) {
    path <- file.path("FILEPATH")
    file_check <- list.files(path = path)
    if (length(file_check) == 0) topics <- topics[topics != paste(topic, type, sep = "_")]
  }
}
print(topics)

# create function to launch collapse, append results, and move files out of subfolders
collapse.topic <- function(this.topic) {
  print(this.topic)
  
  # create folder to save individual collapsed surveys
  dir.create(file.path("FILEPATH"))
  
  # adjust memory if collapsing countries with subnats
  if (grepl("subnats",this.topic)) {
    m_mem_free <- big_m_mem_free
    h_rt <- big_h_rt
    fthreads <- big_fthreads
  }

  # launch collapse
  new <- collapse.launch(topic = this.topic, config.path = config.path, parallel = parallel, 
                         fthreads = fthreads, m_mem_free = m_mem_free, h_rt = h_rt, logs = logs, 
                         cluster_project = cluster_project, central.root = ubcov_central)
  if (parallel) new <- fread(gsub("DONE! Saved output to ","",new))
  
  # remove data which could not be properly mapped to a subnat, but is still captured in national estimates
  new <- new[!is.na(ihme_loc_id) & ihme_loc_id != '']
  
  # save each survey included in the collapse as its own file 
  new[,nat_loc_id := substr(ihme_loc_id, 1, 3)]
  new[,survey_id := paste(gsub("/","_",survey_name),nat_loc_id,year_start,year_end,nid,sep = '_')]
  for (survey in unique(new$survey_id)) {
    final_file <- file.path("FILEPATH",paste0(survey,".csv"))
    write.csv(new[survey_id==survey], final_file, row.names = FALSE)
  }

  # move files, overwriting older versions where applicable
  folder <- fread(config.path)[topic == this.topic]$input.root
  if (grepl("FILEPATH",folder)) {
    files <- list.files(folder)
    
    # only move files which successfully made it through the collapse 
    files <- files[grepl(paste(unique(paste(gsub("/", "_", new$survey_name), new$nid, sep = "_")), collapse = "|"), files)]
    
    for (file in files){
      file.copy(file.path(folder,file),file.path("FILEPATH",file), overwrite = TRUE)
      file.remove(file.path(folder,file))
    }
  }
}

# launch collapse function
cores.provided <- length(topics)
if (parallel) {
  mclapply(topics, collapse.topic, mc.cores = cores.provided)
} else {
  for (topic in topics) collapse.topic(topic)
}


## OUTPUT FINAL COLLAPSED FILES -------------------------------------------------------------------

# combine all collapsed surveys within a topic folder 
combine.files <- function(this.topic) {
  print(this.topic)
  files <- list.files(file.path("FILEPATH"))
  x <- rbindlist(lapply(file.path("FILEPATH",files),fread), fill = TRUE, use.names = TRUE)
  write.csv(x,file.path("FILEPATH"), row.names = FALSE)
}
mclapply(topics,combine.files,mc.cores = cores.provided)
