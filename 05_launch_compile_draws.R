############################################################################################################
## Purpose: Launch an array job to prepare contraception draws for every location, then compile by indicator
###########################################################################################################

## SET-UP -----------------------------------------------------------------------------

# username is pulled automatically
username <- Sys.getenv("USER") 

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# packages
pacman::p_load(data.table,tidyverse,openxlsx,readxl)

# settings
release <- 10
date <- gsub("-", "_", Sys.Date())
dir.root <- paste0("FILEPATH", date)
dir.create(dir.root, showWarnings = F)

# create the opposite of %in%
'%ni%' <- Negate('%in%')

# load shared functions
functions <- list.files("FILEPATH")
invisible(lapply(paste0("FILEPATH",functions), source))

# get location metadata
locs <- get_location_metadata(release_id = release, location_set_id = 22) %>% .[,.(location_id,ihme_loc_id,location_name,level,region_name,region_id,super_region_id,super_region_name)]

# get sdi and quintiles
sdi <- get_covariate_estimates(covariate_id = 881, release_id = release, year_id = 2019)
sdi_quintiles <- fread("FILEPATH")

for (quintile in sdi_quintiles$sdi_quintile) {
  low <- sdi_quintiles[sdi_quintile == quintile]$lower_bound
  high <- sdi_quintiles[sdi_quintile == quintile]$upper_bound
  
  sdi[mean_value >= low & mean_value < high, sdi_quintile := quintile]
}


## ARRAY JOB SETTINGS -----------------------------------------------------------------

# First create param_map
ids <- read_excel("FILEPATH", sheet = "main_pipeline") %>% as.data.table()
model.dir <- "FILEPATH"
params <- list.files(file.path(model.dir, ids[2,latest], "FILEPATH")) %>% sort
param <- data.table(loc.id = gsub('.csv', '', params), use_method_mix = rep(use_method_mix, length(params)))
write.csv(param, "FILEPATH")
param_map_filepath <- "FILEPATH"

# QSUB Command
job_name <- "compile_contra_draws"
thread_flag <- "-c 2"
mem_flag <- "--mem=25G"
runtime_flag <- "-t 00:10:00"
jdrive_flag <- "-C archive"
partition_flag <- "-p long.q"
throttle_flag <- "%400"
n_jobs <- paste0("1-", nrow(param), throttle_flag)
next_script <- paste0("FILEPATH/05a_compile_draws.R")
error_filepath <- "FILEPATH"
output_filepath <- "FILEPATH"
account_flag <- "-A ADDRESS"


## LAUNCH ARRAY JOB -----------------------------------------------------------------

# add jdrive_flag if needed
sbatch_command <- paste("sbatch", thread_flag, 
                        "-J", job_name, account_flag, mem_flag, runtime_flag, partition_flag, 
                        "-a", n_jobs, "-e", error_filepath, "-o", output_filepath,  
                        "FILEPATH -s", 
                        next_script, param_map_filepath)
if (nrow(param) > 0) {
  # launch job
  jid <- system(sbatch_command, intern = TRUE)
  
  # clean JID
  jid <- str_extract(jid, "[:digit:]+")
  
  # wait for array job to complete before continuing
  while(length(system(paste0("squeue -j ", jid), intern = T)) > 1) {Sys.sleep(10)}
}


## COMPILE LOCATION FILES ----------------------------------------------------------------

# after prepping all the summary and draw files by location, need to produce final results files 
mar_vars <- c("any_contra_mar","mod_contra_mar","need_contra_mar","met_demand_mar","mod_prop_mar","unmet_prop_mar","unmet_need_mar")
unmar_vars <- c("any_contra_unmar","mod_contra_unmar","need_contra_unmar","met_demand_unmar","mod_prop_unmar","unmet_prop_unmar","unmet_need_unmar")
all_vars <- c("marital_status","any_contra","mod_contra","need_contra","met_demand","mod_prop","unmet_prop","unmet_need")
all_indicators <- c(mar_vars,unmar_vars,all_vars)

# reset params if they had been subsetted above
params <- list.files(file.path(model.dir, ids[2,latest], "FILEPATH")) %>% sort

# for each variable in the summaries folder, combine every location file and save final file of mean, lower and upper
produce_final_results <- function(indic) {
  print(indic)
  x <- rbindlist(lapply(file.path(dir.root, "summaries", indic, params), fread), use.names = TRUE)
  write.csv(x, file.path(dir.root, paste0(indic, ".csv")), row.names = FALSE)
}
invisible(lapply(all_indicators, produce_final_results))


## AGGREGATE DRAWS BY REGION --------------------------------------------------------------

# obtain table of final model run_id's
param_map <- ids[grepl("_final",modelable_entity_name)]

# subset location csv's to only level 3 locations
params <- params[gsub(".csv", "", params) %in% locs[level == 3]$location_id]

# function to aggregate draws
aggregate_draws <- function(var = "") {
  print(paste("Aggregating draws for", var))
  
  # 1. Pull draws
  print("Pulling draws...")
  all_draws <- rbindlist(lapply(X = file.path(dir.root, "draws_byloc", params), FUN = fread), use.names = TRUE)
  draws <- all_draws[variable == (var)]
  
  # 2. Merge on location and SDI quintile info
  print("Merging on location metadata...")
  draws <- merge(draws, locs, by = "location_id", all.x = TRUE)
  draws <- merge(draws, sdi[, c("location_id", "sdi_quintile")], by = "location_id", all.x = TRUE)

  # 3. Set pops to match population denominator of variable
  print("Subsetting population to match variable denominator...")
  setnames(draws, gsub("^[a-z]+_[a-z]+", "population", var), "pops")
  
  # if aggregating met demand need to subset pops to women with a need 
  if (grepl("met_demand", var)) {
    
    # load need_contra results
    need_contra <- fread(file.path(dir.root, paste0("need_contra", gsub("met_demand", "", var), ".csv")))
    
    # merge onto draws dt
    draws <- merge(need_contra[, c("location_id", "year_id", "age_group_id", "mean")], draws, by = c("location_id", "year_id", "age_group_id"))
    
    # adjust pops and remove need_contra column
    draws[, pops := pops * mean]
    draws$mean <- NULL
  }
  
  # if aggregating mod prop need to subset pops to women using any contraception
  if (grepl("mod_prop", var)) {
    
    # load any_contra results
    any_contra <- fread(file.path(dir.root, paste0("any_contra", gsub("mod_prop", "", var), ".csv")))
    
    # merge onto draws dt
    draws <- merge(any_contra[, c("location_id", "year_id", "age_group_id", "mean")], draws, by = c("location_id", "year_id", "age_group_id"))
    
    # adjust pops and remove need_contra column
    draws[, pops := pops * mean]
    draws$mean <- NULL
  }
  
  # 4. Calculate draws of aggregate location levels and sdi quintile 
  print("Calculating draws of aggregate location levels...")
  draw_cols <- names(draws)[grepl("draw_[0-9]*", names(draws))]
  non_draw_cols <- setdiff(names(draws), draw_cols)
  
  draws[, (draw_cols) := .SD * pops, .SDcols = draw_cols]
  
  global_draws <- draws[, lapply(.SD, sum), by = c("year_id", "age_group_id"), .SDcols = c(draw_cols, "pops")]
  super_region_draws <- draws[, lapply(.SD, sum), by = c("year_id", "age_group_id", "super_region_name", "super_region_id"), .SDcols = c(draw_cols, "pops")]
  region_draws <- draws[, lapply(.SD, sum), by = c("year_id", "age_group_id", "super_region_name", "region_name", "region_id"), .SDcols = c(draw_cols, "pops")]
  sdi_quintile_draws <- draws[, lapply(.SD, sum), by = c("year_id", "age_group_id", "sdi_quintile"), .SDcols = c(draw_cols, "pops")]
  
  global_draws[, (draw_cols) := .SD / pops, .SDcols = draw_cols]
  super_region_draws[, (draw_cols) := .SD / pops, .SDcols = draw_cols]
  region_draws[, (draw_cols) := .SD / pops, .SDcols = draw_cols]
  sdi_quintile_draws[, (draw_cols) := .SD / pops, .SDcols = draw_cols]
  
  # 5. Calculate the mean, lower and upper of each set of draws
  print("Calculating mean, lower and upper of draws...")
  global_draws[, mean := rowMeans(.SD), .SDcols = draw_cols]
  global_draws[, lower := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.025), .SDcols = draw_cols]
  global_draws[, upper := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.975), .SDcols = draw_cols]
  
  super_region_draws[, mean := rowMeans(.SD), .SDcols = draw_cols]
  super_region_draws[, lower := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.025), .SDcols = draw_cols]
  super_region_draws[, upper := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.975), .SDcols = draw_cols]
  
  region_draws[, mean := rowMeans(.SD), .SDcols = draw_cols]
  region_draws[, lower := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.025), .SDcols = draw_cols]
  region_draws[, upper := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.975), .SDcols = draw_cols]
  
  sdi_quintile_draws[, mean := rowMeans(.SD), .SDcols = draw_cols]
  sdi_quintile_draws[, lower := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.025), .SDcols = draw_cols]
  sdi_quintile_draws[, upper := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.975), .SDcols = draw_cols]
  
  # 6. Save draws
  print("Saving draws...")
  
  dir.create(file.path(dir.root, "aggregates"), showWarnings = F)
  
  write.csv(global_draws, file.path(dir.root, "aggregates", paste0(var, '_global.csv')), row.names = F)
  write.csv(super_region_draws, file.path(dir.root, "aggregates", paste0(var, '_super_region.csv')), row.names = F)
  write.csv(region_draws, file.path(dir.root, "aggregates", paste0(var, '_region.csv')), row.names = F)
  write.csv(sdi_quintile_draws, file.path(dir.root, "aggregates", paste0(var, '_sdi_quintile.csv')), row.names = F)
}

lapply(all_indicators[!grepl("unmet_prop", all_indicators)], aggregate_draws)
