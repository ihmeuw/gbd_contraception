############################################################################################################
## Purpose: Launch an array job to prepare method mix draws for every location
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
pacman::p_load(data.table,tidyverse,openxlsx,readxl,parallel)

# settings
release <- 10
date <- gsub("-", "_", Sys.Date())
dir.root <- paste0("FILEPATH", date)
dir.create(dir.root, showWarnings = F)
dir.root_any_contra <- "FILEPATH" # filepath to directory containing any_contra results

# create the opposite of %in%
'%ni%' <- Negate('%in%')

# load shared functions
functions <- list.files("FILEPATH")
invisible(lapply(paste0("FILEPATH",functions), source))

# get location metadata
locs <- get_location_metadata(release_id = release, location_set_id = 22) %>% .[,.(location_id,ihme_loc_id,location_name,level,region_name,region_id,super_region_id,super_region_name)]

# get World Bank income group classifications
lmic_dt <- fread(file.path(j, "FILEPATH"))
setnames(lmic_dt, "iso", "ihme_loc_id")

# get sdi and sdi quintile cutoffs
sdi <- get_covariate_estimates(covariate_id = 881, release_id = release, year_id = 2019)
sdi_quintiles <- fread("FILEPATH")
for (quintile in sdi_quintiles$sdi_quintile) {
  low <- sdi_quintiles[sdi_quintile == quintile]$lower_bound
  high <- sdi_quintiles[sdi_quintile == quintile]$upper_bound

  sdi[mean_value >= low & mean_value < high, sdi_quintile := quintile]
}


## ARRAY JOB SETTINGS --------------------------------------------------------------

# First create param_map
# get list of locations outputted by ST-GPR 
ids <- read_excel("FILEPATH", sheet = "method_mix") %>% as.data.table()
model.dir <- "FILEPATH"
params <- list.files(file.path(model.dir, ids[2,latest], "FILEPATH")) %>% sort
param <- data.table(loc.id = gsub('.csv', '', params))
param_map_filepath <- "FILEPATH"
write.csv(param, "FILEPATH")


# sbatch Command
job_name <- "compile_method_mix_draws"
thread_flag <- "-c 2"
mem_flag <- "--mem=25G"
runtime_flag <- "-t 00:10:00"
jdrive_flag <- "-C archive"
partition_flag <- "-p long.q"
throttle_flag <- "%500"
n_jobs <- paste0("1-", nrow(param), throttle_flag)
next_script <- paste0("FILEPATH/04da_compile_draws_method_mix.R")
error_filepath <- "FILEPATH"
output_filepath <- "FILEPATH"
account_flag <- "-A ADDRESS"


## LAUNCH ARRAY JOB -----------------------------------------------------------------

# add jdrive_flag if needed
sbatch_command <- paste("sbatch", "-J", job_name,
                        thread_flag, account_flag, mem_flag, runtime_flag, partition_flag,
                        "-a", n_jobs, "-e", error_filepath, "-o", output_filepath,
                        "FILEPATH -s", next_script, param_map_filepath)
if (nrow(param) > 0) {
  # launch job
  jid <- system(sbatch_command, intern = TRUE)

  # clean JID
  jid <- str_extract(jid, "[:digit:]+")

  # wait for array job to complete before continuing
  while(length(system(paste0("squeue -j ", jid), intern = T)) > 1) {Sys.sleep(10)}
}


## COMPILE LOCATION FILES -----------------------------------------------------------

mar_vars <- gsub("_final", "", ids[grepl("current_.*_mar_final", modelable_entity_name)]$modelable_entity_name)
unmar_vars <- gsub("_final", "", ids[grepl("current_.*_unmar_final", modelable_entity_name)]$modelable_entity_name)
all_vars <- gsub("_mar_final", "", ids[grepl("current_.*_mar_final", modelable_entity_name)]$modelable_entity_name)
all_indicators <- c(all_vars,mar_vars,unmar_vars,"mod_prop","mod_prop_mar","mod_prop_unmar")

# for each variable in the summaries folder, combine every location file and save final file of mean, lower and upper
x <- rbindlist(lapply(file.path(dir.root, "summaries", params), fread), use.names = TRUE)
produce_final_results <- function(indic) {
  print(indic)
  x <- x[me_name == indic]
  write.csv(x, file.path(dir.root, paste0(indic, ".csv")), row.names = FALSE)
}
invisible(lapply(all_indicators, produce_final_results))


## AGGREGATE DRAWS ------------------------------------------------------------------

# obtain table of final model ST-GPR run_id's
param_map <- ids[grepl("_final",modelable_entity_name)]

# subset location csv's to only level 3 locations
params <- params[gsub(".csv", "", params) %in% locs[level == 3]$location_id]

# function to aggregate draws
aggregate_draws <- function(var = "") {
  print(paste("Aggregating draws for", var))
  
  # 1. Pull draws
  print("Pulling draws...")
  draws <- rbindlist(lapply(params, function(loc) fread(file.path(dir.root, "draws_byloc", loc))[variable == (var)]), use.names = TRUE)
  
  # 2. Merge on location info, SDI quintile, and WB income group
  print("Merging on location metadata, SDI, WB income group...")
  draws <- merge(draws, locs, by = "location_id", all.x = TRUE)
  draws <- merge(draws, sdi[, c("location_id", "sdi_quintile")], by = "location_id", all.x = TRUE)
  draws <- merge(draws, lmic_dt[, c("ihme_loc_id", "income_grp_2015")], by = "ihme_loc_id", all.x = TRUE)
  draws[is.na(income_grp_2015), income_grp_2015 := "High income"]
  
  # 3. Set pops to match population denominator of variable
  print("Subsetting population to match variable denominator...")
  if (grepl("_mar", var)) {
    setnames(draws, "population_mar", "pops")
  } else if (grepl("_unmar", var)) {
    setnames(draws, "population_unmar", "pops")
  } else{
    setnames(draws, "population", "pops")
  }
  
  # if aggregating mod_prop need to subset pops to women using any contraception
  if (grepl("mod_prop", var)) {
    
    # load any_contra results
    any_contra <- fread(file.path(dir.root_any_contra, paste0("any_contra", gsub("mod_prop", "", var), ".csv")))
    
    # merge onto draws dt
    draws <- merge(any_contra[, c("location_id", "year_id", "age_group_id", "mean")], draws, by = c("location_id", "year_id", "age_group_id"))
    
    # adjust pops and remove any_contra mean column
    draws[, pops := pops * mean]
    draws$mean <- NULL
  }
  
  # 4. Calculate draws of aggregate groupings 
  print("Calculating draws of aggregate groupings...")
  draw_cols <- names(draws)[grepl("draw_[0-9]*", names(draws))]
  non_draw_cols <- setdiff(names(draws), draw_cols)
  
  draws[, (draw_cols) := .SD * pops, .SDcols = draw_cols]
  
  global_draws <- draws[, lapply(.SD, sum), by = c("year_id", "age_group_id"), .SDcols = c(draw_cols, "pops")]
  super_region_draws <- draws[, lapply(.SD, sum), by = c("year_id", "age_group_id", "super_region_name", "super_region_id"), .SDcols = c(draw_cols, "pops")]
  region_draws <- draws[, lapply(.SD, sum), by = c("year_id", "age_group_id", "super_region_name", "region_name", "region_id"), .SDcols = c(draw_cols, "pops")]
  sdi_quintile_draws <- draws[, lapply(.SD, sum), by = c("year_id", "age_group_id", "sdi_quintile"), .SDcols = c(draw_cols, "pops")]
  income_draws <- draws[, lapply(.SD, sum), by = c("year_id", "age_group_id", "income_grp_2015"), .SDcols = c(draw_cols, "pops")]
  lmic_draws <- draws[income_grp_2015 != "High income", lapply(.SD, sum), by = c("year_id", "age_group_id"), .SDcols = c(draw_cols, "pops")]
  
  global_draws[, (draw_cols) := .SD / pops, .SDcols = draw_cols]
  super_region_draws[, (draw_cols) := .SD / pops, .SDcols = draw_cols]
  region_draws[, (draw_cols) := .SD / pops, .SDcols = draw_cols]
  sdi_quintile_draws[, (draw_cols) := .SD / pops, .SDcols = draw_cols]
  income_draws[, (draw_cols) := .SD / pops, .SDcols = draw_cols]
  lmic_draws[, (draw_cols) := .SD / pops, .SDcols = draw_cols]
  
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
  
  income_draws[, mean := rowMeans(.SD), .SDcols = draw_cols]
  income_draws[, lower := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.025), .SDcols = draw_cols]
  income_draws[, upper := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.975), .SDcols = draw_cols]
  
  lmic_draws[, mean := rowMeans(.SD), .SDcols = draw_cols]
  lmic_draws[, lower := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.025), .SDcols = draw_cols]
  lmic_draws[, upper := matrixStats::rowQuantiles(as.matrix(.SD), probs = 0.975), .SDcols = draw_cols]
  
  
  # 6. Save draws
  print("Saving draws...")
  
  dir.create(file.path(dir.root, "aggregates"), showWarnings = F)
  
  write.csv(global_draws, file.path(dir.root, "aggregates", paste0(var, '_global.csv')), row.names = F)
  write.csv(super_region_draws, file.path(dir.root, "aggregates", paste0(var, '_super_region.csv')), row.names = F)
  write.csv(region_draws, file.path(dir.root, "aggregates", paste0(var, '_region.csv')), row.names = F)
  write.csv(sdi_quintile_draws, file.path(dir.root, "aggregates", paste0(var, '_sdi_quintile.csv')), row.names = F)
  write.csv(income_draws, file.path(dir.root, "aggregates", paste0(var, '_wb_income_grp.csv')), row.names = F)
  write.csv(lmic_draws, file.path(dir.root, "aggregates", paste0(var, '_lmics.csv')), row.names = F)
}

mclapply(all_indicators, aggregate_draws, mc.cores = 1)
