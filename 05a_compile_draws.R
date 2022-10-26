############################################################################################################
## Purpose: Combine nested contraception proportions to create final variables and aggregate to all women 15-49
###########################################################################################################


## CLUSTER ARRAY JOB SETTINGS ------------------------------------------------

library(data.table)

# Getting normal QSub arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]

# Retrieving array task_id
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
param_map <- fread(param_map_filepath)

# variable from the param_map that would have been in your loop
# only edit this line!!!
specific.loc <- param_map[task_id, loc.id]
use_method_mix <- param_map[task_id, use_method_mix]


## SET-UP --------------------------------------------------------------------

# Username is pulled automatically
username <- Sys.getenv("USER") 

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# settings
release <- 10
years <- seq(1970,2022)
age_set_id <- 19
use_method_mix <- TRUE
std_year <- 2019  # year to which standardize marital status to 
date <- gsub("-", "_", Sys.Date())
id.vars <- c("location_id","year_id","sex_id","age_group_id")

# in/out
model.dir <- "FILEPATH"
in.dir_method_mix <- "FILEPATH"
out.dir <- "FILEPATH"

# packages
pacman::p_load(magrittr,parallel,plyr,tidyverse,openxlsx,readxl)

# load shared functions
functions <- list.files("FILEPATH")
invisible(lapply(paste0("FILEPATH",functions), source))

# source ST-GPR shared functions 
source("FILEPATH")

# load populations and locations
pops <- get_population(location_id = specific.loc, year_id = years, sex_id = 2, age_group_id = seq(8,14), release_id = release) %>% .[,-c('run_id')]
locs <- get_location_metadata(release_id = release, location_set_id = 22) %>% .[,c('location_id','ihme_loc_id','location_name','super_region_name','level')]
allpops <- get_population(location_id = -1, location_set_id = 22, year_id = std_year, sex_id = 3, age_group_id = 22, release_id = release)
biglocs <- allpops[location_id %in% locs[level == 3, location_id] & population >= 5000000, location_id]

# get age weights and rescale to sum to 1
age_wts <- get_age_metadata(age_group_set_id = age_set_id, release_id = release) %>% .[age_group_id %in% c(8:14)]
age_wts[,age_group_weight_value := age_group_weight_value / age_wts[, sum(age_group_weight_value)]]
if (sum(age_wts$age_group_weight_value) != 1) stop("Age weights do NOT sum to 1!")


## PREPARE AND COMBINE DRAWS -------------------------------------------------

# get latest model run_ids
print('PULLING LATEST RUN_IDS')
param_map <- read_excel("FILEPATH", sheet = "main_pipeline") %>% as.data.table()
ids <- param_map[grepl('_final', modelable_entity_name)] # subset to final runs only
ids[, modelable_entity_name := gsub('_final', '', modelable_entity_name)]

# if using mod_prop from method mix we want to use those draws, not the draws associated with these run_id's
if (use_method_mix) ids <- ids[!grepl("mod_prop", modelable_entity_name)]

runs <- data.table(
  me_name = ids$modelable_entity_name,
  run_id = ids$latest
)
print(runs)

# load model draws and return as single data table 
compile.draws <- function(var) {
  
  # read in draws, save, and melt 
  draws <- rbindlist(lapply(file.path(model.dir,runs[me_name == var,run_id],"draws_temp_0",paste0(specific.loc,".csv")),fread),use.names = T)
  draws <- melt(draws, id.vars = id.vars, value.name = var, variable.name = "draw")
  
  return(as.data.table(draws))
}

# run compile.draws and merge for all variables, return as single data table
dt <- lapply(runs$me_name,compile.draws) %>% reduce(full_join, by = c(id.vars,'draw')) %>% as.data.table()

# if applicable, merge on mod_prop draws produced from method mix
if (use_method_mix) {
  dt2 <- fread(file.path(in.dir_method_mix, "draws_byloc", paste0(specific.loc, ".csv"))) %>% filter(age_group_id %in% seq(8,14))
  dt2 <- melt(dt2[variable %in% c("mod_prop_mar", "mod_prop_unmar")], id.vars = c("location_id", "year_id", "sex_id", "age_group_id", "population", "population_mar", "population_unmar", "variable"), variable.name = "draw")
  dt2 <- dcast(dt2, location_id + year_id + sex_id + age_group_id + draw ~ variable)
  dt <- merge(dt, dt2, by = c(id.vars, "draw"))
}


## CALCULATE NEW VARIABLES AND ALL-AGE ESTIMATES -----------------------------

## offset any_contra draws if equal to 0 to avoid NA's later in met_demand
dt[any_contra_mar == 0, any_contra_mar := .0001, by = c(id.vars, "draw")]
dt[any_contra_unmar == 0, any_contra_unmar := .0001, by = c(id.vars, "draw")]

print("CALCULATING BY-AGE NEW VARIABLES")
## calculate new variables
dt[, any_contra := (any_contra_mar * marital_status) + (any_contra_unmar * (1 - marital_status))]

dt[, mod_contra_mar := mod_prop_mar * any_contra_mar]
dt[, mod_contra_unmar := mod_prop_unmar * any_contra_unmar]
dt[, mod_contra := (mod_contra_mar * marital_status) + (mod_contra_unmar * (1 - marital_status))]

dt[, need_contra_mar := any_contra_mar + (unmet_prop_mar * (1 - any_contra_mar))]
dt[, need_contra_unmar := any_contra_unmar + (unmet_prop_unmar * (1 - any_contra_unmar))]
dt[, need_contra := (need_contra_mar * marital_status) + (need_contra_unmar * (1 - marital_status))]


print("AGE-STANDARDIZING")
# standardize across ages
dt <- merge(dt, age_wts[,.(age_group_id, age_weight = age_group_weight_value)], by = "age_group_id")
mar_vars <- c("any_contra_mar", "mod_contra_mar", "need_contra_mar")
unmar_vars <- c("any_contra_unmar", "mod_contra_unmar", "need_contra_unmar")
all_vars <- c("marital_status", "any_contra", "mod_contra", "need_contra")
std <- dt[,lapply(.SD,function(x) {sum(x*age_weight)}), .SDcols = c(all_vars,mar_vars,unmar_vars), by = c(id.vars[1:3],"draw")]
std[, age_group_id := 27]


print("CALCULATING POPULATIONS")
# merge on populations
dt <- merge(dt, pops, by = id.vars)

# create marital status-specific populations
dt[, population_mar := population * marital_status]
dt[, population_unmar := population * (1 - marital_status)]

# sum populations for easy pop-weighting later on
dt[, poptotal := sum(population), by = c(id.vars[1:3],"draw")]
dt[, poptotal_mar := sum(population_mar), by = c(id.vars[1:3],"draw")]
dt[, poptotal_unmar := sum(population_unmar), by = c(id.vars[1:3],"draw")]


print('CALCULATING ALL-AGE NEW VARIABLES')
# pop-weight to get all-age estimates
pmar <- dt[, lapply(.SD, function(x) {sum(x * population_mar / poptotal_mar)}), .SDcols = mar_vars, by = c(id.vars[1:3],"draw")]
punmar <- dt[, lapply(.SD, function(x) {sum(x * population_unmar / poptotal_unmar)}), .SDcols = unmar_vars, by = c(id.vars[1:3],"draw")]
pall <- dt[, lapply(.SD, function(x) {sum(x * population / poptotal)}), .SDcols = all_vars, by = c(id.vars[1:3],"draw")]
p <- merge(pmar, punmar, by = c(id.vars[1:3],"draw"))
p <- merge(p, pall, by = c(id.vars[1:3],"draw"))
p[, age_group_id := 24]


print("COMBINING ALL DATASETS")
dt <- rbindlist(list(dt, p, std), fill = T, use.names = T) # if age-standardizing add 'std' to list


print("BACK-CALCULATING ALL-AGE MODELED VARIABLES")
# calculate all-women estimates for standardized and global estimates, back-calculate other variables
dt[is.na(mod_prop_mar), mod_prop_mar := mod_contra_mar / any_contra_mar]
dt[is.na(mod_prop_unmar), mod_prop_unmar := mod_contra_unmar / any_contra_unmar]
dt[, mod_prop := mod_contra / any_contra]
dt[is.na(unmet_prop_mar), unmet_prop_mar := (need_contra_mar - any_contra_mar) / (1 - any_contra_mar)]
dt[is.na(unmet_prop_unmar), unmet_prop_unmar := (need_contra_unmar - any_contra_unmar) / (1 - any_contra_unmar)]
dt[, unmet_prop := (need_contra - any_contra) / (1 - any_contra)]


print("CALCULATING MET DEMAND")
## calculate met demand
dt[, met_demand_mar := mod_contra_mar / need_contra_mar]
dt[, met_demand_unmar := mod_contra_unmar / need_contra_unmar]
dt[, met_demand := mod_contra / need_contra]


print("CALCULATING UNMET NEED")
## calculate unmet need
dt[, unmet_need_mar := need_contra_mar - any_contra_mar]
dt[, unmet_need_unmar := need_contra_unmar - any_contra_unmar]
dt[, unmet_need := need_contra - any_contra]


## SAVE FINAL ESTIMATES --------------------------------------------------

print("OUTPUTTING SUMMARIES")
# output means, lowers, and uppers for indicators of interest
all_indicators <- c(mar_vars,unmar_vars,all_vars,"met_demand","met_demand_mar","met_demand_unmar",
                    "mod_prop","mod_prop_mar","mod_prop_unmar","unmet_prop","unmet_prop_mar","unmet_prop_unmar",
                    "unmet_need", "unmet_need_mar", "unmet_need_unmar")

for (indic in all_indicators) {
  print(indic)
  dir.create(file.path(out.dir, date, 'summaries', indic), recursive = T, showWarnings = F)
  x <- dt[!is.na(get(indic)),.(mean = mean(get(indic)), lower = quantile(get(indic),.025), upper = quantile(get(indic),.975)), by = c(id.vars)]
  x <- merge(x, locs, by = "location_id", all.x = T)
  x[, me_name := indic]
  write.csv(x, file.path(out.dir, date, "summaries", indic, paste0(specific.loc, ".csv")), row.names = F)
}


print("SAVING SNAPSHOT OF COMBINED DRAWS TABLE")
# order table to ensure years and draws line up for all age groups
dt <- dt[order(year_id, draw)]

# impute populations for age groups 24 and 27
dt[age_group_id == 24, population := dt[age_group_id != 24 & age_group_id != 27, unique(poptotal), by = c(id.vars[1:3], "draw")]$V1]
dt[age_group_id == 24, population_mar := dt[age_group_id != 24 & age_group_id != 27, mean(poptotal_mar), by = c(id.vars[1:3], "draw")]$V1]
dt[age_group_id == 24, population_unmar := dt[age_group_id != 24 & age_group_id != 27, mean(poptotal_unmar), by = c(id.vars[1:3], "draw")]$V1]

dt[age_group_id == 27, population := dt[age_group_id != 24 & age_group_id != 27, unique(poptotal), by = c(id.vars[1:3], "draw")]$V1]
dt[age_group_id == 27, population_mar := dt[age_group_id != 24 & age_group_id != 27, mean(poptotal_mar), by = c(id.vars[1:3], "draw")]$V1]
dt[age_group_id == 27, population_unmar := dt[age_group_id != 24 & age_group_id != 27, mean(poptotal_unmar), by = c(id.vars[1:3], "draw")]$V1]

# average populations by location-year-age
dt[, population := mean(population), by = id.vars]
dt[, population_mar := mean(population_mar), by = id.vars]
dt[, population_unmar := mean(population_unmar), by = id.vars]

# drop poptotal columns
dt <- dt %>% select(-contains("poptotal"))

# arrange data table so that every draw is a separate column
dt <- melt(dt, id.vars = c("location_id", "year_id", "sex_id", "age_group_id", "draw", "population", "population_mar", "population_unmar"))
dt <- dcast(dt, location_id + year_id + sex_id + age_group_id + variable + population + population_mar + population_unmar ~ draw)

# save all draws by location
dir.create(file.path(out.dir, date, 'draws_byloc'), recursive = T, showWarnings = F)
write.csv(dt, file.path(out.dir, date, 'draws_byloc', paste0(specific.loc, '.csv')), row.names = F)
