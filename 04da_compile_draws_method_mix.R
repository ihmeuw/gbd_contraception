############################################################################################################
## Purpose: Compile method mix draws and squeeze to fit within any contraceptive use 
############################################################################################################


## CLUSTER ARRAY JOB SETTINGS ------------------------------------------------

library(data.table)

# Getting normal QSub arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]

# Retrieving array task_id
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
param_map <- fread(param_map_filepath)

# variable from the param_map that would have been in your for-loop
# only edit this line!!!
specific.loc <- param_map[task_id, loc.id]


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
global.indicator_group <- "method_mix"
gbd_round <- 7
dstep <- 'iterative'
release <- 10
years <- seq(1970,2022)
age_set_id <- 19
std_year <- 2019  # year to which standardize marital status to 
date <- gsub("-", "_", Sys.Date())
id.vars <- c("location_id","year_id","sex_id","age_group_id")

# in/out
model.dir <- "FILEPATH"
out.dir <- "FILEPATH"

# packages
pacman::p_load(magrittr,parallel,plyr,tidyverse,openxlsx,readxl)

# load shared functions
functions <- list.files("FILEPATH")
invisible(lapply(paste0("FILEPATH",functions), source))

# source ST-GPR shared functions 
source("FILEPATH")

# load populations and locations
pops <- get_population(location_id = specific.loc,year_id = years,sex_id = 2,age_group_id = seq(8,14),gbd_round_id = gbd_round,decomp_step = dstep) %>% .[,-c('run_id')]
locs <- get_location_metadata(release_id = release,location_set_id = 22) %>% .[,c('location_id','ihme_loc_id','location_name','super_region_name','level')]
allpops <- get_population(location_id = -1,location_set_id = 22,year_id = std_year,sex_id = 3,age_group_id = 22,gbd_round_id = gbd_round,decomp_step = dstep)
biglocs <- allpops[location_id %in% locs[level == 3,location_id] & population >= 5000000,location_id]

# get age weights and rescale to sum to 1
age_wts <- get_age_metadata(age_group_set_id = age_set_id, gbd_round_id = gbd_round) %>% .[age_group_id %in% c(8:14)]
age_wts[,age_group_weight_value := age_group_weight_value/age_wts[,sum(age_group_weight_value)]]
if (sum(age_wts$age_group_weight_value) != 1) stop("Age weights do NOT sum to 1!")


## PREPARE AND COMBINE DRAWS -------------------------------------------------

# get latest model ST-GPR run_ids
print('PULLING LATEST RUN_IDS')
ids <- read_excel("FILEPATH", sheet = global.indicator_group) %>% as.data.table()
ids <- ids[grepl('_final',modelable_entity_name)] # subset to final runs only
ids[, modelable_entity_name := gsub('_final','',modelable_entity_name)]

runs <- data.table(
  me_name = ids$modelable_entity_name,
  run_id = ids$latest
)
print(runs)

# load model draws and return as single data table 
compile.draws <- function(var) {
  
  # read in draws, save, and melt 
  draws <- rbindlist(lapply(file.path(model.dir,runs[me_name == var,run_id],"FILEPATH",paste0(specific.loc,".csv")),fread),use.names = T)
  draws <- melt(draws,id.vars=id.vars,value.name=var,variable.name="draw")
  
  return(as.data.table(draws))
}

# run compile.draws and merge for all variables, return as single data table
dt <- lapply(runs$me_name, compile.draws) %>% reduce(full_join, by = c(id.vars,'draw')) %>% as.data.table()

# merge on marital status and any_contra
main_pipeline <- read_excel("FILEPATH", sheet = "main_pipeline") %>% as.data.table()
main_ids <- main_pipeline[grepl('_final', modelable_entity_name) & grepl("marital_status|any_contra", modelable_entity_name)] 
main_ids[, modelable_entity_name := gsub('_final','',modelable_entity_name)]

runs <- data.table(
  me_name = main_ids$modelable_entity_name,
  run_id = main_ids$latest
)

dt2 <- lapply(runs$me_name, compile.draws) %>% reduce(full_join, by = c(id.vars,'draw')) %>% as.data.table()

dt <- merge(dt, dt2, by = c(id.vars, "draw"))

## offset any_contra draws if equal to 0 to avoid NA's later in mod_prop
dt[any_contra_mar == 0, any_contra_mar := .0001, by = c(id.vars, "draw")]
dt[any_contra_unmar == 0, any_contra_unmar := .0001, by = c(id.vars, "draw")]


## SQUEEZE DRAWS TO FIT ENVELOPE --------------------------------------------

# get names of all method columns
mar_method_cols <- names(dt)[grepl("current_.*_mar", names(dt))]
unmar_method_cols <- names(dt)[grepl("current_.*_unmar", names(dt))]

# sum method estimates
dt[, any_sum_mar := rowSums(.SD), .SDcols = mar_method_cols]
dt[, any_sum_unmar := rowSums(.SD), .SDcols = unmar_method_cols]

setnames(dt, 
         old = c("any_contra_mar", "any_contra_unmar", "any_sum_mar", "any_sum_unmar"), 
         new = c("any_contra_mar_model", "any_contra_unmar_model", "any_contra_mar", "any_contra_unmar"))

# calculate adjustment factors 
dt[, adjust_mar := 1 + ((any_contra_mar_model - any_contra_mar) / any_contra_mar)]
dt[, adjust_unmar := 1 + ((any_contra_unmar_model - any_contra_unmar) / any_contra_unmar)]

# multiple methods by adjustment factors
dt[, (mar_method_cols) := .SD * adjust_mar, .SDcols = (mar_method_cols)]
dt[, (unmar_method_cols) := .SD * adjust_unmar, .SDcols = (unmar_method_cols)]

# check
dt[, any_sum_mar_check := rowSums(.SD), .SDcols = mar_method_cols]
dt[, any_sum_unmar_check := rowSums(.SD), .SDcols = unmar_method_cols]
  

## CALCULATE NEW VARIABLES AND ALL-AGE ESTIMATES -----------------------------

print("CALCULATING BY-AGE NEW VARIABLES")
## calculate new variables
dt[, any_contra_model := (any_contra_mar_model * marital_status) + (any_contra_unmar_model * (1 - marital_status))]

for (method in gsub("_mar", "", names(dt)[grepl("current_.*_mar", names(dt))])) {
  method_mar <- paste0(method, "_mar")
  method_unmar <- paste0(method, "_unmar")
  dt[[method]] <- dt[, (get(method_mar) * marital_status) + (get(method_unmar) * (1 - marital_status))]
}

print("AGE-STANDARDIZING")
# standardize across ages
dt <- merge(dt, age_wts[,.(age_group_id, age_weight = age_group_weight_value)], by = "age_group_id")
mar_vars <- c("any_contra_mar_model", names(dt)[grepl("current_.*_mar", names(dt))])
unmar_vars <- c("any_contra_unmar_model", names(dt)[grepl("current_.*_unmar", names(dt))])
all_vars <- c("marital_status", "any_contra_model", gsub("_mar", "", names(dt)[grepl("current_.*_mar", names(dt))]))
std <- dt[, lapply(.SD,function(x) {sum(x * age_weight)}), .SDcols = c(all_vars,mar_vars,unmar_vars), by = c(id.vars[1:3],"draw")] 
std[, age_group_id := 27]


print("POP-WEIGHTING")
# merge on populations
dt <- merge(dt, pops, by = id.vars)
dt[, population_mar := population * marital_status]
dt[, population_unmar := population * (1 - marital_status)]
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


print("CALCULATING NEW MOD_PROP")
dt[, mod_prop_mar := rowSums(.SD) / any_contra_mar_model, .SDcols = mar_mod_cols]
dt[, mod_prop_unmar := rowSums(.SD) / any_contra_unmar_model, .SDcols = unmar_mod_cols]
dt[, mod_prop := rowSums(.SD) / any_contra_model, .SDcols = gsub("_mar", "", mar_mod_cols)]


print("OUTPUTTING SUMMARIES")
all_indicators <- c(mar_vars, unmar_vars, all_vars, "mod_prop", "mod_prop_mar", "mod_prop_unmar")
x <- data.table()
dir.create(file.path(out.dir, date, 'summaries'), recursive = T, showWarnings = F)
for (indic in all_indicators) {
  x_indic <- dt[!is.na(get(indic)),.(mean = mean(get(indic)), lower = quantile(get(indic),.025), upper = quantile(get(indic),.975)), by = c(id.vars)]
  x_indic <- merge(x_indic, locs, by = "location_id", all.x = T)
  x_indic[, me_name := indic]
  x <- rbind(x, x_indic)
}
write.csv(x, file.path(out.dir, date, "summaries", paste0(specific.loc, ".csv")), row.names = F)


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
dt[, population := mean(population), by = c(id.vars)]
dt[, population_mar := mean(population_mar), by = c(id.vars)]
dt[, population_unmar := mean(population_unmar), by = c(id.vars)]

# drop poptotal columns
dt <- dt %>% select(-contains("poptotal"))

# drop other unnecessary columns
dt <- dt[, -c("adjust_mar", "adjust_unmar", "age_weight", "any_sum_mar_check", "any_sum_unmar_check",
              "mod_sum_mar", "mod_sum_unmar", "trad_sum_mar", "trad_sum_unmar")]

# arrange data table so that every draw is a separate column
dt <- melt(dt, id.vars = c("location_id", "year_id", "sex_id", "age_group_id", "draw", "population", "population_mar", "population_unmar"))
dt <- dcast(dt, location_id + year_id + sex_id + age_group_id + variable + population + population_mar + population_unmar ~ draw)
dir.create(file.path(out.dir, date, 'draws_byloc'), recursive = T, showWarnings = F)
write.csv(dt, file.path(out.dir, date, 'draws_byloc', paste0(specific.loc, '.csv')), row.names = F)
