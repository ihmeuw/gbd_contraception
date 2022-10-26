###########################################################################################################
## Purpose: Prepare data for ST-GPR modeling
###########################################################################################################


## SET-UP ----------------------------------------------------------------------

# clear memory except for global settings
rm(list=ls()[!grepl('global',ls())])

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# libraries
pacman::p_load(data.table,magrittr,parallel,rhdf5,tidyverse,msm,readxl)

# settings
param_map <- read_excel("FILEPATH", sheet = global.indicator_group) %>% as.data.table()
settings <- param_map[modelable_entity_name == global.me_name]
release <- settings$release_id
years <- seq(settings$year_start, settings$year_end)
denominator <- settings$denominator
measure <- settings$measure
input_file <- settings$input_file
output_file <- settings$output_file
offset <- 0.001
id.vars <- c("location_id","year_id",'sex_id',"age_group_id")

# run_id's
main_pipeline <- read_excel("FILEPATH", sheet = "main_pipeline") %>% as.data.table()
marital_status_final <- main_pipeline[modelable_entity_name == 'marital_status_final']$latest
any_contra_mar_prelim <- main_pipeline[modelable_entity_name == 'any_contra_mar_prelim']$latest
any_contra_unmar_prelim <- main_pipeline[modelable_entity_name == 'any_contra_unmar_prelim']$latest
any_contra_mar_final <- main_pipeline[modelable_entity_name == 'any_contra_mar_final']$latest
any_contra_unmar_final <- main_pipeline[modelable_entity_name == 'any_contra_unmar_final']$latest
mod_prop_mar_prelim <- main_pipeline[modelable_entity_name == 'mod_prop_mar_prelim']$latest
mod_prop_unmar_prelim <- main_pipeline[modelable_entity_name == 'mod_prop_unmar_prelim']$latest
mod_prop_mar_final <- main_pipeline[modelable_entity_name == 'mod_prop_mar_final']$latest
unmet_prop_mar_prelim <- main_pipeline[modelable_entity_name == 'unmet_prop_mar_prelim']$latest
unmet_prop_unmar_prelim <- main_pipeline[modelable_entity_name == 'unmet_prop_unmar_prelim']$latest
unmet_prop_mar_final <- main_pipeline[modelable_entity_name == 'unmet_prop_mar_final']$latest   

# load shared functions
functions <- list.files("FILEPATH")
invisible(lapply(paste0("FILEPATH",functions), source))

# source ST-GPR shared functions 
source("FILEPATH")

# create function for the opposite of %in%
'%ni%' <- Negate('%in%')

# in/out
dir.root <- "FILEPATH"


## PREP DATA ---------------------------------------------------------------

# read in data
df <- fread(file.path(dir.root,"FILEPATH",input_file))

# get rid of years not being modelled
df <- df[year_id %in% years]

# create survey_id for easy tracking 
df[, survey_id := paste(survey_name,ihme_loc_id,year_id,nid)]

# impute sample sizes where missing
ss_impute <- df[!grepl("_",ihme_loc_id), .(ss_impute = round(quantile(sample_size,.05,na.rm=T))), by = age_group]
df <- merge(df, ss_impute ,by = "age_group", all.x = T)
df[is.na(sample_size), sample_size := ss_impute]
df[, ss_impute := NULL]

# remove small sample sizes
df <- df[sample_size >= 20]

# offset extreme values and fill in missing standard_error's
df[, val := as.numeric(val)]
df[val < offset, val := offset]
df[val > 1 - offset, val := 1 - offset]
df[is.na(standard_error), standard_error := sqrt(val*(1-val)/sample_size)]


## MARITAL STATUS SPLITTING ------------------------------------------------

# marital status data does not need to undergo marital status splitting
if (!grepl('marital_status', global.me_name)) {
  
  # split all women data during prep for married final runs
  if (grepl('_mar_final', global.me_name)) {
    print('Marital status splitting...')
    
    # source marital status-splitting script and function
    source(paste0(h,'FILEPATH/04ba_marital_split.R'), local = T)
    
    # split data
    df <- marital_split(data = df,
                        me_name = global.me_name,
                        denominator = denominator)
  
  } else if (grepl('_unmar_final', global.me_name)) {
    # check there is all women-split unmarried data to append on
    if (file.exists(file.path(dir.root,"FILEPATH/marital_split_data.csv"))) {
      
      print('Appending unmarried split all women data...')
      
      # read in split unmarried estimates and append to df
      allwomen <- fread(file.path(dir.root,"FILEPATH/marital_split_data.csv"))
      allwomen <- allwomen[, names(allwomen) %in% names(df), with = F]
      df <- rbind(df, allwomen[marital_group == "unmarried"], fill = T)
    }
  }
}


## CROSSWALKING ------------------------------------------------------------

if (grepl('unmet_prop', global.me_name)) {
  print('Crosswalking...')
  
  # source crosswalking script
  source(paste0(h,'FILEPATH/04bb_xwalk_unmet_prop.R'), echo = F)
  
  # detach crosswalk package to avoid 'object xwalk not found' error 
  detach(package:crosswalk002, unload = T) 
}

# remove age-aggregated microdata (was used for crosswalking purposes but no longer needed)
df <- df[!is.na(age_group_id) | cv_report == 1]


## AGE SPLITTING -----------------------------------------------------------

if (grepl('_final', global.me_name)) {
  print('Age splitting...')
  
  # source age-splitting script and function
  source(paste0(h,'FILEPATH/04bc_age_split.R'), local = T)
  
  # age-split data
  df <- age_split(data = df,
                  me_name = global.me_name,
                  denominator = denominator,
                  measure = measure)
} else {
  
  # drop age-aggregated data
  df <- df[!is.na(age_group_id)]
}


## FINAL DATA CLEAN-UP -----------------------------------------------------

# remove data with tiny sample sizes post-splitting
df <- df[sample_size >= 20]

# calculate variance
df[!is.na(standard_error), variance := standard_error**2]
df[is.na(variance) | variance < val*(1-val)/sample_size, variance := val*(1-val)/sample_size]
df[val < offset & variance < offset*(1-offset)/sample_size, c("val","variance") := .(offset,offset*(1-offset)/sample_size)]
df[val < offset,val := offset]
df[val > (1 - offset) & variance < offset*(1-offset)/sample_size, c("val","variance") := .(1 - offset,offset*(1-offset)/sample_size)]
df[val > (1 - offset), val := 1 - offset]
  

## OUTLIERING --------------------------------------------------------------

# source outliering script and function
source("FILEPATH")

# mark outliers
df <- mark.outliers(data = df,
                    me_name = global.me_name,
                    sheet = global.indicator_group,
                    outlier_db = "FILEPATH")


## OUTPUT RESULTS ----------------------------------------------------------

# remove 'cv_' from any column names as they are not covariates and set measure_id
setnames(df,names(df)[grepl("cv_",names(df))],gsub("cv_","",names(df)[grepl("cv_",names(df))]))
df[, measure_id := 18] # proportion

# output dataset
write.csv(df, "FILEPATH", row.names = F)


## PREP CUSTOM COVARIATES --------------------------------------------------

# any contra 
if (global.me_name == 'any_contra_unmar_final') {
  dt <- model_load(any_contra_mar_final,"raked")[,-c("gpr_lower","gpr_upper"),with=F]
  setnames(dt,"gpr_mean","cv_any_contra_mar")
  write.csv(dt,"FILEPATH",row.names=F)
} else if (global.me_name == 'any_contra_unmar_prelim') {
  dt <- model_load(any_contra_mar_prelim,"raked")[,-c("gpr_lower","gpr_upper"),with=F]
  setnames(dt,"gpr_mean","cv_any_contra_mar_prelim")
  write.csv(dt,"FILEPATH",row.names=F)
}

# mod prop 
if (global.me_name == 'mod_prop_unmar_final') {
  dt <- model_load(mod_prop_mar_final,"raked")[,-c("gpr_lower","gpr_upper"),with=F]
  setnames(dt,"gpr_mean","cv_mod_prop_mar")
  write.csv(dt,"FILEPATH",row.names=F)
} else if (global.me_name == 'mod_prop_unmar_prelim') {
  dt <- model_load(mod_prop_mar_prelim,"raked")[,-c("gpr_lower","gpr_upper"),with=F]
  setnames(dt,"gpr_mean","cv_mod_prop_mar_prelim")
  write.csv(dt,"FILEPATH",row.names=F)
}

# unmet prop 
if (global.me_name == 'unmet_prop_unmar_final') {
  dt <- model_load(unmet_prop_mar_final,"raked")[,-c("gpr_lower","gpr_upper"),with=F]
  setnames(dt,"gpr_mean","cv_unmet_prop_mar")
  dt2 <- model_load(any_contra_unmar_final,"raked")[,-c("gpr_lower","gpr_upper"),with=F]
  setnames(dt2,"gpr_mean","cv_any_contra_unmar")
  dt <- merge(dt,dt2,by=c("location_id","year_id","age_group_id","sex_id"))
  write.csv(dt,"FILEPATH",row.names=F)
} else if (global.me_name == 'unmet_prop_unmar_prelim') {
  dt <- model_load(unmet_prop_mar_prelim,"raked")[,-c("gpr_lower","gpr_upper"),with=F]
  setnames(dt,"gpr_mean","cv_unmet_prop_mar_prelim")
  dt2 <- model_load(any_contra_unmar_final,"raked")[,-c("gpr_lower","gpr_upper"),with=F]
  setnames(dt2,"gpr_mean","cv_any_contra_unmar")
  dt <- merge(dt,dt2,by=c("location_id","year_id","age_group_id","sex_id"))
  write.csv(dt,"FILEPATH",row.names=F)
}

# method mix
if (global.indicator_group %in% c("method_mix")) {
  if (grepl("unmar_prelim", global.me_name)) {
    dt <- model_load(param_map[modelable_entity_name == gsub("unmar_prelim", "mar_prelim", global.me_name)]$latest, "raked")[,-c("gpr_lower","gpr_upper"), with = F]
    dt[gpr_mean < offset, gpr_mean := offset]
    setnames(dt, "gpr_mean", paste0("cv_", gsub("unmar_prelim", "mar_prelim", global.me_name)))
    write.csv(dt, "FILEPATH", row.names = F)
  } else if (grepl("unmar_final", global.me_name)) {
    dt <- model_load(param_map[modelable_entity_name == gsub("unmar_final", "mar_final", global.me_name)]$latest, "raked")[,-c("gpr_lower","gpr_upper"), with = F]
    dt[gpr_mean < offset, gpr_mean := offset]
    setnames(dt, "gpr_mean", paste0("cv_", gsub("unmar_final", "mar", global.me_name)))
    write.csv(dt, "FILEPATH", row.names = F)
  }
}
