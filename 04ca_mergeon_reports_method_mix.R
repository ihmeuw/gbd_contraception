############################################################################################################
## Purpose: Compile collapsed microdata with reports for method mix and separate them into input data files
## for running in ST-GPR 
############################################################################################################


## SET-UP ----------------------------------------------------------------------

# clear memory
rm(list=ls()[!grepl('global',ls())])

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# load packages, install if missing
lib.loc <- "FILEPATH"
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("tidyverse","data.table","plyr","magrittr","parallel","readxl")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

## settings
param_map <- read_excel("FILEPATH", sheet = global.indicator_group) %>% as.data.table()
release <- unique(param_map$release_id)
dstep <- unique(param_map$decomp_step)
gbd_round <- unique(param_map$gbd_round_id)
years <- seq(unique(param_map$year_start), unique(param_map$year_end))

# turn off scientific notation
options(scipen=999)

# load shared functions
functions <- list.files("FILEPATH")
invisible(lapply(paste0("FILEPATH", functions), source))

# source ST-GPR shared functions 
source("FILEPATH")

# in/out
in.dir <- "FILEPATH"
out.dir <- "FILEPATH"

# create function for the opposite of %in%
'%ni%' <- Negate('%in%')

# load locations, SDI, age groups, and ST-GPR functions
locations <- get_location_metadata(location_set_id = 22, release_id = release) 
locs <- locations %>% select(location_id,ihme_loc_id,location_name,region_id,region_name,super_region_id,super_region_name,level)

sdi <- get_covariate_estimates(covariate_id = 881, release_id = release) %>% select(location_id,year_id,sdi = mean_value)

age_map <- get_ids(table = "age_group")
age_map <- age_map[age_group_id %in% seq(8,14)]
age_map[,age_group := gsub(" to ","-",age_group_name)]
age_map[,age_group_name := NULL]

## run_id's
main_pipeline <- read_excel("FILEPATH", sheet = "main_pipeline") %>% as.data.table()
marital_status_final <- main_pipeline[modelable_entity_name == "marital_status_final"]$latest


## PREP MICRODATA ---------------------------------------------------------

prep_microdata <- function(indicator = "") {
  print('Prepping microdata...')
  
  # read in collapsed contraception microdata
  micro <- fread(file.path(in.dir, "FILEPATH", paste0(indicator, ".csv")))
  
  # merge on locations, ages, and other formatting
  micro <- merge(micro[ihme_loc_id != "" & !is.na(ihme_loc_id)], locs, by = "ihme_loc_id", all.x = TRUE)
  micro <- merge(micro, age_map, by = "age_group", all.x = TRUE)
  micro[, age_start := as.numeric(substr(age_group, 1, 2))]
  micro[, age_end := as.numeric(substr(age_group, 4, 5))]
  micro[, year_id := floor((as.numeric(year_start) + as.numeric(year_end)) / 2)]
  micro[, sex_id := 2]
  
  # remove duplicates (keep whichever has the larger sample size)
  micro[, keep_ss := max(sample_size), by = .(nid,file_path,ihme_loc_id,year_id,age_group,var)]
  micro <- unique(micro[keep_ss == sample_size])
  micro[,keep_ss := NULL]
  
  # add on SDI
  micro <- merge(micro, sdi, by = c("location_id","year_id"))
  
  # create outlier variable
  micro[, is_outlier := 0]
  
   # create marital group variable
  micro[grepl("_mar", var), marital_group := "married"]
  micro[grepl("_unmar", var), marital_group := "unmarried"]
  micro[!grepl("_unmar|_mar", var), marital_group := "all"]
  
  # remove marital suffixes from var
  micro[, var := gsub("_unmar|_mar", "", var)]
  
  # change mean to val; ST-GPR expects val
  setnames(micro, "mean", "val")
  
  return(micro)
}


## PREP REPORT DATA ---------------------------------------------------------------------
print('Prepping report data...')

## read in report extractions and format
reports <- data.table(read_excel(file.path(in.dir,"FILEPATH"), guess_max = 10000))[-1]
string_cols <- c("notes","survey_name","file_path","ihme_loc_id","contra_aggregated_methods","category_mismatch")
numeric_cols <- names(reports)[!names(reports) %in% string_cols]
percentage_cols <- numeric_cols[grepl("contra|unmet_need|met_demand|prop_",numeric_cols) & !grepl("sample_size|page_number|mean|multi_method",numeric_cols)]
reports[, c(numeric_cols) := lapply(.SD,as.numeric),.SDcols = numeric_cols]
reports[, c(percentage_cols) := lapply(.SD,function(x) {x/100}),.SDcols = percentage_cols]
reports[,year_id := floor((year_start + year_end)/2)]
setnames(reports,"outlier","is_outlier")

## use reports to impute sample sizes where missing (using 5th percentile)
ss_impute <- quantile(reports[!is.na(sample_size) & !is.na(age_group_id) & !grepl("_",ihme_loc_id),sample_size],.05)

## subset to real ihme_loc_ids (some fake aggregations in the sheet should be dropped), merge on sdi
reports[!ihme_loc_id %in% locations$ihme_loc_id,unique(ihme_loc_id)]
reports <- reports[ihme_loc_id %in% locations$ihme_loc_id]
reports <- merge(reports,locs,by="ihme_loc_id",all.x=T)
reports[,super_region := as.factor(super_region_id)]
reports <- merge(reports,sdi,by=c("location_id","year_id"))

## clean up ages 
reports[!is.na(age_group_id),c("age_start","age_end") := .((age_group_id-5)*5,((age_group_id-5)*5) + 4)] # fill in age_start and age_end 
reports <- reports[age_end-age_start > 1] # remove data that span 2 or less years of ages 
reports <- reports[age_end > 16 & age_start < 48] # remove data out of the 15-49 age range 
reports[,age_start := round_any(age_start,5,floor)] # adjust age_start to the nearest lowest 5-integer
reports[age_start>age_end] # check after rounding age_start is not larger than age_end 
reports[15 - age_start >= 5,is_outlier := 1] # outlier data outside of our age groups 15-49 (could split but tricky)
reports[,age_end := round_any(age_end,5,ceiling) - 1] # adjust age_end to the nearest highest 5-integer
if (nrow(reports[age_end<age_start])>0) stop('age_end coming out lower than age_start, check age cleaning code or look for incorrect extraction!') 
reports[age_end - 49 >= 5,is_outlier := 1] # outlier data outside of our age groups 15-49 (could split but tricky)
reports[,age_group := paste0(age_start,"-",age_end)]

## fill in age_group_id for data that now properly fits within an age bin after cleaning 
reports[is.na(age_group_id) & age_group %in% age_map$age_group,unique(nid)]
reports <- merge(reports,age_map[,.(age_id = age_group_id,age_group)],all.x=T,by="age_group")
reports[is.na(age_group_id) & !is.na(age_id),age_group_id := age_id]
reports[,age_id := NULL]

## fill in missingness in flagging variables
reports[is.na(verified), verified := 0]
reports[is.na(is_outlier), is_outlier := 0]
reports[is.na(currmar_only), currmar_only := 0]
reports[is.na(evermar_only), evermar_only := 0]
reports[is.na(formermar_only), formermar_only := 0]
reports[is.na(unmar_only), unmar_only := 0]
reports[is.na(unmar_sex_active_only), unmar_sex_active_only := 0]
reports[is.na(main_analysis), main_analysis := 0]
reports[is.na(multi_method), multi_method := 0]
reports[is.na(demand_row_only), demand_row_only := 0]
reports[is.na(marital_row_only), marital_row_only := 0]

## only keep entries not marked as outliers and flagged for analysis
reports <- reports[is_outlier == 0 & main_analysis == 1]

# create marital group variable
reports[currmar_only == 1, marital_group := "married"]
reports[unmar_only == 1, marital_group := "unmarried"]
reports[currmar_only == 0 & formermar_only == 0 & evermar_only == 0 & unmar_only == 0 & unmar_sex_active_only == 0, marital_group := "all"]

# drop any data that does not fit into our main marital modeling categories 
reports <- reports[!is.na(marital_group)]

# format important modeling variables
reports[, cv_report := 1]
reports[, sex_id := 2]

# create survey id
reports[, survey_id := paste(survey_name,ihme_loc_id,year_id,nid)]


## EXTRACT UNMARRIED DATA FUNCTION ---------------------------------------------

extract_unmarried <- function(dt,
                              indic_col = "",
                              ss_col = "") {
  
  ## identify those surveys that have data for all women and married women, but do not report out for unmarried women
  dt[, survey_id := paste(survey_name,ihme_loc_id,year_id,nid)]
  data <- dt[!is.na(get(indic_col))]
  data[, all_flag := "all" %in% marital_group, by = survey_id] ## flag surveys that have all-women data
  data[, currmar_flag := "married" %in% marital_group, by = survey_id] ## flag surveys that have married/in-union data
  data[, unmar_flag := "unmarried" %in% marital_group, by = survey_id] ## flag surveys that have unmarried data
  imputers <- data[all_flag == T & currmar_flag == T & unmar_flag == F, .(survey_id,nid)] %>% unique ## identify surveys with all and married data but no unmarried data
  
  ## create imputing dataset and drop unnecessary columns
  calc_df <- data[survey_id %in% imputers$survey_id & (marital_group == "married" | marital_group == "all")]
  meta_cols <- c("survey_name", "nid", "ihme_loc_id", "year_start", "year_end", "survey_id", "file_path")
  metadata <- unique(calc_df[, (meta_cols), with = F])
  calc_df <- calc_df[, .(survey_id, age_group_id, age_group, age_start, age_end, marital_group, sample_size = get(ss_col), indicator = get(indic_col))]
  
  ## ensure that age groups match between all-women and married women data before merging together
  calc_df_all <- calc_df[marital_group == "all"]
  calc_df_mar <- calc_df[marital_group == "married", .(survey_id, age_group_id, age_group, age_start, age_end, ss_mar = sample_size, indicator_mar = indicator)]
  calc_df <- merge(calc_df_all, calc_df_mar, all = T, by = c("survey_id", "age_group_id", "age_group", "age_start", "age_end"))
  
  ## separate out those surveys where age-aggregation is required first
  aggs <- calc_df[survey_id %in% calc_df[is.na(indicator) | is.na(indicator_mar), unique(survey_id)]]
  calc_df <- calc_df[!survey_id %in% aggs$survey_id]
  
  ## check that surveys have all the data required for age-aggregation 
  
  # flag rows requiring age-aggregation
  aggs[, agg_mar_needed := ifelse(is.na(age_group_id) & is.na(indicator_mar), 1, 0)]
  aggs[, agg_all_needed := ifelse(is.na(age_group_id) & is.na(indicator), 1, 0)]
  
  # give each row an id for tracking purposes; surveys could require multiple aggregations so cannot use survey id
  aggs[, row_id := .I]
  
  # for each row needing age-aggregation check if all the required data is present and flag which rows to keep
  for (row in aggs[agg_mar_needed == 1 | agg_all_needed == 1]$row_id) {
    if (aggs[row_id == row]$agg_mar_needed == 1) {
      # identify min/max of the age range we want to check rows exist for and the total number of age years needed to aggregate
      age_min <- aggs[row_id == row & agg_mar_needed == 1]$age_start
      age_max <- aggs[row_id == row & agg_mar_needed == 1]$age_end
      ages <- age_max - age_min + 1
      
      # identify corresponding survey id
      survey <- aggs[row_id == row]$survey_id
      
      # identify which rows correspond to which age-aggregated row
      aggs[survey_id == survey & age_start >= age_min & age_end <= age_max, agg_age_group := paste0(age_min, "-", age_max)]
      
      # check that all the necessary age years required for aggregation are in the dataset, if so flag to keep relevant rows
      aggs[survey_id == survey & agg_age_group == paste0(age_min, "-", age_max), keep := ages == aggs[survey_id == survey & is.na(agg_mar_needed) & agg_age_group == paste0(age_min, "-", age_max) & !is.na(indicator_mar), sum(age_end - age_start + 1, na.rm = T)]]
    } else {
      # identify min/max of the age range we want to check rows exist for and the total number of age years needed to aggregate
      age_min <- aggs[row_id == row & agg_all_needed == 1]$age_start
      age_max <- aggs[row_id == row & agg_all_needed == 1]$age_end
      ages <- age_max - age_min + 1
      
      # identify corresponding survey id
      survey <- aggs[row_id == row]$survey_id
      
      # identify which rows correspond to which age-aggregated row
      aggs[survey_id == survey & age_start >= age_min & age_end <= age_max, agg_age_group := paste0(age_min, "-", age_max)]
      
      # check that all the necessary age years required for aggregation are in the dataset, if so flag to keep relevant rows
      aggs[survey_id == survey & agg_age_group == paste0(age_min, "-", age_max), keep := ages == aggs[survey_id == survey & is.na(agg_all_needed) & agg_age_group == paste0(age_min, "-", age_max) & !is.na(indicator), sum(age_end - age_start + 1, na.rm = T)]]
    }
  }
    
  if (nrow(aggs[agg_mar_needed == 1 | agg_all_needed == 1]) > 0) {
    # return rows that did not meet the needs/requirements for age-aggregation but have married and unmarried data to calc_df
    # keep rows to be used for age-aggregating 
    calc_df <- rbind(calc_df, aggs[!is.na(indicator) & !is.na(indicator_mar) & (keep == F | is.na(keep)), names(calc_df), with = F])
    aggs <- aggs[keep == T]
    
    # if there are no rows requiring age-aggregation, skip
    if (nrow(aggs) > 0) {
      ## preform age-aggregations 
      aggs[, c("indicator", "indicator_mar", "sample_size", "ss_mar") := .(weighted.mean(indicator, sample_size, na.rm = T), weighted.mean(indicator_mar, ss_mar, na.rm = T), 
                                                                           sum(sample_size, na.rm = T), sum(ss_mar, na.rm = T)), by = .(survey_id, agg_age_group)]
      
      ## subset to appropriate aged row for the aggregation
      aggs <- aggs[age_group == agg_age_group]
      calc_df <- rbind(calc_df, aggs[, names(calc_df), with = F]) 
    }
  } else {
    # return rows that did not meet the needs/requirements for age-aggregation but have married and unmarried data to calc_df
    calc_df <- rbind(calc_df, aggs[!is.na(indicator) & !is.na(indicator_mar), names(calc_df), with = F])
  }
  
  ## now that ages match between all-women data and married women data, calculate out out unmarried values using sample sizes
  ## however, in some cases sample sizes are not reported and we must use modeled marital status estimates to perform proportional splits
  
  ## identify rows where calculating out unmarried data requires imputing some sample sizes
  needs_ss <- calc_df[is.na(sample_size) | is.na(ss_mar)]
  calc_df <- calc_df[!is.na(sample_size) & !is.na(ss_mar)]
  
  ## use latest marital status model to inform the proportional splits
  needs_ss <- merge(needs_ss, metadata[, .(survey_id, ihme_loc_id, year_id = floor((year_start + year_end) / 2))], by = "survey_id", all.x = T)
  needs_ss <- merge(needs_ss, locs[, .(location_id, ihme_loc_id)], by = "ihme_loc_id")
  marital <- model_load(marital_status_final, 'raked')[, .(location_id, year_id, age_group_id, prop_mar = gpr_mean)]
  needs_ss <- merge(needs_ss, marital, all.x = T, by = c("location_id", "year_id", "age_group_id"))
  
  ## sum modeled marital status estimates across ages using pop-weighting if there are aggregate-age rows to split
  if (needs_ss[is.na(age_group_id), .N] > 0) {
    pops <- get_population(location_id = needs_ss[is.na(age_group_id), unique(location_id)], year_id = needs_ss[is.na(age_group_id), unique(year_id)],
                           sex_id = 2, age_group_id = seq(8,14), decomp_step = dstep, gbd_round_id = gbd_round)[, -c("run_id", "sex_id"), with = F]
    marital <- merge(marital, pops, by = c("location_id", "year_id", "age_group_id"))
    for (sid in needs_ss[is.na(age_group_id), unique(survey_id)]) {
      for (age_grp in needs_ss[survey_id == sid, unique(age_group)]) {
        locid <- needs_ss[survey_id == sid, unique(location_id)]
        yearid <- needs_ss[survey_id == sid, unique(year_id)]
        ages <- needs_ss[survey_id == sid & age_group == age_grp, seq((age_start / 5) + 5, ((age_end + 1) / 5) + 4)]
        weighted_prop <- marital[location_id == locid & year_id == yearid & age_group_id %in% ages, weighted.mean(prop_mar, population)]
        needs_ss[survey_id == sid, prop_mar := weighted_prop]
      }
    }
    needs_ss[is.na(prop_mar), .N]
  }
  
  ## impute sample sizes
  needs_ss[is.na(ss_mar) & is.na(sample_size), c("sample_size", "ss_mar") := .(ss_impute, prop_mar*ss_impute)]
  needs_ss[is.na(sample_size), sample_size := ss_mar / prop_mar]
  needs_ss[is.na(ss_mar), ss_mar := sample_size * prop_mar]
  
  ## drop extra cols
  needs_ss <- needs_ss[, c(names(needs_ss) %in% names(calc_df)), with = F]
  calc_df <- rbind(calc_df, needs_ss)
  
  ## calculate out unmarried data when sample sizes are present
  calc_df[, indicator_unmar := ((indicator * sample_size) - (indicator_mar * ss_mar)) / (sample_size - ss_mar)]
  
  ## if weighted mean led to negatives, probably an issue with survey weights or small numbers 
  ## revisit extraction and make sure you are using weighted sample sizes if negative is large
  ## otherwise can assume it corresponds to 0
  calc_df[, indicator_unmar := .(ifelse(indicator_unmar > 0 & indicator_unmar < 1, indicator_unmar, 0))]
  calc_df <- calc_df[!is.na(indicator_unmar)]
  
  ## assign marital group and calculate unmarried sample size
  calc_df[, c("marital_group", "ss_unmar") := .("unmarried", sample_size - ss_mar)]
  
  ## if the unmarried survey-age group entry exists, update corresponding columns 
  ## otherwise need to add entry to indicator reports table 
  for (entry in calc_df[, paste(survey_id, age_group, marital_group)]) {
    if (entry %in% dt[, paste(survey_id, age_group, marital_group)]) {
      dt[paste(survey_id, age_group, marital_group) == entry, c(indic_col, ss_col) := .(calc_df[paste(survey_id, age_group, marital_group) == entry]$indicator_unmar,
                                                                                     calc_df[paste(survey_id, age_group, marital_group) == entry]$ss_unmar)]
    } else {
      x <- calc_df[paste(survey_id, age_group, marital_group) == entry]
      x <- x[, .(survey_id, age_group_id, age_group, age_start, age_end, marital_group, ss_col = ss_unmar, indic_col = indicator_unmar)]
      setnames(x, c("indic_col", "ss_col"), c(indic_col, ss_col))
      x <- merge(metadata, x, by = "survey_id")
      x <- merge(x, locs[,.(location_id, ihme_loc_id)], by = "ihme_loc_id", all.x = T)
      x[, year_id := floor((year_start + year_end) / 2)]
      x[, is_outlier := 0]
      x[, age_group := paste0(age_start, "-", age_end)]
      dt <- rbind(dt, x, fill = T)
    }
  }
  
  # format important modeling variables
  dt[, cv_report := 1]
  dt[, sex_id := 2]
  dt[, multi_method := 0]
  
  return(dt)
}


## METHOD MIX --------------------------------------------------------------------

if (global.indicator_group == "method_mix") {
  
  # microdata
  micro <- prep_microdata("contra_method_mix")
  
  # remove none and abstinence as these should not be counted in the proportion of any use
  # plus we are not concerned with modelling these
  micro <- micro[!grepl("none|abstinence|no_response", var)]
  
  # correct sample size 
  micro[, any_contra := sum(val), by = c("survey_id", "age_group", "marital_group")]
  micro[, sample_size := any_contra * sample_size]
  
  
  # reports 
  # general columns to keep regardless of the indicator being modeled 
  general_cols <- c("nid", "file_path", "ihme_loc_id", "location_id", "survey_id", "survey_name", "main_anaylsis", "verified", "is_outlier", "cv_subgeo", "year_id", 
                    "year_start", "year_end", "notes", "age_group", "age_group_id", "age_start", "age_end", "marital_group", "sex_id", "cv_report")
  
  # indicator specific columns to keep
  indicator_cols <- c("sample_size", "multi_method", names(reports)[grepl("^contra_", names(reports))], "category_mismatch", "mod_contra", "trad_contra", "any_contra")
  
  # only keep necessary columns 
  reports_subset <- reports[, colnames(reports) %in% c(general_cols, indicator_cols), with = F]
  
  # only keep rows with method breakdown data
  reports_subset <- reports_subset[!is.na(contra_female_ster) | !is.na(contra_male_ster) | !is.na(contra_pill) | !is.na(contra_iud) | !is.na(contra_implant) |
                                     !is.na(contra_inject) | !is.na(contra_male_condom) | !is.na(contra_female_condom) | !is.na(contra_diaphragm) |
                                     !is.na(contra_foam_gel_sponge) | !is.na(contra_patch) | !is.na(contra_ring) | !is.na(contra_emergency) | !is.na(contra_modern_other) |
                                     !is.na(contra_rhythm) | !is.na(contra_withdrawal) | !is.na(contra_calendar_beads_sdays) | !is.na(contra_lactate_amen) | !is.na(contra_trad_other)]
  
  ## drop reports which reported all methods a women is using
  ## do not have a method to parse out the most effective method a women is using
  reports_subset <- reports_subset[multi_method != 1]
  
  # calculate out unmarried data when all and married women data available
  for (column in names(reports)[grepl("^contra_", names(reports))][names(reports)[grepl("^contra_", names(reports))] != "contra_aggregated_methods"]) {
    reports_subset <- extract_unmarried(reports_subset, indic_col = column, ss_col = "sample_size")
  }
  
  # sum up mod_contra, trad_contra, and any_contra assuming no miscategorization of "other" categories
  methods <- names(reports_subset)[grepl("^contra_",names(reports_subset)) & !grepl("aggregated",names(reports_subset))]
  trad_methods <- methods[grepl("lactat|withdraw|rhythm|calendar|trad",methods)]
  mod_methods <- methods[!methods %in% trad_methods]
  
  # if mod_contra or trad_contra is missing and the specific methods underlying them are not also missing (so method-specific	
  # data was actually collected by the survey), sum up the specific methods	
  # Do not do this for surveys that report multiple methods used per respondent or it may surpass 100%!
  reports_subset[is.na(mod_contra) & multi_method == 0 & rowSums(!is.na(reports_subset[, mod_methods, with = F])) != 0, mod_contra := rowSums(.SD, na.rm = T), .SDcols = mod_methods]
  reports_subset[is.na(trad_contra) & multi_method == 0 & rowSums(!is.na(reports_subset[, trad_methods, with = F])) != 0, trad_contra := rowSums(.SD, na.rm = T), .SDcols = trad_methods]
  reports_subset[is.na(any_contra) & multi_method == 0, any_contra := mod_contra + trad_contra]
  
  # melt table so every variable is in its own row
  reports_subset <- melt(data = reports_subset, 
                         measure.vars = methods, variable.name = "var", 
                         value.name = "val", na.rm = TRUE)
  
  # note any extracted reports that have since been extracted in microdata form and remove
  reports_subset[nid %in% micro[, unique(nid)], unique(nid)]
  reports_subset <- reports_subset[!nid %in% micro[, unique(nid)]]
  
  # subset to most age-specific report data
  reports_subset[, age_diff := age_end - age_start]
  reports_subset[, duplicate := .N, by = .(survey_id, marital_group, var, age_start)]
  reports_subset[duplicate > 1, keep_row := min(age_diff), by = .(survey_id, marital_group, var, age_start)]
  reports_subset <- reports_subset[is.na(keep_row) | keep_row == age_diff]
  reports_subset[, duplicate := .N, by = .(survey_id, marital_group, var, age_end)]
  reports_subset[duplicate > 1, keep_row := min(age_diff), by = .(survey_id, marital_group, var, age_end)]
  reports_subset <- reports_subset[is.na(keep_row) | keep_row == age_diff]
  
  # remove duplicates 
  reports_subset <- unique(reports_subset, by = c("survey_id", "marital_group", "age_group", "var"))
  
  # rename var to match between microdata and reports 
  xwalk_methods <- c("contra_female_ster" = "female_sterilization",
                     "contra_male_ster" = "male_sterilization",
                     "contra_pill" = "pill",
                     "contra_iud" = "iud",
                     "contra_inject" = "injections",
                     "contra_implant" = "implants",
                     "contra_male_condom" = "condom",
                     "contra_female_condom" = "female_condom",
                     "contra_diaphragm" = "diaphragm",
                     "contra_foam_gel_sponge" = "foam_jelly_sponge",
                     "contra_emergency" = "emergency_contraception",
                     "contra_modern_other" = "other_modern_method",
                     "contra_lactate_amen" = "lactational_amenorrhea_method",
                     "contra_withdrawal" = "withdrawal",
                     "contra_rhythm" = "rhythm",
                     "contra_calendar_beads_sdays" = "calendar_methods",
                     "contra_trad_other" = "other_traditional_method")
  micro[, var := gsub("current_method_", "", var)]
  reports_subset[, var := xwalk_methods[var]]
  
  # create combined microdata and reports datasets
  combined <- rbind(micro[, -c("location_name")], reports_subset, fill = T)
  
  # group less common methods
  combined[var %in% c("contraceptive_ring", "contraceptive_patch", "foam_jelly_sponge"), var := "other_modern_method"]
  combined[var == "female_condom", var := "condom"]
  combined[var == "calendar_methods", var := "rhythm"]
  combined[, val := sum(val), by = c("location_id", "age_group", "year_id", "nid", "var", "marital_group")]
  combined <- unique(combined, by = c("location_id", "age_group", "year_id", "nid", "var", "marital_group"))
  
  for (variable in unique(combined$var)) {
    mar <- combined[var == variable & marital_group == "married"]
    write.csv(mar, file.path(out.dir, paste0("current_", variable, "_mar_merged.csv")), row.names = FALSE, fileEncoding = "UTF-8")
    
    unmar <- combined[var == variable & marital_group == "unmarried"]
    write.csv(unmar, file.path(out.dir, paste0("current_", variable, "_unmar_merged.csv")), row.names = FALSE, fileEncoding = "UTF-8")
    
    combined[cv_report == 1, tosplit := "all" %in% marital_group & ("married" %ni% marital_group & "unmarried" %ni% marital_group), by = c("survey_id", "age_group", "var")]
    all <- combined[var == variable & marital_group == "all" & tosplit == 1 & cv_report == 1]
    write.csv(all, file.path(out.dir, paste0("current_", variable, "_all_reports.csv")), row.names = FALSE, fileEncoding = "UTF-8")
  }
}
