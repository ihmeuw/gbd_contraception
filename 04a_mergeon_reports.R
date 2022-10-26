############################################################################################################
## Purpose: Compile collapsed contraception microdata with reports and separate them into input data files
## for running in ST-GPR for thesis identifying disparities in indicators by marital status
###########################################################################################################


# SET-UP ----------------------------------------------------------------------

## clear memory
rm(list=ls()[!grepl('global|param_map',ls())])

## runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# packages
pacman::p_load(data.table,plyr,dplyr,magrittr,ggplot2,parallel,readxl,boot,lme4)

## settings
param_map <- read_excel("FILEPATH", sheet = global.indicator_group) %>% as.data.table()
marital_only <- global.marital_only
release <- unique(param_map$release_id)
dstep <- unique(param_map$decomp_step)
gbd_round <- unique(param_map$gbd_round_id)
years <- seq(unique(param_map$year_start), unique(param_map$year_end))
census_data <- "FILEPATH"

## run_id's
main_pipeline <- read_excel("FILEPATH", sheet = "main_pipeline") %>% as.data.table()
marital_status_final <- main_pipeline[modelable_entity_name == "marital_status_final"]$latest

## load shared functions
functions <- list.files("FILEPATH")
invisible(lapply(paste0("FILEPATH", functions), source))

# source ST-GPR shared functions 
source("FILEPATH")

## in/out
in.dir <- "FILEPATH"
out.dir <- "FILEPATH"

## load locations, SDI, age groups, and ST-GPR functions
locations <- get_location_metadata(location_set_id = 22, release_id = release) 
locs <- locations %>% select(location_id,ihme_loc_id,location_name,region_id,region_name,super_region_id,super_region_name,level)

sdi <- get_covariate_estimates(covariate_id = 881, release_id = release) %>% select(location_id,year_id,sdi = mean_value)

age_map <- get_ids(table = "age_group")
age_map <- age_map[age_group_id %in% seq(8,14)]
age_map[,age_group := gsub(" to ","-",age_group_name)]
age_map[,age_group_name := NULL]


## PREP MICRODATA ------------------------------------------------------------------
print('Prepping microdata...')

## read in collapsed contraception microdata
micro <- fread(file.path(in.dir, "FILEPATH"))

## merge on locations, and other formatting
micro <- merge(micro[ihme_loc_id != "" & !is.na(ihme_loc_id)], locs, by = "ihme_loc_id", all.x = TRUE)
micro <- merge(micro, age_map, by = "age_group", all.x = TRUE)
micro[, age_start := as.numeric(substr(age_group, 1, 2))]
micro[, age_end := as.numeric(substr(age_group, 4, 5))]
micro[, year_id := floor((as.numeric(year_start) + as.numeric(year_end)) / 2)]
micro[, sex_id := 2]
flag_cols <- c("nid","mar_restricted","contra_currmar_only","contra_evermar_only","currmar_only","evermar_only","missing_fecund","missing_desire","missing_desire_later","no_preg","no_ppa","cv_subgeo")
micro[,(flag_cols) := lapply(.SD, as.numeric), .SDcols = flag_cols]

## remove duplicates (keep whichever has the larger sample size) and small sample sizes
micro[, keep_ss := max(sample_size), by = .(nid,ihme_loc_id,year_id,age_group,var)]
micro[nid %in% c("NID","NID"), keep_ss := max(sample_size), by = .(nid,file_path,ihme_loc_id,year_id,age_group,var)]
micro <- unique(micro[keep_ss == sample_size])
micro[,keep_ss := NULL]
micro <- micro[sample_size >= 20]

## add on SDI
micro <- merge(micro, sdi, by = c("location_id","year_id"))

## create outlier column
micro[, is_outlier := 0]

## remove microdata that does not have marital status breakdown 
micro <- micro[!nid %in% c("NID","NID","NID","NID")] 

## create marital group variable
micro[grepl("_mar_", var), marital_group := "married"]
micro[grepl("_unmar_", var), marital_group := "unmarried"]
micro[!grepl("_unmar_|_mar_", var), marital_group := "all"]

## Split into modelable entity datasets
if (marital_only) {
  ## subset microdata to proportion of women who are married
  micro_marital <- micro[var == "curr_cohabit" & mar_restricted == 0]
  micro_marital[, var := NULL]
  setnames(micro_marital, "mean", "val")
  
} else {
  ## subset microdata to any contraceptive usage for married women
  micro_any_mar <- micro[var == "any_contra_mar"]
  micro_any_mar[, var := NULL]
  setnames(micro_any_mar, "mean", "val")
  
  ## subset microdata to any contraceptive usage for unmarried women
  micro_any_unmar <- micro[var=="any_contra_unmar" & contra_evermar_only == 0]
  micro_any_unmar[,var:=NULL]
  setnames(micro_any_unmar,"mean","val")
  
  ## subset microdata to any contraceptive usage for all women (not by marital status) - for xwalk purposes below
  micro_any_all <- micro[var=="any_contra" & contra_currmar_only == 0 & contra_evermar_only == 0 & sample_size >= 20]
  micro_any_all[,var:=NULL]
  setnames(micro_any_all,"mean","val")
  
  ## subset microdata to proportion of use that is modern for married women
  micro_mod_mar <- micro[var=="mod_prop_mar"]
  micro_mod_mar[,var:=NULL]
  setnames(micro_mod_mar,"mean","val")
  
  ## subset microdata to proportion of use that is modern for unmarried women
  micro_mod_unmar <- micro[var=="mod_prop_unmar" & contra_evermar_only == 0]
  micro_mod_unmar[,var:=NULL]
  setnames(micro_mod_unmar,"mean","val")
  
  ## subset microdata to proportion of use that is modern for all women (not by marital status) - for xwalk purposes below
  micro_mod_all <- micro[var=="mod_contra" & contra_currmar_only == 0 & contra_evermar_only == 0 & sample_size >= 20]
  micro_mod_all[,var:=NULL]
  setnames(micro_mod_all,"mean","val")
  
  ## subset microdata to proportion of married non-users that still have a need
  micro_unmet_mar <- micro[var=="unmet_prop_mar"]
  micro_unmet_mar[,var:=NULL]
  setnames(micro_unmet_mar,"mean","val")
  
  ## subset microdata to proportion of unmarried non-users that still have a need
  micro_unmet_unmar <- micro[var=="unmet_prop_unmar" & evermar_only == 0 & currmar_only == 0]
  micro_unmet_unmar[,var:=NULL]
  setnames(micro_unmet_unmar,"mean","val")
  
  ## create training datasets for xwalks later for all women, married women and unmarried women 
  basic_cols <- c("nid","ihme_loc_id","location_id","super_region_id","super_region_name","level","year_id","sdi","age_group_id","age_group")
  train_all <- merge(micro_any_all[,c(basic_cols,"val"),with=F],micro_mod_all[,c(basic_cols,"val"),with=F],by=basic_cols)
  setnames(train_all,c("val.x","val.y"),c("any_contra","mod_contra"))
  
  train_mar <- merge(micro_any_mar[,c(basic_cols,"val"),with=F],micro_mod_mar[,c(basic_cols,"val"),with=F],by=basic_cols)
  setnames(train_mar,c("val.x","val.y"),c("any_contra","mod_prop"))
  
  train_unmar <- merge(micro_any_unmar[,c(basic_cols,"val"),with=F],micro_mod_unmar[,c(basic_cols,"val"),with=F],by=basic_cols)
  setnames(train_unmar,c("val.x","val.y"),c("any_contra","mod_prop")) 
}


## PREP REPORT DATA ---------------------------------------------------------------------
print('Prepping report data...')

## read in report extractions and format
reports <- data.table(read_excel(file.path(in.dir,"FILEPATH")))[-1]
string_cols <- c("notes","survey_name","file_path","ihme_loc_id","contra_aggregated_methods","category_mismatch")
numeric_cols <- names(reports)[!names(reports) %in% string_cols]
percentage_cols <- numeric_cols[grepl("contra|unmet_need|met_demand|prop_",numeric_cols)]
reports[, c(numeric_cols) := lapply(.SD,as.numeric),.SDcols = numeric_cols]
reports[, c(percentage_cols) := lapply(.SD,function(x) {x/100}),.SDcols = percentage_cols]
reports[,year_id := floor((year_start + year_end)/2)]
setnames(reports,"outlier","is_outlier")

## use microdata and reports to impute sample sizes where missing (using 5th percentile)
ss_impute <- quantile(c(micro[var == "any_contra" & !grepl("_",ihme_loc_id) & age_group %in% age_map$age_group,sample_size],
                        reports[!is.na(sample_size) & !is.na(age_group_id) & !grepl("_",ihme_loc_id),sample_size]),.05)

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
reports[15 - age_start >= 5,is_outlier := 1] # outlier data outside of our age groups 15-49 
reports[,age_end := round_any(age_end,5,ceiling) - 1] # adjust age_end to the nearest highest 5-integer
if (nrow(reports[age_end<age_start])>0) stop('age_end coming out lower than age_start, check age cleaning code or look for incorrect extraction!') 
reports[age_end - 49 >= 5,is_outlier := 1] # outlier data outside of our age groups 15-49 
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

## if marital data, load UN census data and append to reports table
if (marital_only) {
  
  # load UN data and handle formatting
  df_un <- read_excel(census_data, sheet = 'CURRENTLY MARRIED', skip = 2) %>% as.data.table()
  numeric_cols <- c('YearStart','YearEnd','AgeStart','AgeEnd','DataValue')
  df_un[, (numeric_cols) := lapply(.SD, as.numeric), .SDcols = numeric_cols]
  setnames(df_un, c('Country or area','YearStart','YearEnd','AgeStart','AgeEnd','DataValue'), 
           c('location_name','year_start','year_end','age_start','age_end','prop_currmar'), skip_absent = TRUE)
  df_un[, prop_currmar := prop_currmar / 100]
  
  # subset to census data which includes consensual unions 
  df_un <- df_un[DataProcess == "Census" & Including_consensual_unions == 1 & Sex == "Women"]
  
  # adjust names to match those in GBD
  df_un[grepl('hong kong', tolower(location_name)), location_name := 'Hong Kong Special Administrative Region of China']
  df_un[grepl("dem. people's rep. of korea", tolower(location_name)), location_name := "Democratic People's Republic of Korea"]
  df_un[grepl("state of palestine", tolower(location_name)), location_name := "Palestine"]
  df_un[grepl("macedonia", tolower(location_name)), location_name := "North Macedonia"]
  df_un[grepl("taiwan province", tolower(location_name)), location_name := "Taiwan (Province of China)"]
  df_un[grepl("micronesia", tolower(location_name)), location_name := "Micronesia (Federated States of)"]
  df_un[grepl("lao", tolower(location_name)), location_name := "Lao People's Democratic Republic"]
  df_un[grepl("macao", tolower(location_name)), location_name := "Macao Special Administrative Region of China"]
  
  # merge UN data with IHME locations and remove extra locations we do not produce estimates for 
  df_un <- merge(df_un, locs, by = "location_name", all.x = TRUE)
  unique(df_un[is.na(ihme_loc_id)]$location_name)
  df_un <- df_un[!is.na(ihme_loc_id)]
  
  # calculate year_id and age_group_id
  df_un[, year_id := floor((year_start + year_end) / 2)]
  df_un <- df_un[age_start >=15 & age_end <= 49 & year_id >= 1970]
  df_un[, age_group_id := (age_start / 5) + 5]
  df_un[, age_group := paste(age_start, age_end, sep = "-")]
  
  # miscellaneous formatting
  df_un[, nid := "NID"]
  df_un[, survey_name := "UN Marriage Database 2019"]
  df_un[, file_path := census_data]
  
  # pull populations and merge on in order to impute sample size because censuses should account for everyone 
  pops <- get_population(location_id = unique(df_un$location_id), age_group_id = unique(df_un$age_group_id), 
                         year_id = unique(df_un$year_id), release_id = release, sex_id = 2)
  pops$run_id <- NULL
  setnames(pops, "population", "marital_sample_size")
  
  # merge on populations/sample sizes
  df_un <- merge(df_un, pops, by = c("location_id", "age_group_id", "year_id"))
  
  # append onto our report data
  reports <- rbind(reports, df_un[, names(df_un) %in% names(reports), with = FALSE], fill = TRUE)
  
  
# if contraception data, prep contraception indicators   
} else {
  
  ## back-calculate out mod_contra/trad_contra/any_contra when applicable
  reports[is.na(trad_contra) & !is.na(mod_contra) & !is.na(any_contra), trad_contra := any_contra - mod_contra]
  reports[is.na(mod_contra) & !is.na(trad_contra) & !is.na(any_contra), mod_contra := any_contra - trad_contra]
  reports[is.na(any_contra) & !is.na(mod_contra) & !is.na(trad_contra), any_contra := mod_contra + trad_contra]
  
  ## sum up mod_contra, trad_contra, and any_contra assuming no miscategorization of "other" categories
  methods <- names(reports)[grepl("^contra_",names(reports)) & !grepl("aggregated",names(reports))]
  trad_methods <- methods[grepl("lactat|withdraw|rhythm|calendar|trad",methods)]
  mod_methods <- methods[!methods %in% trad_methods]
  
  ## if mod_contra or trad_contra is missing and the specific methods underlying them are not also missing (so method-specific	
  ## data was actually collected by the survey), sum up the specific methods	
  ## Don't do this for surveys that report multiple methods used per respondent or it may surpass 100%!
  reports[is.na(mod_contra) & multi_method == 0 & rowSums(!is.na(reports[,mod_methods,with=F])) != 0,mod_contra := rowSums(.SD,na.rm = T),.SDcols = mod_methods]
  reports[is.na(trad_contra) & multi_method == 0 & rowSums(!is.na(reports[,trad_methods,with=F])) != 0,trad_contra := rowSums(.SD,na.rm = T),.SDcols = trad_methods]
  reports[is.na(any_contra) & multi_method == 0,any_contra := mod_contra + trad_contra]
  
  ## drop reports which reported all methods a women is using
  ## do not have a method to parse out the most effective method a women is using
  reports <- reports[multi_method == 0 | !is.na(any_contra)]
  
  ## check that individual methods do not sum to more than most (or more than 100%) of women in any_contra
  ## at this point in time should just be women in China aged >30
  reports[(mod_contra >= .95 | any_contra >= .95) & multi_method == 0, c('location_name', 'age_group_id', 'any_contra')]
  
  ## calculate met_demand
  reports[!is.na(met_demand_mod),met_demand := met_demand_mod]
  reports[is.na(met_demand) & !is.na(met_demand_all),met_demand := met_demand_all*(mod_contra/any_contra)]
  reports[is.na(met_demand) & !is.na(unmet_need_all),demand_sample_size := round(sample_size*(any_contra + unmet_need_all))]
  reports[is.na(met_demand) & !is.na(unmet_need_all),met_demand := mod_contra/(any_contra + unmet_need_all)]
  reports[is.na(met_demand) & !is.na(unmet_need_mod),demand_sample_size := round(sample_size*(mod_contra + unmet_need_mod))]
  reports[is.na(met_demand) & !is.na(unmet_need_mod),met_demand := mod_contra/(mod_contra + unmet_need_mod)]
  
  ## check for issues/inconsistencies such as any value going above 1 or below 0, or instances where mod contra is larger than met demand or
  ## we have met_demand but are missing mod_contra or any_contra 
  reports[mod_contra > 1 | mod_contra < 0 | any_contra > 1 | any_contra < 0 | met_demand < 0 | met_demand > 1 | mod_contra > met_demand]
  reports[!is.na(met_demand) & (is.na(mod_contra) | is.na(any_contra))]
  
  ## all sources with met demand data should also have any contraceptive use data to calculate unmet_prop	
  ## (but sometimes not possible from DHS/MICS reports for unmarried women)	
  reports[!is.na(met_demand) & unmar_only == 0 & (is.na(mod_contra) | is.na(any_contra))]
  
  
  ## CROSSWALK REPORT DATA MISSING ANY_CONTRA -------------------------------------------------------
  print('Crosswalking report data missing any_contra...')
  
  ## identify those reports where mod_contra exists but any_contra does not (data only collected for modern methods)
  ## for these cases we will crosswalk from mod_contra to any_contra to leverage the available data
  xwalk <- reports[!is.na(mod_contra) & is.na(any_contra) & marital_row_only == 0 & demand_row_only == 0]
  reports <- reports[!(!is.na(mod_contra) & is.na(any_contra) & marital_row_only == 0 & demand_row_only == 0)]
  xwalk_all <- xwalk[marital_group == "all"]
  xwalk_mar <- xwalk[marital_group == "married"]
  xwalk_unmar <- xwalk[marital_group == "unmarried"]
  
  ## combine the training report data containing both any_contra and mod_contra with the microdata training sets created above
  train_all <- rbind(train_all,reports[marital_group == "all" & !is.na(any_contra) & !is.na(mod_contra),c(basic_cols,"any_contra","mod_contra"),with=F])
  train_mar[,mod_contra := mod_prop*any_contra]
  train_mar <- rbind(train_mar[,-c("mod_prop"),with=F],reports[marital_group == "married" & !is.na(any_contra) & !is.na(mod_contra),c(basic_cols,"any_contra","mod_contra"),with=F])
  train_unmar[,mod_contra := mod_prop*any_contra]
  train_unmar <- rbind(train_unmar[,-c("mod_prop"),with=F],reports[marital_group == "unmarried" & !is.na(any_contra) & !is.na(mod_contra),c(basic_cols,"any_contra","mod_contra"),with=F])
  
  ## use model in logit space with fixed effect on age groups and random effect on intercept for super region to predict any_contra
  if (nrow(xwalk_all) > 0) {
    x <- copy(train_all[!mod_contra %in% c(0,1) & !any_contra %in% c(0,1)])
    x[,c("logit_mod","logit_any") := .(logit(mod_contra),logit(any_contra))]
    x[,super_region := as.factor(super_region_id)]
    mod <- lmer(logit_any ~ as.factor(age_group) + logit_mod + (1|super_region),data=x)
    all_coefs <- as.data.table(coef(summary(mod)), keep.rownames = T)
    xwalk_all[,logit_pred := predict(mod,xwalk_all[,list(age_group,super_region,logit_mod = logit(mod_contra))])]
    ## check for inconsistencies (mod contra surpassing any contra)
    if (nrow(xwalk_all[nid != "NID" & inv.logit(logit_pred) < mod_contra]) > 0) break('Any contra surpassing mod contra after predicting out any contra for all women data!') 
    xwalk_all[,any_contra := inv.logit(logit_pred)]
    xwalk_all[any_contra <= mod_contra, any_contra := mod_contra]
  }
  
  ## do the same xwalk for married women
  if (nrow(xwalk_mar) > 0) {
    x <- copy(train_mar[!mod_contra %in% c(0,1) & !any_contra %in% c(0,1)])
    x[,c("logit_mod","logit_any") := .(logit(mod_contra),logit(any_contra))]
    x[,super_region := as.factor(super_region_id)]
    mod <- lmer(logit_any ~ as.factor(age_group) + logit_mod + (1|super_region),data=x)
    mar_coefs <- as.data.table(coef(summary(mod)), keep.rownames = T)
    xwalk_mar[,logit_pred := predict(mod,xwalk_mar[,list(age_group,super_region,logit_mod = logit(mod_contra))])]
    if (nrow(xwalk_mar[inv.logit(logit_pred) < mod_contra]) > 0) break('Any contra surpassing mod contra after predicting out any contra for married women data!') 
    xwalk_mar[,any_contra := inv.logit(logit_pred)]
    xwalk_mar[any_contra <= mod_contra, any_contra := mod_contra]
  }
  
  ## ... and the same xwalk for unmarried women
  if (nrow(xwalk_unmar) > 0) {
    x <- copy(train_unmar[!mod_contra %in% c(0,1) & !any_contra %in% c(0,1)])
    x[,c("logit_mod","logit_any") := .(logit(mod_contra),logit(any_contra))]
    x[,super_region := as.factor(super_region_id)]
    mod <- lmer(logit_any ~ as.factor(age_group) + logit_mod + (1|super_region),data=x)
    unmar_coefs <- as.data.table(coef(summary(mod)), keep.rownames = T)
    xwalk_unmar[,logit_pred := predict(mod,xwalk_unmar[,list(age_group,super_region,logit_mod = logit(mod_contra))])]
    if (nrow(xwalk_unmar[inv.logit(logit_pred) < mod_contra]) > 0) break('Any contra surpassing mod contra after predicting out any contra for unmarried women data!') 
    xwalk_unmar[,any_contra := inv.logit(logit_pred)]
    xwalk_unmar[any_contra <= mod_contra, any_contra := mod_contra]
  }
  
  ## append xwalked data back onto main dataset
  xwalk <- rbindlist(list(xwalk_all, xwalk_mar, xwalk_unmar), fill = TRUE)
  xwalk[, logit_pred := NULL]
  xwalk[, cv_any_contra_xwalk := 1]
  reports <- rbind(reports, xwalk, fill = TRUE)
  
  
  ## FINALIZE REPORT DATA ----------------------------------------------------------------------
  
  ## create modelable entity variables
  reports[demand_row_only == 0, mod_prop := mod_contra / any_contra]
  reports[, unmet_prop := unmet_need_all / (1 - any_contra)]
  reports[is.na(unmet_prop), unmet_prop := (unmet_need_mod - trad_contra) / (1 - any_contra)]
  reports[is.na(unmet_prop), unmet_prop := ((mod_contra / met_demand) - any_contra)/(1 - any_contra)]
  reports[marital_group == "married" & demand_row_only == 0, any_contra_mar := any_contra]
  reports[marital_group == "unmarried" & demand_row_only == 0, any_contra_unmar := any_contra]
  reports[marital_group == "married", c("mod_prop_mar","unmet_prop_mar") := .(mod_prop,unmet_prop)]
  reports[marital_group == "unmarried", c("mod_prop_unmar","unmet_prop_unmar") := .(mod_prop,unmet_prop)]
  
  ## if outputting unmarried data, will first want to calculate it out for some surveys that have all-women data and
  ## married women data, but do not report the unmarried data. This requires either reported sample sizes or modeled
  ## estimates of marital proportions
  print('Calculating out unmarried data from all women and married data...')
  
  ## identify those surveys that have data for all women and married women, but do not report out for unmarried women
  reports[,survey_id := paste(survey_name,ihme_loc_id,year_id,nid)]
  reports[, all_flag := "all" %in% marital_group, by = survey_id] ## flag surveys that have all-women data
  reports[, currmar_flag := "married" %in% marital_group, by = survey_id] ## flag surveys that have married/in-union data
  reports[, unmar_flag := "unmarried" %in% marital_group, by = survey_id] ## flag surveys that have unmarried data
  imputers <- reports[all_flag == T & currmar_flag == T & unmar_flag == F,.(survey_id,nid)] %>% unique ## identify surveys with all and married data but no unmarried data
  imputers <- imputers[!nid %in% micro$nid] ## make sure none of these have since been extracted as microdata
    
  ## create imputing dataset and drop unnecessary columns
  calc_df <- reports[survey_id %in% imputers$survey_id & marital_row_only == 0 & demand_row_only == 0 & (currmar_only == 1 | (formermar_only == 0 & evermar_only == 0 & unmar_only == 0))]
  meta_cols <- c("survey_name","nid","ihme_loc_id","year_start","year_end","survey_id")
  metadata <- unique(calc_df[,(meta_cols),with=F])
  calc_df <- calc_df[,.(survey_id,age_group_id,age_group,age_start,age_end,currmar_only,sample_size,mod_contra,any_contra,demand_sample_size,met_demand)]
    
  ## ensure that age groups match between all-women and married women data before merging together
  calc_df_all <- calc_df[currmar_only == 0]
  calc_df_mar <- calc_df[currmar_only == 1,.(survey_id,age_group_id,age_group,age_start,age_end,ss_mar = sample_size,mod_contra_mar = mod_contra,any_contra_mar = any_contra,dss_mar = demand_sample_size,met_demand_mar = met_demand)]
  calc_df <- merge(calc_df_all,calc_df_mar,all=T,by=c("survey_id","age_group_id","age_group","age_start","age_end"))
    
  ## separate out those surveys where aggregation is required first
  aggs <- calc_df[survey_id %in% calc_df[is.na(any_contra) | is.na(any_contra_mar),unique(survey_id)]]
  calc_df <- calc_df[!survey_id %in% aggs$survey_id]
    
  ## exception for a survey where aggregate data point already exists (for other reasons such as specific method breakdown)
  calc_df <- rbind(calc_df,aggs[survey_id %in% c("SURVEY","SURVEY") & !is.na(any_contra) & !is.na(any_contra_mar)])	
  aggs <- aggs[!survey_id %in% c("SURVEY","SURVEY")]
    
  ## aggregate across ages where necessary
  aggs[,c("mod_contra","any_contra","mod_contra_mar","any_contra_mar") := .(weighted.mean(mod_contra,sample_size),weighted.mean(any_contra,sample_size),
                                                                            weighted.mean(mod_contra_mar,ss_mar),weighted.mean(any_contra_mar,ss_mar)),by=.(survey_id,currmar_only)]
  ## fill in all rows for each survey to match aggregations
  aggs[,c("sample_size","ss_mar","mod_contra","any_contra","mod_contra_mar","any_contra_mar") := .(sum(sample_size,na.rm=T),sum(ss_mar,na.rm=T),
                                                                                                    max(mod_contra,na.rm=T),max(any_contra,na.rm=T),
                                                                                                    max(mod_contra_mar,na.rm=T),max(any_contra_mar,na.rm=T)),by=survey_id]
    
  ## subset to appropriate aged row for the aggregation
  aggs[,N := .N,by=.(survey_id,currmar_only)]
  aggs <- aggs[N == 1,-c("N")]
  calc_df <- rbind(calc_df,aggs)
    
  ## now that ages match between all-women data and married women data, calculate out out unmarried values using sample sizes
  ## however, in some cases sample sizes are not reported and we must use modeLled estimates to perform proportional splits
    
  ## identify rows where calculating out unmarried data requires imputing some sample sizes
  needs_ss <- calc_df[is.na(sample_size) | is.na(ss_mar)]
  calc_df <- calc_df[!is.na(sample_size) & !is.na(ss_mar)]
    
  ## merge on survey and location info
  needs_ss <- merge(needs_ss,metadata[,.(survey_id,ihme_loc_id,year_id = floor((year_start+year_end)/2))],by="survey_id",all.x=T)
  needs_ss <- merge(needs_ss,locs[,.(location_id,ihme_loc_id)],by="ihme_loc_id")
    
  ## read in current best results for marital status model to inform proportional splits
  marital <- model_load(marital_status_final,'raked')[,.(location_id,year_id,age_group_id,prop_mar = gpr_mean)]
  needs_ss <- merge(needs_ss,marital,all.x=T,by=c("location_id","year_id","age_group_id"))
    
  ## sum modeled marital status estimates across ages using pop-weighting if there are aggregate-age rows to split
  if (needs_ss[is.na(age_group_id),.N] > 0) {
    pops <- get_population(location_id = needs_ss[is.na(age_group_id),unique(location_id)],year_id = needs_ss[is.na(age_group_id),unique(year_id)],
                            sex_id = 2,age_group_id = seq(8,14),release_id = release)[,-c("run_id","sex_id"),with=F]
    marital <- merge(marital,pops,by=c("location_id","year_id","age_group_id"))
    for (sid in needs_ss[is.na(age_group_id),unique(survey_id)]) {
      for (age_grp in needs_ss[survey_id == sid,unique(age_group)]) {
        locid <- needs_ss[survey_id == sid,unique(location_id)]
        yearid <- needs_ss[survey_id == sid,unique(year_id)]
        ages <- needs_ss[survey_id == sid & age_group == age_grp,seq((age_start/5)+5,((age_end + 1)/5)+4)]
        weighted_prop <- marital[location_id == locid & year_id == yearid & age_group_id %in% ages,weighted.mean(prop_mar,population)]
        needs_ss[survey_id == sid,prop_mar := weighted_prop]
      }
    }
    needs_ss[is.na(prop_mar),.N]
  }
    
  ## impute sample sizes
  needs_ss[is.na(sample_size),sample_size := ss_mar/prop_mar]
  needs_ss[is.na(ss_mar),ss_mar := sample_size*prop_mar]
  needs_ss[is.na(ss_mar) & is.na(sample_size),c("sample_size","ss_mar") := .(ss_impute,prop_mar*ss_impute)]
    
  ## drop extra cols
  needs_ss <- needs_ss[,c(names(needs_ss) %in% names(calc_df)),with=F]
  calc_df <- rbind(calc_df,needs_ss)
    
  ## calculate out unmarried data when sample sizes are present
  calc_df[,c("mod_contra_unmar","any_contra_unmar","met_demand_unmar") := .(((mod_contra*sample_size) - (mod_contra_mar*ss_mar))/(sample_size - ss_mar),
                                                                            ((any_contra*sample_size) - (any_contra_mar*ss_mar))/(sample_size - ss_mar),
                                                                            ((met_demand*demand_sample_size) - (met_demand_mar*dss_mar))/(demand_sample_size - dss_mar))]
  
  ## if weighted mean led to negatives, probably an issue with survey weights
  ## revisit extraction and make sure you are using weighted sample sizes, otherwise have to drop
  calc_df[,c("mod_contra_unmar","any_contra_unmar","met_demand_unmar") := .(ifelse(mod_contra_unmar > 0 & mod_contra_unmar < 1,mod_contra_unmar,NA),
                                                                            ifelse(any_contra_unmar > 0 & any_contra_unmar < 1,any_contra_unmar,NA),
                                                                            ifelse(met_demand_unmar > 0 & met_demand_unmar < 1,met_demand_unmar,NA))]
  
  ## calculate proportion modern 
  ## if some surpass 1, trust any_contra calculation and impute modern proportion using top 5th percentile
  ## observed in microdata
  calc_df[,mod_prop_unmar := mod_contra_unmar/any_contra_unmar]
  max_imp <- micro_mod_unmar[val != 1,quantile(val,.95)]
  calc_df[mod_prop_unmar > max_imp,mod_prop_unmar := max_imp]
    
  ## calculate proportion of non-users that have an unmet need
  calc_df[,unmet_prop_unmar := ((mod_contra_unmar/met_demand_unmar) - any_contra_unmar)/(1 - any_contra_unmar)]
    
  ## add calculated data back onto total reports dataset
  calc_df[,c("unmar_only","sample_size","demand_sample_size") := .(1,sample_size - ss_mar,demand_sample_size - dss_mar)]
  calc_df <- calc_df[,-c("mod_contra","any_contra","met_demand","mod_contra_mar","any_contra_mar","met_demand_mar","ss_mar",
                         "dss_mar","mod_contra_unmar","met_demand_unmar"),with=F]
  calc_df <- merge(metadata,calc_df,by="survey_id")
  calc_df <- merge(calc_df,locs[,.(location_id,ihme_loc_id)],by="ihme_loc_id",all.x=T)
  calc_df[,year_id := floor((year_start+year_end)/2)]
  calc_df[,is_outlier := 0]
  calc_df[,age_group := paste0(age_start,"-",age_end)]
  reports <- rbind(reports,calc_df,fill=T)
  
  ## remove unnecessary columns 
  reports <- reports %>% select(-one_of(methods))
}

## format important modeling variables
reports[, cv_report := 1]
reports[, sex_id := 2]

## fill in missingness in flagging variables
reports[, mar_restricted := 0]
reports[is.na(currmar_only), currmar_only := 0]
reports[is.na(evermar_only), evermar_only := 0]
reports[, contra_currmar_only := currmar_only]
reports[, contra_evermar_only := evermar_only]
reports[is.na(missing_desire), missing_desire := 0]
reports[is.na(missing_desire_later), missing_desire_later := 0]
reports[is.na(missing_fecund), missing_fecund := 0]
reports[is.na(no_preg), no_preg := 0]
reports[is.na(no_ppa), no_ppa := 0]
reports[is.na(cv_subgeo), cv_subgeo := 0]

## COMBINE MICRODATA AND REPORTS AND WRITE TO FILE -------------------------------------------

if (marital_only) {
  ## marital status
  marital <- reports[!is.na(prop_currmar)]
  marital[,c("val","sample_size") := .(prop_currmar,marital_sample_size)]
  marital <- marital[!nid %in% micro_marital[,unique(nid)]] ## remove report data if we have it from the microdata
  marital <- rbind(micro_marital,marital,fill=T)
  marital[,cv_any_contra_xwalk := 0]
  write.csv(marital,"FILEPATH",row.names=F)
  
} else {
  ## flag unmet need/met demand report data missing any_contra, thus unable to calculate unmet_prop
  unmet_need_reports <- reports[is.na(unmet_prop) & (!is.na(unmet_need_all)|!is.na(unmet_need_mod)|!is.na(met_demand_all)|!is.na(met_demand_mod))]
  write.csv(unmet_need_reports,"FILEPATH",row.names=F)
  
  ## any_contra_all (all women any_contra data)
  any_all <- reports[marital_group == "all" & !is.na(any_contra)]
  any_all[, val := any_contra]
  any_all <- any_all[!nid %in% micro_any_mar[, unique(nid)] | !nid %in% micro_any_unmar[, unique(nid)]] ## remove report data if we have it from the microdata
  # subset to most age-granular report data; duplicates exist in any_conta bc sometimes age-aggregate data is only available for mod_prop/unmet_prop
  any_all[, age_diff := age_end - age_start]
  any_all[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_start)]
  any_all[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_start)]
  any_all <- any_all[is.na(keep_row) | keep_row == age_diff]
  any_all[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_end)]
  any_all[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_end)]
  any_all <- any_all[is.na(keep_row) | keep_row == age_diff]
  write.csv(any_all,"FILEPATH",row.names=F)
  
  ## mod_prop_all (all women mod_prop data)
  mod_all <- reports[marital_group == "all" & !is.na(mod_prop)]
  mod_all[,c("val","sample_size") := .(mod_prop,round(any_contra*sample_size))]
  mod_all <- mod_all[!nid %in% micro_mod_mar[,unique(nid)] | !nid %in% micro_mod_unmar[,unique(nid)]] ## remove report data if we have it from the microdata
  # subset to most age-granular report data
  mod_all[, age_diff := age_end - age_start]
  mod_all[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_start)]
  mod_all[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_start)]
  mod_all <- mod_all[is.na(keep_row) | keep_row == age_diff]
  mod_all[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_end)]
  mod_all[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_end)]
  mod_all <- mod_all[is.na(keep_row) | keep_row == age_diff]
  write.csv(mod_all,"FILEPATH",row.names=F)
  
  ## unmet_prop_all (all women unmet_prop data)
  unmet_all <- reports[marital_group == "all" & !is.na(unmet_prop)]
  unmet_all[,c("val","sample_size") := .(unmet_prop,round((1-any_contra)*sample_size))]
  unmet_all <- unmet_all[!nid %in% micro_unmet_mar[,unique(nid)] | !nid %in% micro_unmet_unmar[,unique(nid)]] ## remove report data if we have it from the microdata
  # subset to most age-granular report data
  unmet_all[, age_diff := age_end - age_start]
  unmet_all[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_start)]
  unmet_all[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_start)]
  unmet_all <- unmet_all[is.na(keep_row) | keep_row == age_diff]
  unmet_all[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_end)]
  unmet_all[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_end)]
  unmet_all <- unmet_all[is.na(keep_row) | keep_row == age_diff]
  write.csv(unmet_all,"FILEPATH",row.names=F)
  
  print('Finished creating all women data files!')
  
  ## any_contra_mar
  any_mar <- reports[!is.na(any_contra_mar)]
  any_mar[,val := any_contra_mar]
  any_mar <- any_mar[!nid %in% micro_any_mar[,unique(nid)]] ## remove report data if we have it from the microdata
  # subset to most age-granular report data; duplicates exist in any_conta bc sometimes age-aggregate data is only available for mod_prop/unmet_prop
  any_mar[, age_diff := age_end - age_start]
  any_mar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_start)]
  any_mar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_start)]
  any_mar <- any_mar[is.na(keep_row) | keep_row == age_diff]
  any_mar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_end)]
  any_mar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_end)]
  any_mar <- any_mar[is.na(keep_row) | keep_row == age_diff]
  any_mar <- rbind(micro_any_mar,any_mar,fill=T)
  write.csv(any_mar,"FILEPATH",row.names=F)

  ## mod_prop_mar
  mod_mar <- reports[!is.na(mod_prop_mar)]
  mod_mar[,c("val","sample_size") := .(mod_prop_mar,round(any_contra_mar*sample_size))]
  mod_mar <- mod_mar[!nid %in% micro_mod_mar[,unique(nid)]] ## remove report data if we have it from the microdata
  # subset to most age-granular report data
  mod_mar[, age_diff := age_end - age_start]
  mod_mar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_start)]
  mod_mar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_start)]
  mod_mar <- mod_mar[is.na(keep_row) | keep_row == age_diff]
  mod_mar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_end)]
  mod_mar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_end)]
  mod_mar <- mod_mar[is.na(keep_row) | keep_row == age_diff]
  mod_mar <- rbind(micro_mod_mar,mod_mar,fill=T)
  write.csv(mod_mar,"FILEPATH",row.names=F)

  ## unmet_prop_mar
  unmet_mar <- reports[!is.na(unmet_prop_mar)]
  unmet_mar[,c("val","sample_size") := .(unmet_prop_mar,round((1-any_contra)*sample_size))]
  unmet_mar <- unmet_mar[!nid %in% micro_unmet_mar[,unique(nid)]] ## remove report data if we have it from the microdata
  # subset to most age-granular report data
  unmet_mar[, age_diff := age_end - age_start]
  unmet_mar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_start)]
  unmet_mar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_start)]
  unmet_mar <- unmet_mar[is.na(keep_row) | keep_row == age_diff]
  unmet_mar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_end)]
  unmet_mar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_end)]
  unmet_mar <- unmet_mar[is.na(keep_row) | keep_row == age_diff]
  unmet_mar <- rbind(micro_unmet_mar,unmet_mar,fill=T)
  write.csv(unmet_mar,"FILEPATH",row.names=F)

  print('Finished creating married data files!')
  
  ## any_contra_unmar
  any_unmar <- reports[!is.na(any_contra_unmar)]
  any_unmar[,val := any_contra_unmar]
  any_unmar <- any_unmar[!nid %in% micro_any_unmar[,unique(nid)]] ## remove report data if we have it from the microdata
  # subset to most age-granular report data; duplicates exist in any_conta bc sometimes age-aggregate data is only available for mod_prop/unmet_prop
  any_unmar[, age_diff := age_end - age_start]
  any_unmar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_start)]
  any_unmar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_start)]
  any_unmar <- any_unmar[is.na(keep_row) | keep_row == age_diff]
  any_unmar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_end)]
  any_unmar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_end)]
  any_unmar <- any_unmar[is.na(keep_row) | keep_row == age_diff]
  any_unmar <- rbind(micro_any_unmar,any_unmar,fill=T)
  write.csv(any_unmar,"FILEPATH",row.names=F)
  
  ## mod_prop_unmar
  mod_unmar <- reports[!is.na(mod_prop_unmar)]
  mod_unmar[,c("val","sample_size") := .(mod_prop_unmar,round(any_contra_unmar*sample_size))]
  mod_unmar <- mod_unmar[!nid %in% micro_mod_unmar[,unique(nid)]] ## remove report data if we have it from the microdata
  # subset to most age-granular report data
  mod_unmar[, age_diff := age_end - age_start]
  mod_unmar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_start)]
  mod_unmar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_start)]
  mod_unmar <- mod_unmar[is.na(keep_row) | keep_row == age_diff]
  mod_unmar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_end)]
  mod_unmar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_end)]
  mod_unmar <- mod_unmar[is.na(keep_row) | keep_row == age_diff]
  mod_unmar <- rbind(micro_mod_unmar,mod_unmar,fill=T)
  write.csv(mod_unmar,"FILEPATH",row.names=F)
  
  ## unmet_prop_unmar
  unmet_unmar <- reports[!is.na(unmet_prop_unmar)]
  unmet_unmar[,c("val","sample_size") := .(unmet_prop_unmar,round((1-any_contra)*sample_size))]
  unmet_unmar <- unmet_unmar[!nid %in% micro_unmet_unmar[,unique(nid)]] ## remove report data if we have it from the microdata
  # subset to most age-granular report data
  unmet_unmar[, age_diff := age_end - age_start]
  unmet_unmar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_start)]
  unmet_unmar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_start)]
  unmet_unmar <- unmet_unmar[is.na(keep_row) | keep_row == age_diff]
  unmet_unmar[, duplicate := .N, by = .(nid, ihme_loc_id, year_id, age_end)]
  unmet_unmar[duplicate > 1, keep_row := min(age_diff), by = .(nid, ihme_loc_id, year_id, age_end)]
  unmet_unmar <- unmet_unmar[is.na(keep_row) | keep_row == age_diff]
  unmet_unmar <- rbind(micro_unmet_unmar,unmet_unmar,fill=T)
  write.csv(unmet_unmar,"FILEPATH",row.names=F)
  
  print('Finished creating unmarried data files!')
}
