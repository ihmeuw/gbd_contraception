############################################################################################################
## Purpose: Launches extraction and indicator construction code for contraception ubcov (non-counterfactual) outputs
###########################################################################################################

## CLUSTER ARRAY JOB SETTINGS ------------------------------------------------

library(data.table)

# Getting normal QSub arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]

# Retrieving array task_id
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
param_map <- fread(param_map_filepath, header = T)

# variable from the param_map that would have been in your loop
# only edit this line!!!
survey <- param_map[task_id, survey]
input_dir <- param_map[task_id, input_dir]
vars <- param_map[task_id, variables]


## SET-UP --------------------------------------------------------------------

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
}

# settings
pacman::p_load(magrittr,ggplot2,haven,stringr,parallel,plyr,dplyr)
subnat_list <- c("USA","MEX","BRA","GBR","NOR","POL","ITA","RUS","CHN","JPN","PHL","IDN","IND","PAK","IRN","ETH","KEN","NGA","ZAF")
# ^ countries where we have a decently large number of subnats to collapse (requires extra memory)
subnat_list <- paste0(subnat_list, collapse = "|")

# in/out
out.dir <- "FILEPATH"

# shared functions
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# create function for the opposite of %in%
'%ni%' <- Negate('%in%')

# create function to return unique characters in a string
uniqchars <- function(x) unique(strsplit(x, "")[[1]])

# create function to convert CMC date to year and month 
get_cmc_year <- function(x) 1900 + as.integer((as.integer(x) - 1) / 12)
get_cmc_month <- function(x) as.integer(x) - 12 * (get_cmc_year(x) - 1900)

# create function to count number of digits in a number
nDigits <- function(x) nchar( trunc( abs(x) ) )

# demographics
all_age_map <- fread("FILEPATH")
age_ids <- get_ids(table = "age_group")
age_ids <- age_ids[age_group_id %in% seq(8,14)]
age_ids[,age_group := gsub(" to ", "-", age_group_name)]
age_ids[,age_group_name := NULL]
age_ids <- all_age_map[age_group %in% age_ids$age_group]

# set values for gateways corresponding to counterfactual extractions (turn them all off for normal extraction)
counterfac_missing_fecund <- 0
counterfac_missing_desire <- 0
counterfac_missing_desire_timing <- 0
counterfac_missing_desire_later <- 0
counterfac_no_preg <- 0
counterfac_no_ppa <- 0


## PREP EXTRACTION ----------------------------------------------------------

print(survey)

# read in survey
df <- as.data.table(read_dta(file.path(input_dir, survey)))

if (nrow(df) == 0) next()

# those surveys that are gold standard will be used for crosswalking, and thus must be collapsed on every combination 
# of consecutive ages (not just the most-detailed ones); non-gold data can just be collapsed on most-detailed ages
if (all(c("missing_fecund","missing_desire","missing_desire_timing","missing_desire_later","no_preg", "no_ppa") %ni% names(df)) 
    & "current_contra" %in% names(df)) {
  age_map <- copy(all_age_map)
} else {
  age_map <- copy(age_ids)
}

# by default, study-level covariates are false unless otherwise specified
if ("mar_restricted" %ni% names(df)) df[, mar_restricted := 0]
if ("contra_currmar_only" %ni% names(df)) df[, contra_currmar_only := 0]
if ("contra_evermar_only" %ni% names(df)) df[, contra_evermar_only := 0]
if ("currmar_only" %ni% names(df)) df[, currmar_only := 0]
if ("evermar_only" %ni% names(df)) df[, evermar_only := 0]
if ("missing_fecund" %ni% names(df)) df[, missing_fecund := 0]
if ("missing_desire" %ni% names(df)) df[, missing_desire := 0]
if ("missing_desire_timing" %ni% names(df)) df[, missing_desire_timing := 0]
if ("missing_desire_later" %ni% names(df)) df[, missing_desire_later := 0]
if ("no_preg" %ni% names(df)) df[, no_preg := 0]
if ("no_ppa" %ni% names(df)) df[, no_ppa := 0]
if ("cv_subgeo" %ni% names(df)) df[, cv_subgeo := 0]

# source script containing survey fixes before starting the need for family planning algorithm
print("Implementing survey fixes...")
source("FILEPATH", local = T)

# convert string variables with accents into non-accented characters
# this is not foolproof!!! pay special attention to surveys in foreign languages
string_cols <- c("current_contra", "reason_no_contra", "contra_first_source", "contra_last_source", "knowledge_contra",
                 "intent_contra", "intent_reason_no_contra", "marital_status", "calendar_contra_use", "calendar_contra_reason_discont")

for (col in string_cols[string_cols %in% names(df)]) {
  df[, (col) := gsub(pattern = "\xe0|\xe1|\xe3", replacement = "a", x = get(col))]
  df[, (col) := gsub(pattern = "\xe7", replacement = "c", x = get(col))]
  df[, (col) := gsub(pattern = "\x82|\xe9", replacement = "e", x = get(col))]
  df[, (col) := gsub(pattern = "\xf3|\xf4", replacement = "o", x = get(col))]
  df[, (col) := gsub(pattern = "\xed", replacement = "i", x = get(col))]
  df[, (col) := str_replace_all(iconv(get(col), to = "ASCII//TRANSLIT"), "'", "")]
}

# remove data in age groups that span 2 or less years of ages 
# temporarily merge on GBD age groups to do so
df[, age_year := floor(age_year)] # round down when age was calculated as a decimal (if you're 34.6, you would say you're 34)
df <- merge(df, age_ids, by = "age_year", allow.cartesian = T)
df <- df[, .SD[max(age_year) - min(age_year) > 1], by = "age_group"]
df[, age_group := NULL]

# merge on appropriate age groups for the collapse code (but restrict if survey itself does not cover full age range)
df[, age_year := floor(age_year)]
age_map <- age_map[(as.numeric(substr(age_group, 1, 2)) + 5) > min(df$age_year) & (as.numeric(substr(age_group, 4, 5)) - 5) < max(df$age_year)]
df <- merge(df, age_map, by = "age_year", allow.cartesian = T)


## RUN INDICATOR CONSTRUCTION CODE ------------------------------------------

# run extraction calculation script if survey reports on contraception 
# do not want to run for surveys which only report on marital status
print("Running prep_ubcov_data_calc.R...")
if ("current_contra" %in% names(df)) source(file.path(h, "FILEPATH/01ab_prep_ubcov_data_calc.R"), local = T)


## MOVE FILES ----------------------------------------------------------------

# copy and remove raw file out of current folder and into folder for counterfactual re-extraction
if ("current_contra" %in% names(df)) {
  file.copy(
    from = file.path(input_dir, survey), to = file.path("FILEPATH", survey), overwrite = T)
} else {
  # surveys which only report on marital status should not go through 02_prep_counterfac.R, put in other folder
  file.copy(from = file.path(input_dir, survey), to = file.path("FILEPATH", survey), overwrite = T)
}
file.remove(file.path(input_dir, survey))
