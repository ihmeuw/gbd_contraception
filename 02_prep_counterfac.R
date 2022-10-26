############################################################################################################
## Purpose: Compile gold standard contraception surveys in one folder. Then, for every combination
##          of possible missing variables perform a counterfactual re-extraction of the gold-standard
##          surveys to inform our crosswalk of non-gold-standard surveys
###########################################################################################################


## SET-UP --------------------------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# load libraries
pacman::p_load(data.table,magrittr,ggplot2,haven,stringr,parallel,purrr)

# in/out
input_dir <- "FILEPATH"
output_dir <- "FILEPATH"
final_dir_root <- "FILEPATH"

# settings
calc_script <- file.path(h,"FILEPATH/01ab_prep_ubcov_data_calc.R")
file_prep <- T ## do files need to be identified as gold standard and saved to appropriate folder first?
Sys.umask(mode = "0002")
cores.provided <- 15
subnat_list <- c("USA","MEX","BRA","GBR","NOR","POL","ITA","RUS","CHN","JPN","PHL","IDN","IND","PAK","IRN","ETH","KEN","NGA","ZAF","IPUMS_CENSUS")
## ^countries where we have a decently large number of subnats to collapse or IPUMS data (requires extra memory)
subnat_list <- paste0(subnat_list,collapse="|")

# create the opposite of %in%
'%ni%' <- Negate('%in%')

# load custom age mapping table containing every possible range of age groups to expand gold standard surveys on
all_age_map <- fread("FILEPATH")


## IDENTIFY GOLD STANDARD DATA -----------------------------------------------

if (file_prep) {
  
  # obtain list of all new microdata files
  ubcov_all <- list.files(input_dir)

  # compile folder of all gold standard surveys to prepare for counterfactual re-extractions
  compile_gold_data <- function(survey) {
    
    # read in microdata survey
    df <- read_dta(file.path(input_dir, survey)) %>% data.table
    
    # check whether survey is gold standard (not marked as missing any necessary variables for prop_unmet)
    if (all(c("missing_fecund","missing_desire","missing_desire_timing","missing_desire_later","no_preg", "no_ppa") %ni% names(df)) & "current_contra" %in% names(df)) {
      
      print(paste0("GOLD STANDARD : ", survey))

      # convert string variables with accents into non-accented characters
      df[,current_contra := gsub("\x82|\xe9", "e", current_contra)]
      df[,current_contra := gsub("\U3e33663c", "o", current_contra)]
      df[,current_contra := gsub("\xf3|\xf4", "o", current_contra)]
      df[,current_contra := gsub("\U3e64653c", "i", current_contra)]
      df[,current_contra := gsub("\xed", "i", current_contra)]
      df[,current_contra := str_replace_all(iconv(current_contra, from="latin1", to="ASCII//TRANSLIT"), "'", "")]
      if ("reason_no_contra" %in% names(df)) {
        df[,reason_no_contra := gsub("\x82|\xe9", "e", reason_no_contra)]
        df[,reason_no_contra := gsub("\U3e33663c", "o", reason_no_contra)]
        df[,reason_no_contra := gsub("\xf3|\xf4", "o", reason_no_contra)]
        df[,reason_no_contra := gsub("\U3e64653c", "i", reason_no_contra)]
        df[,reason_no_contra := gsub("\xed", "i", reason_no_contra)]
        df[,reason_no_contra := str_replace_all(iconv(reason_no_contra, from="latin1", to="ASCII//TRANSLIT"), "'", "")]
      }
      
      # save gold standard files to their own folder
      write.csv(df, file.path(output_dir, str_replace(survey, ".dta", ".csv")), row.names = F)
    } 
    
    # move all microdata out of the new folder, overwriting older versions where applicable
    file.copy(from = file.path(input_dir, survey),
              to = file.path(gsub("FILEPATH", "", input_dir), survey), overwrite = T)
    file.remove(file.path(input_dir,survey))
  }

  # run the consolidation of gold standard surveys in parallel
  mclapply(ubcov_all, compile_gold_data, mc.cores = cores.provided)
}


## PREFORM COUNTERFACTUAL EXTRACTIONS ----------------------------------------

# obtain list of all gold standard surveys
gold <- list.files(output_dir)

# list all possible combinations of missing variables; will do a separate counterfactual extraction for each
counterfacs <- c("desire","desire_later","desire_timing","fecund","no_preg","no_ppa")
counterfactuals <- c(counterfacs, lapply(seq_along(counterfacs)[-1L], function(y) combn(counterfacs, y, paste0, collapse = "_")), recursive = TRUE) %>% keep(~ str_count(.x, 'desire') <= 1)

# loop through each counterfactual and preform re-extractions
for (counterfactual in counterfactuals) {
  print(paste0("COUNTERFACTUAL EXTRACTION FOR ", toupper(counterfactual)))

  # set up variables corresponding to which counterfactual extraction this is (correspond to gateways in contraception_ubcov_calc.R)
  counterfac_missing_fecund <- ifelse(grepl("fecund", counterfactual), 1, 0)
  counterfac_missing_desire <- ifelse(grepl("desire", counterfactual) & !grepl("_later|_timing", counterfactual), 1, 0)
  counterfac_missing_desire_timing <- ifelse(grepl("desire_timing", counterfactual), 1, 0)
  counterfac_missing_desire_later <- ifelse(grepl("desire_later", counterfactual), 1, 0)
  counterfac_no_preg <- ifelse(grepl("preg", counterfactual), 1, 0)
  counterfac_no_ppa <- ifelse(grepl("ppa", counterfactual), 1, 0)
  
  # create the folder for saving the output from this counterfactual extraction
  final_output_dir <- file.path(final_dir_root, counterfactual, "FILEPATH")
  dir.create(file.path(final_output_dir,"FILEPATH"), recursive = T, showWarnings = F)
  dir.create(file.path(final_output_dir,"FILEPATH"), recursive = T, showWarnings = F)

  # re-extract each gold-standard survey according to this counterfactual scenario
  reextract_survey <- function(survey) {
    print(survey)
    
    # read in survey
    df <- fread(file.path(output_dir,survey))
    
    # surveys completely missing desire, fecundity (and consequently info on postpartum amenorrheic women), as well as info 
    # on pregnant women essentially provide no information on need, except for unmarried women and their sexual activity
    # thus it is not necessary to run counterfactual extraction for married-only surveys, exclude
    if (counterfactual == 'desire_fecund_no_preg_no_ppa' & "currmar_only" %in% names(df)) return()

    # source script containing survey fixes before starting the need for family planning algorithm
    source("FILEPATH", local = T)
    
    # run topic-specific code
    source(calc_script, local = T)

    # merge on all potential age groups for the collapse code
    df[,age_year := floor(age_year)]
    age_map <- all_age_map[(as.numeric(substr(age_group,1,2)) + 5) > min(df$age_year) & (as.numeric(substr(age_group,4,5)) - 5) < max(df$age_year)]
    df <- merge(df, age_map, by = "age_year", allow.cartesian = T)

    # write output file
    folder <- ifelse(grepl(subnat_list, unique(df$ihme_loc_id)), "FILEPATH", "FILEPATH")
    write.csv(df, file.path(final_output_dir, folder, str_replace(survey, ".dta", ".csv")), row.names = F)
  }

  # preform counterfactual extractions for each counterfactual topic 
  mclapply(gold, reextract_survey, mc.cores = cores.provided) %>% invisible
}

# after final counterfactual extraction remove gold standard survey from FILEPATH
for (survey in gold) {
  file.copy(from = file.path(output_dir, survey),
            to = file.path(gsub("FILEPATH", "", output_dir), survey), overwrite = T)
  file.remove(file.path(output_dir, survey))
}
