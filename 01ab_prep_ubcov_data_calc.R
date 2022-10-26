####################################################################################################
## Purpose: Topic-specific code for contraception UbCov extractions
##          Main goals are to:
##          1. Determine which women were asked about contraceptive usage, and how many of them are using a modern method
##          2. Determine which women who are not using a method fit our definition of having a need for contraception
##          3. Classify women as having a met demand for contraception (or an unmet demand)
##          4. Allow re-extractions of surveys under certain counterfactual scenarios (ex. if missing all info on fecundity)
####################################################################################################

# for reference: %ni% is opposite of %in%

####################################################################################################
###### CALCULATE SEXUAL ACTIVITY AND TIMING OF DESIRE FOR CHILDREN #################################
####################################################################################################

## For some surveys, time since last ___ was coded with two variables, a number and its units (ex. days, weeks, months), so have
## to calculate who falls into which category manually

###### LAST SEXUAL ACTIVITY #######################################################
if (all(c("last_sex","last_sex_unit") %in% names(df))) {

  ## SURVEY NAMES
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & ((last_sex >= 100 & last_sex <= 131) | (last_sex >= 200 & last_sex <= 204) | last_sex == 300), sex_in_last_month := 1]

  ## SURVEY NAME
  df[nid == "NID" & (last_sex <= 30 | last_sex_unit <= 4), sex_in_last_month := 1]
  
  ## SURVEY NAME
  df[nid == "NID" & ((last_sex_unit == 3 & last_sex <= 30) | (last_sex_unit == 4 & last_sex <= 4) | (last_sex_unit == 5 & last_sex < 1)), sex_in_last_month := 1]
  
  ## SURVEY NAME
  df[nid == "NID" & last_sex <= 4, sex_in_last_month := 1]
  
  ## SURVEY NAMES
  df[(nid %in% c("NID","NID") | grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path))) &
       ((last_sex_unit == 1 & last_sex <= 30) | (last_sex_unit == 2 & last_sex <= 4) | (last_sex_unit == 3 & last_sex < 1)), sex_in_last_month := 1]
}

##### TIMING FOR DESIRE OF FUTURE CHILDREN #########################################
## Women who do not want any more children or are unsure OR want children in 2+ years or are undecided on timing have need for contraception and should be marked as 1 for desire_later

# identify women who do not want any (more) children
# answer "no more" when asked about desire for future birth, or answer "0" when asked about number of future children
if ("desire_gate" %in% names(df)) {
  
  # catch desire_gate for pregnant women when asked separately (necessary for counterfactual re-extractions)
  if ("desire_gate_preg" %in% names(df)) df[is.na(desire_gate), desire_gate := desire_gate_preg]
  
  # create desire_limiting variable to capture women who do not want additional children (irrelevant of timing)
  df[, desire_limiting := as.numeric(NA)]
  
  # non-standardized surveys
  df[nid %in% c("NID","NID","NID") & desire_gate == 0, desire_limiting := 1]
  
  df[nid %in% c("NID","NID","NID") & desire_gate == 1, desire_limiting := 1]
  
  df[nid %in% c("NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID",
                "NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID",
                "NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID",
                "NID","NID","NID","NID","NID","NID","NID") & desire_gate == 2, desire_limiting := 1]
  
  df[nid %in% c("NID","NID","NID","NID","NID","NID","NID","NID","NID") & desire_gate == 3, desire_limiting := 1]
  
  df[nid %in% c("NID","NID","NID","NID","NID") & desire_gate == 5, desire_limiting := 1]
  
  df[nid %in% c("NID","NID","NID","NID") & desire_gate %in% c(2,4), desire_limiting := 1]
  
  # survey series
  if (all(is.na(df$desire_limiting))) {
    
    # SURVEY NAMES
    df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & desire_gate == 2, desire_limiting := 1]
    
    # SURVEY NAME
    df[grepl('SURVEY_NAME|SURVEY_NAME',tolower(file_path)) & desire_gate == 3, desire_limiting := 1]
  }
  
  # check all surveys with desire_gate variable were processed 
  if (all(is.na(df$desire_limiting))) stop(paste0(survey, ": desire_gate not processed! Please add to custom code in prep_ubcov_data_calc.R"))
  
  # update desire_later
  df[desire_limiting == 1, desire_later := 1]
}

# identify women who do want (more) children, but want to wait 2+ years or are unsure of timing/if they want (more) children
# after marriage = desire for spacing
# at god's will = no desire for spacing 
if (all(c("desire_gate","desire_timing","desire_unit") %in% names(df)) ) {

  # create desire_spacing variable
  df[, desire_spacing := as.numeric(NA)]
  
  # unsure if they want (additional) children
  df[nid %in% c("NID","NID") & desire_gate == 5, desire_spacing := 1]
  df[nid %in% c("NID","NID","NID","NID","NID","NID","NID","NID") & desire_gate == 7, desire_spacing := 1] 
  df[nid %in% c("NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID",
                "NID","NID","NID","NID") & desire_gate == 8, desire_spacing := 1]
  df[nid %in% c("NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID","NID",
                "NID","NID","NID","NID","NID","NID","NID","NID","NID","NID") & desire_gate == 9, desire_spacing := 1]
  df[nid %in% c("NID") & desire_gate == 98, desire_spacing := 1]
  df[nid %in% c("NID","NID") & desire_gate %in% c(3,4), desire_spacing := 1]
  df[nid %in% c("NID","NID","NID","NID","NID","NID","NID","NID") & desire_gate %in% c(4,5), desire_spacing := 1]
  df[nid %in% c("NID","NID") & desire_gate %in% c(5,6), desire_spacing := 1]
  df[nid %in% c("NID") & desire_gate %in% c(4,5,88), desire_spacing := 1]
  
  # want to wait 2+ years
  
  # SURVEY NAME
  df[nid == "NID" & (desire_gate %in% c(4,5) | (desire_unit == 2 & desire_timing >= 2) | desire_unit %in% c(995,996,998)), desire_spacing := 1]
  
  # SURVEY NAME
  df[nid == "NID" & desire_timing %in% c(3,5,98), desire_spacing := 1]
  
  # SURVEY NAMES
  df[nid %in% c("NID","NID") & desire_unit >= 2 & desire_unit < 99, desire_spacing := 1]
  df[nid %in% c("NID") & (desire_gate == 4 | (desire_unit >= 2 & desire_unit < 99)), desire_spacing := 1]
  
  # SURVEY NAME
  df[nid == "NID" & desire_gate %ni% c(0, 99) & desire_timing - 84 >= 2, desire_spacing := 1]
  
  # SURVEY NAME
  df[nid %in% c("NID") & desire_gate == 4, desire_spacing := 1]
  
  # SURVEY NAMES
  df[nid %in% c("NID","NID") & ((desire_timing >= 202 & desire_timing < 333) | desire_timing %in% c(444,999)), desire_spacing := 1]

  # SURVEY NAMES
  df[nid %in% c("NID","NID") & ((desire_timing >= 202 & desire_timing < 994) | desire_timing %in% c(995,998)), desire_spacing := 1]
  df[nid %in% c("NID") & ((desire_timing >= 202 & desire_timing < 994) | desire_timing %in% c(995,998)), desire_spacing := 1]

  # SURVEY NAME
  df[nid %in% c("NID") & ((desire_timing >= 202 & desire_timing < 990) | desire_timing %in% c(994,999)), desire_spacing := 1]

  # SURVEY NAMES
  df[nid %in% c("NID","NID") & ((desire_timing >= 202 & desire_timing < 888) | desire_timing %in% c(995,998)), desire_spacing := 1]

  # SURVEY NAMES
  df[nid %in% c("NID","NID") & ((desire_timing >= 202 & desire_timing < 994) | desire_timing %in% c(995,998)), desire_spacing := 1]
  df[nid %in% c("NID") & ((desire_timing >= 202 & desire_timing < 994) | desire_timing %in% c(995,999)), desire_spacing := 1]

  # SURVEY NAMES
  df[nid %in% c("NID","NID","NID","NID","NID") & desire_timing %in% c(4,5,6,8), desire_spacing := 1]

  # SURVEY NAME
  df[nid == "NID" & ((desire_timing >= 202 & desire_timing < 993) | desire_timing %in% c(994,995,998)), desire_spacing := 1]

  # SURVEY NAME
  df[nid == "NID" & (desire_timing %in% c(4,5,6,7) | desire_gate == 7), desire_spacing := 1]

  # SURVEY NAME
  df[nid == "NID" & ((desire_timing >= 202 & desire_timing < 555) | desire_timing %in% c(666,999)), desire_spacing := 1]

  # SURVEY NAME
  df[nid == "NID" & (desire_timing == 130 | desire_timing >= 202), desire_spacing := 1]

  # SURVEY NAME
  df[nid == "NID" & (desire_timing %in% c(4,5,6,9)), desire_spacing := 1]
  
  # SURVEY NAMES
  df[nid %in% c("NID","NID","NID") & (desire_gate == 4 | (desire_unit == 1 & desire_timing >= 2) | (desire_timing %in% c(95,96,98))), desire_spacing := 1]

  # SURVEY NAME
  df[nid == "NID" & ((desire_timing >= 2 & desire_timing != 99) | desire_unit == 888), desire_spacing := 1]

  # SURVEY NAMES
  df[nid %in% c("NID") & ((desire_timing >= 24 & desire_timing < 96) | desire_timing %in% c(97, 98, 99) | desire_gate == 3), desire_spacing := 1]
  df[nid %in% c("NID") & ((desire_timing >= 24 & desire_timing < 99) | desire_gate == 3), desire_spacing := 1]
  df[nid %in% c("NID") & ((desire_timing >= 202 & desire_timing < 993) | desire_timing == 998), desire_spacing := 1]
  df[nid %in% c("NID") & (desire_gate == 4 | (desire_timing >= 202 & desire_timing < 993) | desire_timing == 998), desire_spacing := 1]
  
  # SURVEY NAME
  df[nid == "NID" & (desire_unit == 88 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_spacing := 1]

  # SURVEY NAME
  df[nid == "NID" & (desire_timing %in% c(95,98) | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_spacing := 1]

  # SURVEY NAME
  df[grepl("SURVEY_NAME",tolower(file_path)) & (desire_timing >= 202 & desire_timing != 993), desire_spacing := 1]

  # SURVEY NAMES
  df[nid %in% c("NID","NID","NID","NID") & (desire_timing >= 2), desire_spacing := 1]
  
  # SURVEY NAME 
  df[nid %in% c("NID") & (desire_timing == 95 | desire_timing == 98 | desire_unit >= 2), desire_spacing := 1]

  # SURVEY NAMES
  df[grepl("SURVEY_NAME",tolower(file_path)) & (desire_timing %% 2 == 0) & desire_timing >= 22 & desire_timing < 190, desire_spacing := 1]
  df[grepl("SURVEY_NAME",tolower(file_path)) & (desire_timing == 998 | desire_unit == 998 | (desire_unit >= 2 & desire_unit < 900)), desire_spacing := 1]

  ## SURVEY NAMES
  df[nid %in% c("NID","NID","NID","NID","NID","NID","NID","NID") & (desire_timing >= 2 | desire_unit >= 2), desire_spacing := 1]
  
  # SURVEY NAMES
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & (desire_timing >= 96 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_spacing := 1]
  df[grepl("SURVEY_NAME",tolower(file_path)) & (desire_timing >= 98 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_spacing := 1]
  df[grepl("SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & (desire_timing >= 98 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_spacing := 1]
  df[grepl("SURVEY_NAME",tolower(file_path)) & (desire_timing >= 98 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_spacing := 1]
  df[grepl("SURVEY_NAME",tolower(file_path)) & (desire_timing >= 95 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_spacing := 1]
  df[grepl("SURVEY_NAME",tolower(file_path)) & (desire_timing ==2 | desire_timing ==3), desire_spacing := 1]
  df[grepl("SURVEY_NAME",tolower(file_path)) & (desire_timing >= 24 | desire_unit >= 2), desire_spacing := 1]
  
  # SURVEY NAMES
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & (desire_timing >= 96 | (desire_unit == 2 & desire_timing >= 2)), desire_spacing := 1]
  df[grepl("SURVEY_NAME",tolower(file_path)) & desire_timing >= 2, desire_spacing := 1]
  
  # SURVEY NAME
  df[nid == "NID" & ((desire_timing >= 124 & desire_timing < 200) | (desire_timing >= 202 & desire_timing < 996) | desire_timing == 998), desire_spacing := 1] 
  df[nid == "NID" & (desire_timing >= 95 | (desire_unit == 2 & desire_timing >= 24) | (desire_unit == 1 & desire_timing >= 2)), desire_spacing := 1 ]  
  
  # survey series
  if (all(is.na(df$desire_spacing))) {
    
    # SURVEY NAME
    df[grepl("SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & (desire_gate == 2 | (desire_timing >= 202 & desire_timing <= 299) | desire_timing == 993 | desire_timing == 996 | desire_timing == 998), desire_spacing := 1]
    
    # SURVEY NAME
    df[grepl("SURVEY_NAME",tolower(file_path)) & (desire_gate == -88 | desire_unit == 5 | desire_unit == -88 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_spacing := 1]
    
    # SURVEY NAME
    df[grepl("SURVEY_NAME",tolower(file_path)) & desire_timing >= 99, desire_timing := NA]
    df[grepl("SURVEY_NAME",tolower(file_path)) & (desire_gate == 8 | desire_timing >= 95 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_spacing := 1]
  }
  
  # check all surveys with desire_timing and desire_gate variables were processed 
  if (all(is.na(df$desire_spacing))) stop(paste0(survey, ": desire_timing and desire_unit not processed! Please add to custom code in prep_ubcov_data_calc.R"))
  
  # update desire_later
  df[desire_spacing == 1, desire_later := 1]
}

# flag surveys with desire_later that were not also split into desire_spacing and desire_limiting
if ("desire_later" %in% names(df) & "desire_limiting" %ni% names(df)) message("ATTENTION: Have desire_later, but missing desire_limitng for ", survey)
if ("desire_later" %in% names(df) & "desire_spacing" %ni% names(df)) message("ATTENTION: Have desire_later, but missing desire_spacing for ", survey)


####################################################################################################
###### LAST MENSTRUATION (CONTINUOUS IN MONTHS) ####################################################
####################################################################################################

## generate continuous estimate (in months) of time since last menstruation
## set months_since_last_menses to 999 for "before last birth" responses to identify amenorrheic women 

if (all(c("last_menses","last_menses_unit") %in% names(df))) {
  
  ## SURVEY NAME
  df[nid %in% c("NID") & last_menses_unit == 0, months_since_last_menses := 0]
  df[nid %in% c("NID") & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[nid %in% c("NID") & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[nid %in% c("NID") & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[nid %in% c("NID") & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[nid %in% c("NID") & last_menses == 95, months_since_last_menses := 999]
  
  ## SURVEY NAME
  df[nid %in% c("NID"), months_since_last_menses := last_menses]
  df[nid %in% c("NID") & last_menses_unit == 96, months_since_last_menses := 999]
  
  ## SURVEY NAMES
  df[nid %in% c("NID","NID") & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[nid %in% c("NID","NID") & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[nid %in% c("NID","NID") & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[nid %in% c("NID","NID") & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[nid == "NID" & last_menses == 95, months_since_last_menses := 999]
  df[nid == "NID" & last_menses == 995, months_since_last_menses := 999]

  ## SURVEY NAME
  if (all(c("last_birth_year","last_birth_month","interview_year","interview_month") %in% names(df))){
    df[nid == "NID", months_since_last_menses := interview_month - last_birth_month + ((interview_year - last_birth_year)*12)]
    if (unique(df$nid) == "NID") df[,c("last_birth_year","last_birth_month") := NULL]
  }

  ## SURVEY NAME
  df[nid == "NID" & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[nid == "NID" & last_menses_unit == 2, months_since_last_menses := extravar_cont_1/4.3]
  df[nid == "NID" & last_menses_unit == 3, months_since_last_menses := extravar_cont_2]
  df[nid == "NID" & last_menses_unit == 4, months_since_last_menses := extravar_cont_3*12]
  
  ## SURVEY NAME
  if (unique(df$nid) == "NID") {
    df[last_menses < 88, months_since_last_menses := last_menses / 30]  # days
    df[last_menses == 0, months_since_last_menses := 0]
    df[, months_since_last_menses := last_menses_unit / 4.3]            # weeks
    df[, months_since_last_menses := extravar_catmult_1]                # months
    df[, months_since_last_menses := extravar_catmult_2 * 12]           # years
  }
  df[nid == "NID" & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[nid == "NID" & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[nid == "NID" & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[nid == "NID" & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  
  ## SURVEY NAME
  df[grepl("SURVEY_NAME",tolower(file_path)) & nid != "NID" & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[grepl("SURVEY_NAME",tolower(file_path)) & nid != "NID" & last_menses_unit == 1 & last_menses > 99, months_since_last_menses := 1/30]
  df[grepl("SURVEY_NAME",tolower(file_path)) & nid != "NID" & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[grepl("SURVEY_NAME",tolower(file_path)) & nid != "NID" & last_menses_unit == 2 & last_menses > 99, months_since_last_menses := 1/4.3]
  df[grepl("SURVEY_NAME",tolower(file_path)) & nid != "NID" & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[grepl("SURVEY_NAME",tolower(file_path)) & nid != "NID" & last_menses_unit == 3 & last_menses > 99, months_since_last_menses := 1]
  df[grepl("SURVEY_NAME",tolower(file_path)) & nid != "NID" & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[grepl("SURVEY_NAME",tolower(file_path)) & nid != "NID" & last_menses_unit == 4 & last_menses > 99, months_since_last_menses := 1*12]
  df[grepl("SURVEY_NAME",tolower(file_path)) & nid != "NID" & last_menses_unit == 6, months_since_last_menses := 999]

  ## SURVEY NAME
  df[nid == "NID" & last_menses_unit == 2, months_since_last_menses := last_menses/30]
  df[nid == "NID" & last_menses_unit == 2 & last_menses > 99, months_since_last_menses := 1/30]
  df[nid == "NID" & last_menses_unit == 3, months_since_last_menses := last_menses/4.3]
  df[nid == "NID" & last_menses_unit == 3 & last_menses > 99, months_since_last_menses := 1/4.3]
  df[nid == "NID" & last_menses_unit == 4, months_since_last_menses := last_menses]
  df[nid == "NID" & last_menses_unit == 4 & last_menses > 99, months_since_last_menses := 1]
  df[nid == "NID" & last_menses_unit == 5, months_since_last_menses := last_menses*12]
  df[nid == "NID" & last_menses_unit == 5 & last_menses > 99, months_since_last_menses := 1*12]
  df[nid == "NID" & last_menses_unit == 7, months_since_last_menses := 999]
  
  ## SURVEY NAME
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 1 & last_menses == 99, months_since_last_menses := 1/30]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 2 & last_menses == 99, months_since_last_menses := 1/4.3]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 3 & last_menses == 99, months_since_last_menses := 1]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 4 & last_menses == 99, months_since_last_menses := 1*12]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 9 & last_menses == 95, months_since_last_menses := 999]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 9 & last_menses == 94, months_since_last_menses := 999]

  ## SURVEY NAMES
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 9 & last_menses == 95, months_since_last_menses := 999]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 9 & last_menses == 94, months_since_last_menses := 999]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses == 95, months_since_last_menses := 999]
  
  ## SURVEY NAME
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 1 & last_menses == 98, months_since_last_menses := 1/30]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 2 & last_menses == 98, months_since_last_menses := 1/4.3]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 3 & last_menses == 98, months_since_last_menses := 1]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses_unit == 4 & last_menses == 98, months_since_last_menses := 1*12]
  df[grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path)) & last_menses == 95, months_since_last_menses := 999]

  ## Fixes for no_menses in some SURVEY NAME 
  if ("interview_year" %in% names(df)) df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses_unit == 2 & (interview_year - last_menses) > 2, no_menses := 1]
  df[grepl("SURVEY_NAME",tolower(file_path)) & last_menses %in% c(4,5), no_menses := 1]
  
  ## SURVEY NAME
  df[nid == "NID", months_since_last_menses := last_menses / 30] # days
  df[nid == "NID" & last_menses_unit <= 12, months_since_last_menses := last_menses_unit] # months
  df[nid == "NID" & last_menses_unit >= 100, months_since_last_menses := (floor(last_menses_unit / 100) * 12) + (last_menses_unit %% 100)] # years and months
  
  ## SURVEY NAME
  if (unique(df$nid) == "NID") {
    df[, last_menses_year := get_cmc_year(last_menses)]
    df[, last_menses_month := get_cmc_month(last_menses)]
    df[, months_since_last_menses := ((interview_year - last_menses_year) * 12) + (interview_month - last_menses_month)]
  }

} else if ("last_menses" %in% names(df)) {
  ## SURVEY NAMES
  df[last_menses >= 100 & last_menses <= 190, months_since_last_menses := (last_menses-100)/30]
  df[last_menses == 199, months_since_last_menses := 1/30]
  df[last_menses >= 200 & last_menses <= 290, months_since_last_menses := (last_menses-200)/4.3]
  df[last_menses == 299, months_since_last_menses := 1/4.3]
  df[last_menses >= 300 & last_menses <= 390, months_since_last_menses := last_menses - 300]
  df[last_menses == 399, months_since_last_menses := 1]
  df[last_menses >= 400 & last_menses <= 490, months_since_last_menses := (last_menses-400)*12]
  df[last_menses == 499, months_since_last_menses := 1*12]
  
  ## SURVEY NAMES
  df[nid %in% c("NID","NID","NID") & last_menses < 94, months_since_last_menses := last_menses]

  ## SURVEY NAME
  df[nid %in% c("NID") & last_menses >= 200 & last_menses <= 290, months_since_last_menses := last_menses-200]
  df[nid %in% c("NID") & last_menses >= 300 & last_menses <= 390, months_since_last_menses := (last_menses - 300)*12]
  
  ## SURVEY NAMES
  df[nid %in% c("NID","NID") & last_menses < 61, months_since_last_menses := last_menses]
  
  ## SURVEY NAME
  df[nid %in% c("NID") & last_menses < 95, months_since_last_menses := last_menses]
  df[nid %in% c("NID") & last_menses == 66, months_since_last_menses := 0]
  
  ## amenorrheic women
  df[nid %in% c("NID") & last_menses == 46, months_since_last_menses := 999]
  df[nid %in% c("NID","NID","NID") & last_menses == 96, months_since_last_menses := 999]
  df[nid %in% c("NID","NID") & last_menses == 97, months_since_last_menses := 999]
  df[nid %in% c("NID","NID") & last_menses == 444, months_since_last_menses := 999]
  df[nid %in% c("NID","NID") & last_menses == 555, months_since_last_menses := 999]
  df[(nid %in% c("NID","NID","NID","NID") | grepl("SURVEY_NAME",tolower(file_path))) & last_menses == 994,months_since_last_menses := 999]
  df[(nid %in% c("NID","NID","NID") | grepl("SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME|SURVEY_NAME",tolower(file_path))) & last_menses == 995,months_since_last_menses := 999]
  
}

####################################################################################################
##### TIMING OF LAST BIRTH #########################################################################
####################################################################################################

## calculate time since most recent birth (done very differently depending on info available in each survey)
if (all(c("last_birth_year","last_birth_month","interview_year","interview_month") %in% names(df))) {
  ## determine months since most recent birth
  df[last_birth_month > 12,last_birth_month := 6]
  df[interview_month > 12,interview_month := 6]
  df[last_birth_year > 9990, last_birth_year := NA]
  df[interview_year > 9990, interview_year := NA]
  df[last_birth_month > interview_month, months_since_last_birth := ((interview_year-last_birth_year-1)*12) + (interview_month + 12 - last_birth_month)]
  df[interview_month >= last_birth_month, months_since_last_birth := ((interview_year-last_birth_year)*12) + (interview_month - last_birth_month)]
}

if (all(c("last_birth_date","interview_date") %in% names(df)) & grepl("SURVEY_NAME",tolower(df[1,file_path]))) {
  ## determine months since most recent birth (mostly just parsing dates)
  ## lots of variation in way it is coded across SURVEY NAME surveys
  df[last_birth_date == ".", last_birth_date := NA]
  df[interview_date == ".", interview_date := NA]
  if (any(grepl("-",df$interview_date))) {
    df[,year_birth := str_sub(last_birth_date,1,4) %>% as.numeric]
    df[,year_interview := str_sub(interview_date,1,4) %>% as.numeric]
    df[,month_birth := str_sub(interview_date,6,7) %>% as.numeric]
    df[,month_interview := str_sub(interview_date,6,7) %>% as.numeric]
  } else if (any(grepl(",",df$interview_date))) {
    df[,year_birth := str_sub(last_birth_date,-4) %>% as.numeric]
    df[,year_interview := sapply(1:nrow(df),function(x) str_sub(strsplit(df$interview_date[x],", ")[[1]][2],1,4)) %>% as.numeric]
    df[tolower(str_sub(last_birth_date,1,3)) == "jan", month_birth := 1]
    df[tolower(str_sub(last_birth_date,1,3)) == "feb", month_birth := 2]
    df[tolower(str_sub(last_birth_date,1,3)) == "mar", month_birth := 3]
    df[tolower(str_sub(last_birth_date,1,3)) == "apr", month_birth := 4]
    df[tolower(str_sub(last_birth_date,1,3)) == "may", month_birth := 5]
    df[tolower(str_sub(last_birth_date,1,3)) == "jun", month_birth := 6]
    df[tolower(str_sub(last_birth_date,1,3)) == "jul", month_birth := 7]
    df[tolower(str_sub(last_birth_date,1,3)) == "aug", month_birth := 8]
    df[tolower(str_sub(last_birth_date,1,3)) == "sep", month_birth := 9]
    df[tolower(str_sub(last_birth_date,1,3)) == "oct", month_birth := 10]
    df[tolower(str_sub(last_birth_date,1,3)) == "nov", month_birth := 11]
    df[tolower(str_sub(last_birth_date,1,3)) == "dec", month_birth := 12]
    df[tolower(str_sub(interview_date,1,3)) == "jan", month_interview := 1]
    df[tolower(str_sub(interview_date,1,3)) == "feb", month_interview := 2]
    df[tolower(str_sub(interview_date,1,3)) == "mar", month_interview := 3]
    df[tolower(str_sub(interview_date,1,3)) == "apr", month_interview := 4]
    df[tolower(str_sub(interview_date,1,3)) == "may", month_interview := 5]
    df[tolower(str_sub(interview_date,1,3)) == "jun", month_interview := 6]
    df[tolower(str_sub(interview_date,1,3)) == "jul", month_interview := 7]
    df[tolower(str_sub(interview_date,1,3)) == "aug", month_interview := 8]
    df[tolower(str_sub(interview_date,1,3)) == "sep", month_interview := 9]
    df[tolower(str_sub(interview_date,1,3)) == "oct", month_interview := 10]
    df[tolower(str_sub(interview_date,1,3)) == "nov", month_interview := 11]
    df[tolower(str_sub(interview_date,1,3)) == "dec", month_interview := 12]
  } else {
    df[,year_birth := str_sub(last_birth_date,-4) %>% as.numeric]
    df[,year_interview := str_sub(interview_date,-4) %>% as.numeric]
    df[tolower(str_sub(last_birth_date,3,5)) == "jan", month_birth := 1]
    df[tolower(str_sub(last_birth_date,3,5)) == "feb", month_birth := 2]
    df[tolower(str_sub(last_birth_date,3,5)) == "mar", month_birth := 3]
    df[tolower(str_sub(last_birth_date,3,5)) == "apr", month_birth := 4]
    df[tolower(str_sub(last_birth_date,3,5)) == "may", month_birth := 5]
    df[tolower(str_sub(last_birth_date,3,5)) == "jun", month_birth := 6]
    df[tolower(str_sub(last_birth_date,3,5)) == "jul", month_birth := 7]
    df[tolower(str_sub(last_birth_date,3,5)) == "aug", month_birth := 8]
    df[tolower(str_sub(last_birth_date,3,5)) == "sep", month_birth := 9]
    df[tolower(str_sub(last_birth_date,3,5)) == "oct", month_birth := 10]
    df[tolower(str_sub(last_birth_date,3,5)) == "nov", month_birth := 11]
    df[tolower(str_sub(last_birth_date,3,5)) == "dec", month_birth := 12]
    df[tolower(str_sub(interview_date,3,5)) == "jan", month_interview := 1]
    df[tolower(str_sub(interview_date,3,5)) == "feb", month_interview := 2]
    df[tolower(str_sub(interview_date,3,5)) == "mar", month_interview := 3]
    df[tolower(str_sub(interview_date,3,5)) == "apr", month_interview := 4]
    df[tolower(str_sub(interview_date,3,5)) == "may", month_interview := 5]
    df[tolower(str_sub(interview_date,3,5)) == "jun", month_interview := 6]
    df[tolower(str_sub(interview_date,3,5)) == "jul", month_interview := 7]
    df[tolower(str_sub(interview_date,3,5)) == "aug", month_interview := 8]
    df[tolower(str_sub(interview_date,3,5)) == "sep", month_interview := 9]
    df[tolower(str_sub(interview_date,3,5)) == "oct", month_interview := 10]
    df[tolower(str_sub(interview_date,3,5)) == "nov", month_interview := 11]
    df[tolower(str_sub(interview_date,3,5)) == "dec", month_interview := 12]
  }

  df[month_birth > month_interview, months_since_last_birth := ((year_interview-year_birth-1)*12) + (month_interview + 12 - month_birth)]
  df[month_interview >= month_birth, months_since_last_birth := ((year_interview-year_birth)*12) + (month_interview - month_birth)]
}

## determine who is postpartum amenorrheic (regardless of how long they have been amenorrheic)
if (all(c("months_since_last_menses","months_since_last_birth") %in% names(df))) df[pregnant == 0 & months_since_last_menses >= months_since_last_birth, ppa := 1]
if ("reason_no_contra" %in% names(df)) df[pregnant == 0 & grepl("amenorr|no menses since last birth",reason_no_contra), ppa := 1]

## correct women who identified as ppa but are actually not
if (all(c("ppa","months_since_last_menses","months_since_last_birth") %in% names(df))) df[ppa == 1 & months_since_last_menses <= months_since_last_birth, ppa := 0]

## women without their period for 6 or more months are considered infecund as long as they are not postpartum amenorrheic
## if they are postpartum amenorrheic, must have been 5 years or more since last child was born to be considered infecund
## when missing_fecund or no_ppa, not possible to calculate this
if (all(c("ppa","months_since_last_menses") %in% names(df))) {
  df[is.na(ppa) & months_since_last_menses >= 6, no_menses := 1]
  if ("months_since_last_birth" %in% names(df)) df[ppa == 1 & months_since_last_birth >= 60, no_menses := 1]
} else if ("months_since_last_menses" %in% names(df)) {
  df[months_since_last_menses >= 6, no_menses := 1]
} else if ("ppa" %in% names(df)){
  df[ppa == 1, no_menses := 1]
}


####################################################################################################
##### MODERN CONTRACEPTIVE USAGE ###################################################################
####################################################################################################

## clarify missingness that arises when question about former cohabitation is separate and asked to a subset of those who said "no" to
## current cohabitation
if (all(c("curr_cohabit","former_cohabit") %in% names(df))) {
  df[is.na(former_cohabit) & !is.na(curr_cohabit), former_cohabit := 0]
  df[is.na(curr_cohabit) & !is.na(former_cohabit), curr_cohabit := 0]
  df[curr_cohabit == 1, former_cohabit := 0]
}

## Most surveys use the answer to whether or not a woman is pregnant as a gateway for asking about contraceptive usage. The codebook is set up such that
## any answer that passes through the gateway is coded as 0 (since it is always "no" or "not sure"), and those that specifically say they are pregnant are 1.
## Therefore, any missingness in pregnant variable correspond to individuals who were not asked about contraception, and should be dropped
## However, some surveys only filled out the variable when pregnant == 1, leaving everything else NA. In these scenarios, must assume that everyone with NA was not pregnant
if (nrow(df[pregnant == 0]) == 0) df[is.na(pregnant), pregnant := 0]
## This gateway relies on the assumption everyone survey asks every if they are currently pregnant
## Some surveys use marital status as a gateway for current pregnant which will throw off our marital
## status calculations, be wary of very large numbers missing pregnancy status
if (nrow(df[is.na(pregnant)]) > 0) print(paste0(survey," HAS ",nrow(df[is.na(pregnant)]), " MISSING CURRENT PREGNANT RESPONSES!!"))

## identify specific methods using search strings
## modern methods 
male_sterilization <- paste(c("^male ster", "^male_steril", "vasect", "operacion masc", "m ster", "est.*masc", " male ster", "sterilisation masc", " male ster",
                              "esterilizacion macul", "lisation pour les hommes", "^male ester", "^male-ster", "sterilisation \\(male\\)", " male ester",
                              "^males steril", "m\\.steril", "^ms$", "#male ster", "#male_steril", "#male ester", "#male-ster", "#males steril"), collapse = "|")
female_sterilization <- paste(c("female ster", "ligation", "ligature", "ligadura", "legation", "fem ster", "esterilizada", "operacion fem", 
                                "est.*fem", "steril.*fem", "fem.*steril", "female-ster", "f\\.steril", "^fs$"), collapse = "|")
sterilization <- paste(c("sterili", "ester", "-ster", "fem ster", "m ster", "vasect", "ale steril", "operacion", "ligation", "ligature", "ligadura", 
                         'ester\\.', "est\\.", "sterliz", "female ster", "laquea", "legation"), collapse = "|") # make sure to not capture "sterilet" which is actually French for IUD
pill <- paste(c("pill", "pilul", "pastill", "pildor", "oral", "phill"), collapse = "|")
iud <- paste(c("iud", "diu", "dui", "spiral", "sterilet", "st\\?rilet", "coil", "iucd", "i\\.u\\.d.", "loop", "copper", "strilet"), collapse = "|")
injections <- paste(c("inyec", "injec", "depo", "sayana", "piqure", "injeta"), collapse = "|")
implants <- paste(c("implant", "norplant", "zadelle", "transplant", "inplant", "norolant"), collapse = "|")
condom <- paste(c("condom", "condon", "preservati", "camisinha", "codom"), collapse = "|")
emergency <- paste(c("morning-after", "morning after", "emergenc", "du lendemain", "dia siguiente", "contraception after sex", "^ec$"), collapse = "|")
patch <- paste(c("patch", "parche"), collapse = "|")
ring <- paste(c("contraceptive ring", "vaginal ring", "/ring", "anillo"), collapse = "|")
diaphragm <- paste(c("diaphr", "diafrag", "cervical cap", "cones"), collapse = "|")
foam_gel_sponge <- paste(c("tablet", "foam", "jelly", "jalea", "mousse", "espuma", "creme", "cream", "crema", "gel", "gelee", "spermicid", "eponge", "intravag",
                           "esponja", "esonja", "sponge", "vaginale", "comprimidos vaginais", "vaginal method", "suppository", "vaginals", "metodos vaginal"), collapse = "|")
other_mod <- paste(c("modern", "other mod", "other_mod", "fem sci", "sci fem", "scien fem", "other female", "oth fsci", "menstrual regulation", "campo de latex"), collapse = "|") 

## traditional methods
lam <- paste(c("lactat", "lactan", "amenor", "lam", "mela", "breastf", "allaite", "mama", "prolonged bf", "lact\\. amen\\.",
               "prolonged brst", "lact\\.amen\\."), collapse = "|")
withdrawal <- paste(c("withdr", "retiro", "retrait", "coitus interr", "coito interr", "interruption"), collapse = "|")
rhythm <- paste(c("rhyth", "rythm", "ritmo", "periodic abst", "abstinence period", "ncia peri", "safe period", "periodical abst", 
                  "rhtyhm", "sporadic abstinence", "periodiqu", "contnence peridque", "uterus inversion", "cont per"), collapse = "|")
calendar_beads_sdays <- paste(c("calen", "beads", "sdm", "standard days", "billing", "mucus", "moco cervical", "ovulat", "fixed days", "string", "collar", 
                                "collier", "dangerous days", "fertility wheel", "calandrier"), collapse = "|")
other_trad <- paste(c("other", "otro", "autre", "other trad", "other_trad", "tradition", "herb", "hierba", "douch", "yuyo", "symptothermal", "temperature",
                      "gris", "medicinal plant", "plantes medici", "charm", "quinine", "natural", "lavado vaginal", "trad meth", "persona", "outro",
                      "candle", "washing", "meth tradi", "massage", "exercise", "akar kayu", "give suck", "stout", "mjf", "trad\\. intra",
                      "pijat", "jamu", "isolation", "folkloric", "coran", "marabout", "temperatr", "cerv mucs"), collapse = "|")

## abstinence 
## we currently do not count abstinence (not to be confused with periodic abstinence/rhythm) as a contraceptive method
## still want to identify in order to easily include or exclude from current_use 
## if there are instances where a survey refers to period abstinence as abstinence, write survey fix changing current_contra string 
abstinence <- paste(c("prolonged abstinence", "abstinence", "abstinencia", "abstinence prolong", "not sexually active",
                      "abstention"), collapse = "|")

## non-use
nonuse <- paste(c("none", "not using", "no method", "not current user", "not currently using", "no contraceptive", "no usa",
                  "no esta usa", "no estan usa", "nonuser", "non-user", "not expose", "non expose", "n expose", "not user",
                  "no current use", "nutilise", "did not use"), collapse = "|")

## no response or unknown if a woman is using a method or what method 
unknown <- paste(c("refused", "^dna", "don't know", "dont know", "don t know", "not stated", "no respon", "non repons", "not eligible", 
                   "inconsistent", "-99", "^na$", "refusal", "missing", "manquant", "does not know", "^$", "^ns$", "s/inf", "^dk$", 
                   "unsure", "sin informacion"), collapse = "|")

## search current_contra for the most effective method a women is currently using; the order of these is very important!!! 
## want to identify most effective methods last in order to overwrite less effective methods 
## abstinence and rhythm (also called "periodic abstinence") will identify the same strings, ensure rhythm is always after abstinence to differentiate 
df[is.na(current_contra) | (current_contra == "" & nid %in% c("NID", "NID")), current_method := "blank"]
df[grepl(unknown, tolower(current_contra)), current_method := "no_response"]
df[grepl(nonuse, tolower(current_contra)), current_method := "none"]
df[grepl(abstinence, tolower(current_contra)), current_method := "abstinence"]
df[grepl(other_trad, tolower(current_contra)), current_method := "other_traditional_method"]
df[grepl(other_mod, tolower(current_contra)), current_method := "other_modern_method"]
df[grepl(withdrawal, tolower(current_contra)), current_method := "withdrawal"]
df[grepl(calendar_beads_sdays, tolower(current_contra)), current_method := "calendar_methods"]
df[grepl(rhythm, tolower(current_contra)), current_method := "rhythm"]
df[grepl(lam, tolower(current_contra)), current_method := "lactational_amenorrhea_method"]
df[grepl(emergency, tolower(current_contra)), current_method := "emergency_contraception"]
df[grepl(foam_gel_sponge, tolower(current_contra)), current_method := "foam_jelly_sponge"]
df[grepl(diaphragm, tolower(current_contra)), current_method := "diaphragm"]
df[grepl(condom, tolower(current_contra)), current_method := "condom"]
df[grepl(ring, tolower(current_contra)), current_method := "contraceptive_ring"]
df[grepl(patch, tolower(current_contra)), current_method := "contraceptive_patch"]
df[grepl(pill, tolower(current_contra)), current_method := "pill"]
df[grepl(implants, tolower(current_contra)), current_method := "implants"]
df[grepl(injections, tolower(current_contra)), current_method := "injections"]
df[grepl(iud, tolower(current_contra)), current_method := "iud"]
df[grepl(male_sterilization, tolower(current_contra)), current_method := "male_sterilization"] 
df[grepl(female_sterilization, tolower(current_contra)), current_method := "female_sterilization"] 

## check to ensure all current_contra responses were processed, otherwise need to revise/add search strings 
if (any(is.na(df$current_method))) stop(paste0(survey, ": current_contra response ", df[is.na(current_method), unique(current_contra)], " not getting captured by search strings!!!"))

## identify modern and traditional contraceptive users
df[current_method %in% c("female_sterilization", "male_sterilization", "iud", "injections", "implants", "pill", "contraceptive_patch", "contraceptive_ring", "condom",
                         "diaphragm", "foam_jelly_sponge", "emergency_contraception", "other_modern_method"), mod_contra := 1]

df[current_method %in% c("lactational_amenorrhea_method", "rhythm", "calendar_methods", "withdrawal", "other_traditional_method"), trad_contra := 1]


####################################################################################################
##### SEPARATING MISSINGNESS FROM LACK OF USE ######################################################
####################################################################################################

## Have to set 0s and NAs correctly for the different indicators so that, when collapsed, the right denominator is used (NAs are excluded in collapse code)

## While DHS surveys seem to have asked all women about contraceptive usage (or at least fill in assumed answers to the variable),
## other surveys only ask this to certain women, making missingess in current_contra
## ambiguous with respect to whether a woman is not using a contraceptive or was
## just never asked/didn't answer. This distinction is important for determining the appropriate
## denominator for the proportion we are calculating

## Some surveys have a variable corresponding to the gateway question "Are you currently
## using any method?" which helps clarify the issue, but others do not.

## If there is no variable corresponding to the question of whether a woman is
## using a method at all, then we assume that all non-pregnant women were asked
## about their contraceptive usage. This is necessary because of surveys like the early MICS
## surveys, which code non-use as missing but do not have a gateway variable.
## Therefore, missing answers for the variable in such surveys
## are taken to mean that she is not using a method

## NOTE: DHS surveys often have a current_use variable, but it is redundant
## because DHS codes non-use as an explicit answer in every survey encountered thus far,
## so it is usually not filled out in the codebook (which is fine)

## When current_use variable is missing, must assume everyone not pregnant was asked about contraception
if ("current_use" %ni% names(df)) {
  
  ## unless specifically marked as missing/no response, assume women were asked and should therefore be in denominator
  df[current_method != "no_response" & is.na(mod_contra), mod_contra := 0]
  df[current_method != "no_response" & is.na(trad_contra), trad_contra := 0]
  
  ## some surveys leave current_contra as missing if women already answered that she had never used a method
  if ("never_used_contra" %in% names(df)) df[never_used_contra == 1, c("mod_contra","trad_contra") := .(0, 0)]

} else {
  ## survey does have a gateway variable corresponding to current use

  ## some surveys still leave current usage as missing if women already answered that she had never used a method
  if ("never_used_contra" %in% names(df)) df[never_used_contra == 1, current_use := 0]

  ## Some surveys do not ask about current usage if the woman was sterilized, even if it was for family planning purposes,
  ## so current_contra may say "sterilized" when current_use = 0
  ## This fixes that issue by adjusting current_use based on current_method processing
  ## NOTE: this relies on the list of strings corresponding to non-use and unknown being exhaustive!
  df[current_method != "no_response", current_use := 0]
  df[current_method != "no_response" & current_method != "none", current_use := 1]
  
  ## any women who gave an answer regarding current usage should be in the denominator
  df[!is.na(current_use) & is.na(mod_contra), mod_contra := 0]
  df[!is.na(current_use) & is.na(trad_contra), trad_contra := 0]
}

## Want to keep pregnant women in data set to be included in all-women denominator, but most surveys don't actually ask them
## about contraception, so must assume the answer
df[pregnant == 1, c("any_contra", "mod_contra", "trad_contra") := .(0, 0, 0)]

## If women are using any method, mark that
df[!is.na(mod_contra), any_contra := 0]
df[trad_contra == 1 | mod_contra == 1, any_contra := 1]


####################################################################################################
##### NEED FOR CONTRACEPTION #######################################################################
####################################################################################################

## Women who have have a need for contraceptives are women who:
## 1. have had sex in the last 30 days or are married/in union
## 2. said that they do not want a child in the next 2 years
## 3. have not hit menopause, had a hysterectomy, or are otherwise known to be
##    infecund (including having never menstruated or not having menstruated
##    in at least 6 months if not postpartum amenorrheic, or having been postpartum
##    amenorrheic for 5 or more years)
## 4. have not been continuously married/living with a man for 5 years without
##    having a child despite answering that they have never used any method of
##    contraception.
## 5. For pregnant women and women who are postpartum amenorrheic from a birth in
##    the last 2 years, need is determined separately based on whether they wanted
##    to space or limit their current/most recent pregnancy

## Since many surveys are missing one or more of these variables, have to check
## that the codebook is filled out for a variable before using it, otherwise you
## will get an error. Some variables are used as strings below based on their labels
## even though they are technically coded as numbers. When such variables exist in the dataset
## but are entirely missing, an error will be thrown when they are treated as strings.
## Therefore you first have to check if there are any nonmissing values for those vars

## first, assume no need for contraception
df[, need_contra := 0]

## The only women who are eligible for having a need for contraception are those that are married/in-union or
## have been sexually active in the last month, and have expressed that they do not want children in the next 2 years
## For some surveys, the period for when they wanted children differed by a small amount, but in others women were only asked
## about the desire for a child right now (the desire_soon variable. Is crosswalked later). In others, no question about
## desire for children is asked at all, and all married/in-union/sexually active women are assumed to have a need (also crosswalked later)

if (counterfac_missing_desire_timing == 1) {
  # Counterfactual gateway if re-extracting as if survey had no information regarding timing of desire for children
  # survey only had information such as desire_gate and desire_soon
  df[is.na(desire_limiting), desire_later := NA]
}

if (counterfac_missing_desire == 1) {
  ## Counterfactual gateway if re-extracting as if survey had no information regarding desire for children
  df[curr_cohabit == 1, need_contra := 1]
  if ("sex_in_last_month" %in% names(df)) df[sex_in_last_month == 1, need_contra := 1]

} else if (counterfac_missing_desire_later == 1) {
  ## Counterfactual gateway if re-extracting as if survey only had information regarding desire for children right now
  df[curr_cohabit == 1 & (is.na(desire_soon) | desire_soon == 0), need_contra := 1]
  if ("sex_in_last_month" %in% names(df)) df[sex_in_last_month == 1 & (is.na(desire_soon) | desire_soon == 0), need_contra := 1]

} else { ## normal extraction
  if ("desire_later" %in% names(df)) {
    ## Survey has information on women's desire for a child in the next 2 years
    df[curr_cohabit == 1 & desire_later == 1, need_contra := 1]
    if ("sex_in_last_month" %in% names(df)) df[sex_in_last_month == 1 & desire_later == 1, need_contra := 1]

  } else if ("desire_soon" %in% names(df)) {
    ## Survey only has info regarding desire for a child right now. Assume that anyone who is married/sexually active
    ## and did not answer that they want a child right now has a need
    df[curr_cohabit == 1 & (is.na(desire_soon) | desire_soon == 0), need_contra := 1]
    if ("sex_in_last_month" %in% names(df)) df[sex_in_last_month == 1 & (is.na(desire_soon) | desire_soon == 0), need_contra := 1]

  } else {
    ## No info regarding desire for children, assume that everyone who's married/sexually active has a need
    if ("curr_cohabit" %in% names(df)) df[curr_cohabit == 1,need_contra := 1]
    if ("sex_in_last_month" %in% names(df)) df[sex_in_last_month == 1, need_contra := 1]
  }
}

## Now we exclude women from having a need for contraception if they are pregnant, postpartum amenorrheic, have expressed that
## they are infecund, or can be assumed to be infecund based on their recent menstruation or lack of children
## after 5 years of marriage with no contraceptive usage

## If re-extracting as if survey had no information regarding fecundity, skip these
if (counterfac_missing_fecund == 0) {
  ## Indicated lack of menstruation (in any way)
  if ("no_menses" %in% names(df)) df[no_menses == 1, infecund := 1]

  ## Indicated inability to have a child
  if ("desire_children_infertile" %in% names(df)) df[desire_children_infertile == 1, infecund := 1]

  ## Indicated lack of menstruation or inability to have a child as reason why she was not using a contraceptive method
  infecund <- paste(c("infecund",
                      "menopaus",
                      "hyst",
                      "histerect",
                      "never mens",
                      "menstrua",
                      "removal of uterus",
                      "cannot have children",
                      "cant become pregnant",
                      "no puede quedar emb",
                      "^infertil",
                      "self infertil",
                      "both infertil",
                      "nao pode",
                      "impossible to have"),
                    collapse="|")
  if ("reason_no_contra" %in% names(df)) {
    x <- unlist(lapply(tolower(df$reason_no_contra), grepl, pattern = infecund))
    df[x, infecund := 1]
  } 
  if ("intent_reason_no_contra" %in% names(df)) {
    x <- unlist(lapply(tolower(df$intent_reason_no_contra), grepl, pattern = infecund))
    df[x, infecund := 1]
  } 

  ## Exclude others from need based on assumed infertility after 5 years without a child. Relevant variables
  ## coded differently depending on the survey series)
  if (all(c("curr_cohabit","never_used_contra") %in% names(df))) {
    ## SURVEY NAME
    ## can only deduce criteria among women who are still in their first marriage/union
    if (all(c("in_first_cohabit","years_since_first_cohabit","months_since_last_birth") %in% names(df))) {
      df[curr_cohabit == 1 & ##currently married/in union
           never_used_contra == 1 & ##never used contraception
           in_first_cohabit == 1 & ##still in first marriage/union
           !is.na(years_since_first_cohabit) & ##make sure years of marriage is known
           years_since_first_cohabit > 4 & ##married for at least 5 years
           (months_since_last_birth >= 60 | ##5 years since last birth or never given birth
              is.na(months_since_last_birth)),
         infecund := 1]
    }

    ## SURVEY NAMES
    ## similar to above, but uses different variables
    if (all(c("in_first_cohabit","first_cohabit_year","last_birth_year","interview_year") %in% names(df))) {
      df[curr_cohabit == 1 & ##currently married/in union
           never_used_contra == 1 & ##never used contraception
           in_first_cohabit == 1 & ##still in first marriage/union
           interview_year < 9990 & ##make sure interview year is known
           interview_year - first_cohabit_year > 5 & ##been together for 5 years
           (interview_year - last_birth_year > 5 | ##5 years since last birth or never given birth
              is.na(last_birth_year)),
         infecund := 1]
    }

    ## SURVEY NAMES
    ## actually asks about most recent marriage/union, so can deduce for all women
    if (all(c("recent_cohabit_start_date","interview_date","last_birth_date") %in% names(df))) {
      if (class(df$recent_cohabit_start_date) == "character" & "year_interview" %in% names(df)) {
        ##parse dates for the years
        df[recent_cohabit_start_date == ".",recent_cohabit_start_date := NA]
        df[,year_married := str_sub(recent_cohabit_start_date,-4) %>% as.numeric]
        df[curr_cohabit == 1 & ##currently married/in union
             never_used_contra == 1 & ##never used contraception
             !is.na(year_interview) & ##make sure interview year is known
             year_interview - year_married > 5 & ##been together for 5 years
             (year_interview - year_birth > 5 | ##5 years since last birth or never given birth
                is.na(year_birth)),
           infecund := 1]
      } else { ##assume CMC format
        df[curr_cohabit == 1 & ##currently married/in union
             never_used_contra == 1 & ##never used contraception
             !is.na(interview_date) & ##make sure interview year is known
             interview_date - recent_cohabit_start_date > 60 & ##been together for 5 years
             (interview_date - last_birth_date > 60 | ##5 years since last birth or never given birth
                is.na(last_birth_date)),
           infecund := 1]
      }
    }
  }
  
  ## pregnant women are not infecund even though their last menses may have been >6 months ago
  df[pregnant == 1, infecund := 0]
  
  ## any woman that is infecund does NOT have a need for contraception; skip if survey will be crosswalked for missing fecundity
  if ("missing_fecund" %ni% names(df) | all(df$missing_fecund == 0)) df[infecund == 1, need_contra := 0]
}


####################################################################################################
##### PREGNANT AND POST-PARTUM AMENORRHEIC WOMEN ###################################################
####################################################################################################

## Pregnant and postpartum amenorrheic women from a birth in the last 2 years can still contribute
## to unmet demand for contraception if they indicate that they wanted to space or limit their
## current/most recent pregnancy

## pregnant women are assumed to not need contraception as they are not at risk for pregnancy
df[pregnant == 1, need_contra := 0]

## If re-extracting as if survey had no information regarding pregnant women, skip
if (counterfac_no_preg == 0) {
  
  ## Pregnant women who had a need
  if ("preg_not_wanted" %in% names(df)) df[pregnant == 1 & preg_not_wanted == 1, need_contra := 1]
}

## If re-extracting as if survey had no information regarding postpartum amenorrheic women, skip
## if we are missing fecundity, then we are also unable to identify ppa women, so skip for fecundity re-extraction
if (counterfac_no_ppa == 0) {
  
  ## Determine which postpartum amenorrheic women gave birth within the last 2 years
  if (all(c("ppa","months_since_last_birth") %in% names(df))) df[ppa == 1 & months_since_last_birth <= 24, ppa24 := 1]
  if (all(c("ppa","birth_in_last_two_years") %in% names(df))) df[ppa == 1 & birth_in_last_two_years == 1, ppa24 := 1]
  
  ## postpartum amenorrheic are asssumed to not be using/need contraception as their periods have not returned 
  if ("ppa24" %in% names(df)) {
    df[ppa24 == 1 & is.na(mod_contra), mod_contra := 0]
    df[ppa24 == 1 & is.na(trad_contra), trad_contra := 0]
    df[ppa24 == 1 & is.na(any_contra), any_contra := 0]
    df[ppa24 == 1, need_contra := 0]
  }
  
  ## Designate which postpartum amenorrheic women had a need, and adjust the all-woman contraception denominator to match (in case
  ## postpartum women were not asked about contraception, similar to pregnant women)
  if (all(c("ppa24","ppa_not_wanted") %in% names(df))) df[ppa24 == 1 & ppa_not_wanted == 1, need_contra := 1]
}


####################################################################################################
##### NEED FOR CONTRACEPTION MET WITH MODERN METHODS ###############################################
####################################################################################################

## Restrict need_contra to those observations where we know women were actually asked about contraception,
## making it consistent with the denominator of modern contraceptive usage (since otherwise it's impossible
## to know whether that need was met or not)
df[is.na(mod_contra), need_contra := NA]

## create variable for proportion of users where definition of need actually captured their need before we add those who 
## are assumed to have a need because of contraception use (i.e. proportion of women with need where need was determined 
## by definition, not by contraceptive use). BUT need to subset to non-sterilized women/couples because in such cases sterilized 
## people retain "need" (through use) even as context/demand changes (need_contra may have been marked as 0 from infecundity) 
sterile <- paste(c("sterilization", "sterilisation", "sterilized", "ester", "esterizacao", "vasectom",
                   "ligation", "ligadura", "ligature", 'ester\\.', "est\\.", "m ster", "male ster",
                   "male-ster", "other meths inc ster"), #male or female sterilization
                 collapse = "|")
df[,need_prop := ifelse(any_contra == 1 & !grepl(sterile,tolower(current_contra)), need_contra, NA)]

## regardless of answers to any other questions, if a woman is currently using any contraceptive method then
## she is considered to have a need for contraception
df[any_contra == 1, need_contra := 1]


####################################################################################################
##### MARITAL STATUS ADJUSTMENTS ###################################################################
####################################################################################################

## if surveys are restricted by marital status, adjust denominators in order to reflect those restrictions 
if ("curr_cohabit" %in% names(df)) {
  # surveys restricted to currently married women only
  if ("currmar_only" %in% names(df)) {
    if (unique(df$currmar_only) == 1) df[, need_contra := ifelse(curr_cohabit == 1, need_contra, NA)]
  }
  if ("contra_currmar_only" %in% names(df)) {
    if (unique(df$contra_currmar_only) == 1) {
      df[, mod_contra := ifelse(curr_cohabit == 1, mod_contra, NA)]
      df[, trad_contra := ifelse(curr_cohabit == 1, trad_contra, NA)]
      df[, any_contra := ifelse(curr_cohabit == 1, any_contra, NA)]
      df[, current_method := ifelse(curr_cohabit == 1, current_method, NA)]
    }
  }
    
  # surveys restricted to ever-married women only
  # unmarried ever-married women are not representative of all unmarried women, restrict survey to just married women  
  if ("evermar_only" %in% names(df)) {
    if (unique(df$evermar_only) == 1) df[,need_contra := ifelse(curr_cohabit == 1, need_contra, NA)]
  } 
  if ("contra_evermar_only" %in% names(df)) {
    if (unique(df$contra_evermar_only) == 1) {
      df[, mod_contra := ifelse(curr_cohabit == 1, mod_contra, NA)]
      df[, trad_contra := ifelse(curr_cohabit == 1, trad_contra, NA)]
      df[, any_contra := ifelse(curr_cohabit == 1, any_contra, NA)]
      df[, current_method := ifelse(curr_cohabit == 1, current_method, NA)]
    }
  } 
}

## Of those with a need for contraception, those using a modern method have a met demand, while those that
## aren't have an unmet demand
df[, met_need_demanded := ifelse(need_contra == 1, mod_contra, NA)]


####################################################################################################
##### NEW VARS #####################################################################################
####################################################################################################

## proportion of any contraceptive use that is modern
df[,mod_prop := ifelse(any_contra == 1, mod_contra, NA)]

## proportion of non-users that have an (unmet) need
df[,unmet_prop := ifelse(any_contra == 0, need_contra, NA)]


####################################################################################################
##### MARITAL STATUS DEPENDENT VARS ################################################################
####################################################################################################

if ("curr_cohabit" %in% names(df)) {
  ## any contraceptive use, just among married women
  df[, any_contra_mar := ifelse(curr_cohabit == 1, any_contra, NA)]

  ## any contraceptive use, just among unmarried women
  df[, any_contra_unmar := ifelse(curr_cohabit == 0, any_contra, NA)]

  ## proportion of any contraceptive use that is modern, just among married women
  df[, mod_prop_mar := ifelse(curr_cohabit == 1, mod_prop, NA)]

  ## proportion of any contraceptive use that is modern, just among unmarried women
  df[, mod_prop_unmar := ifelse(curr_cohabit == 0, mod_prop, NA)]

  ## proportion of non-users that have an (unmet) need, just among married women
  df[, unmet_prop_mar := ifelse(curr_cohabit == 1, unmet_prop, NA)]

  ## proportion of non-users that have an (unmet) need, just among unmarried women
  df[, unmet_prop_unmar := ifelse(curr_cohabit == 0, unmet_prop, NA)]
  
  ## current method, just among married women
  df[, current_method_mar := ifelse(curr_cohabit == 1, current_method, NA)]
  
  ## current method, just among unmarried women
  df[, current_method_unmar := ifelse(curr_cohabit == 0, current_method, NA)]
}


####################################################################################################
# OUTPUT VARIABLE FILES
####################################################################################################

# write output file ("mapped data") for contraception
folder <- ifelse(grepl(subnat_list , unique(df$ihme_loc_id)), "subnats", "nats")
write.csv(df, file.path(out.dir, "FILEPATH", folder, str_replace(survey, ".dta", ".csv")), row.names = F)

# write output file ("mapped data") for method mix
folder <- ifelse(grepl(subnat_list, unique(df$ihme_loc_id)), "subnats", "nats")
write.csv(df, file.path(out.dir, "FILEPATH", folder, str_replace(survey, ".dta", ".csv")), row.names = F)
