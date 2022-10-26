###########################################################################################################
## Purpose: Crosswalk imperfect unmet_prop df by obtaining adjustment coefficients from gold standard
##          surveys and their corresponding counterfactual extractions 
###########################################################################################################

## SET-UP ------------------------------------------------------------------------------

id.vars <- c("nid","ihme_loc_id","year_start","age_group")
library(crosswalk002, lib.loc = "FILEPATH")

## create function for the opposite of %in%
'%ni%' <- Negate('%in%')

# obtain list of counterfactuals to crosswalk for
df$counterfactual <- ""

df[missing_desire == 1, counterfactual := paste(counterfactual, "desire", sep = "")]
df[missing_desire_later == 1, counterfactual := paste(counterfactual, "desire_later", sep = "")]
df[missing_desire_timing == 1, counterfactual := paste(counterfactual, "desire_timing", sep = "")]
df[missing_fecund == 1, counterfactual := ifelse(counterfactual == "",paste(counterfactual, "fecund", sep = ""),paste(counterfactual, "fecund", sep = "_"))]
df[no_preg == 1, counterfactual := ifelse(counterfactual == "",paste(counterfactual, "no_preg", sep = ""),paste(counterfactual, "no_preg", sep = "_"))]
df[no_ppa == 1, counterfactual := ifelse(counterfactual == "",paste(counterfactual, "no_ppa", sep = ""),paste(counterfactual, "no_ppa", sep = "_"))]

topics <- unique(df[counterfactual != "", counterfactual])
  
# for married women we do not crosswalk (nor preform counterfactual extractions) for fecund_desire_no_preg_ppa
# because these surveys essentially provide no information on need for married women, however, for unmarried 
# women they still may provide valuable information on sexual activity 
if (grepl('_mar', global.me_name)) topics <- topics[topics != 'desire_fecund_no_preg_no_ppa']

# offset extreme values from marital splitting and fill in missing standard_error's
df[val < offset, val := offset]
df[val > 1 - offset, val := 1 - offset]
df[is.na(standard_error), standard_error := sqrt(val*(1-val)/sample_size)]

  
## 1. PREP DATA -----------------------------------------------------------------------
  
# transform val and standard_error into logit space for adjustment later on
df[, c("logit_val", "logit_standard_error")] <- as.data.table(crosswalk002::delta_transform(mean = df$val,
                                                                                         sd = df$standard_error,
                                                                                         transformation = 'linear_to_logit'))
 
# prevent confusing column name overlap from merging on counterfactuals
setnames(df, old = c("no_preg", "no_ppa"), new = c("missing_no_preg", "missing_no_ppa"))

# drop married data missing fecundity, desire, and pregnant/ppa information (reduces data by a fair amount!)
# essentially no real information on need, just contraceptive use 
if (grepl('mar', global.me_name)) df <- df[missing_fecund != 1 | missing_desire != 1 | missing_no_preg != 1 | missing_no_ppa != 1]


## 2. OBTAIN TOPIC-SPECIFIC COEFFICIENTS ----------------------------------------------
  
# create empty tables to store the xwalk coefficients and nid's which have extremely small standard errors
# small se's will break the CWModel() function; likely due to incorrect weight assignments during extraction
all_coeffs <- data.table()
small_se_nids <- list()
  
for (topic in topics) {
  topic_coeffs <- data.table()
    
  # load counterfactual data
  df2 <- fread(file.path(dir.root,"FILEPATH",paste0(topic,".csv")))[ihme_loc_id != "" & !is.na(ihme_loc_id)]
    
  # subset to variable of interest, cut out small sample sizes and extreme points
  df2 <- df2[var == gsub('_prelim|_final','',global.me_name) & sample_size >= 20 & mean %ni% c(0,1)]
    
  # format counterfactual extractions 
  df2 <- df2[,c(id.vars,"mean","standard_error"), with = F]
  setnames(df2, c("mean","standard_error"), c("prev_alt","prev_se_alt"))
  df2[,dorm_alt := topic]
    
  # subset df to gold standard extractions which have corresponding counterfactual extractions
  gold <- df[paste0(nid, ihme_loc_id, year_start, age_group) %in% paste0(df2$nid, df2$ihme_loc_id, df2$year_start, df2$age_group)]
    
  # format gold standard extractions
  gold <- gold[,c(id.vars,"val","standard_error"), with = F]
  setnames(gold, c("val","standard_error"), c("prev_ref","prev_se_ref"))
  gold[,dorm_ref := "gold_standard"]
    
  # combine gold standard and counterfactual extractions
  df_matched <- merge(gold, df2, by = id.vars)
    
  # prepare data for crosswalking
  dat_diff <- as.data.frame(cbind(
    crosswalk002::delta_transform(
      mean = df_matched$prev_alt, 
      sd = df_matched$prev_se_alt,
      transformation = "linear_to_logit" ),
    crosswalk002::delta_transform(
      mean = df_matched$prev_ref, 
      sd = df_matched$prev_se_ref,
      transformation = "linear_to_logit")
  ))
  names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")
    
  # get table of matched reference and gold standard data pairs 
  df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
    df = dat_diff, 
    alt_mean = "mean_alt", alt_sd = "mean_se_alt",
    ref_mean = "mean_ref", ref_sd = "mean_se_ref" )
    
  # drop differences of 0, likely no women affected by the topic
  df_matched <- df_matched[logit_diff != 0]
    
  # remove any entries with abnormally small standard errors; return to ubcov and fix weighting scheme
  bad_nids <- df_matched[prev_se_alt < .00001 | prev_se_ref < .00001, unique(nid)]
  small_se_nids <- append(small_se_nids, bad_nids)
  df_matched <- df_matched[prev_se_alt > .00001 & prev_se_ref > .00001]
    
  # crosswalk time
  # do separately for each age group
  for (ag in unique(df_matched$age_group)) {
    cat("Topic:",topic,"Age Group:",ag)
      
    # subset to all matched pairs for a particular age group
    df_xwalk <- df_matched[age_group == ag]
    df_xwalk$id2 <- as.integer(as.factor(df_xwalk$nid)) ## housekeeping
      
    # format data for meta-regression; pass in data.frame and variable names
    dat1 <- CWData(
      df = df_xwalk,
      obs = "logit_diff",       # matched differences in logit space
      obs_se = "logit_diff_se", # SE of matched differences in logit space
      alt_dorms = "dorm_alt",   # var for the alternative def/method
      ref_dorms = "dorm_ref",   # var for the reference def/method
      covs = list(),            # list of (potential) covariate columns
      study_id = "id2",         # var for random intercepts; i.e. (1|study_id)
      add_intercept = T
    )
      
    # create crosswalk object called fit1
    fit1 <- CWModel(
      cwdata = dat1,               # result of CWData() function call
      obs_type = "diff_logit",     # must be "diff_logit" or "diff_log"
      cov_models = list(           # specify covariate details
        CovModel("intercept")),
      gold_dorm = "gold_standard"  # level of 'ref_dorms' that's the gold standard
    )
      
    # pull coefficients and other variables from crosswalk object, save to all_coeffs
    df_result <- data.table(fit1$create_result_df())
    df_result$age_group <- ag
    df_result <- df_result[dorms == topic, c("age_group","dorms","cov_names","beta","beta_sd","gamma")]
    df_result[is.na(gamma), gamma := 0]
    topic_coeffs <- rbind(topic_coeffs, df_result, fill = T)
    all_coeffs <- rbind(all_coeffs, df_result, fill = T)
  }
  df <- merge(df, topic_coeffs, by = "age_group", all.x = T)
    
  # adjust val and standard error in logit space
  df[,(topic) := logit_val - beta]
  df[,(paste0(topic,'_se')) := sqrt(logit_standard_error^2 + beta_sd^2 + gamma^2)]  
  df <- df[,-c("dorms","cov_names","beta","beta_sd","gamma")]
    
  # transform the adjusted val and standard error back into linear space 
  df[, c((topic),(paste0(topic,'_se')))] <- as.data.table(crosswalk002::delta_transform(mean = df[[(topic)]],
                                                                                       sd = df[[(paste0(topic,'_se'))]],
                                                                                       transformation = 'logit_to_linear'))
}
  

## 3. ADJUST IMPERFECT DATA -----------------------------------------------------------
  
# save original val and standard error values
df[,raw_val := val]
df[,raw_se := standard_error]
  
# adjust df according to which combination of missing need components applies
for (topic in topics) {
  df[counterfactual == topic, c('val', 'standard_error') := list(get(topic), get(paste0(topic, '_se')))]
}

# remove aggregate age microdata
df <- df[!is.na(age_group_id) | cv_report == 1]
  
# flag data which have been crosswalked
df[raw_val != val, cv_unmet_prop_xwalk := 1]


## 4. SAVE CROSSWALKED DATA -----------------------------------------------------------
  
# save lists of nid's whose standard errors need to be fixed
write.csv(small_se_nids, file.path(dir.root,'FILEPATH',paste0(global.me_name,'_small_se.csv')), row.names = F)
  
# save xwalk coefficients
write.csv(all_coeffs, file.path(dir.root,'FILEPATH',paste0(global.me_name,"_xwalk_coeffs.csv")), row.names =F)

