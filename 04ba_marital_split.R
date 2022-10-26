###########################################################################################################
## Purpose: Marital status splitting function to split all women estimates into married and unmarried
###########################################################################################################

#############################################################################################
##
## Description: split all women data with no marital status breakdown into married and unmarried 
##
##  REQUIRED INPUT:
##    data        -> data table of input data, must have:
##      me_name     -> name of modelable entity which pertains to the data
##      denominator -> indicator denominator/subgroup of women; options: all, users or nonusers
##
#############################################################################################

marital_split <- function(data, 
                          me_name = "",
                          denominator = "") {
  
  # runtime configuration
  if (Sys.info()["sysname"] == "Linux") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  # set-up
  id.vars <- c("location_id","year_id",'sex_id',"age_group_id")
  source(paste0(h,"FILEPATH"))  # sources get_stgpr_draws function
  
  # read in all women report data 
  tosplit <- fread(file.path(dir.root,"FILEPATH"))
  
  # end marital splitting if there is no all women data to split
  if (nrow(tosplit) == 0) return(data)
  
  # impute sample sizes where missing
  tosplit <- merge(tosplit, ss_impute, by = "age_group", all.x = T)
  tosplit[, sample_size := as.numeric(sample_size)]
  tosplit[is.na(sample_size), sample_size := ss_impute]
  tosplit[,ss_impute := NULL]
  
  # remove small sample sizes
  tosplit <- tosplit[sample_size >= 20]
  
  # offset values of 0 and 1 for proportions, impute standard error as these are all reports 
  tosplit[, standard_error := as.numeric(standard_error)]
  tosplit[val < offset, val := offset]
  tosplit[val > 1 - offset, val := 1 - offset]
  tosplit[is.na(standard_error), standard_error := sqrt(val*(1-val)/sample_size)]
  
  
  ## 1. CREATE NEW MARITAL STATUS ROWS --------------------------------------------------
  
  # give each row that needs to be split an id
  tosplit[,split.id := .I]
  
  # expand each survey into two rows, one for unmarried and married data
  expanded <- data.table(split.id = rep(tosplit$split.id, 2), age_group = rep(tosplit$age_group, 2), marital_group = c(rep("unmarried",nrow(tosplit)),rep("married",nrow(tosplit))))
  tosplit <- merge(expanded, tosplit[,-c('marital_group')], by = c('split.id','age_group'))
  
  
  ## 2. EXPAND AGE-AGGREGATE DATA -------------------------------------------------------
  
  # in order to marital status-split age-aggregated data we need to also aggregate population and the estimate
  # of interest to match the aggregated age group, so separating these estimates in order to do those calculations 
  
  # separate data which pertains to age-aggregate estimates as those will require extra steps
  agg_tosplit <- tosplit[is.na(age_group_id)] 
  tosplit <- tosplit[!is.na(age_group_id)]
  
  # if there is no age-aggregated data to expand, carry on
  if (nrow(agg_tosplit) != 0) {
    
    # give each row an id and determine how many age groups are contained in each row
    agg_tosplit[,reps := (age_end + 1 - age_start) / 5]
    
    # expand each survey by the number of age groups and determine the new age_group_ids 
    # age_group/age_start/age-end will not change to preserve original values; keying off only age_group_id for remainder of script
    expanded <- data.table(split.id = rep(agg_tosplit$split.id, agg_tosplit$reps), age_start = rep(agg_tosplit$age_start, agg_tosplit$reps), marital_group = rep(agg_tosplit$marital_group, agg_tosplit$reps))
    expanded[,split_id := seq(1,.N), by=c("split.id","marital_group")]
    expanded[,age_start := age_start + 5 * (split_id - 1)]
    expanded[,age_group_id := (age_start/5) + 5]
    agg_tosplit <- merge(agg_tosplit[,-c("reps","age_group_id")], expanded[,-c("age_start")], by = c("split.id","marital_group"))
    
    # flag aggregate data and append back on to the rest of data to be split
    agg_tosplit[,agg_data := 1]
    tosplit <- rbind(tosplit, agg_tosplit, fill = T)
  }
  
  
  ## 3. MERGE ON POPULATION -------------------------------------------------------------

  # pull populations for every year and location of data that need to be split
  pops <- get_population(year_id = unique(tosplit$year_id), sex_id = 2, 
                         location_id = unique(tosplit$location_id), 
                         age_group_id = unique(tosplit$age_group_id),
                         release_id = release) %>% as.data.table()
  pops$run_id <- NULL
  
  # get marital status draws 
  marital <- get_stgpr_draws(marital_status_final)
  draw_cols <- names(marital)[grepl("draw_[0-9]*", names(marital))]
  
  # subset population to match the denominator of the indicator being modeled 
  # i.e. for mod_prop_mar the denominator is only married/in-union women using any method of contraception 
  
  # merge marital status estimates with population  
  pops <- merge(pops, marital, by = id.vars)
  pops <- melt.data.table(pops, id.vars = c(id.vars,"population"), variable.name = "draw.id", value.name = "marital_status")
  
  # if age-splitting mod_prop or unmet_prop need to subset denominator to women using/not using any contraception  
  if (denominator %in% c("users", "nonusers")) {
    
    any_contra_mar <- get_stgpr_draws(any_contra_mar_prelim, melt = T)
    setnames(any_contra_mar, "model.result", "any_contra_mar")
    
    any_contra_unmar <- get_stgpr_draws(any_contra_unmar_prelim, melt = T)
    setnames(any_contra_unmar, "model.result", "any_contra_unmar")
    
    pops <- merge(pops, any_contra_mar, by = c(id.vars, "draw.id"))
    pops <- merge(pops, any_contra_unmar, by = c(id.vars, "draw.id"))
    
    if (denominator == "users") {
      pops[, pop_mar := population * marital_status * any_contra_mar]
      pops[, pop_unmar := population * (1 - marital_status) * any_contra_unmar]
    } else {
      pops[, pop_mar := population * marital_status * (1-any_contra_mar)]
      pops[, pop_unmar := population * (1 - marital_status) * (1 - any_contra_unmar)]
    }
    
  # if age-splitting an all women indicator regardless of user status no need to further subset the denominator 
  } else {
    
    pops[, pop_mar := population * marital_status]
    pops[, pop_unmar := population * (1 - marital_status)]
    
  }
  
  pops <- pops[,.(pop_mar = mean(pop_mar),pop_unmar = mean(pop_unmar)),by=id.vars]
  
  pops <- melt.data.table(data = pops, id.vars = id.vars, 
                          variable.name = 'marital_group', value.name = 'population')
  pops[, marital_group := ifelse(marital_group == 'pop_mar', "married", "unmarried")]
  
  # merge pops on to the table of data to be split
  tosplit <- merge(tosplit, pops, by = c(id.vars, "marital_group"), all.x = T)
  
  # calculate population totals for each split.id
  tosplit[,poptotal := sum(population), by = "split.id"]
  tosplit[,population := sum(population), by = c("marital_group", "split.id")]
  
  
  ## 4. PREPARE DRAWS ---------------------------------------------------------
  
  # create an expand id for tracking the draws, so 1000 draws for every expand id
  tosplit[,expand_id := .I, by = c("split.id","marital_group")]
  
  # delta transform mean and standard error into logit space
  # we want to split the all women data in logit space to ensure points stay between 0 and 1
  tosplit$mean_logit <- log(tosplit$val / (1- tosplit$val))
  tosplit$standard_error_logit <- sapply(1:nrow(tosplit), function(i) {
    mean_i <- as.numeric(tosplit[i, "val"])
    se_i <- as.numeric(tosplit[i, "standard_error"])
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })

  # pull preliminary model draws and merge onto dataset
  draws_mar <- get_stgpr_draws(param_map[modelable_entity_name == gsub('mar_final', 'mar_prelim', me_name)]$latest)
  draws_mar[, marital_group := "married"]
  draws_unmar <- get_stgpr_draws(param_map[modelable_entity_name == gsub('mar_final', 'unmar_prelim', me_name)]$latest)
  draws_unmar[, marital_group := "unmarried"]
  draws <- rbind(draws_mar, draws_unmar)
  
  draws <- merge(tosplit, draws, by = c(id.vars, "marital_group"))
  
  # aggregate draws for aggregate-age input data
  if ("agg_data" %in% names(draws)) draws[agg_data == 1, (draw_cols) := lapply(.SD, function(x) {sum(x * population / sum(population))}), .SDcols = draw_cols, by = c("marital_group","split.id")]
  
  # remove extra rows used to aggregate data
  draws <- unique(draws, by = c("marital_group","split.id"))
  
  # melt draws column so there is a column called draw.id with values from "draw_0" through "draw_999"
  # and a column called model.result which contains the actual draw value
  draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw_[0-9]*", names(draws))], measure.vars = patterns("draw_[0-9]*"),
                           variable.name = "draw.id", value.name = "model.result")
  
  # SAMPLE FROM THE RAW INPUT DATA: 
  # Save a dataset of 1000 rows per every input data point that needs to be split.
  # Keep columns for mean and standard error, so that you now have a dataset with 
  # 1000 identical copies of the mean and standard error of each input data point.
  orig.data.to.split <- unique(draws[, .(split.id, draw.id, mean_logit, standard_error_logit)])
  
  # Generate 1000 draws from the input data (assuming a normal distribution), and replace the identical means
  # in orig.data.to.split with draws of that mean
  
  # split the data table into a list, where each data point to be split is now its own data table 
  orig.data.to.split <- split(orig.data.to.split, by = "split.id")
  
  # then apply this function to each of those data tables in that list
  orig.data.to.split <- lapply(orig.data.to.split, function(input_i){
    
    mean.vector <- input_i$mean_logit
    se.vector <- input_i$standard_error_logit
    # Generate a random draw for each mean and se 
    set.seed(123)
    input.draws <- rnorm(length(mean.vector), mean.vector, as.numeric(se.vector))
    input_i[, input.draw := input.draws]
    
    return(input_i)
  })
  
  orig.data.to.split <- rbindlist(orig.data.to.split)
  
  # Now each row of the dataset draws has a random draw from the distribution N(mean, SE) of the original
  # data point in that row. The mean of these draws will not be exactly the same as the input data point 
  # mean that it was randomly sampled from (because the sampling is random and the standard error can be
  # large).
  draws <- merge(draws, orig.data.to.split[,.(split.id, draw.id, input.draw)], by = c('split.id','draw.id'))
  
  
  ## 5. CALCULATE WEIGHTS AND DRAW SPLITS -----------------------------------------------
  
  # Calculate count of cases for a specific age-loc-yr
  # based on the modeled prevalence
  draws[, numerator := model.result * population]
  
  # Calculate count of cases across all the age/sex groups that cover an original aggregate data point, 
  # based on the modeled prevalence.
  # (The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point)
  draws[, denominator := sum(numerator), by = .(split.id, draw.id)]
  
  # Calculate the weight for a specific age/sex group as: model prevalence of specific age/sex group (logit_split) divided by 
  # model prevalence of aggregate age/sex group (logit_aggregate)
  draws[, logit_split := log(model.result / (1 - model.result))]
  draws[, logit_aggregate := log((denominator / poptotal) / (1 - (denominator / poptotal)))]
  draws[, logit_weight := logit_split - logit_aggregate]
    
  # Apply the weight to the original mean (in draw space and in logit space)
  draws[, logit_estimate := input.draw + logit_weight]
  
  # calculate new sample sizes according to the marital-age-population distribution
  draws[, sample_size_new := sample_size * population / poptotal]
  
  # Save weight in linear space to use in numeric check (recalculation of original data point)
  draws <- draws[, weight := model.result / (denominator / poptotal)]
  
  
  ## 6. COLLAPSE DRAW SPLITS INTO FINAL ESTIMATES ----------------------------------------------
  
  # Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
  # Each expand ID has 1000 draws associated with it, so just take summary statistics by expand ID
  final <- draws[, .(mean.est = mean(logit_estimate),
                     sd.est = sd(logit_estimate),
                     upr.est = quantile(logit_estimate, .975),
                     lwr.est = quantile(logit_estimate, .025),
                     sample_size_new = unique(sample_size_new),
                     cases.est = mean(numerator),
                     orig.cases = mean(denominator),
                     orig.standard_error = unique(standard_error),
                     mean.input.draws = mean(input.draw),
                     mean.weight = mean(weight),
                     mean.logit.weight = mean(logit_weight)), by = expand_id] %>% merge(tosplit, by = "expand_id")
  
  # Convert mean and SE back to linear space 
  if (measure == "proportion") {
    final$sd.est <- sapply(1:nrow(final), function(i) {
      mean_i <- as.numeric(final[i, "mean.est"])
      mean_se_i <- as.numeric(final[i, "sd.est"])
      deltamethod(~exp(x1) / (1 + exp(x1)), mean_i, mean_se_i^2)
    })
    final[,mean.est := exp(mean.est) / (1 + exp(mean.est))]
    final[,lwr.est := exp(lwr.est) / (1 + exp(lwr.est))]
    final[,upr.est := exp(upr.est) / (1 + exp(upr.est))]
    final[,mean.input.draws := exp(mean.input.draws) / (1 + exp(mean.input.draws))]
  }
  
  # column clean up
  final[,se.est := sd.est]
  final[,orig.sample.size := sample_size]
  final[,sample_size := sample_size_new]
  final[,sample_size_new := NULL]
  
  final[, case_weight := cases.est / orig.cases]
  final$orig.cases <- NULL
  final$standard_error <- NULL
  setnames(final, c("val"), c("orig_val"))
  setnames(final, c("mean.est", "se.est"), c("val", "standard_error"))
  
  # remove inputted age_group_id from aggregate data otherwise will not be identified for age-splitting 
  if ("agg_data" %in% names(final)) final[agg_data == 1, age_group_id := NA]
  
  
  ## 7. SAVE SPLIT DATA -------------------------------------------------------------
  
  if ("agg_data" %in% names(final)) {
    split_data <- final[, c('split.id','marital_group','nid','sex_id','val','ihme_loc_id',
                            'standard_error','sample_size', 'age_start', 'age_end',
                            'orig_val','orig.standard_error','orig.sample.size',
                            'population','poptotal','age_group_id','agg_data',
                            'location_id', 'year_start','year_end','year_id')]
  } else {
    split_data <- final[, c('split.id','marital_group','nid','sex_id','val','ihme_loc_id',
                            'standard_error','sample_size', 'age_start', 'age_end',
                            'orig_val','orig.standard_error','orig.sample.size',
                            'population','poptotal','age_group_id',
                            'location_id', 'year_start','year_end','year_id')]
  }
  
  split_data <- split_data[order(split.id)]
  
  write.csv(split_data, file.path(dir.root, "FILEPATH"), row.names = F)
  
  
  ## 8. APPEND SPLIT DATA BACK ONTO AGE-SPECIFIC DATA -------------------------------
  
  final <- final[,names(final) %in% names(data), with = FALSE]
  final[, cv_maritalsplit := 1]
  
  write.csv(final, file.path(dir.root, "FILEPATH"), row.names = F)
  
  # remove any duplicates before adding split data back to main dataset
  final <- final[marital_group == "married" & paste(survey_id, age_group) %ni% data[, paste(survey_id, age_group)]]
  data <- rbind(data, final[marital_group == "married"], fill = T)
  
  return(data)
}
