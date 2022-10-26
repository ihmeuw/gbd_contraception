###########################################################################################################
## Purpose: Age-splitting function to split age aggregated into corresponding 5-year age bins
###########################################################################################################

#############################################################################################
##
## Description: age-split data in logit space that spans multiple GBD age groups using preliminary ST-GPR draws 
##
##  REQUIRED INPUT:
##    data        -> data table of input data, must have...
##      me_name     -> name of modelable entity which pertains to the data
##      denominator -> indicator denominator/subgroup of women; options: all, users or nonusers
##
#############################################################################################

age_split <- function(data,
                      me_name,
                      denominator) {
  
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
  
  # offset extreme values from crosswalking and fill in missing standard_error's
  data[, standard_error := as.numeric(standard_error)]
  data[val < offset, val := offset]
  data[val > 1 - offset, val := 1 - offset]
  data[is.na(standard_error), standard_error := sqrt(val*(1-val)/sample_size)]
 
  # separate out data to be age-split and age-specific data
  tosplit <- data[is.na(age_group_id)]
  good_data <- data[!is.na(age_group_id)]
  
  # if there is no data to split, end
  if (nrow(tosplit) == 0) return(data)
  
  
  ## 1. CREATE NEW AGE GROUP ROWS -------------------------------------------------------
  
  # give each row an id and determine how many splits need to occur
  tosplit[, split.id := .I]
  tosplit[, reps := (age_end + 1 - age_start) / 5]
  
  # check for surveys that do not require age-splitting
  tosplit[reps == 1, unique(nid)] 
  
  # expand each survey by the number of splits and determine the new age_group_ids
  expanded <- data.table(split.id = rep(tosplit$split.id, tosplit$reps), age_start = rep(tosplit$age_start, tosplit$reps))
  expanded[, split_id := seq(1,.N), by = split.id]
  expanded[, age_start := age_start + 5 * (split_id - 1)]
  expanded[, age_end := age_start + 4]
  expanded[, age_group_id := (age_start / 5) + 5]
  tosplit <- merge(tosplit[,-c("age_start","age_end","reps","age_group_id")], expanded, by = "split.id")
  
  
  ## 2. MERGE ON POPULATION ------------------------------------------------------------
  
  # pull populations for every year and location of data that need to be split
  pops <- get_population(year_id = unique(tosplit$year_id), sex_id = 2, 
                         location_id = unique(tosplit$location_id), 
                         age_group_id = unique(tosplit$age_group_id),
                         release_id = release) %>% as.data.table()
  pops$run_id <- NULL
  
  # subset population to match the denominator of the indicator being modelled; marital_status is all women no subsetting needed
  # i.e. for mod_prop_mar the denominator is only married/in-union women using any method of contraception 
  if (!grepl('marital_status', me_name)) {
    
    # get marital status draws
    marital <- get_stgpr_draws(marital_status_final)
    draw_cols <- names(marital)[grepl("draw_[0-9]*", names(marital))]
    
    # subset to women of a particular marital status
    if (grepl('_mar', me_name)) {
      
      # use married any contra estimates 
      any_contra_id <- any_contra_mar_prelim
      
    } else if (grepl('unmar', me_name)) {
      
      # use unmarried any contra estimates 
      any_contra_id <- any_contra_unmar_prelim
      
      # adjust marital status estimates to reflect proportion unmarried  
      marital[, (draw_cols) := (1 - .SD), .SDcols = draw_cols]
    }
    
    # merge marital status estimates with population  
    pops <- merge(pops, marital, by = id.vars)
    pops <- melt.data.table(pops, id.vars = c(id.vars,"population"), variable.name = "draw.id", value.name = "marital_status")
    
    # if age-splitting mod_prop or unmet_prop need to subset denominator to women using/not using any contraception  
    if (denominator %in% c("users", "nonusers")) {
      
      any_contra <- get_stgpr_draws(any_contra_id, melt = T)
      setnames(any_contra, "model.result", "any_contra")
      
      pops <- merge(pops, any_contra, by = c(id.vars, "draw.id"))
      
      if (denominator == "users") {
        pops[, population := population * marital_status * any_contra]
      } else {
        pops[, population := population * marital_status * (1-any_contra)]
      }
      
      pops <- pops[,.(population = mean(population)), by = id.vars]
      
      # if age-splitting any_contra no need to further subset the denominator 
    } else {
      
      pops[, population := population * marital_status]
      
      pops <- pops[,.(population = mean(population)), by = id.vars]
      
    } 
  }
  
  # merge pops on to the table of data to be split
  tosplit <- merge(tosplit, pops[,c("location_id","year_id",'sex_id',"age_group_id","population")], by = id.vars, all.x = T)
  
  # calculate the total population within each survey that pertains to the aggregate age group
  tosplit[, poptotal := sum(population), by = split.id]
  

  ## 3. PREPARE DRAWS ----------------------------------------------------
  
  # create an expand id for tracking the draws, so 1000 draws for every expand id
  tosplit[, expand_id := .I]
  
  # delta transform mean and standard error into logit space
  # we want to split the aggregate-age data in logit space to ensure points stay between 0 and 1
  tosplit$mean_logit <- log(tosplit$val / (1- tosplit$val))
  tosplit$standard_error_logit <- sapply(1:nrow(tosplit), function(i) {
    mean_i <- as.numeric(tosplit[i, "val"])
    se_i <- as.numeric(tosplit[i, "standard_error"])
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
    
  # pull preliminary model draws and merge onto dataset
  draws <- get_stgpr_draws(param_map[modelable_entity_name == gsub('_final','_prelim',me_name)]$latest)
  draws <- merge(tosplit, draws, by = id.vars, all.x = T)
  
  # melt draws column so there is a column called draw.id with values from "draw_0" through "draw_999"
  # and a column called model.result which contains the actual draw value
  draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw_[0-9]*", names(draws))], measure.vars = patterns("draw_[0-9]*"),
                           variable.name = "draw.id", value.name = "model.result")
  
  # offset extreme model values 
  if (measure == "proportion") {
    draws[model.result < offset, model.result := offset]
    draws[model.result > 1 - offset, model.result := 1 - offset]
  }
  
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
  
  
  ## 4. CALCULATE AGE WEIGHTS AND DRAW SPLITS -----------------------------------------------
  
  # Calculate count of cases for a specific age-sex-loc-yr
  # based on the modeled prevalence
  draws[, numerator := model.result * population]
  
  # Calculate count of cases across all the age/sex groups that cover an original aggregate data point, 
  # based on the modeled prevalence.
  # (The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point)
  draws[, denominator := sum(numerator), by = .(split.id, draw.id)]
  
  # calculate the weight for a specific age/sex group as: model prevalence of specific age/sex group (logit_split) divided by 
  # model prevalence of aggregate age/sex group (logit_aggregate)
  draws[, logit_split := log(model.result / (1 - model.result))]
  draws[, logit_aggregate := log((denominator / poptotal) / (1 - (denominator / poptotal)))]
  draws[, logit_weight := logit_split - logit_aggregate]
  
  # apply the weight to the original mean (in draw space and in logit space)
  draws[, logit_estimate := input.draw + logit_weight]
  
  # calculate new age group sample sizes according to the age-marital-population distribution
  draws[, sample_size_new := sample_size * population / poptotal]
  
  # save weight in linear space to use in numeric check (recalculation of original data point)
  draws <- draws[, weight := model.result / (denominator / poptotal)]
  
  
  ## 5. COLLAPSE DRAW SPLITS INTO FINAL ESTIMATES ----------------------------------------------
  
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
  final$sd.est <- sapply(1:nrow(final), function(i) {
    mean_i <- as.numeric(final[i, "mean.est"])
    mean_se_i <- as.numeric(final[i, "sd.est"])
    deltamethod(~exp(x1) / (1 + exp(x1)), mean_i, mean_se_i^2)
  })
  final[,mean.est := exp(mean.est) / (1 + exp(mean.est))]
  final[,lwr.est := exp(lwr.est) / (1 + exp(lwr.est))]
  final[,upr.est := exp(upr.est) / (1 + exp(upr.est))]
  final[,mean.input.draws := exp(mean.input.draws) / (1 + exp(mean.input.draws))]
  
  # column clean up
  final[, se.est := sd.est]
  final[, orig.sample.size := sample_size]
  final[, sample_size := sample_size_new]
  final[, sample_size_new := NULL]
  
  final[, case_weight := cases.est / orig.cases]
  final$orig.cases <- NULL
  final$standard_error <- NULL
  setnames(final, c("val"), c("orig_val"))
  setnames(final, c("mean.est", "se.est"), c("val", "standard_error"))
  
  
  ## 6. SAVE SPLIT DATA -------------------------------------------------------------
  
  split_data <- final[, c('split.id', 'nid','sex_id','val', 'ihme_loc_id',
                          'standard_error','sample_size', 'age_start', 'age_end',
                          'orig_val','orig.standard_error','orig.sample.size',
                          'population','poptotal','age_group_id',
                          'location_id', 'year_start','year_end')]
  split_data <- split_data[order(split.id)]
  
  write.csv(split_data, file.path(dir.root, 'FILEPATH'), row.names = F)

  
  ## 7. APPEND SPLIT DATA BACK ONTO AGE-SPECIFIC DATA -------------------------------
  
  final <- final[, names(final) %in% names(good_data), with = FALSE]
  final[, cv_agesplit := 1]
  
  # remove any duplicates before adding split data back to main dataset
  final <- final[paste(survey_id, age_group_id) %ni% good_data[, paste(survey_id, age_group_id)]]
  data <- rbind(good_data, final, fill = T)
  
  return(data)
}
  
  