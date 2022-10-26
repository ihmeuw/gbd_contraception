# _Measuring contraceptive method mix, prevalence, and demand satisfied by age and marital status in 204 countries and territories, 1970–2019: a systematic analysis for the Global Burden of Disease Study 2019_

The scripts in this repository were used to produce the estimates published in the Lancet article *Measuring contraceptive method mix, prevalence, and demand satisfied by age and marital status in 204 countries and territories, 1970–2019: a systematic analysis for the Global Burden of Disease Study 2019*. Article and additional supplementary material accessible at https://www.thelancet.com/journals/lancet/article/PIIS0140-6736(22)00936-9/fulltext. 

### Order of Operations
1) Run *01_launch_prep_ubcov_data.R* to process all microdata and calculate contraceptive indicators of interest. Calls the following:
    - *01a_prep_ubcov_data.R* 
    - *01ab_prep_ubcov_data_calc.R*
2) Run *02_prep_counterfac.R* to run counterfactual extractions of gold standard data. Calls the following:
    -	*01ab_prep_ubcov_data_calc.R*
3) Run *03_collapse_microdata.R* to obtain tabulations of all individual-level microdata.
4) Run *04_launch_pipline.R* to combine microdata and report data, make adjustments to data, launch all ST-GPR models, and compile method mix results. Calls the following:
    - *04a_mergeon_reports.R*
    - *04b_prep_input_data.R*
    -- *04ba_marital_split.R*
    -- *04bb_xwalk_unmet_prop.R*
    -- *04bc_age_split.R*
    - *04c_launch_method_mix.R*
    -- *04ca_mergeon_reports_method_mix.R*
    -- *04cb_model_method_mix.R*
    - *04d_launch_compile_draws_method_mix.R*
    -- *04da_compile_draws_method_mix.R*
5)	Run *05_launch_compile_draws.R* to launch an array job for every CSV of location draws outputted by ST-GPR. Waits for the array job to finish, then produces final indicator files of mean, lower, and upper for every location-year-age group, plus aggregated global, super-region, region, and SDI quintile estimates. Calls the following:
    - *05a_compile_draws.R*