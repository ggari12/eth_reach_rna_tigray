###############################################################################

library(tidyverse)
library(srvyr)
library(supporteR)
library(analysistools)

source("R/composite_indicators.R")

# clean data
clean_loc_data <- "inputs/ETH24XX_RNA_Tigray_HH_cleaned_data.xlsx"

# main data
clean_data_nms <- names(readxl::read_excel(path = clean_loc_data, n_max = 300, sheet = "cleaned_main_data"))
clean_c_types <- ifelse(str_detect(string = clean_data_nms, pattern = "_other$"), "text", "guess")
df_main_clean_data <- readxl::read_excel(clean_loc_data, col_types = clean_c_types, sheet = "cleaned_main_data")

# loops
# roster
clean_data_nms_roster <- names(readxl::read_excel(path = clean_loc_data, n_max = 2000, sheet = "cleaned_roster_data"))
clean_c_types_roster <- ifelse(str_detect(string = clean_data_nms_roster, pattern = "_other$"), "text", "guess")
df_clean_roster_data <- readxl::read_excel(clean_loc_data, col_types = clean_c_types_roster, sheet = "cleaned_roster_data")

# individual to hh level ------------------------------------------------------
list_individual_to_hh <- list()

# hh_with_disabled_member
df_hh_with_disabled_member <- df_clean_roster_data %>% 
  create_composites_roster_loop() %>% 
  group_by(`_submission__uuid`) %>% 
  summarise(int.hh_disability_status = paste(i.disability, collapse = " : ")) %>% 
  mutate(i.hh_with_disabled_member = case_when(str_detect(string = int.hh_disability_status, pattern = "yes") ~ "Yes Disable HHM",
                                               !str_detect(string = int.hh_disability_status, pattern = "yes") ~ "No Disable HHM")) %>% 
  select(-int.hh_disability_status)

add_checks_data_to_list(input_list_name = "list_individual_to_hh", input_df_name = "df_hh_with_disabled_member")

# combine the calculated indicators
df_combined_hh_indicators_from_roster <- list_individual_to_hh %>%
  reduce(.f = full_join, by = '_uuid')

# data with composites --------------------------------------------------------
# main data with composites
df_data_with_composites <- df_main_clean_data %>% 
  left_join(df_combined_hh_indicators_from_roster, by = c("_uuid" = "_submission__uuid")) 

# run the following after update hh individual level
loop_support_data <- df_data_with_composites %>%
  select(`_uuid`, admin4, hh_situation, i.hh_with_disabled_member, i.hoh_gender_cat, i.hoh_age_cat)

df_clean_loop_roster <- loop_support_data %>%
  inner_join(df_clean_roster_data, by = c("_uuid" = "_submission__uuid")) 

# health
clean_data_nms_health <- names(readxl::read_excel(path = clean_loc_data, n_max = 2000, sheet = "cleaned_health_data"))
clean_c_types_health <- ifelse(str_detect(string = clean_data_nms_health, pattern = "_other$"), "text", "guess")
df_health_clean_data <- readxl::read_excel(clean_loc_data, col_types = clean_c_types_health, sheet = "cleaned_health_data")

df_clean_loop_health <- loop_support_data %>%  
  inner_join(df_health_clean_data, by = c("_uuid" = "_submission__uuid")) 

# tool
loc_tool <- "inputs/ETH24XX_RNA_Tigray_HH_tool.xlsx"
df_survey <- readxl::read_excel(loc_tool, sheet = "survey")
df_choices <- readxl::read_excel(loc_tool, sheet = "choices")

# loa // list of analysis
all_loa <- read_csv("inputs/r_loa_eth_rna.csv") 

#  analysis - main ------------------------------------------------------------
# main
df_main <- df_data_with_composites 

# survey object
main_svy <- as_survey(.data = df_main)

# loa
df_main_loa <- all_loa %>% 
  filter(dataset %in% c("main_data"))

# analysis
df_main_analysis <- analysistools::create_analysis(design = main_svy, 
                                                   loa = df_main_loa,
                                                   sm_separator = "/")

# analysis - roster -----------------------------------------------------------
# roster
df_roster <- df_clean_loop_roster 

# survey object
roster_svy <- as_survey(.data = df_roster)

# loa roster
df_roster_loa <- all_loa %>% 
  filter(dataset %in% c("roster"))

# analysis
df_roster_analysis <- analysistools::create_analysis(design = roster_svy, 
                                                     loa = df_roster_loa, 
                                                     sm_separator = "/")

# loops analysis - health ----------------------------------------------------
# health
df_health <- df_clean_loop_health 

# survey object - health
health_svy <- as_survey(.data = df_health)

# loa health
df_health_loa <- all_loa %>% 
  filter(dataset %in% c("health_ind"))

# analysis
df_health_analysis <- analysistools::create_analysis(design = health_svy, 
                                                     loa = df_health_loa, 
                                                     sm_separator = "/")

# analysis tables -------------------------------------------------------------
# combine the tables
df_combined_tables <- bind_rows(df_main_analysis$results_table %>% mutate(dataset = "main_data"),
                                df_roster_analysis$results_table %>% mutate(dataset = "roster"),
                                df_health_analysis$results_table %>% mutate(dataset = "health_ind")) %>% 
  filter(!(analysis_type %in% c("prop_select_one", "prop_select_multiple") & (is.na(analysis_var_value) | analysis_var_value %in% c("NA"))))

openxlsx::write.xlsx(x = df_combined_tables,
                     file = paste0("outputs/", butteR::date_file_prefix(), 
                                   "_ETH24XX_RNA_non_formatted_analysis_tables.xlsx"))

write_csv(df_combined_tables, paste0("outputs/non_formatted_analysis_ETH24XX_RNA.csv"), na = "")

###############################################################################   
