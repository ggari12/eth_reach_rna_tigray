###############################################################################
# checks for data collection

# read packages
library(tidyverse)
library(cleaningtools)
library(httr)
library(sf)
library(glue)
library(supporteR)
library(openxlsx)
library(cluster)

source("R/support_functions.R")
source("R/composite_indicators.R")
source("support_files/credentials.R")

# read data -------------------------------------------------------------------

data_path <- "inputs/ETH24XX_RNA_Tigray_HH_data.xlsx" 

# main data
data_nms <- names(readxl::read_excel(path = data_path, n_max = 300))
c_types <- ifelse(str_detect(string = data_nms, pattern = "_other$"), "text", "guess")
df_tool_data <- readxl::read_excel(data_path, col_types = c_types) |> 
create_composite_indicators() |> 
  rowwise() |> 
  ungroup()

# loops -----------------------------------------------------------------------
# roster
data_nms_roster <- names(readxl::read_excel(path = data_path, n_max = 2000, sheet = "roster"))
c_types_roster <- ifelse(str_detect(string = data_nms_roster, pattern = "_other$"), "text", "guess")
df_loop_roster <- readxl::read_excel(data_path, col_types = c_types_roster, sheet = "roster")

# joining roster loop to main sheet
df_repeat_hh_roster_data <- df_tool_data %>% 
  left_join(df_loop_roster, by = c("_uuid" = "_submission__uuid"))

# health
data_nms_health <- names(readxl::read_excel(path = data_path, n_max = 2000, sheet = "health_ind"))
c_types_health <- ifelse(str_detect(string = data_nms_health, pattern = "_other$"), "text", "guess")
df_loop_health <- readxl::read_excel(data_path, col_types = c_types_health, sheet = "health_ind")

# tool
loc_tool <- "inputs/ETH24XX_RNA_Tigray_HH_tool.xlsx"
df_survey <- readxl::read_excel(loc_tool, sheet = "survey") 
df_choices <- readxl::read_excel(loc_tool, sheet = "choices")

# download audit files folder
download_audit_files(df= df_tool_data,
                     uuid_column = "_uuid",
                     audit_dir = "inputs/audit_files",
                     usr = user_acc,
                     pass = user_pss)

# zip audit files folder

if (dir.exists("inputs/audit_files")) {
  zip::zip(zipfile = "inputs/audit_files.zip",
           files = list.dirs(path = "inputs/audit_files/", full.names = TRUE, recursive = FALSE),
           mode = "cherry-pick")
}

# check pii ---------------------------------------------------------------
pii_cols <- c("telephone", "contact", "name", "gps", "latitude", "longitude", "contact", "geopoint")
pii_from_data <- cleaningtools::check_pii(dataset = df_tool_data, 
                                          element_name = "checked_dataset", 
                                          uuid_column = "_uuid")
pii_from_data$potential_PII

# duration ----------------------------------------------------------------
# read audit file
audit_list_data <- cleaningtools::create_audit_list(audit_zip_path = "inputs/audit_files.zip")

# add duration from audit
df_tool_data_with_audit_time <- cleaningtools::add_duration_from_audit(dataset = df_tool_data, 
                                                                       uuid_column = "_uuid", 
                                                                       audit_list = audit_list_data)

# outliers columns not to check
outlier_cols_not_4_checking <- df_tool_data %>% 
  select(matches("_hh_location_latitude|_hh_location_longitude|_hh_location_altitude|_hh_location_precision|_index|_submit|submission|_sample_|^_id$")) %>% 
  colnames()

# logical checks data ----------------------------------------------------
df_list_logical_checks <- read_csv("support_files/rna_logical_check_list.csv") %>% 
  filter(!is.na(check_id))

# combine cleaning tools checks
list_log <- df_tool_data_with_audit_time %>%
  check_duration(column_to_check = "duration_audit_sum_all_minutes",
                 uuid_column = "_uuid",
                 log_name = "duration_log",
                 lower_bound = 20,
                 higher_bound = 90) %>% 
  check_outliers(uuid_column = "_uuid", sm_separator = "/",
                 strongness_factor = 3, columns_not_to_check = outlier_cols_not_4_checking) %>% 
  check_soft_duplicates(kobo_survey = df_survey,
                        uuid_column = "_uuid",
                        idnk_value = "dnk",
                        sm_separator = "/",
                        log_name = "soft_duplicate_log",
                        threshold = 7,
                        return_all_results = FALSE) %>%
  check_value(uuid_column = "_uuid", values_to_look = c(99, 999, 999, 88, 888, 888)) %>% 
  check_logical_with_list(uuid_column = "_uuid",
                          list_of_check = df_list_logical_checks,
                          check_id_column = "check_id",
                          check_to_perform_column = "check_to_perform",
                          columns_to_clean_column = "columns_to_clean",
                          description_column = "description",
                          bind_checks = TRUE)

# others checks ---------------------------------------------------------------
df_other_checks <- cts_other_specify(input_tool_data = df_tool_data, 
                                     input_uuid_col = "_uuid", 
                                     input_survey = df_survey, 
                                     input_choices = df_choices)
# add other checks to the list
list_log$other_log <- df_other_checks

# repeat_other_specify --------------------------------------------------------
df_repeat_other_checks <- cts_other_specify_repeats(input_repeat_data = df_loop_health,
                                                    input_uuid_col = "_submission__uuid",
                                                    input_survey = df_survey,
                                                    input_choices = df_choices,
                                                    input_sheet_name = "health_ind",
                                                    input_index_col = "_index")

# add repeat other checks to the list
list_log$repeat_other_log <- df_repeat_other_checks

# testing data ----------------------------------------------------------------
df_testing_data_check <- df_tool_data %>%
  filter(start < as_date("2024-11-23")) %>% 
  mutate(i.check.uuid = `_uuid`,
         i.check.change_type = "remove_survey",
         i.check.question = "",
         i.check.old_value = "",
         i.check.new_value = "",
         i.check.issue = "testing_data",
         i.check.description = "logic_c_testing_data",
         i.check.other_text = "",
         i.check.comment = "",
         i.check.reviewed = "1",
         i.check.so_sm_choices = "") %>% 
  batch_select_rename() 

list_log$df_testing_data_log <- df_testing_data_check

# no consent -----------------------------------------------------------------
df_no_consent_check <- df_tool_data %>% 
  filter(consent == "no") %>%  
  mutate(i.check.uuid = `_uuid`,
         i.check.change_type = "remove_survey",
         i.check.question = "",
         i.check.old_value = "",
         i.check.new_value = "",
         i.check.issue = "no_consent",
         i.check.description = "logic_c_requirement_no_consent",
         i.check.other_text = "",
         i.check.comment = "",
         i.check.reviewed = "1",
         i.check.so_sm_choices = "") %>% 
  batch_select_rename() 

# add other checks to the list
list_log$df_no_consent_log <- df_no_consent_check

# check duplicate uuids ---------------------------------------------------
df_duplicate_uuids <- cts_checks_duplicate_uuids(input_tool_data = df_tool_data)
list_log$duplicate_uuid_log <- df_duplicate_uuids

# other logical checks --------------------------------------------------------
# HH size and roster count mismatch
df_logic_hh_size_roster_mismatch <- df_repeat_hh_roster_data %>%
  group_by(`_uuid`) %>%
  summarise(roster_count = n(), hh_size = unique(hh_size)) %>%
  filter(hh_size != roster_count) %>%
  mutate(i.check.uuid = `_uuid`,
         i.check.change_type = "verify_data",
         i.check.question = "hh_size",
         i.check.old_value = as.character(hh_size),
         i.check.new_value = "",
         i.check.issue = "hh_size_and_roster_count_mismatch",
         i.check.description = "",
         i.check.other_text = "",
         i.check.comment = "",
         i.check.reviewed = "",
         i.check.so_sm_choices = "") %>%
  batch_select_rename()

# Add the check to the list
list_log$df_logic_hh_size_roster_mismatch_log <- df_logic_hh_size_roster_mismatch

# Health services are not accessible to persons with disabilities, 
# but all washington group indicators say no difficulty 
df_logic_c_barriers_but_wgq_no_difficulty <- df_repeat_hh_roster_data %>% 
  filter(health_barriers %in% c("health_services_are_not_accessible_to_persons_with_disabilities"),
         wgq_vision %in% c("no_difficulty"),
         wgq_hearing %in% c("no_difficulty"),
         wgq_mobility %in% c("no_difficulty"),
         wgq_cognition %in% c("no_difficulty"),
         wgq_self_care %in% c( "no_difficulty"),
         wgq_communication %in% c( "no_difficulty")) %>%
  mutate(i.check.uuid = `_uuid`,
         i.check.change_type = "change_response",
         i.check.question = "health_barriers",
         i.check.old_value = as.character(health_barriers),
         i.check.new_value = "",
         i.check.issue = "logic_c_barriers_but_wgq_no_difficulty",
         i.check.description = "",
         i.check.other_text = "",
         i.check.comment = "",
         i.check.reviewed = "",
         i.check.so_sm_choices = "") %>% 
  batch_select_rename()

# add checks to the list
list_log$df_logic_c_barriers_but_wgq_no_difficulty_log <- df_logic_c_barriers_but_wgq_no_difficulty

# HH reports difficulty walking, but main transportation mode to nearest health post is walking.
df_logic_c_hh_reports_difficulty_walking_but_walking_hp <- df_repeat_hh_roster_data %>% 
  filter(wgq_mobility == "some_difficulty" |
         wgq_mobility == "lot_of_difficulty" |
         wgq_mobility == "cannot_do",
         health_post_transportation == "walking") %>%
  mutate(i.check.uuid = `_uuid`,
         i.check.change_type = "change_response",
         i.check.question = "wgq_mobility",
         i.check.old_value = as.character(wgq_mobility),
         i.check.new_value = "",
         i.check.issue = "hh_reports_difficulty_walking_but_walking_health_post",
         i.check.description = "",
         i.check.other_text = "",
         i.check.comment = "",
         i.check.reviewed = "",
         i.check.so_sm_choices = "") %>% 
  batch_select_rename()

# add checks to the list
list_log$df_logic_c_hh_reports_difficulty_walking_but_walking_hp_log <- df_logic_c_hh_reports_difficulty_walking_but_walking_hp

# HH reports difficulty walking, but main transportation mode to nearest health centre is walking.
df_logic_c_hh_reports_difficulty_walking_but_walking_hc <- df_repeat_hh_roster_data %>% 
  filter(wgq_mobility == "some_difficulty" |
         wgq_mobility == "lot_of_difficulty" |
         wgq_mobility == "cannot_do",
         health_centre_transportation == "walking") %>%
  mutate(i.check.uuid = `_uuid`,
         i.check.change_type = "change_response",
         i.check.question = "wgq_mobility",
         i.check.old_value = as.character(wgq_mobility),
         i.check.new_value = "",
         i.check.issue = "hh_reports_difficulty_walking_but_walking_health_centre",
         i.check.description = "",
         i.check.other_text = "",
         i.check.comment = "",
         i.check.reviewed = "",
         i.check.so_sm_choices = "") %>% 
  batch_select_rename()

# add checks to the list
list_log$df_logic_c_hh_reports_difficulty_walking_but_walking_hc_log <- df_logic_c_hh_reports_difficulty_walking_but_walking_hc

# HH reports difficulty walking, but main transportation mode to nearest hospital is walking.
df_logic_c_hh_reports_difficulty_walking_but_walking_hospital <- df_repeat_hh_roster_data %>% 
  filter(wgq_mobility == "some_difficulty" |
         wgq_mobility == "lot_of_difficulty" |
         wgq_mobility == "cannot_do",
         hospital_transportation == "walking") %>%
  mutate(i.check.uuid = `_uuid`,
         i.check.change_type = "change_response",
         i.check.question = "wgq_mobility",
         i.check.old_value = as.character(wgq_mobility),
         i.check.new_value = "",
         i.check.issue = "hh_reports_difficulty_walking_but_walking_hospital",
         i.check.description = "",
         i.check.other_text = "",
         i.check.comment = "",
         i.check.reviewed = "",
         i.check.so_sm_choices = "") %>% 
  batch_select_rename()

# add checks to the list
list_log$df_logic_c_hh_reports_difficulty_walking_but_walking_hospital_log <- df_logic_c_hh_reports_difficulty_walking_but_walking_hospital

# log 999
cols_with_integer_values <- df_survey  %>% filter(type %in% c("integer"))  %>% pull(name)

df_999_data <- purrr::map_dfr(.x = cols_with_integer_values,
                              .f = ~ {df_repeat_hh_roster_data  %>% 
                                  dplyr::filter(str_detect(string = !!sym(.x), pattern = "^-[9]{2,4}$|^[9]{2,4}$")) %>%
                                  dplyr::mutate(i.check.type = "change_response",
                                                i.check.question = .x,
                                                i.check.old_value = as.character(!!sym(.x)),
                                                i.check.new_value = "",
                                                i.check.issue = " logic_c_handle_999",
                                                i.check.description = "",
                                                i.check.other_text = "",
                                                i.check.comment = "",
                                                i.check.reviewed = "",
                                                i.check.so_sm_choices = "") %>% 
                                  dplyr::select(starts_with("i.check"))}) %>% 
                                  rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))

# add checks to the list
list_log$logic_c_handle_999_log <- df_999_data

# log 999
cols_with_text_values <- df_survey %>% filter(type %in% c("text"), name %in% colnames(df_tool_data)) %>% pull(name)

df_999_data_other <- purrr::map_dfr(.x = cols_with_text_values, 
                                    .f = ~ {df_tool_data  %>% 
                                  dplyr::filter(str_detect(string = !!sym(.x), pattern = "^-[9]{2,4}$|^[9]{2,4}$")) %>%
                                  dplyr::mutate(i.check.type = "change_response",
                                                i.check.question = .x,
                                                i.check.old_value = as.character(!!sym(.x)),
                                                i.check.new_value = "",
                                                i.check.issue = "logic_c_handle_999_other",
                                                i.check.description = "",
                                                i.check.other_text = "",
                                                i.check.comment = "",
                                                i.check.reviewed = "",
                                                i.check.so_sm_choices = "") %>% 
                                  dplyr::select(starts_with("i.check"))}) %>% 
  rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))

# add checks to the list
list_log$logic_c_handle_999_other_log <- df_999_data_other

# silhouette --------------------------------------------------------------
# NOTE: the column for "col_admin" is kept in the data
omit_cols_sil <- c("start", "end", "today", "duration", "duration_minutes","duration_audit_sum_all_ms",	
                   "duration_audit_sum_all_minutes", "deviceid", "audit", "audit_URL", "instance_name", 
                   "end_survey", "hh_location",	"_hh_location_latitude",	"_hh_location_longitude",	
                   "_hh_location_altitude", "_hh_location_precision", "_id", "_submission_time", 
                   "_validation_status", "_notes", "_status", "_submitted_by", "_tags", "_index" )

data_similartiy_sil <- df_tool_data %>% 
  select(- any_of(omit_cols_sil), - matches("_note$|^note_"))

df_sil_data <- calculateEnumeratorSimilarity(data = data_similartiy_sil,
                                             input_df_survey = df_survey, 
                                             col_enum = "enumerator_id",
                                             col_admin = "admin4") %>% 
  mutate(si2 = abs(si))

df_sil_processed <- df_sil_data[order(df_sil_data$`si2`, decreasing = TRUE), !colnames(df_sil_data) %in% "si2"] %>%  
  # filter(si > 0.6) %>% 
  mutate(i.check.uuid = "all",
         i.check.question = NA_character_,
         i.check.issue = paste("silhouette flag"),
         i.check.description = glue::glue("Potential similar responses for enumerator:{enumerator_id}, location:{admin4}. si: {si}")) %>% 
  batch_select_rename()

# add other checks to the list
list_log$enum_similarity <- df_sil_processed

# combine the checks ------------------------------------------------------
df_combined_log <- create_combined_log_keep_change_type(dataset_name = "checked_dataset", 
                                                        list_of_log = list_log)
# prep data
cols_to_add_to_log <- c("enumerator_id", "today", "admin4")

# # add_info_to_cleaning_log()
# add_with_info <- add_info_to_cleaning_log(list_of_log = df_combined_log,
#                                          dataset = "checked_dataset",
#                                          cleaning_log = "cleaning_log",
#                                          dataset_uuid_column = "_uuid",
#                                          cleaning_log_uuid_column = "uuid",
#                                          information_to_add = cols_to_add_to_log) 
# 
# # create_xlsx_cleaning_log()
# add_with_info %>%
#  create_xlsx_cleaning_log(cleaning_log_name = "cleaning_log",
#                           change_type_col = "change_type",
#                           column_for_color = "check_binding",
#                           kobo_survey = df_survey,
#                           kobo_choices = df_choices,
#                           use_dropdown = TRUE,
#                           sm_dropdown_type = "logical",
#                           output_path = paste0("outputs/", butteR::date_file_prefix(), 
#                                                "_combined_checks_eth_rna.xlsx"))
#
# openXL(wb_log)
# openXL(file = paste0("outputs/", butteR::date_file_prefix(),
#                     "_combined_checks_eth_rna.xlsx"))

# create workbook ---------------------------------------------------------
tool_support <- df_combined_log$checked_dataset %>% 
  select(uuid = `_uuid`, any_of(cols_to_add_to_log))

df_prep_checked_data <- df_combined_log$checked_dataset
df_prep_cleaning_log <- df_combined_log$cleaning_log %>%
  left_join(tool_support, by = "uuid") %>% 
  relocate(any_of(cols_to_add_to_log), .after = uuid) %>% 
  add_qn_label_to_cl(input_cl_name_col = "question",
                     input_tool = df_survey, 
                     input_tool_name_col = "name", 
                     input_tool_label_col = "label::English") %>% 
  mutate(enum_id = ifelse(issue %in% c("silhouette flag"), 
                                str_replace(string = str_extract(string = description, 
                                                                 pattern = "enumerator:[0-9]{1,3}"), 
                                            pattern = "enumerator:", ""), enumerator_id))

df_prep_readme <- tibble::tribble(
  ~change_type_validation,                       ~description,
  "change_response", "Change the response to new_value",
  "blank_response",       "Remove and NA the response",
  "remove_survey",                "Delete the survey",
  "no_action",               "No action to take.")

wb_log <- createWorkbook()

hs1 <- createStyle(fgFill = "#E34443", textDecoration = "Bold", fontName = "Arial Narrow", fontColour = "white", fontSize = 12, wrapText = F)
modifyBaseFont(wb = wb_log, fontSize = 11, fontName = "Arial Narrow")

addWorksheet(wb_log, sheetName="checked_dataset")
setColWidths(wb = wb_log, sheet = "checked_dataset", cols = 1:ncol(df_prep_checked_data), widths = 24.89)
writeDataTable(wb = wb_log, sheet = "checked_dataset", x = df_prep_checked_data, 
               startRow = 1, startCol = 1, 
               tableStyle = "TableStyleLight9", headerStyle = hs1)

# freeze pane
freezePane(wb = wb_log, "checked_dataset", firstActiveRow = 2, firstActiveCol = 2)

addWorksheet(wb_log, sheetName="cleaning_log")
setColWidths(wb = wb_log, sheet = "cleaning_log", cols = 1:ncol(df_prep_cleaning_log), widths = 24.89)
writeDataTable(wb = wb_log, sheet = "cleaning_log", x = df_prep_cleaning_log, 
               startRow = 1, startCol = 1, 
               tableStyle = "TableStyleLight9", headerStyle = hs1)

# freeze pane
freezePane(wb = wb_log, "cleaning_log", firstActiveRow = 2, firstActiveCol = 2)

addWorksheet(wb_log, sheetName="readme")
setColWidths(wb = wb_log, sheet = "readme", cols = 1:ncol(df_prep_readme), widths = 24.89)
writeDataTable(wb = wb_log, sheet = "readme", x = df_prep_readme, 
               startRow = 1, startCol = 1, 
               tableStyle = "TableStyleLight9", headerStyle = hs1)

# freeze pane
freezePane(wb = wb_log, "readme", firstActiveRow = 2, firstActiveCol = 2)

# openXL(wb_log)
saveWorkbook(wb_log, paste0("outputs/", butteR::date_file_prefix(),
                            "_combined_checks_eth_rna.xlsx"), overwrite = TRUE)

openXL(file = paste0("outputs/", butteR::date_file_prefix(),
                     "_combined_checks_eth_rna.xlsx"))

###############################################################################