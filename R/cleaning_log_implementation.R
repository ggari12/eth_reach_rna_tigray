###############################################################################
# Applying the cleaning log to clean the data

rm(list = ls())

library(tidyverse)
library(glue)
library(cleaningtools)
library(httr)
library(supporteR)

source("R/composite_indicators.R")

loc_data <- "inputs/ETH24XX_RNA_Tigray_HH_data.xlsx"

# main data
data_nms <- names(readxl::read_excel(path = loc_data, n_max = 300))
c_types <- ifelse(str_detect(string = data_nms, pattern = "_other$"), "text", "guess")
df_tool_data <- readxl::read_excel(loc_data, col_types = c_types)  %>% 
  mutate(point_number = paste0("pt_", row_number()),
         start = as_datetime(start),
         end = as_datetime(end)) %>%
  checks_add_extra_cols(input_enumerator_id_col = "enumerator_id", 
                        input_location_col = "admin4") 

# loops
# roster
data_nms_roster <- names(readxl::read_excel(path = loc_data, n_max = 2000, sheet = "roster"))
c_types_roster <- ifelse(str_detect(string = data_nms_roster, pattern = "_other$"), "text", "guess")
df_loop_roster <- readxl::read_excel(loc_data, col_types = c_types_roster, sheet = "roster") 

# health
data_nms_health <- names(readxl::read_excel(path = loc_data, n_max = 2000, sheet = "health_ind"))
c_types_health <- ifelse(str_detect(string = data_nms_health, pattern = "_other$"), "text", "guess")
df_loop_health <- readxl::read_excel(loc_data, col_types = c_types_health, sheet = "health_ind")

# tool
loc_tool <- "inputs/ETH24XX_RNA_Tigray_HH_tool.xlsx"
df_survey <- readxl::read_excel(loc_tool, sheet = "survey")
df_choices <- readxl::read_excel(loc_tool, sheet = "choices") 

# cleaning log 
df_filled_cl <- readxl::read_excel("inputs/main_combined_checks_eth_rna_filled.xlsx", sheet = "cleaning_log") %>% 
  select(-type) %>%
  filter(!is.na(reviewed), !question %in% c("_index"), !uuid %in% c("all"))

# surveys for deletion
df_remove_survey_cl <- df_filled_cl %>% 
  filter(change_type %in% c("remove_survey"))

# check pii -------------------------------------------------------------------
pii_from_data <- cleaningtools::check_pii(dataset = df_tool_data, 
                                          element_name = "checked_dataset", 
                                          uuid_column = "_uuid")
pii_from_data$potential_PII

# then determine which columns to remove from both the raw and clean data
cols_to_remove <- c("deviceid", "audit", "audit_URL", "instance_name", "end_survey", "team_leader_name", 
                    "team_leader_name_other", "women_health_needs_not_helped", "hh_location", "_hh_location_latitude", 
                    "_hh_location_longitude", "_hh_location_altitude", "_hh_location_precision", "_id", 
                    "_submission__id", "_submission_time", "_validation_status", "_notes",	"_status",	
                    "_submitted_by",	"__version__", 	"_tags", "_submission__submission_time", 
                    "_submission__validation_status",	"_submission__notes",	"_submission__status",
                    "_parent_table_name", "_submission__submitted_by", "_submission___version__",	"_submission__tags")

# Main dataset ----------------------------------------------------------------

# filtered log
df_filled_cl_main <- df_filled_cl %>% 
  filter(is.na(sheet))

# updating the main dataset with new columns
df_data_with_added_cols <- cts_add_new_sm_choices_to_data(input_df_tool_data = df_tool_data,
                                                          input_df_filled_cl = df_filled_cl_main, 
                                                          input_df_survey = df_survey,
                                                          input_df_choices = df_choices)

# check the cleaning log
df_cl_review <- cleaningtools::review_cleaning_log(raw_dataset = df_data_with_added_cols,
                                                   raw_data_uuid_column = "_uuid",
                                                   cleaning_log = df_filled_cl_main,
                                                   cleaning_log_change_type_column = "change_type",
                                                   change_response_value = "change_response",
                                                   cleaning_log_question_column = "question",
                                                   cleaning_log_uuid_column = "uuid",
                                                   cleaning_log_new_value_column = "new_value")

# filter log for cleaning
df_final_cleaning_log <- df_filled_cl_main %>% 
  filter(!question %in% c("duration_audit_sum_all_ms", "duration_audit_sum_all_minutes"), 
         !uuid %in% c("all")) %>% 
  filter(!str_detect(string = question, pattern = "\\w+\\/$"))

# create the clean data from the raw data and cleaning log
df_cleaning_step <- cleaningtools::create_clean_data(raw_dataset = df_data_with_added_cols %>% select(-any_of(cols_to_remove)),
                                                     raw_data_uuid_column = "_uuid",
                                                     cleaning_log = df_final_cleaning_log,
                                                     cleaning_log_change_type_column = "change_type",
                                                     change_response_value = "change_response",
                                                     NA_response_value = "blank_response",
                                                     no_change_value = "no_action",
                                                     remove_survey_value = "remove_survey",
                                                     cleaning_log_question_column = "question",
                                                     cleaning_log_uuid_column = "uuid",
                                                     cleaning_log_new_value_column = "new_value")

# handle parent question columns
df_updating_sm_parents <- cts_update_sm_parent_cols(input_df_cleaning_step_data = df_cleaning_step, 
                                                    input_uuid_col = "_uuid",
                                                    input_point_id_col = "point_number",
                                                    input_collected_date_col = "today",
                                                    input_location_col = "admin4")

# tool data to support loops ----------------------------------------------
df_tool_support_data_for_loops <- df_updating_sm_parents$updated_sm_parents %>% 
  filter(!`_uuid` %in% df_remove_survey_cl$uuid) %>% 
  select(`_uuid`, admin4, today, enumerator_id, point_number)

# roster cleaning ----------------------------------------------------------
# then determine which columns to remove from both the raw and clean data
cols_to_remove_roster <- c("name")

# filtered log
df_filled_cl_roster <- df_filled_cl %>% 
  filter(sheet %in% c("roster"), !is.na(index))

# updating the main dataset with new columns
df_data_with_added_cols_roster <- cts_add_new_sm_choices_to_data(input_df_tool_data = df_loop_roster %>% 
                                                                   left_join(df_tool_support_data_for_loops, by = c("_submission__uuid" = "_uuid")),
                                                                 input_df_filled_cl = df_filled_cl_roster, 
                                                                 input_df_survey = df_survey,
                                                                 input_df_choices = df_choices)

# check the cleaning log
df_cl_review <- cleaningtools::review_cleaning_log(raw_dataset = df_data_with_added_cols_roster,
                                                   raw_data_uuid_column = "_submission__uuid",
                                                   cleaning_log = df_filled_cl_roster,
                                                   cleaning_log_change_type_column = "change_type",
                                                   change_response_value = "change_response",
                                                   cleaning_log_question_column = "question",
                                                   cleaning_log_uuid_column = "uuid",
                                                   cleaning_log_new_value_column = "new_value")

# filter log for cleaning
df_final_cleaning_log_roster <- df_filled_cl_roster %>% 
  filter(!question %in% c("duration_audit_sum_all_ms", "duration_audit_sum_all_minutes",
                          "_index", "_parent_index"), 
         !uuid %in% c("all")) %>% 
  filter(!str_detect(string = question, pattern = "\\w+\\/$"))

# create the clean data from the raw data and cleaning log
df_cleaning_step_roster <- cleaningtools::create_clean_data(raw_dataset = df_loop_roster %>% 
                                                              mutate(cleaning_uuid = paste0(`_submission__uuid`, `_index`)),
                                                            raw_data_uuid_column = "cleaning_uuid",
                                                            cleaning_log = df_final_cleaning_log_roster %>% 
                                                              mutate(log_cleaning_uuid = paste0(uuid, index)),
                                                            cleaning_log_change_type_column = "change_type",
                                                            change_response_value = "change_response",
                                                            NA_response_value = "blank_response",
                                                            no_change_value = "no_action",
                                                            remove_survey_value = "remove_survey",
                                                            cleaning_log_question_column = "question",
                                                            cleaning_log_uuid_column = "log_cleaning_uuid",
                                                            cleaning_log_new_value_column = "new_value")

# health_ind cleaning ---------------------------------------------------------
# then determine which columns to remove from both the raw and clean data
cols_to_remove_health <- c("name")

# filtered log
df_filled_cl_health <- df_filled_cl %>% 
  filter(sheet %in% c("health_ind"), !is.na(index))

# check the cleaning log
df_cl_review <- cleaningtools::review_cleaning_log(raw_dataset = df_loop_health,
                                                   raw_data_uuid_column = "_submission__uuid",
                                                   cleaning_log = df_filled_cl_health,
                                                   cleaning_log_change_type_column = "change_type",
                                                   change_response_value = "change_response",
                                                   cleaning_log_question_column = "question",
                                                   cleaning_log_uuid_column = "uuid",
                                                   cleaning_log_new_value_column = "new_value")

# filter log for cleaning
df_final_cleaning_log_health <- df_filled_cl_health %>% 
  filter(!question %in% c("duration_audit_sum_all_ms", "duration_audit_sum_all_minutes",
                          "_index", "_parent_index"), 
         !uuid %in% c("all")) %>% 
  filter(!str_detect(string = question, pattern = "\\w+\\/$"))

# create the clean data from the raw data and cleaning log
df_cleaning_step_health <- cleaningtools::create_clean_data(raw_dataset = df_loop_health %>% 
                                                              mutate(cleaning_uuid = paste0(`_submission__uuid`, `_index`)),
                                                            raw_data_uuid_column = "cleaning_uuid",
                                                            cleaning_log = df_final_cleaning_log_health %>% 
                                                              mutate(log_cleaning_uuid = paste0(uuid, index)),
                                                            cleaning_log_change_type_column = "change_type",
                                                            change_response_value = "change_response",
                                                            NA_response_value = "blank_response",
                                                            no_change_value = "no_action",
                                                            remove_survey_value = "remove_survey",
                                                            cleaning_log_question_column = "question",
                                                            cleaning_log_uuid_column = "log_cleaning_uuid",
                                                            cleaning_log_new_value_column = "new_value")

# main data with composites, and exclude empty columns
df_main_clean_data_with_composites <- df_updating_sm_parents$updated_sm_parents %>% 
  create_composite_indicators() %>%
  select(-matches("^note_|_notes|_note$|^qn_note_|_other$"),
         -c(healthcare_improvement_needed_first:mobile_clinics_owner),
         -priorty_healthcare_needs_expansion_of_services_offered,
         -priorty_healthcare_needs_different_staff_or_staff_who_understand_specific_conditions,
         -point_number) %>% 
  filter(!`_uuid` %in% df_remove_survey_cl$uuid) 

# export datasets -------------------------------------------------------------
list_of_datasets <- list("raw_main_data" = df_tool_data %>% select(-c(healthcare_improvement_needed_first:mobile_clinics_owner_other),
                                                                   -any_of(cols_to_remove)), 
                         "cleaned_main_data" = df_main_clean_data_with_composites %>% select(-any_of(cols_to_remove)),
                         "main_cleaning_log" = df_filled_cl_main,
                         "main_deletion_log" = df_deletion_log,
                         "raw_roster_data" = df_loop_roster %>% select(-any_of(cols_to_remove_roster)), 
                         "cleaned_roster_data" = df_cleaning_step_roster %>% select(-any_of(cols_to_remove)) %>% 
                           select(-matches("^note_|_notes|_note$|^qn_note_|_other$")) %>%
                           filter(!`_submission__uuid` %in% df_remove_survey_cl$uuid), #%>% 
                         #select(-cleaning_uuid),
                         "roster_cleaning_log" = df_filled_cl_roster,
                         "roster_deletion_log" = df_loop_roster %>% 
                           left_join(df_deletion_log, by = c("_submission__uuid" = "uuid")) %>% 
                           select(`_submission__uuid`, `_index`, `enumerator ID`,	Issue, `Type of Issue (Select from dropdown list)`) %>% 
                           filter(`Type of Issue (Select from dropdown list)` %in% c("remove_survey")) %>%
                           mutate(Comment = "The roster was deleted due to the parent survey's removal"),
                         "raw_health_data" = df_loop_health, 
                         "cleaned_health_data" = df_cleaning_step_health %>% select(-any_of(cols_to_remove)) %>% 
                           select(-matches("^note_|_notes|_note$|^qn_note_|_other$")) %>% 
                           filter(!`_submission__uuid` %in% df_remove_survey_cl$uuid) %>% 
                           select(-cleaning_uuid),
                         "health_cleaning_log" = df_filled_cl_health,
                         "health_deletion_log" = df_loop_health %>% 
                           left_join(df_deletion_log, by = c("_submission__uuid" = "uuid")) %>% 
                           select(`_submission__uuid`, `_index`, `enumerator ID`,	Issue, `Type of Issue (Select from dropdown list)`) %>% 
                           filter(`Type of Issue (Select from dropdown list)` %in% c("remove_survey")) %>%
                           mutate(Comment = "The health information was deleted due to the parent survey's removal"),
                         "Kobo survey" = df_survey,
                         "Kobo choices" = df_choices)

# run this formatting part after running the 'formatting_log_to_template' script
# Create a new workbook
wb <- createWorkbook()

# Define the style
hs1 <- createStyle(fgFill = "#E34443", textDecoration = "Bold", fontName = "Arial Narrow", fontColour = "white", fontSize = 12, wrapText = F)
modifyBaseFont(wb, fontSize = 11, fontName = "Arial Narrow")

# Add each dataset to the workbook and apply styling as needed
for (name in names(list_of_datasets)) {
  addWorksheet(wb, name)
  writeData(wb, sheet = name, list_of_datasets[[name]])
  
  # Apply style to specific sheets
  if (name %in% c("main_cleaning_log", "main_deletion_log", "roster_cleaning_log", "roster_deletion_log", 
                  "health_cleaning_log", "health_deletion_log", "Kobo survey", "Kobo choices")) {
    addStyle(wb, sheet = name, style = hs1, rows = 1, cols = 1:ncol(list_of_datasets[[name]]), gridExpand = TRUE)
  }
}

# Freeze panes for specific sheets
sheets_to_freeze <- c("main_cleaning_log", "roster_cleaning_log", "health_cleaning_log", "Kobo survey", "Kobo choices")

for (sheet_name in sheets_to_freeze) {
  freezePane(wb, sheet = sheet_name, firstActiveRow = 2, firstActiveCol = 2)
}

# freeze pane
freezePane(wb, "main_deletion_log", firstActiveRow = 2, firstActiveCol = 2)
writeData(wb, sheet = "main_deletion_log", df_deletion_log, startRow = 1, startCol = 1)
addStyle(wb, sheet = "main_deletion_log", hs1, rows = 1, cols = 1:6, gridExpand = FALSE)
setColWidths(wb, sheet = "main_deletion_log", cols = 1, widths = 36)
setColWidths(wb, sheet = "main_deletion_log", cols = 2, widths = 20)
setColWidths(wb, sheet = "main_deletion_log", cols = 3, widths = 50)
setColWidths(wb, sheet = "main_deletion_log", cols = 4, widths = 40)
setColWidths(wb, sheet = "main_deletion_log", cols = 5, widths = 40)

# roster deletion log
freezePane(wb, "roster_deletion_log", firstActiveRow = 2, firstActiveCol = 2)
writeData(wb, sheet = "roster_deletion_log", df_deletion_log, startRow = 1, startCol = 1)
addStyle(wb, sheet = "roster_deletion_log", hs1, rows = 1, cols = 1:6, gridExpand = FALSE)
setColWidths(wb, sheet = "roster_deletion_log", cols = 1, widths = 36)
setColWidths(wb, sheet = "roster_deletion_log", cols = 2, widths = 20)
setColWidths(wb, sheet = "roster_deletion_log", cols = 3, widths = 50)
setColWidths(wb, sheet = "roster_deletion_log", cols = 4, widths = 40)
setColWidths(wb, sheet = "roster_deletion_log", cols = 5, widths = 40)

# health deletion log
freezePane(wb, "health_deletion_log", firstActiveRow = 2, firstActiveCol = 2)
writeData(wb, sheet = "health_deletion_log", df_deletion_log, startRow = 1, startCol = 1)
addStyle(wb, sheet = "health_deletion_log", hs1, rows = 1, cols = 1:6, gridExpand = FALSE)
setColWidths(wb, sheet = "health_deletion_log", cols = 1, widths = 36)
setColWidths(wb, sheet = "health_deletion_log", cols = 2, widths = 20)
setColWidths(wb, sheet = "health_deletion_log", cols = 3, widths = 50)
setColWidths(wb, sheet = "health_deletion_log", cols = 4, widths = 40)
setColWidths(wb, sheet = "health_deletion_log", cols = 5, widths = 40)

# Kobo survey
freezePane(wb, "Kobo survey", firstActiveRow = 2, firstActiveCol = 2)
writeData(wb, sheet = "Kobo survey", df_survey, startRow = 1, startCol = 1)
addStyle(wb, sheet = "Kobo survey", hs1, rows = 1, cols = 1:6, gridExpand = FALSE)
setColWidths(wb, sheet = "Kobo survey", cols = 1, widths = 36)
setColWidths(wb, sheet = "Kobo survey", cols = 2, widths = 20)
setColWidths(wb, sheet = "Kobo survey", cols = 3, widths = 50)
setColWidths(wb, sheet = "Kobo survey", cols = 4, widths = 40)
setColWidths(wb, sheet = "Kobo survey", cols = 5, widths = 40)

# Kobo choices
freezePane(wb, "Kobo choices", firstActiveRow = 2, firstActiveCol = 2)
writeData(wb, sheet = "Kobo choices", df_choices, startRow = 1, startCol = 1)
addStyle(wb, sheet = "Kobo choices", hs1, rows = 1, cols = 1:6, gridExpand = FALSE)
setColWidths(wb, sheet = "Kobo choices", cols = 1, widths = 36)
setColWidths(wb, sheet = "Kobo choices", cols = 2, widths = 20)
setColWidths(wb, sheet = "Kobo choices", cols = 3, widths = 50)
setColWidths(wb, sheet = "Kobo choices", cols = 4, widths = 40)
setColWidths(wb, sheet = "Kobo choices", cols = 5, widths = 40)

#write final datasets out
saveWorkbook(wb, paste0("outputs/", butteR::date_file_prefix(),
                        "_ETH24XX_eth_rna_cleaned_data.xlsx"), overwrite = TRUE)

###############################################################################

# extra log for recreated select multiple -------------------------------------
openxlsx::write.xlsx(df_updating_sm_parents$extra_log_sm_parents, 
                     paste0("outputs/", butteR::date_file_prefix(), 
                            "_extra_sm_parent_changes_checks_eth_rna.xlsx"))

#write final datasets out
openxlsx::write.xlsx(x = list_of_datasets, file = paste0("outputs/", butteR::date_file_prefix(),
                                                 "_ETH24XX_eth_rna_cleaned_data2.xlsx"),
                     overwrite = TRUE, keepNA = TRUE, na.string = "NA")
