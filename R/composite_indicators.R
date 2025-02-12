create_composite_indicators <- function(input_df) {
  input_df |> 
    dplyr::mutate(
      # Handle missing values for head of household age
      int.hoh_age = ifelse(is.na(hoh_age), resp_age, hoh_age),
      # Categorize household size
      i.hh_comp_cat = case_when(hh_size <= 3 ~ "1 - 3 members",
                                hh_size <= 6 ~ "4 - 6 members",
                                hh_size <= 9 ~ "7 - 9 members",
                                TRUE ~ "10 or more members"),
      
      # Assign category for head of household gender and age
      i.hoh_gender_cat = ifelse(is.na(hoh_gender), resp_gender, hoh_gender),
      i.hoh_age_cat = case_when(int.hoh_age <= 29 ~ "18 - 29 years old",
                                int.hoh_age <= 59 ~ "30 - 59 years old",
                                TRUE ~ "60 years old and above"),
      
      # Categorize respondent age
      i.resp_age_cat = case_when(resp_age <= 24 ~ "18 - 24 years old",
                                 resp_age <= 39 ~ "25 - 39 years old",
                                 resp_age <= 59 ~ "40 - 59 years old",
                                 TRUE ~ "60 years old and above"))
}

# loop_roster
create_composites_roster_loop <- function(input_df) {
  input_df |> 
    mutate(int.disability = paste(wgq_vision, wgq_hearing, wgq_mobility, 
                                  wgq_cognition, wgq_self_care, wgq_communication),
           i.disability = ifelse(str_detect(string = int.disability, pattern = "lot_of_difficulty|cannot_do"), "yes", "no")
           ) |>  
    select(-c(starts_with("int.")))
}