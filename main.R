library(devtools)
library(edfr)
library(readr)
library(dplyr)
library(data.table)

# Store the repeated file path in a variable
base_path <- "C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source/"

# # the below is commented out to save on load times and £cost of Snowflake.
# # Read entire query into a single string variable
# sqlquery_sup <- read_file(file.path(base_path, "query-snowflake-sup.txt"))
# sqlquery_mop <- read_file(file.path(base_path, "query-snowflake-mop.txt"))
# # run query request using variable
# df_sup <- query_sf(sqlquery_sup, show.query = TRUE) # current load time: <1min
# df_mop <- query_sf(sqlquery_mop, show.query = TRUE) # current load time: ~25mins
# # Export queries to CSV files
# write_csv(df_sup, file.path(base_path, "exported-data/df_sup_exported.csv"))
# write_csv(df_mop, file.path(base_path, "exported-data/df_mop_exported.csv"))

# Read exported CSV files
df_sup <- read_csv(file.path(base_path, "exported-data/df_sup_exported.csv"), 
                   col_types = cols(
                     mpan = col_character(),
                     date_installed = col_date(format = "%Y-%m-%d"),
                     .default = col_guess()   # Guess the rest
                   ))

df_mop <- read_csv(file.path(base_path, "exported-data/df_mop_exported.csv"), 
                   col_types = cols(
                     mpan = col_character(),
                     mop_start = col_date(format = "%Y-%m-%d"),
                     mop_end = col_date(format = "%Y-%m-%d"),
                     supplier_start = col_date(format = "%Y-%m-%d"),
                     supplier_end = col_date(format = "%Y-%m-%d"),
                     dc_start = col_date(format = "%Y-%m-%d"),
                     dc_end = col_date(format = "%Y-%m-%d"),
                     commission_status_date = col_date(format = "%Y-%m-%d"),
                     energised_status_date = col_date(format = "%Y-%m-%d"),
                     date_installed = col_date(format = "%Y-%m-%d"),
                     .default = col_guess()   # Guess the rest
                   ))

df_ltnr <- read_csv(file.path(base_path, "external-data", "LTNR.csv"),
                    col_types = cols(Service_Point = col_character()), col_select = "Service_Point"
                    )


df_rts_ssc_codes <- read_csv(file.path(base_path, "external-data", "RTS SSC Codes.csv"),
                             col_types = cols(SSC_Id = col_double()))

                    
# # RENAME COLUMNS         
# df_sup <- df_sup %>%
#   rename(
#     install_date = date_installed
#   )

# DROP COLUMNS
df_mop <- df_mop %>% select(-meter_category)
df_sup <- df_sup %>% select(-postcode_in)

# ADD COLUMNS
df_mop <- df_mop %>%
  mutate(import_export_flag = ifelse(grepl("-0000[0-9]$", meter_qualifier), "Export", "Import"))

# # Filter to keep only the row with the latest `date_installed` for each `record_id` in df_sup
#   # This is because there are multiple differing values for the same `record_id`
# df_sup <- df_sup %>%
#   group_by(record_id) %>%
#   slice_max(install_date) %>%
#   ungroup()


# Order both dfs by msn and mpan before the join
df_sup <- df_sup %>%
  arrange(msn, mpan)
df_mop <- df_mop %>%
  arrange(msn, mpan)

# Perform a full join, keeping all (distinct) rows and columns
df_combined <- full_join(df_sup, df_mop, by = c("msn", "mpan")) %>% 
  distinct()



# Ensuring date_installed is in date format
df_combined <- df_combined %>%
  mutate(
    date_installed.x = as.Date(date_installed.x),
    date_installed.y = as.Date(date_installed.y)
  )

# ADDING HELPER COLUMN TO COUNT DUPLICATE record_id VALUES
df_combined <- df_combined %>%
  mutate(meter_count = ave(record_id, record_id, FUN = length)) %>%
  select(1, meter_count, everything())  # Move meter_count to the second position



# List of columns to process
# These are all columns that appear in BOTH sup AND mop dfs and therefore need merging
columns_to_merge <- c(
  "meter_type", 
  "outstation_type", 
  "communication_method", 
  "communication_address",
  "customer_name",
  "import_export_flag",
  "ssc",
  "street1",
  "street2",
  "street3",
  "street4",
  "city",
  "postcode_out",
  "postcode",
  "eac",
  "profile_class",
  "meter_operator",
  "date_installed",
  "supplier"
)

# the below looks at df_combine columns that have duplicates (.x & .y suffixes) and handles them 
# best suited to the individual parameter.

for (col in columns_to_merge) {
  if (col == "date_installed") {
    # Handle 'date_installed' by choosing the most recent date
    df_combined <- df_combined %>%
      mutate(
        !!sym(col) := case_when(
          !is.na(!!sym(paste0(col, ".x"))) & !is.na(!!sym(paste0(col, ".y"))) ~ pmax(!!sym(paste0(col, ".x")), !!sym(paste0(col, ".y")), na.rm = TRUE),
          is.na(!!sym(paste0(col, ".x"))) ~ !!sym(paste0(col, ".y")),
          TRUE ~ !!sym(paste0(col, ".x"))
        ),
        # Compute mismatch flag
        !!sym(paste0(col, "_mismatch")) := if_else(
          !is.na(!!sym(paste0(col, ".x"))) & !is.na(!!sym(paste0(col, ".y"))) &
            (!!sym(paste0(col, ".x")) != !!sym(paste0(col, ".y"))),
          TRUE,
          FALSE
        )
      )
  } else if (col %in% c("meter_type", "communication_method", "communication_address",
                        "eac", "profile_class", "meter_operator")) {
    # Prioritize MOP (.y) for these columns
    df_combined <- df_combined %>%
      mutate(
        !!sym(col) := case_when(
          !is.na(!!sym(paste0(col, ".x"))) &
            !is.na(!!sym(paste0(col, ".y"))) &
            (!!sym(paste0(col, ".x")) != !!sym(paste0(col, ".y"))) ~ !!sym(paste0(col, ".y")),
          TRUE ~ coalesce(!!sym(paste0(col, ".y")), !!sym(paste0(col, ".x")))
        ),
        # Compute mismatch flag
        !!sym(paste0(col, "_mismatch")) := if_else(
          !is.na(!!sym(paste0(col, ".x"))) &
            !is.na(!!sym(paste0(col, ".y"))) &
            (!!sym(paste0(col, ".x")) != !!sym(paste0(col, ".y"))),
          TRUE,
          FALSE
        )
      )
  } else {
    # Prioritize SUP (.x) for other columns
    df_combined <- df_combined %>%
      mutate(
        !!sym(col) := case_when(
          !is.na(!!sym(paste0(col, ".x"))) &
            !is.na(!!sym(paste0(col, ".y"))) &
            (!!sym(paste0(col, ".x")) != !!sym(paste0(col, ".y"))) ~ !!sym(paste0(col, ".x")),
          TRUE ~ coalesce(!!sym(paste0(col, ".x")), !!sym(paste0(col, ".y")))
        ),
        # Compute mismatch flag
        !!sym(paste0(col, "_mismatch")) := if_else(
          !is.na(!!sym(paste0(col, ".x"))) &
            !is.na(!!sym(paste0(col, ".y"))) &
            (!!sym(paste0(col, ".x")) != !!sym(paste0(col, ".y"))),
          TRUE,
          FALSE
        )
      )
  }
}



# This counts and prints the number of mismatch values between columns .x and 
# .y for each parameter in columns_to_merge - used for diagnosis.

# Initialize an empty list to store mismatch counts
mismatch_counts <- list()

# Loop over each column to get mismatch counts
for (col in columns_to_merge) {
  mismatch_col <- paste0(col, "_mismatch")
  # Calculate the number of mismatches
  mismatch_count <- sum(df_combined[[mismatch_col]], na.rm = TRUE)
  # Store the count in the list
  mismatch_counts[[col]] <- mismatch_count
}

# Convert the list to a data frame for better presentation
mismatch_report <- data.frame(
  Column = names(mismatch_counts),
  Mismatch_Count = unlist(mismatch_counts)
)

# Print the mismatch report
print(mismatch_report)





# Removes the .x, .y, and _mismatch columns, leaving just the chosen column (as detailed in large logic code up above)
df_combined <- df_combined %>% 
  select(-ends_with(".x"), -ends_with(".y"), -ends_with("_mismatch"))


 # ENSURE COLUMNS ARE IN CORRECT FORMAT
df_combined$ssc <- as.numeric(df_combined$ssc) # Convert ssc to numeric (removes leading zeros)

# Create the 'LTNR' column in 'df_combined'
df_combined$ltnr_flag <- ifelse(df_combined$mpan %in% df_ltnr$Service_Point, "Y", "N")
# Create the 'rts_flag' column in df_combined
df_combined$rts_flag <- ifelse(df_combined$ssc %in% df_rts_ssc_codes$SSC_Id, 'Y', 'N')




write_csv(df_combined, file.path(base_path, "exported-data/df_combined_exported.csv"))



# SPLITTING DF INTO DUPLICATE SUBSETS (TO EVENTUALLY RESOLVE DUPLICATES)

df_combined_green <- subset(df_combined, meter_count == 1)
df_combined_amber <- subset(df_combined, meter_count == 2)
df_combined_red <- subset(df_combined, meter_count > 2)

write_csv(df_combined_green, file.path(base_path, "exported-data/df_combined_green.csv"))
write_csv(df_combined_amber, file.path(base_path, "exported-data/df_combined_amber.csv"))
write_csv(df_combined_red, file.path(base_path, "exported-data/df_combined_red.csv"))







# REDUCED VIEW (I.E., SINGLE VIEW OF DEMAND) EXPORT FOR SCALING PARTNERS

# List of columns to remove
columns_to_remove <- c(
  "record_id", "meter_count",
  "ssc_industry", "county", "postcode_in", "site_address",
  "profile_class_industry", "meter_operator_industry", "eac_date",
  "mop_start", "mop_end", "supplier_tier", "edf_system", "market",
  "supplier_start", "supplier_end", "dc_service_type",
  "dc", "dc_start", "dc_end", "direct_contract_flag", "open_fault",
  "open_service_order", "commission_status", "commission_status_date",
  "energised_status", "energised_status_date", "phase",
  "ebs_energy_segment", "ebs_energy_micro_flag", "sap_contact_name",
  "sap_contact_number", "dno", "street5", "district", "region",
  "meter_qualifier", "cop", "meter_quantity", "multi_mpan",
  "meter_planner_group", "mpan_planner_group", "planner_group_match",
  "manufactured_year", "recert_year", "mpan_map", "meter_map",
  "map_match", "potential_edf_msn", "installed_while_mop",
  "installed_in_current_mop_appointment", "installed_by_edf",
  "device_category", "register_group", "wc_or_ct", "ct_ratio",
  "lv_or_hv", "vt_ratio", "communication_provider", "outstation_id",
  "outstation_pin", "level_2_username", "level_2_password",
  "level_3_username", "level_3_password", "level_4_password",
  "efs_password_reference", "street1", "street2", "street3",
  "street4", "city", "postcode_out", "postcode"
)

# Remove specified columns
df_single_view <- df_combined_green[, !names(df_combined) %in% columns_to_remove]

# Export the modified dataframe to a CSV file
write_csv(df_single_view, file.path(base_path, "exported-data/single_view.csv"))













