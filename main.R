library(devtools)
library(edfr)
library(readr)
library(dplyr)
library(data.table)
library(purrr)
library(writexl)

############################################################################
# IMPORTING FUNCTIONS
############################################################################

# importing functions from other files
source("C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source/functions_addressCleaning.R")
source("C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source/functions_other.R")

############################################################################
# IMPORTING DATA
############################################################################

# Store the repeated file path in a variable
base_path <- "C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source"

# # Read sup & mop queries into single string variables
# sqlquery_sup <- read_file(file.path(base_path, "external-data/query-snowflake-sup.txt"))
# sqlquery_mop <- read_file(file.path(base_path, "external-data/query-snowflake-mop.txt"))
# # run query request using variable
# df_sup <- query_sf(sqlquery_sup, show.query = TRUE) # current load time: <1min
# df_mop <- query_sf(sqlquery_mop, show.query = TRUE) # current load time: 5mins-25mins
# # Export queries to CSV files (for quicker loading in future)
# write_csv(df_sup, file.path(base_path, "exported-data/df_sup_exported.csv"))
# write_csv(df_mop, file.path(base_path, "exported-data/df_mop_exported.csv"))

# Read exported CSV files
df_sup <- read_csv(file.path(base_path, 
                             "exported-data/df_sup_exported.csv"),
                   col_types = cols(
                     mpan                = col_character(),
                     msn                 = col_character(),
                     date_installed      = col_date(format = "%Y-%m-%d"),
                     customer_active_date = col_datetime(format = "%Y-%m-%dT%H:%M:%SZ"),
                     eac_date            = col_datetime(format = "%Y-%m-%dT%H:%M:%SZ"),
                     .default            = col_guess()
                   )
)
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
                     cop = col_character(),
                     .default = col_guess()   # Guess the rest
                   ))

# Read ecoes query into single string variable
sqlquery_ecoes <- read_file(file.path(base_path, "external-data/query-snowflake-ecoes.txt")) # does notcurrently include EDFE (Retail (Resi/SME)) supply
# run query request using variable
df_ecoes <- query_sf(sqlquery_ecoes, show.query = TRUE)

# read external data files
df_rts_ssc_codes <- read_csv(file.path(base_path, "external-data/data-files-that-need-constant-updating", "RTS SSC Codes.csv"),
                             col_types = cols(SSC_Id = col_double()))

df_hh_eac <- read_csv(file.path(base_path, "external-data/data-files-that-need-constant-updating", "EBS_HH_EAC_20241219.csv"),
                             col_types = cols(
                               mpan = col_character(),
                               eac = col_double()
                               ))


df_nhh_eac <- read_csv(file.path(base_path, "external-data/data-files-that-need-constant-updating", "EBS_NHH_EAC_20241229.csv"),
                      col_types = cols(
                        mpan = col_character(),
                        Last_EAC_Pot = col_character(),
                        L0038_RF_Status = col_character(),
                        eac_date = col_date(format = "%d/%m/%Y"),
                        eac = col_double()
                      ))

# Read ecoes query into single string variable
sqlquery_jw_signalStrength <- read_file(file.path(base_path, "external-data/query-snowflake-jwSignal.txt"))
# run query request using variable
df_jw_signalStrength <- query_sf(sqlquery_jw_signalStrength, show.query = TRUE)

df_onSupplyMasters <- read_csv(file.path(base_path, "external-data/data-files-that-need-constant-updating", "onSupply_masterPrep_masterFinal.csv"),
                       col_types = cols(
                         MPAN = col_character(),
                         MeasurementClass = col_character(),
                         TPIName = col_character(),
                         Segment = col_character()
                       )) %>%
  rename(measurement_class = MeasurementClass)

# Read W3W query into single string variable
sqlquery_jw_W3W <- read_file(file.path(base_path, "external-data/query-snowflake-jwW3W.txt"))
# run query request using variable
df_jw_W3W <- query_sf(sqlquery_jw_W3W, show.query = TRUE)

############################################################################
# REMOVING DUPLICATE ROWS FROM df_sup
############################################################################

# see number of rows in df_sup BEFORE duplicate rows removal
print(paste0("No. rows before cleaning: ", nrow(df_sup)))

# apply row duplicate removal function to df_sup
df_sup_deduped <- df_sup %>%
  group_by(record_id) %>%
  group_split() %>%
  map_df(dedupe_one_record_id)

# remove all rows that are fully NA populated
df_sup_deduped_noNA <- df_sup_deduped %>%
  filter( !if_all(everything(), is.na) )

# see number of rows in df_sup AFTER duplicate rows removal
print(paste0("No. rows after cleaning: ", nrow(df_sup_deduped_noNA)))


############################################################################
# CLEANING ADDRESS COLUMNS IN BOTH df_sup AND df_mop
############################################################################

# !!!WARNING!!!
# ON 300K-400K ROWS, CAN TAKE ~60 MINS each

print_column_stats(df_sup_deduped_noNA[, c("street1", "street2", "street3", "street4", "street5", "street6")])
# df_sup_cleaned <- clean_address_data(df_sup_deduped_noNA)
print_column_stats(df_sup_cleaned[, c("street1", "street2", "street3", "street4", "street5")])

print_column_stats(df_mop[, c("street1", "street2", "street3", "street4", "street5", "street6", "street7", "street8", "street9")])
# df_mop_cleaned <- clean_address_data(df_mop)
print_column_stats(df_mop_cleaned[, c("street1", "street2", "street3", "street4", "street5")])


# # safe copies to avoid constant rerunning and waiting times
# df_sup_cleaned_SAFE <- df_sup_cleaned
# df_mop_cleaned_SAFE <- df_mop_cleaned

# # use safe copies if needed:
df_sup_cleaned <- df_sup_cleaned_SAFE
df_mop_cleaned <- df_mop_cleaned_SAFE

############################################################################
# FURTHER df_sup & df_mop CLEANING
############################################################################


# DROP COLUMNS
df_mop_cleaned <- df_mop_cleaned %>% select(-meter_category)

# ADD COLUMNS
df_mop_cleaned <- df_mop_cleaned %>%
  mutate(import_export_flag = ifelse(grepl("-0000[0-9]$", meter_qualifier), "Export", "Import"))

############################################################################
# JOINING df_sup & df_mop
############################################################################

# Order both dfs by msn and mpan before the join
df_sup_cleaned <- df_sup_cleaned %>%
  arrange(msn, mpan)
df_mop_cleaned <- df_mop_cleaned %>%
  arrange(msn, mpan)

#re-assigning data types as these were lost at some point
df_sup_cleaned <- df_sup_cleaned %>%
  mutate(
    mpan                 = as.character(mpan),
    msn                  = as.character(msn),
    date_installed       = parse_date(date_installed, format = "%Y-%m-%d"),
    customer_active_date = parse_datetime(customer_active_date, format = "%Y-%m-%d %H:%M:%S"),
    eac                  = as.numeric(eac),
    eac_date             = parse_date(eac_date, format = "%Y-%m-%d")
  )
  

df_mop_cleaned <- df_mop_cleaned %>%
  mutate(
    mpan                  = as.character(mpan),
    mop_start             = parse_date(mop_start, format = "%Y-%m-%d"),
    mop_end               = parse_date(mop_end, format = "%Y-%m-%d"),
    supplier_start        = parse_date(supplier_start, format = "%Y-%m-%d"),
    supplier_end          = parse_date(supplier_end, format = "%Y-%m-%d"),
    dc_start              = parse_date(dc_start, format = "%Y-%m-%d"),
    dc_end                = parse_date(dc_end, format = "%Y-%m-%d"),
    commission_status_date = parse_date(commission_status_date, format = "%Y-%m-%d"),
    energised_status_date = parse_date(energised_status_date, format = "%Y-%m-%d"),
    date_installed        = parse_date(date_installed, format = "%Y-%m-%d"),
    cop                   = as.character(cop),
    eac                   = as.numeric(eac)
  )


# drop record_id columns before join
df_sup_cleaned <- df_sup_cleaned %>% select(-record_id)
df_mop_cleaned <- df_mop_cleaned %>% select(-record_id)

# Perform a full join, keeping all (distinct) rows and columns
df_combined <- full_join(df_sup_cleaned, df_mop_cleaned, by = c("msn", "mpan")) %>% 
  distinct()

# add record_id column back in
df_combined <- df_combined %>%
  mutate(record_id = paste(mpan, msn, sep = " - ")) %>%  # Create the new column
  select(record_id, everything())                                # Move it to the left-most position

# Ensuring date_installed is in date format
df_combined <- df_combined %>%
  mutate(
    date_installed.x = as.Date(date_installed.x),
    date_installed.y = as.Date(date_installed.y)
  )

# write_csv(df_combined, file.path(base_path, "exported-data/df_combined_ANAL.csv"))

# ADDING HELPER COLUMN TO COUNT DUPLICATE record_id VALUES
df_combined <- df_combined %>%
  mutate(meter_count = ave(record_id, record_id, FUN = length)) %>%
  select(1, meter_count, everything())  # Move meter_count to the second position

# write_csv(df_combined, file.path(base_path, "exported-data/df_combined_ANAL_2.csv"))

############################################################################
# HANDLING COLUMN DUPLICATES (.i.e., .x and .y headers)
############################################################################

# List of columns to process
# These are all columns that appear in BOTH sup AND mop dfs and therefore need merging
columns_to_merge <- c(
  "meter_type", 
  "outstation_type",
  "customer_name",
  "import_export_flag",
  "ssc",
  "street1",
  "street2",
  "street3",
  "street4",
  "street5",
  "postcode",
  "eac",
  "profile_class",
  "meter_operator",
  "date_installed",
  "communication_method",
  "communication_address",
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


############################################################################
# ADDING EXTERNAL DATA INTO NEW/EXISTING COLUMNS
############################################################################

# CURRENTLY COMMENTED OUT BECAUSE I NEED TO BETTER DEFINE WHAT MAKES A METER LTNR (SEE ONENOTE COMMENTS)
# # Create the 'LTNR' column in 'df_combined'
# df_combined$ltnr_flag <- ifelse(df_combined$mpan %in% df_ltnr$Service_Point, "Y", "N")

# fill missing dc values with dc info from ecoes (fills about 65k)
df_combined <- df_combined %>%
  left_join(
    df_ecoes %>% select(record_id, dc),
    by = "record_id",
    suffix = c("", "_ecoes")
  ) %>%
  mutate(
    dc       = if_else(is.na(dc), dc_ecoes, dc),
  ) %>%
  select(-dc_ecoes)


# Create the 'rts_flag' column in df_combined
df_combined$rts_flag <- ifelse(df_combined$ssc %in% df_rts_ssc_codes$SSC_Id, 'Y', 'N')

# fill `eac` & 'eac_date' columns with hh data from file
df_combined <- df_combined %>%
  left_join(df_hh_eac %>% select(mpan, eac), by = "mpan") %>%
  mutate(
    # Check which rows are actually getting a new eac
    eac_filled = is.na(eac.x) & !is.na(eac.y),
    
    # Fill eac from eac.y if eac.x is NA
    eac = coalesce(eac.x, eac.y),
    
    # Update eac_date for any row that was successfully filled
    eac_date = if_else(eac_filled, as.Date("2024-12-19"), eac_date) # this is just the date I "created" the hh eac data
  ) %>%
  # Remove temporary columns
  select(-eac.x, -eac.y, -eac_filled)

# Join df_nhh_eac onto df_combined by mpan
df_combined <- df_combined %>%
  left_join(
    df_nhh_eac %>%
      select(mpan, eac, eac_date, Last_EAC_Pot, L0038_RF_Status),
    by = "mpan"
  ) %>%
  # Fill missing eac and eac_date from df_nhh_eac
  mutate(
    eac_filled_nhh = is.na(eac.x) & !is.na(eac.y),
    eac = coalesce(eac.x, eac.y),
    eac_date = if_else(eac_filled_nhh, eac_date.y, eac_date.x),
    Last_EAC_Pot = tolower(Last_EAC_Pot),
    L0038_RF_Status = tolower(L0038_RF_Status)
  ) %>%
  # Remove temporary columns
  select(-eac.x, -eac.y, -eac_date.x, -eac_date.y, -eac_filled_nhh)


# add JW job details (including signal strength)
df_combined <- df_combined %>%
  left_join(
    df_jw_signalStrength %>%
      select(record_id, status_comment, type_name, completed_date, signal_strength),
    by = "record_id"
  )

# THIS NEEDS TESTING
df_combinedTEST <- df_combined %>%
  left_join(df_jw_W3W, by = c("mpan"))
write_csv(df_jw_W3W, file.path(base_path, "exported-data/df_jw_w3w_anal.csv"))


# OLD
# df_combined <- df_combined %>%
#   left_join(df_onSupplyMasters, by = c("mpan" = "MPAN"))
# NEW
# CHECK THIS WORKS, THEN MAKE IT PART OF REGULAR CODE
df_combinedTEST <- df_combined %>%
  left_join(df_onSupplyMasters, by = c("mpan" = "MPAN")) %>%
  mutate(
    measurement_class = coalesce(measurement_class.x, measurement_class.y)
  ) %>%
  select(-measurement_class.x, -measurement_class.y)




write_csv(df_combined, file.path(base_path, "exported-data/df_combined_full.csv"))




############################################################################
# REDUCED VIEW (I.E., SINGLE VIEW OF DEMAND) EXPORT FOR SCALING PARTNERS
############################################################################

# List of columns to remove
columns_to_remove <- c(
  "record_id",
  "meter_count", 
  
  # below is mop only
  "DATA_COMMENT",
  "MOP_APPOINTMENT_TYPE",
  "MOP_CONTRACT_REFERENCE",
  "EBS_MOP_BILLING_STATUS",
  "EBS_ENERGY_SEGMENT",
  "OUTCODE",
  "GSP_GROUP",
  "RETRIEVAL_METHOD",
  "MSMTD_EFD",
  "COP",
  "MULTI_MPAN",
  "INSTALLED_IN_CURRENT_MOP_APPOINTMENT",
  "METER_LOCATION",
  "CURRENT_RATING",
  "MAP_ID",
  "EDF_MSN",
  "CT_RATIO",
  "VT_RATIO",
  "FEEDER_STATUS",
  "CONFIG_CODE",
  "BAUD_RATE",
  "NUMBER_OF_CHANNELS",
  "NUMBER_OF_DIALS",
)


# Remove specified columns
df_single_view <- df_combined[, !names(df_combined) %in% columns_to_remove]


############################################################################
# COUNT POPULATED/BLANK CELLS IN SINGLE_VIEW
############################################################################

# Define additional blank indicators
blank_indicators <- c(
  "", " ", "NA", "N/A", "na", "n/a", "NULL", "null", "None", "NONE",
  "Missing", "missing", "MISSING", "--", "?", "Unknown", "unknown",
  "#N/A", ".", "\\N", "-", "NaN", "nan", "none"
)

# Convert columns to character and compute counts
calc_counts <- function(x) {
  x_char <- as.character(x)
  populated <- sum(!is.na(x_char) & !(tolower(x_char) %in% tolower(blank_indicators)) & x_char != "")
  blank <- sum(is.na(x_char) | (tolower(x_char) %in% tolower(blank_indicators)) | x_char == "")
  c(Populated = populated, Blank = blank)
}

counts <- sapply(df_single_view, calc_counts)

############################################################################
# EXPORTING SINGLE VIEW OF DEMAND
############################################################################

# Export the modified dataframe to a CSV file
# write_csv(df_single_view, file.path(base_path, "exported-data/single_view.csv"))
write_xlsx(df_single_view, file.path(base_path, "exported-data/single_view.xlsx"))











