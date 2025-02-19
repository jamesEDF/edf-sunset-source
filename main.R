library(tidyr)
library(stringr)
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

# Read sup & mop queries into single string variables
sqlquery_sup <- read_file(file.path(base_path, "external-data/query-snowflake-sup.txt"))
sqlquery_mop <- read_file(file.path(base_path, "external-data/query-snowflake-mop.txt"))
# run query request using variable
df_sup <- query_sf(sqlquery_sup, show.query = TRUE) # current load time: <1min
df_mop <- query_sf(sqlquery_mop, show.query = TRUE) # current load time: <1min
# clean any bad dates in df_mop
df_mop$date_installed <- fix_invalid_dates(df_mop$date_installed)
# Export queries to CSV files (for quicker loading in future)
write_csv(df_sup, file.path(base_path, "exported-data/df_sup_exported.csv"))
write_csv(df_mop, file.path(base_path, "exported-data/df_mop_exported.csv"))

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
                     # supplier_start = col_date(format = "%Y-%m-%d"),
                     # supplier_end = col_date(format = "%Y-%m-%d"),
                     # dc_start = col_date(format = "%Y-%m-%d"),
                     # dc_end = col_date(format = "%Y-%m-%d"),
                     # commission_status_date = col_date(format = "%Y-%m-%d"),
                     # energised_status_date = col_date(format = "%Y-%m-%d"),
                     date_installed = col_date(format = "%Y-%m-%d"),
                     cop = col_character(),
                     .default = col_guess()   # Guess the rest
                   ))

# Read ecoes query into single string variable
sqlquery_ecoes <- read_file(file.path(base_path, "external-data/query-snowflake-ecoes.txt")) # does not currently include EDFE (Retail (Resi/SME)) supply OR DOES IT?!
# run query request using variable
df_ecoes <- query_sf(sqlquery_ecoes, show.query = TRUE)

# read external data files
df_rts_ssc_codes <- read_csv(file.path(base_path, "external-data/data-files-that-need-constant-updating", "RTS SSC Codes.csv"),
                             col_types = cols(SSC_Id = col_double()))

df_hh_eac <- read_csv(file.path(base_path, "external-data/data-files-that-need-constant-updating", "EBS_HH_EAC.csv"),
                             col_types = cols(
                               mpan = col_character(),
                               eac = col_double()
                               ))


df_nhh_eac <- read_csv(file.path(base_path, "external-data/data-files-that-need-constant-updating", "EBS_NHH_EAC.csv"),
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

df_onSupplyMasters <- read_csv(file.path(base_path, "external-data/data-files-that-need-constant-updating", "onSupply_masterFinal.csv"),
                       col_types = cols(
                         MPAN = col_character(),
                         TPIName = col_character(),
                         Segment = col_character()
                       ))

# Read W3W query into single string variable
sqlquery_jw_W3W <- read_file(file.path(base_path, "external-data/query-snowflake-jwW3W.txt"))
# run query request using variable
df_jw_W3W <- query_sf(sqlquery_jw_W3W, show.query = TRUE)

# Read W3W query into single string variable
sqlquery_jw_address <- read_file(file.path(base_path, "external-data/query-snowflake-jwAddress.txt"))
# run query request using variable
df_jw_address <- query_sf(sqlquery_jw_address, show.query = TRUE)

# Read IFS D1 query into single string variable
sqlquery_ifs_D1s <- read_file(file.path(base_path, "external-data/query-snowflake-ifsD1s.txt"))
# run query request using variable
df_ifs_D1s <- query_sf(sqlquery_ifs_D1s, show.query = TRUE)

# read pre-IFS d1 data into variable df
df_preIfs_D1s <- read_csv(file.path(base_path, "external-data/data-files-that-need-constant-updating", "D1 report 28_10_2024.csv"),
                          col_types = cols(
                            record_id = col_character(),
                            mpan = col_character(),
                            msn = col_character(),
                            notification_creation_date = col_date(format = "%d/%m/%Y"),
                            fault_description = col_character()
                          ))

# read MTS data into variable df
df_MTS <- read_csv(file.path(base_path, "external-data/data-files-that-need-constant-updating", "MTS_success_testid.csv"),
                          col_types = cols(
                            testId = col_character(),
                            remoteAddress = col_character(),
                            serialNumber = col_character(),
                            requestReference = col_character(),
                            meterType = col_character(),
                            receivedTimestamp = col_date(format = "%Y-%m-%dT%H:%M:%SZ"),
                            completedTimestamp = col_date(format = "%Y-%m-%dT%H:%M:%SZ"),
                            plant_number = col_character(),
                            sim_number = col_character()
                          ))

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
# FURTHER df_sup & df_mop CLEANING
############################################################################

# DROP COLUMNS
df_mop <- df_mop %>% select(-meter_category)

# ADD COLUMNS
# # import_export currently unavailable for mop due to meter_qualifier not being 
# # a field in IFS portfolio
# df_mop <- df_mop %>%
#   mutate(import_export_flag = ifelse(grepl("-0000[0-9]$", meter_qualifier), "Export", "Import"))

# add outcode column
df_combined_cleaned <- df_combined_cleaned %>%
  mutate(site_outcode = ifelse(str_detect(site_postcode, " "),
                               word(site_postcode, 1),
                               site_postcode))


############################################################################
# JOINING ALL ADDRESS DATA AND PRIORITISING #1 - df_jw_address INTO df_mop
############################################################################

# 
df_mop <- df_mop %>%
  left_join(
    df_jw_address,
    by = c("mpan" = "reference"), 
    suffix = c(".mop", ".jw")    # Distinguish columns from each source
  ) %>%
  mutate(
    # For each column you want to prioritize df_jw_address where present,
    # otherwise fall back to df_mop.
    site_address  = coalesce(site_address.jw,  site_address.mop),
    site_postcode = coalesce(site_postcode.jw, site_postcode.mop)
  ) %>%
  select(
    # Keep all the columns you need, but drop the .mop and .jw versions
    # once you've coalesced them into final columns:
    -site_address.mop, 
    -site_postcode.mop,
    -site_address.jw, 
    -site_postcode.jw,
    -created_at
  )


############################################################################
# JOINING df_sup & df_mop
# + JOINING ALL ADDRESS DATA AND PRIORITISING #2
############################################################################

# Order both dfs by msn and mpan before the join
df_sup_deduped_noNA <- df_sup_deduped_noNA %>%
  arrange(msn, mpan)
df_mop <- df_mop %>%
  arrange(msn, mpan)

#re-assigning data types as these were lost at some point
df_sup_deduped_noNA <- df_sup_deduped_noNA %>%
  mutate(
    mpan                 = as.character(mpan),
    msn                  = as.character(msn),
    # date_installed       = parse_date(date_installed, format = "%Y-%m-%d"),
    # customer_active_date = parse_datetime(customer_active_date, format = "%Y-%m-%d %H:%M:%S"),
    eac                  = as.numeric(eac),
    # eac_date             = parse_date(eac_date, format = "%Y-%m-%d")
  )
  

df_mop <- df_mop %>%
  mutate(
    mpan                  = as.character(mpan),
    # mop_start             = parse_date(mop_start, format = "%Y-%m-%d"),
    # mop_end               = parse_date(mop_end, format = "%Y-%m-%d"),
    # supplier_start        = parse_date(supplier_start, format = "%Y-%m-%d"),
    # supplier_end          = parse_date(supplier_end, format = "%Y-%m-%d"),
    # dc_start              = parse_date(dc_start, format = "%Y-%m-%d"),
    # dc_end                = parse_date(dc_end, format = "%Y-%m-%d"),
    # commission_status_date = parse_date(commission_status_date, format = "%Y-%m-%d"),
    # energised_status_date = parse_date(energised_status_date, format = "%Y-%m-%d"),
    # date_installed        = parse_date(date_installed, format = "%Y-%m-%d"),
    cop                   = as.character(cop),
    eac                   = as.numeric(eac)
  )


# drop record_id columns before join
df_sup_deduped_noNA <- df_sup_deduped_noNA %>% select(-record_id)
df_mop <- df_mop %>% select(-record_id)

# Tag each data frame to show where data came from
df_sup_deduped_noNA <- df_sup_deduped_noNA %>% mutate(in_sup = TRUE)
df_mop <- df_mop %>% mutate(in_mop = TRUE)


write_csv(df_sup_deduped_noNA, file.path(base_path, "exported-data/df_sup_deduped_noNA.csv"))

# Perform a full join, keeping all (distinct) rows and columns
df_combined <- full_join(df_sup_deduped_noNA, df_mop, by = c("msn", "mpan")) %>%
  # Create the SUP_OR_MOP column based on whether in_sup / in_mop are NA or not
  mutate(
    SUP_OR_MOP = case_when(
      !is.na(in_sup) &  is.na(in_mop) ~ "SUP",   # present only in df_sup
      is.na(in_sup) & !is.na(in_mop) ~ "MOP",   # present only in df_mop
      !is.na(in_sup) & !is.na(in_mop) ~ "BOTH"   # present in both
    )
  ) %>%
  distinct() %>%
  select(-in_sup, -in_mop)

# add record_id column back in
df_combined <- df_combined %>%
  mutate(record_id = paste(mpan, msn, sep = " - ")) %>%  # Create the new column
  select(record_id, everything())                                # Move it to the left-most position

# Ensuring columns are in correct format
df_combined <- df_combined %>%
  mutate(
    date_installed.x = as.Date(date_installed.x),
    date_installed.y = as.Date(date_installed.y)
  )

# ADDING HELPER COLUMN TO COUNT DUPLICATE record_id VALUES
df_combined <- df_combined %>%
  mutate(meter_count = ave(record_id, record_id, FUN = length)) %>%
  select(1, meter_count, everything())  # Move meter_count to the second position


############################################################################
# HANDLING df_combined COLUMN DUPLICATES (.i.e., .x and .y headers)
############################################################################

# write_csv(df_combined, file.path(base_path, "exported-data/df_combined_ANAL.csv"))

# List of columns to process
# These are all columns that appear in BOTH sup AND mop dfs and therefore need merging

# THIS CODE SNIPPET FINDS ALL .x AND .y FIELDS
# Get all column names
all_cols <- names(df_combined)
# Find those ending in .x or .y
x_cols <- all_cols[endsWith(all_cols, ".x")]
y_cols <- all_cols[endsWith(all_cols, ".y")]
# Strip off the ".x" and ".y"
x_base <- sub("\\.x$", "", x_cols)
y_base <- sub("\\.y$", "", y_cols)
# Intersection of both sets (i.e., columns that have .x and .y versions)
columns_to_merge <- intersect(x_base, y_base)


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
                        # "eac", "profile_class", "meter_operator")) {
                        "eac", "outstation_type", "ssc", "meter_operator")) {
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



df_combined <- df_combined %>%
  mutate(
    # Replace "Unknown" with NA. We use NA_character_ because ssc is a character column at first.
    ssc = if_else(ssc == "Unknown", NA_character_, ssc),
    # Convert ssc to numeric
    ssc = as.numeric(ssc)
    )


############################################################################
# JOINING ALL ADDRESS DATA AND PRIORITISING #3 - df_ecoes INTO df_combined
############################################################################

df_combined <- df_combined %>%
  # 1. Join ONLY the columns 'mpan', 'site_address', 'site_postcode' from df_ecoes
  left_join(
    df_ecoes %>% 
      select(record_id, site_address, site_postcode), 
    by = "record_id", 
    suffix = c(".combined", ".ecoes")
  ) %>%
  # 2. Update the columns only if BOTH columns from df_ecoes are non-NA
  mutate(
    site_address = if_else(
      !is.na(site_address.ecoes) & !is.na(site_postcode.ecoes),
      site_address.ecoes,          # Use value from df_ecoes
      site_address.combined        # Otherwise, keep the original from df_combined
    ),
    site_postcode = if_else(
      !is.na(site_address.ecoes) & !is.na(site_postcode.ecoes),
      site_postcode.ecoes,
      site_postcode.combined
    )
  ) %>%
  # 3. Remove the suffix columns, keeping everything else from df_combined
  select(-ends_with(".combined"), -ends_with(".ecoes"))


############################################################################
# CLEANING ADDRESS COLUMNS IN df_combined
############################################################################

# removing any street_address values with a post code at their end

# # A simplified pattern that captures typical postcodes:
# postcode_pattern <- "(?i)(?:[A-Z]{1,2}\\d[A-Z\\d]? ?\\d[A-Z]{2})$"
# Comprehensive pattern for standard UK postcodes + GIR. 
postcode_pattern <- "(?i)(GIR\\s?0AA|((([A-Z][0-9]{1,2})|(([A-Z][A-HK-Y][0-9]{1,2})|(([A-Z][0-9][A-Z])|([A-Z][A-HK-Y][0-9][A-Z]))))\\s?[0-9][A-Z]{2}))"
df_combined <- df_combined %>%
  mutate(
    # Remove if it appears at the end, possibly with a preceding space or comma
    # The pattern includes optional preceding whitespace or punctuation (like a comma or space)
    site_address = str_remove(
      site_address,
      paste0("[,\\s]*", postcode_pattern, "$")
    ),
    # Then trim up any leftover spaces
    site_address = str_squish(site_address)
  )


# split (on comma) single address into as many as needed, ~2mins
df_combined <- df_combined %>%
  mutate(
    # 1) Replace semicolons with commas (or directly split with a regex).
    site_address_std = str_replace_all(site_address, ";", ","),
    
    # 2) Split on one-or-more commas
    site_address_list = str_split(site_address_std, "[,]+")
  ) %>%
  # 3) Remove empty entries after trimming
  mutate(
    site_address_list = map(site_address_list, ~ {
      # Trim each piece
      x_trimmed <- str_squish(.x)   # removes extra interior spaces & leading/trailing
      # Keep only non-empty
      x_trimmed[x_trimmed != ""]
    })
  ) %>%
  # 5) "Unnest" the list column wide, so each piece lands in its own column
  unnest_wider(site_address_list, names_sep = "_") %>%
  # Remove the extra column
  select(-site_address_std)

# write_csv(df_combined_TEST, file.path(base_path, "exported-data/df_combined_TEST.csv"))


# then join back up into 5 columns (keep full address column just in case)

# !!!WARNING!!!
# ON ~500K ROWS AND ~16 STREET COLUMNS), CAN TAKE ~130 MINS
# 
# # Automatically find all column names that start with "street"
# street_cols_1 <- grep("^site_address_list_", names(df_combined), value = TRUE)
# print_column_stats(df_combined[, street_cols_1])
# 
# time_then <- Sys.time()
# df_combined_cleaned <- clean_address_data(df_combined)
# time_now <- Sys.time()
# print(time_now - time_then)
# 
# street_cols_2 <- grep("^site_address_list_", names(df_combined_cleaned), value = TRUE)
# print_column_stats(df_combined_cleaned[, street_cols_2])
# 
# # safe copy to avoid constant rerunning and waiting times
# df_combined_cleaned_SAFE <- df_combined_cleaned

# use safe copies if needed:
df_combined_cleaned <- df_combined_cleaned_SAFE


#write_csv(df_combined_cleaned, file.path(base_path, "exported-data/df_combined_cleaned.csv"))

# clean bad post codes
df_combined_cleaned <- df_combined_cleaned %>%
  mutate(
    site_postcode = str_trim(site_postcode),
    site_postcode = str_remove_all(site_postcode, "[,\\.\\[\\]\\(\\)]"),
    site_postcode = str_remove(site_postcode, "(?i)^Unknown\\s+"),
    site_postcode = if_else(!str_detect(site_postcode, "\\d"), "", site_postcode),
    site_postcode = str_trim(site_postcode), # trim again
  )





############################################################################
# ADDING EXTERNAL DATA INTO NEW/EXISTING COLUMNS
############################################################################

# CURRENTLY COMMENTED OUT BECAUSE I NEED TO BETTER DEFINE WHAT MAKES A METER LTNR (SEE ONENOTE COMMENTS)
# # Create the 'LTNR' column in 'df_combined'
# df_combined$ltnr_flag <- ifelse(df_combined$mpan %in% df_ltnr$Service_Point, "Y", "N")



# # fill missing dc & mc values with data from ecoes
df_combined_cleaned <- df_combined_cleaned %>%
  left_join(df_ecoes %>% select(record_id, measurement_class, dc), by = "record_id") %>%
  mutate(
    measurement_class = coalesce(measurement_class.x, measurement_class.y),
    dc                = coalesce(dc.x, dc.y)
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"))


# Create the 'rts_flag' column in df_combined
df_combined_cleaned$rts_flag <- ifelse(df_combined_cleaned$ssc %in% df_rts_ssc_codes$SSC_Id, 'Y', 'N')



# fill `eac` & 'eac_date' columns with hh data from file
df_combined_cleaned <- df_combined_cleaned %>%
  left_join(df_hh_eac %>% select(mpan, eac), by = "mpan") %>%
  mutate(
    # Check which rows are actually getting a new eac
    eac_filled = is.na(eac.x) & !is.na(eac.y),
    
    # Fill eac from eac.y if eac.x is NA
    eac = coalesce(as.numeric(eac.x), eac.y),
    
    # Update eac_date for any row that was successfully filled
    eac_date = if_else(eac_filled, as.Date("2024-12-19"), as.Date(eac_date)) # this is just the date I "created" the hh eac data
  ) %>%
  # Remove temporary columns
  select(-eac.x, -eac.y, -eac_filled)

# Join df_nhh_eac onto df_combined by mpan
df_combined_cleaned <- df_combined_cleaned %>%
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
df_combined_cleaned <- df_combined_cleaned %>%
  left_join(
    df_jw_signalStrength %>%
      select(record_id, status_comment, type_name, completed_date, signal_strength),
    by = "record_id"
  )

# adding W3W data to df_combined
# this uses some logic to get down to 1 set of data per unique mpan in df_jw_w3w
# basically it uses rows with data, or arbitrarily the first in the group.
df_jw_W3W_deduped <- df_jw_W3W %>%
  # Flag rows that have data in site_w3w or meter_w3w
  mutate(has_data = if_else(
    (!is.na(site_w3w) & site_w3w != "") | 
      (!is.na(meter_w3w) & meter_w3w != ""), 1, 0
  )) %>%
  group_by(mpan) %>%
  # First, pick rows with has_data = 1 if they exist
  # If multiple or none, we’ll just pick the first row out of that subset
  slice_max(order_by = has_data, with_ties = TRUE) %>%
  slice(1) %>%
  ungroup() %>%
  select(-has_data)  # Remove temporary column
# Now you can safely join on mpan with only one row per mpan
df_combined_cleaned <- df_combined_cleaned %>%
  left_join(df_jw_W3W_deduped, by = "mpan")
# write_csv(df_jw_W3W, file.path(base_path, "exported-data/df_jw_w3w_anal.csv"))


# adds tpi and segment to df_combined
df_combined_cleaned <- df_combined_cleaned %>%
  left_join(df_onSupplyMasters %>% select(MPAN, TPIName, Segment), by = c("mpan" = "MPAN"))


# adds legacy (pre-IFS) and IFS D1 fault data

# Step 1: Merge data from df_preifs_D1s
df_combined_cleaned <- df_combined_cleaned %>%
  left_join(
    df_preIfs_D1s %>%
      select(
        record_id,
        notification_creation_date,
        fault_description
      ),
    by = "record_id"
  )

# Step 2: Merge data from df_ifs_D1s, overwriting if not NA
df_combined_cleaned <- df_combined_cleaned %>%
  left_join(
    df_ifs_D1s %>%
      select(
        record_id,
        notification_creation_date,
        short_text,
        fault_description
      ),
    by = "record_id",
    suffix = c("_preifs", "_ifs") # rename overlapping columns
  ) %>%
  mutate(
    notification_creation_date = coalesce(notification_creation_date_ifs, notification_creation_date_preifs),
    fault_description          = coalesce(fault_description_ifs, fault_description_preifs),
    short_text = short_text
  ) %>%
  # Drop the intermediate *_preifs and *_ifs columns
  select(
    -notification_creation_date_preifs,
    -notification_creation_date_ifs,
    -fault_description_preifs,
    -fault_description_ifs
  )



# add latest df_MTS data to meters
df_MTS_cleaned <- df_MTS %>%
  mutate(
    requestReference = sub("^NEXUS:(\\d{13})$", "\\1", requestReference),
    requestReference = sub("^(\\d{13})\\sJan$", "\\1", requestReference),
    requestReference = sub("^(\\d{13})\\sMPAN$", "\\1", requestReference),
    requestReference = sub("^MPAN-(\\d{13})\\s*$", "\\1", requestReference),
    requestReference = sub("^(\\d{13})_9876$", "\\1", requestReference),
    requestReference = sub("^(\\d{13})_10000$", "\\1", requestReference),
    requestReference = sub("^(\\d{13})_10001$", "\\1", requestReference)
  )

df_MTS_cleaned <- df_MTS_cleaned %>%
  mutate(
    record_id_1 = paste(requestReference, serialNumber, sep = " - "),
    record_id_2 = paste(requestReference, plant_number, sep = " - ")
  )


# check for record_id match from MTS success data and add timestamp and "success" if true.
df_combined_cleaned <- df_combined_cleaned %>%
  mutate(
    last_comms_dial_timestamp = case_when(
      record_id %in% df_MTS_cleaned$record_id_1 ~ 
        df_MTS_cleaned$completedTimestamp[ match(record_id, df_MTS_cleaned$record_id_1) ],
      record_id %in% df_MTS_cleaned$record_id_2 ~ 
        df_MTS_cleaned$completedTimestamp[ match(record_id, df_MTS_cleaned$record_id_2) ],
      TRUE ~ as.Date(NA)  
    ),
    # last_comms_dial_outcome is "Success" whenever we have a non-NA timestamp
    last_comms_dial_outcome = if_else(
      !is.na(last_comms_dial_timestamp),
      "Success",
      NA_character_
    ),
    # Overwrite communication_method if we had a successful match
    communication_method = case_when(
      record_id %in% df_MTS_cleaned$record_id_1 ~ 
        df_MTS_cleaned$remoteAddress[ match(record_id, df_MTS_cleaned$record_id_1) ],
      record_id %in% df_MTS_cleaned$record_id_2 ~ 
        df_MTS_cleaned$remoteAddress[ match(record_id, df_MTS_cleaned$record_id_2) ],
      TRUE ~ communication_method
    )
  )



# write_csv(df_combined_cleaned_TESTY, file.path(base_path, "exported-data/df_combined_cleaned_TEST.csv"))




############################################################################
# REDUCED VIEW (I.E., SINGLE VIEW OF DEMAND) EXPORT FOR SCALING PARTNERS
############################################################################

# List of columns to remove
columns_to_remove <- c(
  # "record_id",
  "meter_count", 
  
  # below is mop only
  "customer_name_mop",
  "micro_flag",
  "undumbed_amr",
  "outstation_pin",
  "data_comment",
  "mop_appointment_type",
  "mop_contract_reference",
  "ebs_mop_billing_status",
  "ebs_energy_segment",
  "outcode",
  "gsp_group",
  "retrieval_method",
  "msmtd_efd",
  "cop",
  "multi_mpan",
  "installed_in_current_mop_appointment",
  "meter_location",
  "current_rating",
  "map_id",
  "edf_msn",
  "ct_ratio",
  "vt_ratio",
  "feeder_status",
  "config_code",
  "baud_rate",
  "number_of_channels",
  "phase_wire", # removed because only populating ~3k rows as of jan'25
  "number_of_dials"
)


# Remove specified columns
df_single_view <- df_combined_cleaned[, !names(df_combined_cleaned) %in% columns_to_remove]


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




