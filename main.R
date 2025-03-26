# run_address_code <- TRUE
run_address_code <- FALSE

library(tidyr)
library(stringr)
library(devtools)
library(edfr)
library(readr)
library(dplyr)
library(data.table)
library(purrr)
library(writexl)
library(openxlsx)
library(readxl)
library(lubridate)

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
df_sup <- query_sf(sqlquery_sup, show.query = FALSE) # current load time: <1min
df_mop <- query_sf(sqlquery_mop, show.query = FALSE) # current load time: <1min
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
                   , na = c("Unknown","NA")
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
df_ecoes <- query_sf(sqlquery_ecoes, show.query = FALSE)

# read external data files
df_rts_ssc_codes <- read_csv(file.path(base_path, "external-data", "RTS SSC Codes.csv"),
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
df_jw_signalStrength <- query_sf(sqlquery_jw_signalStrength, show.query = FALSE)

df_masterFinal <- read_excel(list.files(path = file.path(base_path, "external-data", "data-files-that-need-constant-updating"),
    pattern = "^Master Data Final.*\\.xlsx$",
    full.names = TRUE
  ),
  sheet = "Data"
)[, c("MPAN", "Segment")] %>%
  mutate(MPAN = as.character(MPAN), Segment = as.character(Segment)) %>%
  distinct(MPAN, .keep_all = TRUE)


# Read W3W query into single string variable
sqlquery_jw_W3W <- read_file(file.path(base_path, "external-data/query-snowflake-jwW3W.txt"))
# run query request using variable
df_jw_W3W <- query_sf(sqlquery_jw_W3W, show.query = FALSE)

# Read JWaddress query into single string variable
sqlquery_jw_address <- read_file(file.path(base_path, "external-data/query-snowflake-jwAddress.txt"))
# run query request using variable
df_jw_address <- query_sf(sqlquery_jw_address, show.query = FALSE)

# Read IFS D1 query into single string variable
sqlquery_ifs_D1s <- read_file(file.path(base_path, "external-data/query-snowflake-ifsD1s.txt"))
# run query request using variable
df_ifs_D1s <- query_sf(sqlquery_ifs_D1s, show.query = FALSE)

# read pre-IFS d1 data into variable df
df_preIfs_D1s <- read_csv(file.path(base_path, "external-data", "D1 report 28_10_2024.csv"),
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


df_centrestage <- read_csv(
  file.path(base_path, "external-data", "data-files-that-need-constant-updating", "centrestage.csv"),
  col_select = c("rn", "mpan", "msn", "certification_date", "certification_expiry_date", "date_removed"),
  col_types = cols(
    rn = col_integer(),
    mpan = col_character(),
    msn = col_character(),
    certification_date = col_date(format = "%Y-%m-%d"),
    certification_expiry_date = col_date(format = "%Y-%m-%d"),
    date_removed = col_date(format = "%Y-%m-%d")
  )
) %>%
  filter(rn == 1) %>%
  mutate(
    manufacture_date = certification_date,
    recert_date = certification_expiry_date,
    record_id = paste0(mpan, " - ", msn)) %>%
  distinct() %>%
  group_by(record_id) %>%
  arrange(desc(recert_date)) %>%
  slice(1) %>%
  ungroup() %>%
  select(-certification_date, -certification_expiry_date)

# read SMART district codes
df_SMARTDistricts <- read_csv(
  file.path(base_path, "external-data", "Outcode_mappings_final.csv"),
  col_select = c("Postcode", "DF district"),
  col_types = cols(
    Postcode = col_character(),
    "DF district" = col_character()
  ))

# Read sapwam (manufacture date) query into single string variable
sqlquery_sapwamManufacture <- read_file(file.path(base_path, "external-data/query-snowflake-sapwamManufacture.txt"))
# run query request using variable
df_sapwamManufacture <- query_sf(sqlquery_sapwamManufacture, show.query = FALSE)

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



# write_csv(df_combined_cleaned_TEST, file.path(base_path, "exported-data/df_combined_cleaned_TEST.csv"))


############################################################################
# JOINING ALL ADDRESS DATA AND PRIORITISING #1 - df_jw_address INTO df_mop
############################################################################

# 
df_mop <- df_mop %>%
  left_join(
    df_jw_address %>% select(mpan, site_address, site_postcode),
    by = c("mpan"), 
    suffix = c(".mop", ".jw")    # Distinguish columns from each source
  ) %>%
  mutate(
    # For each column you want to prioritize df_jw_address where present,
    # otherwise fall back to df_mop.
    site_address  = coalesce(site_address.jw,  site_address.mop),
    site_postcode = coalesce(site_postcode.jw, site_postcode.mop)
  ) %>%
    # Keep all the columns you need, but drop the .mop and .jw versions
    # once you've coalesced them into final columns:
  select(-ends_with(".mop"), -ends_with(".jw"))
    

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
  distinct() %>%
  group_by(mpan) %>%
  mutate(
    sup_or_mop = if_else(
      any(!is.na(in_sup)) & any(!is.na(in_mop)),
      "BOTH",                              # MPAN appears in both datasets
      if_else(any(!is.na(in_sup)), "SUP",   # only in df_sup
              "MOP")                      # only in df_mop
    )
  ) %>%
  ungroup() %>%
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
                        "eac", "outstation_type", "ssc", "meter_operator", "voltage_type",
                        "ct_ratio"
                        )) {
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
# THIS SNIPPET CAN TAKE 2-3 hours

if (run_address_code) {
  # The entire block below is run if run_address_code == TRUE
  
  # Automatically find all column names that start with "street"
  street_cols_1 <- grep("^site_address_list_", names(df_combined), value = TRUE)
  print_column_stats(df_combined[, street_cols_1])
  
  time_then <- Sys.time()
  df_combined_cleaned <- clean_address_data(df_combined)
  time_now <- Sys.time()
  print(time_now - time_then)
  
  street_cols_2 <- grep("^site_address_list_", names(df_combined_cleaned), value = TRUE)
  print_column_stats(df_combined_cleaned[, street_cols_2])
  
  # safe copy to avoid constant rerunning and waiting times
  df_combined_cleaned_SAFE <- df_combined_cleaned
}

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
    eac_date = if_else(eac_filled, Sys.Date(), as.Date(eac_date)) # this is just the date I "created" the hh eac data
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

# add lat,long columns from jw into df_combined
df_combined_cleaned <- df_combined_cleaned %>%
  # First join on record_id to bring in geolocation columns
  left_join(
    df_jw_address %>%
      select(record_id, geolocation_latitude, geolocation_longitude, geolocation),
    by = "record_id",
    suffix = c("", ".rid")
  ) %>%
  # Then join on mpan to bring in geolocation values for rows that didn't match by record_id
  left_join(
    df_jw_address %>%
      select(mpan, geolocation_latitude, geolocation_longitude, geolocation),
    by = "mpan",
    suffix = c("", ".mpan")
  ) %>%
  # For each geolocation column, use the value from the record_id join if available; 
  # if not, fall back to the value from the mpan join.
  mutate(
    geolocation_latitude = coalesce(geolocation_latitude, geolocation_latitude.mpan),
    geolocation_longitude = coalesce(geolocation_longitude, geolocation_longitude.mpan),
    geolocation = coalesce(geolocation, geolocation.mpan)
  ) %>%
  # Optionally remove the extra columns brought in from the mpan join
  select(-ends_with(".mpan"), -ends_with(".rid"))


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


# adds segment to df_combined
df_combined_cleaned <- df_combined_cleaned %>%
  left_join(df_masterFinal %>% select(MPAN, Segment), by = c("mpan" = "MPAN"))


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
    communication_address = case_when(
      record_id %in% df_MTS_cleaned$record_id_1 ~ 
        df_MTS_cleaned$remoteAddress[ match(record_id, df_MTS_cleaned$record_id_1) ],
      record_id %in% df_MTS_cleaned$record_id_2 ~ 
        df_MTS_cleaned$remoteAddress[ match(record_id, df_MTS_cleaned$record_id_2) ],
      TRUE ~ communication_address
    )
  )

# add outcode column
df_combined_cleaned <- df_combined_cleaned %>%
  mutate(site_outcode = ifelse(str_detect(site_postcode, " "),
                               word(site_postcode, 1),
                               site_postcode))

# create SMART District column
df_combined_cleaned <- df_combined_cleaned %>%
  left_join(df_SMARTDistricts, by = c("site_outcode" = "Postcode")) %>%
  rename(smart_district = `DF district`)

# creating sunset_date column and adding initial data
df_combined_cleaned <- df_combined_cleaned %>%
  mutate(
    sunset_date = case_when(
      rts_flag == "Y"                              ~ as.Date("30-06-2025", format = "%d-%m-%Y"),
      communication_method %in% c("CS", "GS")        ~ as.Date("01-12-2025", format = "%d-%m-%Y"),
      communication_method == "PS"                   ~ as.Date("31-01-2027", format = "%d-%m-%Y"),
      communication_method == "3G"                   ~ as.Date("01-01-2023", format = "%d-%m-%Y"),
      TRUE                                         ~ NA_Date_
    )
  )






# write_csv(df_combined_cleaned_TEST, file.path(base_path, "exported-data/df_combined_cleaned_TEST.csv"))

############################################################################
# RECERT LOGIC AREA
############################################################################

# adds recert_date and manufacture_date from centrestage
df_combined_cleaned <- df_combined_cleaned %>%
  left_join(df_centrestage %>% select(record_id, manufacture_date, recert_date), by = "record_id")

df_combined_cleaned <- df_combined_cleaned %>%
  left_join(df_sapwamManufacture %>% select(record_id, manufacture_date), by = "record_id",
    suffix = c("", ".sap")
  ) %>%
  mutate(
    manufacture_date = coalesce(manufacture_date, manufacture_date.sap)
  ) %>%
  select(-manufacture_date.sap)

# removes rows where removal date is before today
df_combined_cleaned <- df_combined_cleaned %>%
  anti_join(
    df_centrestage %>% 
      filter(date_removed < Sys.Date()),
    by = "record_id"
  )

# using msn to estimate manufacture date
# this does a good chunk of empty rows. 
prefixes <- c("E", "EML", "EM", "D", "FF", "H", "HB", "I", "K", 
              "MG", "N5", "N6", "N7", "N9", "NG", "P", "PN4", "V", "Z")

pattern <- paste0("^(", paste(prefixes, collapse="|"), ")(\\d{2}).*")

convert_two_digit_year <- function(two_digits) {
  # If two_digits is NA or doesn't exist, return NA
  if (is.na(two_digits)) return(NA_integer_)
  
  y <- as.integer(two_digits)
  if (y >= 90 && y <= 99) {
    return(1900 + y)
  } else if (y >= 0 && y <= 25) {
    return(2000 + y)
  } else {
    return(NA_integer_)
  }
}


df_combined_cleaned <- df_combined_cleaned %>%
  mutate(
    # Extract any matching prefixes + two-digit years
    match_matrix    = str_match(msn, pattern),
    prefix_captured = match_matrix[, 2],
    two_digits      = match_matrix[, 3],
    four_digit_year = sapply(two_digits, convert_two_digit_year),
    
    # 1) Build a temporary date column based on your logic
    new_date = as.Date(
      ifelse(
        !is.na(four_digit_year), 
        paste0(four_digit_year, "-01-01"),  # e.g. "2003-01-01"
        NA_character_
      ),
      format = "%Y-%m-%d"
    ),
    
    # 2) Use coalesce to fill in only where the existing manufacture_date is NA
    manufacture_date = coalesce(manufacture_date, new_date)
  ) %>%
  select(-match_matrix, -prefix_captured, -two_digits, -four_digit_year, -new_date)


# if recert date is empty or after today, no recert required
# this is a duplicate row to create the column before logic below
df_combined_cleaned <- df_combined_cleaned %>%
  mutate(
    recert_required = if_else(!is.na(recert_date) & recert_date <= Sys.Date(), "Y", "N")
  )


df_combined_cleaned <- df_combined_cleaned %>%
  mutate(
    ## 1) Convert manufacture_date if not already Date:
    manufacture_date = as.Date(manufacture_date, format = "%Y-%m-%d"),
    
    ## 2) Apply your original rules first:
    #   - measurement_class in (C,E) => recert_date = +10 years
    #   - measurement_class=F & wc_or_ct="ct" => recert_date = +10 years
    #   - measurement_class in (B,D) => recert_required="N"
    recert_date = case_when(
      measurement_class %in% c("C", "E") ~ manufacture_date + years(10),
      measurement_class == "F" & wc_or_ct == "ct" ~ manufacture_date + years(10),
      TRUE ~ recert_date  # leave as-is for others
    ),
    recert_required = case_when(
      measurement_class %in% c("B", "D") ~ "N",
      TRUE ~ recert_required
    )
  ) %>%
  ## 3) Now layer in the NEW logic (1→2→3→4).
  mutate(
    #### LOGIC 2: If row passes Logic 1 AND has meter_type in {RCAMR,RCAMY,NCAMR}
    ####          AND “EDMI” in make_and_type (or outstation_type if that’s empty),
    ####          THEN recert_required depends on manufacture_date < 2008 or ≥ 2008.
    recert_required = case_when(
      # Must pass Logic 1
      (measurement_class %in% c("A", "G") | 
         (measurement_class == "F" & wc_or_ct == "ct")) &
        # meter_type is one of these
        meter_type %in% c("RCAMR", "RCAMY", "NCAMR") &
        # check for EDMI in make_and_type or outstation_type if make_and_type = ""
        (
          grepl("EDMI", make_and_type, ignore.case = TRUE) |
          grepl("MK7", make_and_type, ignore.case = TRUE) |
          grepl("MK10", make_and_type, ignore.case = TRUE) |
            
            (
              make_and_type == "" & 
                (
                grepl("EDMI", outstation_type, ignore.case = TRUE) |
                grepl("MK7", outstation_type, ignore.case = TRUE) |
                grepl("MK10", outstation_type, ignore.case = TRUE)
                )
            )
        ) &
        manufacture_date < as.Date("2008-01-01") ~ "Y",
      
      # Same condition, but manufacture_date >= 01-01-2008 => "N"
      (measurement_class %in% c("A", "G") | 
         (measurement_class == "F" & wc_or_ct == "ct")) &
        meter_type %in% c("RCAMR", "RCAMY", "NCAMR") &
        (
          grepl("EDMI", make_and_type, ignore.case = TRUE) |
          grepl("MK7", make_and_type, ignore.case = TRUE) |
          grepl("MK10", make_and_type, ignore.case = TRUE) |
            
            (
              make_and_type == "" & 
                (
                grepl("EDMI", outstation_type, ignore.case = TRUE) |
                grepl("MK7", outstation_type, ignore.case = TRUE) |
                grepl("MK10", outstation_type, ignore.case = TRUE)
                )
            )
        ) &
        manufacture_date >= as.Date("2008-01-01") ~ "N",
      
      # Otherwise, keep whatever recert_required is currently
      TRUE ~ recert_required
    ),
    
    #### LOGIC 3: If row passes Logic 1 AND has “Elster” in make_and_type
    ####           (or in outstation_type if make_and_type empty),
    ####           set recert_date = +10 years from manufacture_date
    recert_date = case_when(
      (measurement_class %in% c("A", "G") | 
         (measurement_class == "F" & wc_or_ct == "ct")) &
        (
          grepl("Elster", make_and_type, ignore.case = TRUE) |
          grepl("AS230", make_and_type, ignore.case = TRUE) |
          grepl("A1700", make_and_type, ignore.case = TRUE) |
          grepl("A1160", make_and_type, ignore.case = TRUE) |
          grepl("A1140", make_and_type, ignore.case = TRUE) |
          grepl("AS3000P", make_and_type, ignore.case = TRUE) |
          grepl("PB2", make_and_type, ignore.case = TRUE) |
          grepl("PB3", make_and_type, ignore.case = TRUE) |
          grepl("LM3", make_and_type, ignore.case = TRUE) |
            
            (
              make_and_type == "" & 
                (
                  grepl("Elster", outstation_type, ignore.case = TRUE) |
                  grepl("AS230", outstation_type, ignore.case = TRUE) |
                  grepl("A1700", outstation_type, ignore.case = TRUE) |
                  grepl("A1160", outstation_type, ignore.case = TRUE) |
                  grepl("A1140", outstation_type, ignore.case = TRUE) |
                  grepl("AS3000P", outstation_type, ignore.case = TRUE) |
                  grepl("PB2", outstation_type, ignore.case = TRUE) |
                  grepl("PB3", outstation_type, ignore.case = TRUE) |
                  grepl("LM3", outstation_type, ignore.case = TRUE)
                )
            )
        ) ~ manufacture_date + years(10),
      TRUE ~ recert_date
    ),
    
    #### LOGIC 4: If row passes Logic 1 AND has make_and_type that contains
    ####           any of ("Sprint XP", "EDMI MK7B", "ISKRA", "AS230")
    ####           (or outstation_type if make_and_type empty),
    ####           then recert_required = "Y"
    recert_required = case_when(
      (measurement_class %in% c("A", "G") | 
         (measurement_class == "F" & wc_or_ct == "ct")) &
        (
          grepl("Sprint XP", make_and_type, ignore.case=TRUE) |
            grepl("EDMI MK7B", make_and_type, ignore.case=TRUE) |
            grepl("ISKRA", make_and_type, ignore.case=TRUE) |
            grepl("AS230", make_and_type, ignore.case=TRUE) |
            (
              make_and_type == "" & 
                (
                  grepl("Sprint XP", outstation_type, ignore.case=TRUE) |
                    grepl("EDMI MK7B", outstation_type, ignore.case=TRUE) |
                    grepl("ISKRA", outstation_type, ignore.case=TRUE) |
                    grepl("AS230", outstation_type, ignore.case=TRUE)
                )
            )
        ) ~ "Y",
      TRUE ~ recert_required
    )
  ) %>%
  mutate(
    recert_date = case_when(
      (measurement_class %in% c("A", "G") | (measurement_class == "F" & wc_or_ct == "ct")) &
        is.na(recert_date) ~ manufacture_date + years(10),
      TRUE ~ recert_date
    )
  )

# if ANY meter is more than 20 years old, then set recert date to manufacture+20 and set recert_required to "Y".
df_combined_cleaned <- df_combined_cleaned %>%
  # Create a temporary flag for rows where recert_date is missing and manufacture_date is >20 years ago.
  mutate(
    update_flag = is.na(recert_date) & (manufacture_date <= Sys.Date() - years(20))
  ) %>%
  # Update recert_date and recert_required based on the flag.
  mutate(
    recert_date = if_else(update_flag, manufacture_date + years(20), recert_date),
    recert_required = if_else(update_flag, "Y", recert_required)
  ) %>%
  # Remove the temporary flag.
  select(-update_flag)


# if recert date is empty or after today, no recert required
# this is a duplicate row to catch rows after above logic
df_combined_cleaned <- df_combined_cleaned %>%
  mutate(
    recert_required = if_else(!is.na(recert_date) & recert_date <= Sys.Date(), "Y", "N")
  )


# write_csv(df_combined_cleaned_TEST, file.path(base_path, "exported-data/df_combined_cleaned_TEST.csv"))



############################################################################
# REDUCED VIEW (I.E., SINGLE VIEW OF DEMAND) EXPORT FOR SCALING PARTNERS
############################################################################
############################################################################
# REMOVE AND REORDER COLUMNS
############################################################################

# List of columns to remove
columns_to_remove <- c(
  # "record_id",
  "meter_count", 
  
  # below is mop only
  "micro_flag",
  "undumbed_amr",
  "outstation_pin",
  "data_comment",
  "mop_appointment_type",
  "mop_contract_reference",
  "ebs_mop_billing_status",
  "ebs_energy_segment",
  "msmtd_efd",
  "cop",
  "multi_mpan",
  "installed_in_current_mop_appointment",
  "meter_location",
  "current_rating",
  "map_id",
  "edf_msn",
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

df_single_view <- unique(df_single_view)


print(cat(names(df_single_view), sep = "\n"))

# Desired column order
desired_cols <- c(
  "record_id",
  "mpan",
  "msn",
  "meter_type",
  "date_installed",
  "profile_class",
  "import_export_flag",
  "rts_flag",
  "ssc",
  "energisation_status",
  "energisation_status_date",
  "make_and_type",
  "ct_ratio",
  "wc_or_ct",
  "voltage_type",
  "measurement_class",
  "manufacture_date",
  "recert_date",
  "recert_required",
  "outstation_id",
  "outstation_type",
  "meter_time_switch_code",
  "communication_method",
  "communication_provider",
  "communication_address",
  "retrieval_method",
  "level_1_username",
  "level_1_password",
  "level_2_username",
  "level_2_password",
  "level_3_username",
  "level_3_password",
  "last_comms_dial_timestamp",
  "last_comms_dial_outcome",
  "sup_or_mop",
  "supplier",
  "supplier_tier",
  "meter_operator",
  "mop_start",
  "mop_end",
  "dc",
  "da",
  "dno",
  "llf",
  "customer_name",
  "customer_active_date",
  "contract_status",
  "contract_start_date",
  "sa_status",
  "sa_start_date",
  "sa_end_date",
  "market",
  "tpiname",
  "Segment",
  "site_address",
  "site_address_list_1",
  "site_address_list_2",
  "site_address_list_3",
  "site_address_list_4",
  "site_address_list_5",
  "site_postcode",
  "site_outcode",
  "smart_district",
  "gsp_group",
  "geolocation_latitude",
  "geolocation_longitude",
  "geolocation",
  "site_w3w",
  "meter_w3w",
  "Last_EAC_Pot",
  "L0038_RF_Status",
  "eac",
  "eac_date",
  "type_name",
  "signal_strength",
  "status_comment",
  "completed_date",
  "short_text",
  "notification_creation_date",
  "fault_description"
)

# Reorder df_single_view columns
df_single_view <- df_single_view[ , desired_cols]

print(cat(names(df_single_view), sep = "\n"))

# arrange by record_id
df_single_view <- df_single_view %>% arrange(record_id)

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
  total <- length(x_char)
  populated <- sum(!is.na(x_char) & !(tolower(x_char) %in% tolower(blank_indicators)) & x_char != "")
  blank <- sum(is.na(x_char) | (tolower(x_char) %in% tolower(blank_indicators)) | x_char == "")
  percent_full <- (populated / total) * 100
  c(Populated = populated, Blank = blank, Percent_Full = percent_full)
}
counts <- sapply(df_single_view, calc_counts)

# Extract the "Percent_Missing" row (as a vector)
percent_full_row <- counts["Percent_Full", ]


############################################################################
# FORMAT XLSX FILE READY FOR EXPORT
############################################################################

sections_df <- data.frame(
  section = c("Meter details", "Comms details", "Meter agents", 
              "Customer info", "Site location", "Settlement",
              "Job details", "Fault details"),
  ncols   = c(22, 12, 10, 10, 15, 4, 4, 3),
  stringsAsFactors = FALSE
)

sections_df$end_col   <- cumsum(sections_df$ncols)
sections_df$start_col <- sections_df$end_col - sections_df$ncols + 1

wb <- createWorkbook()
addWorksheet(wb, "SINGLEVIEW")

writeData(
  wb,
  sheet = "SINGLEVIEW",
  x = as.data.frame(t(percent_full_row)),
  startRow = 2,
  startCol = 1,
  colNames = FALSE
)

writeData(
  wb,
  sheet = "SINGLEVIEW",
  x = df_single_view,
  startRow = 3,
  startCol = 1,
  headerStyle = createStyle(
    fgFill = "#E2EFDA",
    textDecoration = "bold",
    halign = "center",
    border = c("bottom","left","right")
  )
)

# Merge cells and write the header label for each section:
for (i in seq_len(nrow(sections_df))) {
  start_col <- sections_df$start_col[i]
  end_col   <- sections_df$end_col[i]
  
  mergeCells(
    wb, 
    "SINGLEVIEW", 
    cols = start_col:end_col, 
    rows = 1
  )
  
  writeData(
    wb, 
    "SINGLEVIEW", 
    sections_df$section[i],
    startRow = 1,
    startCol = start_col
  )
}

# Apply a style over all of row 1
# i.e., from col 1 to the maximum end_col from your sections
addStyle(
  wb, 
  sheet = "SINGLEVIEW", 
  style = createStyle(
    fgFill = "#D9E1F2",
    halign = "CENTER",
    border = c("bottom", "left", "right")), 
  rows = 1, 
  cols = 1:max(sections_df$end_col), 
  gridExpand = TRUE
)


setColWidths(wb, sheet = "SINGLEVIEW", cols = 1:max(sections_df$end_col), widths = "auto")

# Number of columns in df_single_view:
df_cols <- ncol(df_single_view)
# Sum of all column widths in sections_df
total_section_cols <- sum(sections_df$ncols)
# If there's a mismatch, stop code before writing to file:
if (df_cols != total_section_cols) {
  stop(
    "Column mismatch:\n",
    "df_single_view has ", df_cols, " columns, but the sum of sections_df$ncols is ",
    total_section_cols, ". Please fix before proceeding."
  )
}



saveWorkbook(wb, file = file.path(base_path, "exported-data/single_view_FORMATTED.xlsx"), overwrite = TRUE)




















