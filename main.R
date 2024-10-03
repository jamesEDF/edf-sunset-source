# library(devtools)
library(edfr)
library(readr)
library(dplyr)

# Get today's date in YYYY-MM-DD format
today_date <- Sys.Date()
# Store the repeated file path in a variable
base_path <- "C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source/"

# Read entire query into a single string variable
sqlquery_sup <- read_file(file.path(base_path, "query-snowflake-sup.txt"))
sqlquery_mop <- read_file(file.path(base_path, "query-snowflake-mop.txt"))

# # the below is commented out to save on load times and £cost of Snowflake
  # # run query request using variable
# df_sup <- query_sf(sqlquery_sup, show.query = TRUE) # current load time: <1min
# df_mop <- query_sf(sqlquery_mop, show.query = TRUE) # current load time: ~25mins
# # Export queries to CSV files
# write_csv(df_sup, file.path(base_path, paste0("exported-data/df_sup_", today_date, ".csv")))
# write_csv(df_mop, file.path(base_path, paste0("exported-data/df_mop_", today_date, ".csv")))


# Read exported CSV files
df_sup <- read_csv(file.path(base_path, "exported-data/df_sup.csv"), 
                   col_types = cols(
                     mpan = col_character(),
                     date_installed = col_date(format = "%Y-%m-%d"),
                     .default = col_guess()   # Guess the rest
                   ))


df_mop <- read_csv(file.path(base_path, "exported-data/df_mop.csv"), 
                   col_types = cols(
                     mpan = col_character(),  # Define specific columns
                     mop_start = col_date(format = "%Y-%m-%d"),
                     mop_end = col_date(format = "%Y-%m-%d"),
                     supplier_start = col_date(format = "%Y-%m-%d"),
                     supplier_end = col_date(format = "%Y-%m-%d"),
                     dc_start = col_date(format = "%Y-%m-%d"),
                     dc_end = col_date(format = "%Y-%m-%d"),
                     commission_status_date = col_date(format = "%Y-%m-%d"),
                     energised_status_date = col_date(format = "%Y-%m-%d"),
                     install_date = col_date(format = "%Y-%m-%d"),
                     .default = col_guess()   # Guess the rest
                   ))


df_sup <- df_sup %>%
  rename(
    install_date = date_installed
  )

# Drop specific columns (e.g., drop column1, column2)
# df_sup <- df_sup %>% select(-column1, -column2)
df_mop <- df_mop %>% select(-meter_category)

# Filter to keep only the row with the latest `date_installed` for each `record_id` in df_sup
  # This is because there are multiple differing values for the same `record_id`
df_sup_latest <- df_sup %>%
  group_by(record_id) %>%
  slice_max(install_date) %>%
  ungroup()


# Perform a full join, keeping all rows and columns
df_combined <- full_join(df_sup, df_mop, by = "record_id")


# the below joins on any duplicated (.x and .y) columns where .y
  # has been filled with NA, and so keeps the non-NA value.
# Get the names of duplicated columns without the suffixes
dup_cols <- intersect(names(df_sup), names(df_mop))
dup_cols <- setdiff(dup_cols, "id")  # Exclude key columns  
# Combine all duplicated columns
clean_df <- df_combined %>%
  mutate(across(
    all_of(dup_cols),
    ~ coalesce(.x, get(paste0(cur_column(), ".y"))),
    .names = "{.col}"
  )) %>%
  select(-ends_with(".x"), -ends_with(".y"))

write_csv(df_combined, file.path(base_path, paste0("exported-data/df_combined_", today_date, ".csv")))
write_csv(clean_df, file.path(base_path, paste0("exported-data/clean_df_", today_date, ".csv")))



