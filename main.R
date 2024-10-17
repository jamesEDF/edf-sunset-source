library(devtools)
library(edfr)
library(readr)
library(dplyr)
library(data.table)

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
                     mpan = col_character(),  # Define specific columns
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
                    
# # RENAME COLUMNS         
# df_sup <- df_sup %>%
#   rename(
#     install_date = date_installed
#   )

# DROP COLUMNS
df_mop <- df_mop %>% select(-meter_category)

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
df_combined <- full_join(df_sup, df_mop, by = "msn") %>% 
  distinct()




# Create the 'LTNR' column in 'df_combined'
df_combined$LTNR <- ifelse(
  df_combined$mpan.x %in% df_ltnr$Service_Point | 
    df_combined$mpan.y %in% df_ltnr$Service_Point, 
  "Y", "N"
)


write_csv(df_combined, file.path(base_path, "exported-data/df_combined_exported.csv"))










