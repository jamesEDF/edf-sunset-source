library(edfr)
library(readr)
library(dplyr)
library(data.table)

# Get today's date in YYYY-MM-DD format
today_date <- Sys.Date()
# Store the repeated file path in a variable
base_path <- "C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source/"

# Read entire query into a single string variable
sqlquery_mop <- read_file(file.path(base_path, "query-snowflake-mop.txt"))

# # the below is commented out to save on load times and £cost of Snowflake
# # run query request using variable
# df_mop <- query_sf(sqlquery_mop, show.query = TRUE) # current load time: ~25mins
# # Export queries to CSV files
# write_csv(df_mop, file.path(base_path, paste0("exported-data/df_mop_", today_date, ".csv")))

df_mop <- read_csv(file.path(base_path, paste0("exported-data/df_mop_", today_date, ".csv")), 
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

# Drop specific columns (e.g., drop column1, column2)
# df_sup <- df_sup %>% select(-column1, -column2)
df_mop <- df_mop %>% select(-meter_category)


# Create a new column with the desired labels
df_mop <- df_mop %>%
  mutate(import_export = ifelse(grepl("-0000[0-9]$", meter_qualifier), "Export", "Import"))

print(df_mop[, c("msn", "meter_qualifier", "import_export")])

write_csv(df_mop, file.path("C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source/exported-data/testing importexport.csv"))



