# library(devtools)
library(edfr)
library(readr)
library(dplyr)
library(data.table)

# Get today's date in YYYY-MM-DD format
today_date <- Sys.Date()
# Store the repeated file path in a variable
base_path <- "C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source/"

# Read entire query into a single string variable
sqlquery_sup <- read_file(file.path(base_path, "query-snowflake-sup.txt"))
sqlquery_mop <- read_file(file.path(base_path, "query-snowflake-mop.txt"))
# sqlquery_sme <- read_file(file.path(base_path, "query-snowflake-sme.txt"))


# # the below is commented out to save on load times and £cost of Snowflake
# # run query request using variable
# df_sup <- query_sf(sqlquery_sup, show.query = TRUE) # current load time: <1min
# df_mop <- query_sf(sqlquery_mop, show.query = TRUE) # current load time: ~25mins
# # df_sme <- query_sf(sqlquery_sme, show.query = TRUE) # current load time: <1min
# # Export queries to CSV files
# write_csv(df_sup, file.path(base_path, paste0("exported-data/df_sup_", today_date, ".csv")))
# write_csv(df_mop, file.path(base_path, paste0("exported-data/df_mop_", today_date, ".csv")))
# # write_csv(df_sme, file.path(base_path, paste0("exported-data/df_sme_", today_date, ".csv")))


# Read exported CSV files
df_sup <- read_csv(file.path(base_path, paste0("exported-data/df_sup_", today_date, ".csv")), 
                   col_types = cols(
                     mpan = col_character(),
                     date_installed = col_date(format = "%Y-%m-%d"),
                     .default = col_guess()   # Guess the rest
                   ))

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

# df_sme <- read_csv(file.path(base_path, paste0("exported-data/df_sme_", today_date, ".csv")), 
#                    col_types = cols(
#                      mpan = col_character(),
#                      meter_id = col_character(),
#                      meter_point_id = col_character(),
#                      product_end_date = col_date(format = "%Y-%m-%d"),
#                      agreement_start_date = col_date(format = "%Y-%m-%d"),
#                      meterpoint_supply_start_date = col_date(format = "%Y-%m-%d"),
#                      .default = col_guess()   # Guess the rest
#                    ))

# read external files
df_ltnr <- read_csv(
  file.path(base_path, "external-data", "LTNR.csv"),
  col_types = cols(Service_Point = col_character()),
  col_select = "Service_Point"
)
                             
df_sup <- df_sup %>%
  rename(
    install_date = date_installed
  )

# Drop specific columns (e.g., drop column1, column2)
# df_sup <- df_sup %>% select(-column1, -column2)
df_mop <- df_mop %>% select(-meter_category)

# # Filter to keep only the row with the latest `date_installed` for each `record_id` in df_sup
#   # This is because there are multiple differing values for the same `record_id`
# df_sup <- df_sup %>%
#   group_by(record_id) %>%
#   slice_max(install_date) %>%
#   ungroup()


# Perform a full join, keeping all rows and columns
df_combined <- full_join(df_sup, df_mop, by = "record_id") # join sup+mop
# df_combined <- df_sup %>% # join sup+mop+sme
#   full_join(df_mop, by = "record_id") %>%
#   full_join(df_sme, by = "record_id")



# # the below merges duplicated columns and keeps the first one
# df_cleaned <- df_combined %>%
#   mutate(
#     mpan = coalesce(mpan.x, mpan.y),
#     msn = coalesce(msn.x, msn.y),
#     meter_type = coalesce(meter_type.x, meter_type.y),
#     install_date = coalesce(install_date.x, install_date.y),
#     outstation_type = coalesce(outstation_type.x, outstation_type.y),
#     communication_method = coalesce(communication_method.x, communication_method.y),
#     communication_address = coalesce(communication_address.x, communication_address.y)
#   ) %>%
#   select(-ends_with(".x"), -ends_with(".y"))  # Remove the .x and .y columns




# Create the 'LTNR' column in 'df_combined'
df_combined$LTNR <- ifelse(
  df_combined$mpan.x %in% df_ltnr$Service_Point | 
    df_combined$mpan.y %in% df_ltnr$Service_Point, 
  "Y", "N"
)




write_csv(df_combined, file.path(base_path, paste0("exported-data/df_combined_", today_date, ".csv")))
# write_csv(df_cleaned, file.path(base_path, paste0("exported-data/clean_df_", today_date, ".csv")))



df_mop |> count(msn,name="n_msn") |> 
  count(n_msn)

df_mop |> group_by(msn) |> filter(n()==4)

.Last.value |> arrange(msn)

.Last.value[1:2,] |> glimpse()

df_mop |> 
  group_by(msn) |> 
  summarise(n=n(),n_mpan=n_distinct(mpan), n_start=n_distinct(mop_start)) |> 
  count(n,n_mpan,n_start) |>
  arrange(n,n_mpan,n_start) |> 
  print(n=100)


df_mop |> 
  group_by(msn) |> 
  summarise(n=n(),n_mpan=n_distinct(mpan), n_start=n_distinct(mop_start)) |> 
  group_by(n,n_mpan,n_start) |> 
  sample_n(1) |> 
  ungroup() |> 
  left_join(df_mop,by="msn") |> 
  arrange(n,n_mpan,n_start,msn,mpan)









