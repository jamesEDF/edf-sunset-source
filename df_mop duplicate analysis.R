library(edfr)
library(readr)
library(dplyr)
library(data.table)
library(tidyr)
library(purrr)

today_date <- Sys.Date()

df <- df_mop
# Group by msn and summarize to check where values differ

# Find differing columns for each msn with duplicates
diff_columns <- df %>%
  group_by(msn) %>%
  filter(n() > 1) %>% # Focus on duplicates only
  summarise(across(everything(), ~ n_distinct(.x) > 1), .groups = 'drop') %>%
  pivot_longer(-msn, names_to = "column", values_to = "is_different") %>%
  filter(is_different) %>%
  group_by(msn) %>%
  summarise(different_columns = list(column))

# Count how many msns have each unique set of differing columns
diff_columns_summary <- diff_columns %>%
  unnest(different_columns) %>%
  group_by(msn) %>%
  summarise(different_columns = toString(unique(different_columns))) %>%
  count(different_columns, name = "msn_count")

# Print or view the summary
print(diff_columns_summary)

write_csv(diff_columns_summary, file.path(base_path, paste0("exported-data/diff_columns_", today_date, ".csv")))






# Read exported CSV files
df_testing <- read_csv(file.path("C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source/exported-data/diff_columns_2024-10-10 - Copy.csv"))

# Stack all columns into a single vector, then count unique values
unique_counts <- as.vector(unlist(df_testing)) %>% 
  table() %>% 
  as.data.frame()

# Rename columns for clarity
colnames(unique_counts) <- c("Value", "Count")

# Display the result
print(unique_counts)











