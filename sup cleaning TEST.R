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
# IMPORTING DATA
############################################################################

# Store the repeated file path in a variable
base_path <- "C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source"

# write_csv(df_mop, file.path(base_path, "exported-data/df_mop_exported.csv"))

# Read exported CSV files
df_sup_clean_TEST_1 <- read_csv(file.path(base_path, 
                             "exported-data/df_sup_deduped_noNA.csv"),
                   col_types = cols(
                     mpan                = col_character(),
                     msn                 = col_character(),
                     date_installed      = col_date(format = "%d/%m/%Y"),
                     customer_active_date = col_datetime(format = "%Y-%m-%dT%H:%M:%SZ"),
                     eac_date            = col_datetime(format = "%Y-%m-%dT%H:%M:%SZ"),
                     .default            = col_guess()
                   )
)


# clean out any with len(msn) < 8 or > 15










