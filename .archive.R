# cleaning addresses TEST
library(stringr)
library(readr)
library(dplyr)
library(data.table)

# Store the repeated file path in a variable
base_path <- "C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source/"


df_addr_mop <- read_csv(file.path(base_path, "address_clean_data_MOP_test.csv"))
df_addr_sup <- read_csv(file.path(base_path, "address_clean_data_SUP_test.csv"))


# function that shows blank-to-populated cells ratio
print_column_stats <- function(df) {
  # Apply function to each column of the data frame
  stats <- sapply(df, function(x) {
    total <- length(x)
    non_blank <- sum(!is.na(x))
    blank <- total - non_blank
    return(c("Populated" = non_blank, "Blank" = blank))
  })
  
  # Print the stats
  print(stats)
}

print_column_stats(df_addr_mop)


# Function to clean spaces in a data frame
clean_spaces <- function(df) {
  df[] <- lapply(df, function(column) {
    if (is.character(column)) {
      # Remove leading/trailing spaces and extra spaces within
      column <- trimws(column)                  # Leading/trailing spaces
      column <- gsub("\\s+", " ", column)       # Extra spaces within strings
    }
    return(column)
  })
  return(df)
}



df_addr_mop_2 <- clean_spaces(df_addr_mop)


# Function to shift non-empty values to the left in the "street" columns
shift_streets_left <- function(row, prefix = "street") {
  # 'row' should be a named vector representing a single row from your dataframe.
  # Identify all columns that match the pattern "street" followed by a number
  street_cols <- grep(paste0("^", prefix, "\\d+$"), names(row))
  
  # If no street columns found, return the row as is
  if (length(street_cols) == 0) {
    return(row)
  }
  
  # Extract the values of these street columns
  streets <- row[street_cols]
  
  # Clean up spaces
  streets <- gsub("\u00A0", " ", streets)  # Replace non-breaking spaces with regular spaces
  streets <- trimws(streets)               # Remove leading and trailing whitespace
  
  # Keep only non-empty, non-NA values
  non_empty_streets <- streets[!is.na(streets) & streets != ""]
  
  # Pad with empty strings so the length remains the same
  non_empty_streets <- c(non_empty_streets, rep("", length(street_cols) - length(non_empty_streets)))
  
  # Put the cleaned, left-aligned street values back into the row
  row[street_cols] <- non_empty_streets
  
  return(row)
}



# this combines two columns if the first one matches any of the conditions:
clean_and_combine <- function(df, col1, col2) {
  # Convert the columns to character type to ensure they are strings
  df[[col1]] <- as.character(df[[col1]])
  df[[col2]] <- as.character(df[[col2]])
  
  # Replace empty strings with NA
  df[[col1]][df[[col1]] == ""] <- NA
  df[[col2]][df[[col2]] == ""] <- NA
  
  # Patterns for each condition
  patterns <- c(
    "^\\d+ \\d+$",                  # number number
    "^\\d+-\\d+$",                  # number-number
    "^\\d+/\\d+$",                  # number/number
    "^\\d+ to \\d+$",               # number to number
    "^\\d+[A-Za-z]$",               # numberletter
    "^\\d+[A-Za-z]-\\d+[A-Za-z]$",  # numberletter-numberletter
    "^flat \\d+$",                  # flat number
    "^flats \\d+-\\d+$",            # flats number-number
    "^\\d+$",                       # Single number
    "^Block \\d+$",                 # Block number
    "^Block [A-Za-z]$",             # Block letter
    "^Block [A-Za-z]\\d+$",         # Block letternumber
    "^Unit \\d+$",                  # Unit number
    "^Unit [A-Za-z]$",              # Unit letter
    "^Unit [A-Za-z]\\d+$"           # Unit letternumber
  )
  
  # Counter to track corrections
  correction_count <- 0
  
  # Apply logic row-wise, track if col1 and col2 are combined
  combined_flag <- vector("logical", length(df[[col1]]))
  
  df[[col1]] <- mapply(function(x, y, idx) {
    # Skip rows where either column is NA
    if (is.na(x) | is.na(y)) {
      return(x)  # If NA, just return the value from col1 (no change)
    }
    
    # Check if col1 and col2 are duplicates (exactly the same)
    if (x == y) {
      df[[col2]][idx] <<- NA  # Set col2 to NA if they are duplicates
      return(x)  # If they are duplicates, just return col1 (no combination)
    }
    
    # Check if the first column matches any pattern
    if (any(sapply(patterns, function(pat) grepl(pat, x, ignore.case = TRUE)))) {
      correction_count <<- correction_count + 1  # Increment counter
      combined_flag[idx] <<- TRUE  # Mark this row as combined
      return(paste(x, y))  # Combine col1 and col2
    } else {
      return(x)  # Keep col1 as is
    }
  }, df[[col1]], df[[col2]], seq_along(df[[col1]]))
  
  # Now, modify col2 based on whether combination happened
  df[[col2]] <- ifelse(combined_flag, NA, df[[col2]])
  
  # Print the number of corrections made
  print(paste("Total corrections made:", correction_count))
  
  return(df)
}


# runs clean_and_combine() for all street combos (street1+street2, street2+street3, etc)
clean_and_combine_all_streets <- function(df, prefix = "street") {
  # Get all column names that match the streetX pattern
  street_cols <- grep(paste0("^", prefix, "\\d+$"), names(df), value = TRUE)
  
  # Before starting combination, ensure streets are shifted left
  df <- as.data.frame(t(apply(df, 1, shift_streets_left, prefix = prefix)))
  
  # Loop through adjacent pairs of street columns
  for (i in seq_along(street_cols)[-length(street_cols)]) {
    col1 <- street_cols[i]
    col2 <- street_cols[i + 1]
    
    # Apply the clean_and_combine function to the adjacent columns
    df <- clean_and_combine(df, col1, col2)
  }
  
  return(df)
}


# apply mass combine function
df_addr_mop_combined <- clean_and_combine_all_streets(df_addr_mop_2)
df_addr_mop_combined_2 <- clean_and_combine_all_streets(df_addr_mop_combined)



# Apply title case to all cells in the data frame
df_addr_mop_4 <- data.frame(lapply(df_addr_mop_3, function(x) if (is.character(x)) str_to_title(x) else x))





