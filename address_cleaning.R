# cleaning addresses TEST
library(stringr)
library(readr)
library(dplyr)
library(data.table)

# Store the repeated file path in a variable
base_path <- "C:/Users/dyer07j/Desktop/Coding/sunset_github REPO/edf-sunset-source/"


df_addr_mop <- read_csv(file.path(base_path, "address_clean_data_MOP_test.csv"))
df_addr_sup <- read_csv(file.path(base_path, "address_clean_data_SUP_test.csv"))
df_addr_eco <- read_csv(file.path(base_path, "address_clean_data_ECOES_test.csv"))

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

# Function to remove cells that are just a symbol
clean_bad_values <- function(df, bad_patterns = c("^\\.$", "^-+$", "^unknown$")) {
  df[] <- lapply(df, function(column) {
    if (is.character(column)) {
      # For each bad pattern, replace the cell with NA if it matches (case-insensitive)
      for (pat in bad_patterns) {
        column[grepl(pat, column, ignore.case = TRUE)] <- NA
      }
    }
    return(column)
  })
  return(df)
}




df_addr_mop_2 <- df_addr_mop %>%
  clean_spaces() %>%
  clean_bad_values()



# Function to shift non-empty values to the left in the "street" columns
shift_streets_left <- function(row, prefix = "street") {
  street_cols <- grep(paste0("^", prefix, "\\d+$"), names(row))
  if (length(street_cols) == 0) {
    return(row)
  }
  
  streets <- row[street_cols]
  
  # Clean up spaces
  streets <- gsub("\u00A0", " ", streets)  # Replace non-breaking spaces
  streets <- trimws(streets)               # Remove leading/trailing whitespace
  
  # Keep only non-empty, non-NA
  non_empty_streets <- streets[!is.na(streets) & streets != ""]
  
  # Pad to match original length
  non_empty_streets <- c(non_empty_streets, rep("", length(street_cols) - length(non_empty_streets)))
  
  # Replace in row
  row[street_cols] <- non_empty_streets
  return(row)
}


# Modified clean_and_combine function to return correction_count
clean_and_combine <- function(df, col1, col2) {
  # Convert the columns to character
  df[[col1]] <- as.character(df[[col1]])
  df[[col2]] <- as.character(df[[col2]])
  
  # Replace empty strings with NA
  df[[col1]][df[[col1]] == ""] <- NA
  df[[col2]][df[[col2]] == ""] <- NA
  
  # Patterns
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
    "^Unit [A-Za-z]\\d+$",          # Unit letternumber
    "^M\\d{1,3}$",                  
    "^A\\d{1,3}$",                  
    "^Site \\d+$",
    "^Landlords Supply$",
    "^Landlord Supply$",
    "^Landlords Supply \\d+$",
    "^Landlord Supply \\d+$",
    "^Landlords Supply \\d+ - \\d+$",
    "^Landlord Supply \\d+ - \\d+$",
    "^Landlords Supply \\d+-\\d+$",
    "^Landlord Supply \\d+-\\d+$",
    "^Landlords Lighting$",
    "^Landlord Lighting$",
    "^Landlord$",
    "^Landlords$",
    "^Landlord \\d+$",
    "^Landlords \\d+$",
    "^Unit$",
    "^Communal Supply$",
    "^Stair Lighting$",
    "^Flats$",
    "^Electric$",
    "^L/L Supply$",
    "^S/C$",
    "^Ground Floor$",
    "^First Floor$",
    "^Adjacent$",
    "^Llrds Supply$",
    "^\\d+ - \\d+$",            # number, space, dash, space, number (e.g., "114 - 115")
    "^Building \\d+$",          # "Building" followed by digits (e.g., "Building 12")
    "^Export Only$",            # "Export Only"
    "^First Floor Supply$",     # "First Floor Supply"
    "^Second Floor Supply$",    # "Second Floor Supply"
    "^Feeder Pillar$",          # "Feeder Pillar"
    "^Temporary Builders Supply$", # "Temporary Builders Supply"
    "^Pumping Station$",        # "Pumping Station"
    "^Communal Lighting$",      # "Communal Lighting"
    "^Temporary Supply$",       # "Temporary Supply"
    "^Public Convenience$",     # "Public Convenience"
    "^Non Postal$",             # "Non Postal"
    "^Garage$",                 # "Garage"
    "^Barn$",                   # "Barn"
    "^Office$",                 # "Office"
    "^Flat$",                   # "Flat"
    "^L/L$",                    # "L/L"
    "^Unit \\d+[A-Za-z]$",      # "Unit" followed by number+letter (e.g., "Unit 1d")
    "^Unit \\d+-\\d+$",         # "Unit" followed by number-number (e.g., "Unit 1-34")
    "^Unit \\d+ - \\d+$",       # "Unit" followed by number space-dash-space number (e.g., "Unit 1 - 34")
    "^Non Paf$",                # "Non Paf"
    "^Extension$",              # "Extension"
    "^Kiosk$",                  # "Kiosk"
    "^Fire Station$",           # "Fire Station"
    "^Llds Supply$",            # "Llds Supply"
    "^Workshop$"                # "Workshop"
    
  )
  
  
  
  correction_count <- 0
  combined_flag <- logical(length(df[[col1]]))
  
  df[[col1]] <- mapply(function(x, y, idx) {
    if (is.na(x) | is.na(y)) return(x)
    if (x == y) {
      df[[col2]][idx] <<- NA
      return(x)
    }
    if (any(sapply(patterns, function(pat) grepl(pat, x, ignore.case = TRUE)))) {
      correction_count <<- correction_count + 1
      combined_flag[idx] <<- TRUE
      return(paste(x, y))
    } else {
      return(x)
    }
  }, df[[col1]], df[[col2]], seq_along(df[[col1]]))
  
  df[[col2]] <- ifelse(combined_flag, NA, df[[col2]])
  
  # Print and return corrections
  print(paste("Total corrections made:", correction_count))
  
  # Return both df and correction_count so the caller can decide what to do next
  return(list(df = df, correction_count = correction_count))
}


# Modified clean_and_combine_all_streets to loop until no corrections are made
clean_and_combine_all_streets <- function(df, prefix = "street") {
  # Identify street columns
  street_cols <- grep(paste0("^", prefix, "\\d+$"), names(df), value = TRUE)
  
  # Apply shift once before starting
  df <- as.data.frame(t(apply(df, 1, shift_streets_left, prefix = prefix)))
  
  # Keep looping until all corrections are zero
  repeat {
    # Track corrections for this pass
    correction_counts <- integer(0)
    
    # Re-shift streets before each full pass to ensure alignment
    df <- as.data.frame(t(apply(df, 1, shift_streets_left, prefix = prefix)))
    
    # Loop through adjacent pairs of street columns
    for (i in seq_along(street_cols)[-length(street_cols)]) {
      col1 <- street_cols[i]
      col2 <- street_cols[i + 1]
      
      # Clean and combine
      result <- clean_and_combine(df, col1, col2)
      df <- result$df
      correction_counts <- c(correction_counts, result$correction_count)
    }
    
    # Check if all corrections are zero
    if (all(correction_counts == 0)) {
      # If yes, break out of the loop
      break
    }
    # If not all zero, it will loop again
  }
  
  return(df)
}

# Apply to your dataset
df_addr_mop_combined <- clean_and_combine_all_streets(df_addr_mop_2)


# Apply title case to all cells in the data frame
street_cols <- grep("^street\\d+$", names(df), value = TRUE)
df_addr_mop_combined_titleCase[street_cols] <- lapply(df_addr_mop_combined_titleCase[street_cols], function(col) {
  if (is.character(col)) {
    str_to_title(col)
  } else {
    col
  }
})



print_column_stats(df_addr_mop)
print_column_stats(df_addr_mop_combined_titleCase)

write_csv(df_addr_mop_combined_titleCase, file.path(base_path, "address_clean_data_MOP_test_COMBINED.csv"))


