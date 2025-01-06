
# complete function that does the whole processing in one step
clean_address_data <- function(df,
                               prefix = "street",
                               max_street_cols = 5,
                               bad_patterns = c("^\\.$", "^-+$", "^unknown$"),
                               patterns = c(
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
                                 "^\\d+ - \\d+$",
                                 "^Building \\d+$",
                                 "^Export Only$",
                                 "^First Floor Supply$",
                                 "^Second Floor Supply$",
                                 "^Feeder Pillar$",
                                 "^Temporary Builders Supply$",
                                 "^Pumping Station$",
                                 "^Communal Lighting$",
                                 "^Temporary Supply$",
                                 "^Public Convenience$",
                                 "^Non Postal$",
                                 "^Garage$",
                                 "^Barn$",
                                 "^Office$",
                                 "^Flat$",
                                 "^L/L$",
                                 "^Unit \\d+[A-Za-z]$",
                                 "^Unit \\d+-\\d+$",
                                 "^Unit \\d+ - \\d+$",
                                 "^Non Paf$",
                                 "^Extension$",
                                 "^Kiosk$",
                                 "^Fire Station$",
                                 "^Llds Supply$",
                                 "^Workshop$",
                                 "^-\\d+$",
                                 "^\\d+ - \\d+[A-Za-z]$",
                                 "^First Floor$"
                               )) {
  # Required libraries
  require(dplyr)
  require(stringr)
  
  # ---------------------------------------------------------------------------
  # Helper Functions
  # ---------------------------------------------------------------------------
  
  # Print column stats function
  print_column_stats <- function(df) {
    stats <- sapply(df, function(x) {
      total <- length(x)
      non_blank <- sum(!is.na(x))
      blank <- total - non_blank
      c("Populated" = non_blank, "Blank" = blank)
    })
    print(stats)
  }
  
  # Clean spaces in character columns
  clean_spaces <- function(df) {
    df[] <- lapply(df, function(column) {
      if (is.character(column)) {
        column <- trimws(column)           # Leading/trailing spaces
        column <- gsub("\\s+", " ", column)# Extra spaces within strings
      }
      column
    })
    df
  }
  
  # Remove bad patterns from character columns
  clean_bad_values <- function(df, bad_patterns) {
    df[] <- lapply(df, function(column) {
      if (is.character(column)) {
        for (pat in bad_patterns) {
          column[grepl(pat, column, ignore.case = TRUE)] <- NA
        }
      }
      column
    })
    df
  }
  
  # Shift non-empty values to the left in "street" columns
  shift_streets_left <- function(row, prefix = "street") {
    street_cols <- grep(paste0("^", prefix, "\\d+$"), names(row))
    if (length(street_cols) == 0) {
      return(row)
    }
    streets <- row[street_cols]
    # Replace non-breaking spaces and trim
    streets <- gsub("\u00A0", " ", streets)
    streets <- trimws(streets)
    # Keep only non-empty, non-NA
    non_empty_streets <- streets[!is.na(streets) & streets != ""]
    # Pad
    non_empty_streets <- c(non_empty_streets, rep("", length(street_cols) - length(non_empty_streets)))
    row[street_cols] <- non_empty_streets
    row
  }
  
  # Clean and combine two adjacent street columns
  clean_and_combine <- function(df, col1, col2, patterns) {
    correction_count <- 0
    combined_flag <- logical(length(df[[col1]]))
    
    # Ensure character
    df[[col1]] <- as.character(df[[col1]])
    df[[col2]] <- as.character(df[[col2]])
    
    # Replace empty strings with NA
    df[[col1]][df[[col1]] == ""] <- NA
    df[[col2]][df[[col2]] == ""] <- NA
    
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
    # Return both df and correction_count
    list(df = df, correction_count = correction_count)
  }
  
  # Repeatedly apply cleaning and combining until no corrections remain
  clean_and_combine_all_streets <- function(df, prefix, patterns) {
    # Identify street columns
    street_cols <- grep(paste0("^", prefix, "\\d+$"), names(df), value = TRUE)
    
    # Apply shift once before starting
    df <- as.data.frame(t(apply(df, 1, shift_streets_left, prefix = prefix)))
    
    # Keep looping until all corrections are zero
    repeat {
      correction_counts <- integer(0)
      # Re-shift before each pass
      df <- as.data.frame(t(apply(df, 1, shift_streets_left, prefix = prefix)))
      
      # Loop through adjacent pairs
      for (i in seq_along(street_cols)[-length(street_cols)]) {
        col1 <- street_cols[i]
        col2 <- street_cols[i + 1]
        result <- clean_and_combine(df, col1, col2, patterns)
        df <- result$df
        correction_counts <- c(correction_counts, result$correction_count)
      }
      
      if (all(correction_counts == 0)) break
    }
    
    df
  }
  
  # ---------------------------------------------------------------------------
  # Main Cleaning Steps
  # ---------------------------------------------------------------------------
  
  # 1. Clean spaces
  df <- clean_spaces(df)
  
  # 2. Clean bad values
  df <- clean_bad_values(df, bad_patterns)
  
  # 3. Clean and combine all streets
  df <- clean_and_combine_all_streets(df, prefix = prefix, patterns = patterns)
  
  # 4. Title case street columns
  street_cols <- grep(paste0("^", prefix, "\\d+$"), names(df), value = TRUE)
  df[street_cols] <- lapply(df[street_cols], function(col) {
    if (is.character(col)) {
      str_to_title(col)
    } else {
      col
    }
  })
  
  # 5. Combine any extra street columns into the last allowed column (e.g., street5)
  # Identify all street columns again in case the number changed
  street_cols <- grep(paste0("^", prefix, "\\d+$"), names(df), value = TRUE)
  
  if (length(street_cols) > max_street_cols) {
    # Columns beyond street5
    extra_street_cols <- street_cols[(max_street_cols + 1):length(street_cols)]
    
    # Combine them into the last allowed street column
    df[[street_cols[max_street_cols]]] <- apply(
      df[, c(street_cols[max_street_cols], extra_street_cols)], 
      1, 
      function(row) {
        main_val <- row[1]
        extras <- row[-1]
        extras <- extras[!is.na(extras) & extras != ""]
        if (length(extras) > 0) {
          if (!is.na(main_val) && main_val != "") {
            return(paste(c(main_val, extras), collapse = ", "))
          } else {
            return(paste(extras, collapse = ", "))
          }
        } else {
          return(main_val)
        }
      }
    )
    
    # Remove the extra street columns
    df <- df[, !(names(df) %in% extra_street_cols)]
  }
  
  # Return the cleaned df
  return(df)
}


print_column_stats <- function(df) {
  stats <- sapply(df, function(x) {
    total <- length(x)
    non_blank <- sum(!is.na(x))
    blank <- total - non_blank
    c("Populated" = non_blank, "Blank" = blank)
  })
  print(stats)
}




