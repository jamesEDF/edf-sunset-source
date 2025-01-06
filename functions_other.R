
############################################################################################
# FUNCTION(S) THAT RUN(S) THROUGH DUPLICATE "SETS" IN DF_SUP AND TRIES TO GET DOWN TO 1 ROW
############################################################################################

# Helper function that keeps ALL rows that share the latest (max) value in the specified col.
pick_latest_keep_ties <- function(df, colname) {
  # Check if all values are NA
  if (all(is.na(df[[colname]]))) {
    return(df)
  }
  
  # Proceed to find the max value
  max_val <- max(df[[colname]], na.rm = TRUE)
  
  # Identify which rows have that max_val
  is_max <- df[[colname]] == max_val
  
  # If none are max (should not happen unless all were NA), do nothing
  if (!any(is_max, na.rm = TRUE)) {
    return(df)
  }
  
  # Keep only the rows that share this latest date
  df <- df[is_max, , drop = FALSE]
  return(df)
}
# Main function
dedupe_one_record_id <- function(df_subset) {
  # If there's only one row, nothing to do
  if (nrow(df_subset) <= 1) return(df_subset)
  
  # Exclude certain columns from differ-check
  exclude_cols <- c("record_id", "record_id_count")
  candidate_cols <- setdiff(names(df_subset), exclude_cols)
  
  # Identify columns that actually differ in this subset
  differ_cols <- candidate_cols[sapply(candidate_cols, function(col) {
    length(unique(df_subset[[col]])) > 1
  })]
  
  # ---- RULE 1: Drop rows that are all-NA in the differ_cols ----
  if (length(differ_cols) > 0) {
    # For each row, check if *all* differ_cols are NA
    all_na_in_differ <- apply(df_subset[differ_cols], 1, function(x) all(is.na(x)))
    if (any(all_na_in_differ)) {
      df_subset <- df_subset[!all_na_in_differ, ]
    }
  }
  if (nrow(df_subset) <= 1) return(df_subset)
  
  # ---- RULE 2: If date_installed differs, keep rows with the latest date_installed ----
  if ("date_installed" %in% differ_cols) {
    df_subset <- pick_latest_keep_ties(df_subset, "date_installed")
  }
  if (nrow(df_subset) <= 1) return(df_subset)
  
  # ---- RULE 3: If customer_name differs, pick rows w/ latest customer_active_date ----
  if ("customer_name" %in% differ_cols && "customer_active_date" %in% names(df_subset)) {
    df_subset <- pick_latest_keep_ties(df_subset, "customer_active_date")
  }
  if (nrow(df_subset) <= 1) return(df_subset)
  
  # ---- RULE 4: If eac_date differs, keep rows w/ latest eac_date ----
  if ("eac_date" %in% differ_cols) {
    df_subset <- pick_latest_keep_ties(df_subset, "eac_date")
  }
  
  # If multiple rows remain after all rules, keep them all
  # ("skip that set" if they're truly tied in all relevant columns).
  return(df_subset)
}

############################################################################################
# NEXT FUNCTION
############################################################################################





