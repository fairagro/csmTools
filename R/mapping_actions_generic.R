#' Join data with a lookup table from the dataset
#'
#' Performs a database-style join between a data frame and a lookup table,
#' with support for filtering, column selection, renaming, and multiple join types.
#'
#' @param action List containing:
#'   - `source$section`: Name of the section in dataset containing the lookup table
#'   - `on_key`: List with `source_key` and `lookup_key` vectors defining join keys
#'   - `select_columns`: Named list mapping new column names to lookup table column names
#'   - `join_type`: Type of join ("left", "right", "inner", "full"); defaults to "left"
#'   - `filter_rows`: Optional list with `column` and `equals` for pre-filtering lookup table
#' @param df Data frame to join to (the main/source data)
#' @param dataset List containing data tables, where names correspond to sections
#'
#' @return Data frame resulting from the join operation, or the input `df` unchanged
#'   if the lookup table doesn't exist
#'
#' @details
#' The function:
#' 1. Extracts and validates the lookup table from the dataset
#' 2. Optionally filters lookup table rows based on `filter_rows` criteria
#' 3. Selects only requested columns (plus join keys) from the lookup table
#' 4. Deduplicates the lookup table
#' 5. Renames selected columns according to `select_columns` mapping
#' 6. Executes the specified join type
#'
#' Column name collisions between tables trigger a warning and add ".x" and ".y"
#' suffixes to distinguish them.
#'
#' @noRd
#' 

.action_join <- function(action, df, dataset) {
  
  lookup_section <- action$source$section
  lookup_table <- dataset[[lookup_section]]
  
  # Return input if lookup table does not exist
  if (is.null(lookup_table)) {
    return(df)
  }
  
  # --- Setup & validation ---
  cols_map <- action$select_columns 
  
  join_type <- tolower(action$join_type %||% "left")  # left join as default
  valid_joins <- c("left", "right", "inner", "full")
  if (!join_type %in% valid_joins) {
    stop(paste0("Invalid join_type: '", join_type, "'; must be one of: ",
                paste(valid_joins, collapse = ", ")),
         call. = FALSE)
  }
  
  # Keys
  if (is.null(action$on_key)) stop("Missing 'on_key' definition.", call. = FALSE)
  source_keys <- unlist(action$on_key$source_key)
  lookup_keys <- unlist(action$on_key$lookup_key)
  
  if (!all(source_keys %in% names(df))) {
    stop(paste0("Source keys missing in main data: ",
                paste(setdiff(source_keys, names(df)), collapse = ", ")),
         call. = FALSE)
  }
  
  
  # --- Pre-process lookup table ---
  ## Filter rows
  if (!is.null(action$filter_rows)) {
    f_col <- action$filter_rows$column
    f_val <- action$filter_rows$equals
    if (!is.null(f_col) && f_col %in% names(lookup_table)) {
      lookup_table <- lookup_table[lookup_table[[f_col]] == f_val, ]
    }
  }
  
  ## Select columns (only those available in the source)
  requested_old_names <- unlist(cols_map)
  requested_new_names <- names(cols_map)
  
  available_cols <- intersect(requested_old_names, names(lookup_table))
  all_cols_needed <- unique(c(lookup_keys, available_cols))
  
  lookup_subset <- lookup_table[, all_cols_needed, drop = FALSE]
  
  ## Deduplicate
  lookup_subset <- dplyr::distinct(lookup_subset)
  
  # --- Rename columns to target names ---
  found_indices <- which(requested_old_names %in% available_cols)
  final_old_names <- requested_old_names[found_indices]
  final_new_names <- requested_new_names[found_indices]
  
  rename_vec <- stats::setNames(final_old_names, final_new_names)
  if (length(rename_vec) > 0) {
    lookup_subset <- dplyr::rename(lookup_subset, !!!rename_vec)
  }
  
  # --- Name collision handling ---
  collisions <- intersect(names(rename_vec), names(df))
  if (length(collisions) > 0) {
    warning(paste0(
      "Join collisions detected: the following columns exist in both data tables: ",
      paste(collisions, collapse = ", ")
    ), call. = FALSE)
  }
  
  # ---  Execute join ---
  join_by <- stats::setNames(lookup_keys, source_keys)

  joined_df <- switch(
    join_type,
    "left"  = dplyr::left_join(df, lookup_subset, by = join_by, suffix = c(".x", ".y")),
    "right" = dplyr::right_join(df, lookup_subset, by = join_by, suffix = c(".x", ".y")),
    "inner" = dplyr::inner_join(df, lookup_subset, by = join_by, suffix = c(".x", ".y")),
    "full"  = dplyr::full_join(df, lookup_subset, by = join_by, suffix = c(".x", ".y"))
  )
  
  return(joined_df)
}


#' Map values from a source column to a new column
#'
#' Creates or updates a column by mapping source values to target values using
#' a lookup dictionary. Handles unmapped values according to specified behavior.
#'
#' @param action List containing:
#'   - `input_map`: Named list identifying the source column
#'   - `map`: Named list of value mappings (source -> target)
#'   - `identity`: Optional vector of values that map to themselves
#'   - `output_header`: Name of the output column
#'   - `unmatched_code`: How to handle unmapped values ("na", "pass_through", "default_value")
#'   - `default_value`: Value to use when `unmatched_code = "default_value"`
#' @param df Data frame to transform
#' @param ... Additional arguments (unused)
#'
#' @return Data frame with the mapped column added or updated
#'
#' @details
#' The mapping dictionary is built by combining `identity` (values mapping to
#' themselves) and `map` (explicit mappings). Source values are coerced to
#' character for lookup. Unmapped values are handled according to `unmatched_code`:
#' - "na": Leave as NA (default)
#' - "pass_through": Keep original value
#' - "default_value": Use `default_value`
#'
#' @noRd
#' 

.action_map_values <- function(action, df, ...) {
  
  unmatched_code <- action$unmatched_code %||% "na"

  # Resolve input maps to handle synonyms
  inputs <- .resolve_input_map(action$input_map, df)
  if (is.null(inputs)) {
    return(df)
  }
  source_vector <- inputs[[1]] # map_values only uses the first input
  
  # Build the values dictionary
  final_map_list <- list()
  if (!is.null(action$identity)) {
    identity_list <- unlist(action$identity)
    identity_map <- stats::setNames(as.list(identity_list), identity_list)
    final_map_list <- c(final_map_list, identity_map)
  }
  if (!is.null(action$map)) {
    final_map_list <- c(final_map_list, action$map)
  }
  if (length(final_map_list) == 0) {
    # warning("map_values action has no map or identity keys. Skipping.", call. = FALSE)
    return(df)
  }
  
  final_map_vector <- unlist(final_map_list)
  
  # --- Apply mapping ---
  data_to_map <- as.character(source_vector)
  mapped_vector <- unname(final_map_vector[data_to_map])
  
  # --- Handle unmapped values ---
  unmapped_indices <- which(!is.na(source_vector) & is.na(mapped_vector))
  
  if (length(unmapped_indices) > 0) {
    unmapped_values <- unique(source_vector[unmapped_indices])
    
    if (unmatched_code == "na") {
    } else if (unmatched_code == "pass_through") {
      mapped_vector[unmapped_indices] <- source_vector[unmapped_indices]
    } else if (unmatched_code == "default_value") {
      default_value <- action$default_value %||% NA
      mapped_vector[unmapped_indices] <- default_value
    } else {
      warning(paste("Unrecognized 'unmatched_code' value:", unmatched_code), call. = FALSE)
    }
  }
  df[[action$output_header]] <- mapped_vector
  
  return(df)
}


#' Add a column with a constant value
#'
#' Creates a new column with a single constant value applied to all rows.
#' Optionally conditional on the presence of other columns.
#'
#' @param action List containing:
#'   - `output_header`: Name of the new column
#'   - `value`: The constant value to assign to all rows
#'   - `condition_on_presence_of`: Optional vector of column names; action only
#'     executes if at least one of these columns exists in the data frame
#' @param df Data frame to transform
#'
#' @return Data frame with the new column added, or unchanged if condition not met
#'
#' @noRd
#' 

.action_add_column <- function(action, df) {
  
  # Check if conditions is met, skip if not
  if (!is.null(action$condition_on_presence_of)) {
    condition_cols <- unlist(action$condition_on_presence_of)
    if (!any(condition_cols %in% names(df))) {
      return(df)
    }
  }
  df <- mutate(df, !!action$output_header := action$value)
  
  return(df)
}


#' Add a column with conditional values
#'
#' Creates a new column by evaluating one or more conditions and assigning values
#' when conditions are met. Multiple condition results can be concatenated.
#'
#' @param action List containing:
#'   - `output_header`: Name of the new column to create
#'   - `conditions`: List of condition specifications, each containing:
#'     - `on_column`: Column name (or synonym) to evaluate
#'     - One operator: `if_equals`, `if_not_equals`, `if_higher_than`, 
#'       `if_higher_equals`, `if_lower_than`, or `if_lower_equals`
#'     - `then_value`: Value to assign when condition is met (can be a literal
#'       value or a column name to pull values from)
#'   - `separator`: String used to join multiple condition results; defaults to "; "
#' @param df Data frame to transform
#'
#' @return Data frame with the new conditional column added
#'
#' @details
#' For each condition:
#' 1. The `on_column` is resolved to an actual column name (handles synonyms)
#' 2. The specified comparison operator is applied
#' 3. When the condition is TRUE, `then_value` is assigned (either as a literal
#'    or by looking up the value from another column)
#' 4. NA values in comparisons are treated as FALSE
#'
#' When multiple conditions are specified, rows where multiple conditions are met
#' have their results concatenated using `separator`. Empty results become NA.
#' The final column is type-converted to preserve numeric types when appropriate.
#'
#' @noRd
#' 

.action_add_column_conditional <- function(action, df) {
  
  output_name <- action$output_header
  conditions  <- action$conditions
  separator   <- action$separator %||% "; "
  

  results_matrix <- sapply(conditions, function(cond) {
    
    # Resolve condition column
    on_col_name <- .resolve_column_name(cond$on_column, names(df))
    if (is.null(on_col_name)) return(rep(NA_character_, nrow(df)))
    
    col_data <- df[[on_col_name]]
    
    # Determine operator/logic
    operator_keys <- c("if_equals", "if_not_equals", "if_higher_than", "if_higher_equals", "if_lower_than", "if_lower_equals")
    operator <- intersect(operator_keys, names(cond))
    
    is_met_vector <- switch(
      operator,
      "if_equals"      = as.character(col_data) == as.character(cond[[operator]]),
      "if_not_equals"  = as.character(col_data) != as.character(cond[[operator]]),
      "if_higher_than" = as.numeric(col_data) >  as.numeric(cond[[operator]]),
      "if_higher_equals"= as.numeric(col_data) >= as.numeric(cond[[operator]]),
      "if_lower_than"  = as.numeric(col_data) <  as.numeric(cond[[operator]]),
      "if_lower_equals" = as.numeric(col_data) <= as.numeric(cond[[operator]]),
      rep(FALSE, nrow(df))
    )
    # Handle NAs in comparison (treat as constraint not met)
    is_met_vector[is.na(is_met_vector)] <- FALSE
    
    # Resolve 'then value'
    raw_then <- cond$then_value
    final_val_to_use <- raw_then   # Default to literal value
    
    # Check if the config value maps to a column name
    if (!is.null(raw_then) && is.character(raw_then) && length(raw_then) == 1) {
      
      resolved_then_col <- .resolve_column_name(raw_then, names(df))
      if (!is.null(resolved_then_col)) {
        final_val_to_use <- df[[resolved_then_col]]
      }
    }
    
    # Create column
    ifelse(is_met_vector, as.character(final_val_to_use), NA_character_)
  })
  
  # collapse Matrix to Vector (handling multiple conditions)
  if (is.matrix(results_matrix)) {
    result_vector <- apply(results_matrix, 1, function(row_values) {
      paste(na.omit(row_values), collapse = separator)
    })
  } else {
    result_vector <- results_matrix
  }
  
  # Clean up
  result_vector[result_vector == ""] <- NA_character_
  # Fix numeric changed to character during the process
  result_vector <- type.convert(result_vector, as.is = TRUE)
  
  df[[output_name]] <- result_vector
  return(df)
}


#' Rename a column
#'
#' Renames a single column if it exists in the data frame.
#'
#' @param action List containing:
#'   - `input_name`: Current name of the column to rename
#'   - `output_name`: New name for the column
#' @param df Data frame to transform
#'
#' @return Data frame with the column renamed, or unchanged if the input column
#'   doesn't exist
#'
#' @noRd
#' 

.action_rename_column <- function(action, df) {
  
  if (action$input_name %in% names(df)) {
    df <- rename(df, !!action$output_name := !!action$input_name)
  }
  
  return(df)
}


#' Delete columns from a data frame
#'
#' Removes one or more columns if they exist in the data frame.
#'
#' @param action List containing:
#'   - `columns`: Character vector or list of column names to delete
#' @param df Data frame to transform
#'
#' @return Data frame with specified columns removed, or unchanged if none of
#'   the specified columns exist
#'
#' @noRd
#' 

.action_delete_column <- function(action, df) {
  
  cols_to_delete <- unlist(action$columns)
  existing_cols_to_delete <- intersect(cols_to_delete, names(df))
  
  if (length(existing_cols_to_delete) > 0) {
    df <- select(df, -all_of(existing_cols_to_delete))
  }
  
  return(df)
}


#' Apply a rowwise transformation using a formula
#'
#' Creates a new column by evaluating a formula expression rowwise using values
#' from one or more input columns.
#'
#' @param action List containing:
#'   - `input_map`: Named list mapping formula variable names to actual column
#'     names (or synonyms)
#'   - `formula`: String expression to evaluate; should reference the names
#'     defined in `input_map`
#'   - `output_header`: Name of the new column to create
#' @param df Data frame to transform
#'
#' @return Data frame with the new transformed column added, or unchanged if
#'   input columns cannot be resolved
#'
#' @details
#' The function:
#' 1. Resolves all input columns from `input_map` using column name synonyms
#' 2. Creates a temporary tibble with the resolved columns
#' 3. Evaluates the formula expression rowwise within this tibble context
#' 4. Adds the results as a new column to the original data frame
#'
#' The formula can use any valid R expression that operates on the mapped
#' column names, although good practice should privilege universal syntax whenever
#' possible to allow the portability of the YAML maps.
#'
#' @noRd
#'

.action_rowwise_transform <- function(action, df) {
  
  inputs <- .resolve_input_map(action$input_map, df)
  if (is.null(inputs) || length(inputs) == 0) {
    #df[[action$output_header]] <- NA
    return(df)
  }
  
  # Evaluate the formula in temp table
  inputs_tbl <- as_tibble(inputs)
  
  transformed_vector <- mutate(
    inputs_tbl,
    .result = !!rlang::parse_expr(action$formula)
  )$.result
  
  # Add new column to the working data frame.
  df[[action$output_header]] <- transformed_vector
  
  return(df)
}


#' Concatenate multiple columns into one
#'
#' Combines values from multiple columns into a single column, separated by a
#' specified delimiter. NA values are automatically excluded from concatenation.
#'
#' @param action List containing:
#'   - `output_header`: Name of the new concatenated column
#'   - `input_map`: Named list mapping local variable names to actual column
#'     names (or synonyms) to concatenate
#'   - `separator`: String to use between values (default: `" "`)
#'   - `on_missing`: How to handle missing input columns; `"skip"` (default)
#'     returns the data frame unchanged with a warning, `"proceed"` continues
#'     with available columns
#' @param df Data frame to transform
#'
#' @return Data frame with the new concatenated column added, or unchanged if
#'   required columns are missing and `on_missing = "skip"`
#'
#' @details
#' The function:
#' - Resolves all input columns from `input_map` using column name synonyms
#' - Concatenates non-NA values from each row with the specified separator
#' - Sets the result to NA if all values in a row are NA or the concatenation
#'   is empty
#' - Issues a warning and returns unchanged data if columns are missing and
#'   `on_missing = "skip"`
#'
#' @noRd
#'

.action_concatenate_columns <- function(action, df) {
  
  output_name <- action$output_header
  input_map <- action$input_map
  separator <- action$separator %||% " "
  on_missing <- action$on_missing %||% "skip"
  
  # New: Support for prefix and suffix
  prefix <- action$prefix %||% ""
  suffix <- action$suffix %||% ""
  
  # Resolve input columns
  inputs <- list()
  all_found <- TRUE
  for (local_name in names(input_map)) {
    col_spec <- input_map[[local_name]]
    found_col_name <- .resolve_column_name(col_spec, names(df))
    
    if (is.null(found_col_name)) {
      all_found <- FALSE
    } else {
      inputs[[found_col_name]] <- df[[found_col_name]]
    }
  }
  
  # Decide whether to proceed based on the 'on_missing' flag
  if (!all_found && on_missing == "skip") {
    warning(paste0("Skipping concatenate for '", output_name, "' because columns were missing."),
            call. = FALSE)
    return(df)
  }
  
  if (length(inputs) < 1) {
    return(df)
  }
  
  # Concatenate
  cols_to_paste <- as.data.frame(inputs)
  pasted_vector <- apply(cols_to_paste, 1, function(row_values) {
    valid_vals <- row_values[!is.na(row_values)]
    
    if (length(valid_vals) == 0) {
      return(NA_character_)
    }
    
    # Join the values with the separator
    return(paste(valid_vals, collapse = separator))
  })
  
  # --- Apply prefix and suffix ---
  pasted_vector <- ifelse(
    !is.na(pasted_vector),
    paste0(prefix, pasted_vector, suffix),
    NA_character_
  )
  
  df[[output_name]] <- pasted_vector
  
  return(df)
}



#' Coalesce multiple columns into one
#'
#' Creates a new column by selecting the first non-NA value from a set of
#' input columns for each row.
#'
#' @param action List containing:
#'   - `output_header`: Name of the new coalesced column
#'   - `input_map`: Named list mapping local variable names to actual column
#'     names (or synonyms) to coalesce, in priority order
#' @param df Data frame to transform
#'
#' @return Data frame with the new coalesced column added, or unchanged if
#'   no valid input columns are found
#'
#' @details
#' The function:
#' - Resolves all input columns from `input_map` using column name synonyms
#' - For each row, selects the first non-NA value from the resolved columns
#'   in the order they appear in `input_map`
#' - Returns the original data frame if no valid input columns can be resolved
#'
#' This is useful for consolidating information from multiple potential source
#' columns into a single standardized column.
#'
#' @noRd
#'

.action_coalesce_columns <- function(action, df) {
  
  output_name <- action$output_header
  input_map <- action$input_map
  
  inputs <- list()
  for (local_name in names(input_map)) {
    col_spec <- input_map[[local_name]]
    found_col_name <- .resolve_column_name(col_spec, names(df))
    if (!is.null(found_col_name)) {
      inputs[[found_col_name]] <- df[[found_col_name]]
    }
  }
  
  if (length(inputs) < 1) {
    # warning(paste0("For '", output_name, "', no valid input columns for coalesce were found. Skipping."), call. = FALSE)
    return(df)
  }
  
  coalesced_vector <- do.call(coalesce, inputs)
  df[[output_name]] <- coalesced_vector
  
  return(df)
}


#' Extract a substring using a regular expression
#'
#' Creates a new column by extracting a substring from an input column using
#' a regular expression pattern with capture groups.
#'
#' @param action List containing:
#'   - `input_map`: Named list mapping to the source column name (or synonym)
#'   - `pattern`: Regular expression pattern with optional capture groups
#'   - `group_index`: Which capture group to extract (default: `1` for the
#'     first capture group, `0` for the full match)
#'   - `output_col`: Name of the new column to create
#' @param df Data frame to transform
#'
#' @return Data frame with the new extracted column added, or unchanged if
#'   the input column cannot be resolved
#'
#' @details
#' The function:
#' - Resolves the input column from `input_map` using column name synonyms
#' - Applies the regex pattern with Perl-compatible regular expressions
#' - Extracts the specified capture group (1-indexed) from each match
#' - Sets the result to NA for rows where no match is found or the requested
#'   group doesn't exist
#'
#' Use `group_index = 0` to extract the full match, or `group_index = 1` (default)
#' to extract the first parenthesized capture group.
#'
#' @noRd
#'

.action_extract_string <- function(action, df) {
  
  # --- Resolve input ---
  inputs <- .resolve_input_map(action$input_map, df)
  if (is.null(inputs)) return(df)
  source_vector <- inputs[[1]]
  
  # --- Setup regex ---
  pattern <- action$pattern
  # Default to extracting Group 1 (the part in parentheses), 
  # or Group 0 (full match) if specified.
  group_idx <- action$group_index %||% 1 
  
  # --- Perform extraction ---
  match_data <- regexec(pattern, source_vector, perl = TRUE)
  extracted_list <- regmatches(source_vector, match_data)
  
  # --- Flatten result ---
  result_vector <- sapply(seq_along(source_vector), function(i) {
    matches <- extracted_list[[i]]
    if (length(matches) == 0) return(NA_character_) # No match found

    target_index <- group_idx + 1 
    
    if (target_index > length(matches)) return(NA_character_)
    return(matches[target_index])
  })
  
  # --- Assign to output ---
  output_col <- action$output_col
  df[[output_col]] <- result_vector
  
  return(df)
}


#' Sort rows by a specified column
#'
#' Reorders the rows of a data frame based on the values in a single column,
#' in ascending order.
#'
#' @param action List containing:
#'   - `by_col`: Name of the column to sort by
#' @param df Data frame to sort
#'
#' @return Data frame with rows sorted by the specified column, or unchanged
#'   if the column is not found (with a warning)
#'
#' @details
#' The function:
#' - Sorts rows in ascending order based on the values in `by_col`
#' - Issues a warning and returns the original data frame if `by_col` doesn't
#'   exist in the data
#' - Uses standard `dplyr::arrange()` sorting behavior (NA values sorted to end)
#'
#' @noRd
#' 

.action_sort_rows <- function(action, df) {
  
  sort_col <- action$by_col
  if (sort_col %in% names(df)) {
    df <- df %>%
      dplyr::arrange(!!rlang::sym(sort_col))
  } else {
    warning(paste0("sort_rows: Column '", sort_col, "' not found."), call. = FALSE)
  }
  
  return(df)
}


#' Filter rows based on column presence or values
#'
#' Removes rows from a data frame based on either the presence of non-NA values
#' in specified columns or matching values in specified columns.
#'
#' @param action List that may contain:
#'   - `on_presence_of_columns`: Character vector of column names (or synonyms).
#'     Keeps rows with at least one non-NA value in these columns
#'   - `on_column_values`: List of filter specifications, each containing:
#'     - `column`: Column name (or synonym) to filter on
#'     - `values`: Vector of values to match
#' @param df Data frame to filter
#'
#' @return Filtered data frame with rows removed based on the specified criteria
#'
#' @details
#' The function applies two types of filters sequentially:
#'
#' **Presence filter** (`on_presence_of_columns`):
#' - Resolves column names using synonyms
#' - Keeps only rows where at least one of the specified columns has a non-NA value
#' - Returns an empty data frame if none of the columns exist
#'
#' **Value filter** (`on_column_values`):
#' - For each filter item, keeps only rows where the specified column's value
#'   is in the provided list of values
#' - Resolves column names using synonyms
#' - Returns an empty data frame if any required column doesn't exist
#'
#' Both filters can be used together, in which case they are applied sequentially
#' (presence filter first, then value filters).
#'
#' @noRd
#'
 

.action_filter_rows <- function(action, df) {
  
  # --- On presence of columns ---
  if (!is.null(action$on_presence_of_columns)) {
    
    # Resolve all column names to handle synonyms
    resolved_cols <- sapply(action$on_presence_of_columns, .resolve_column_name, names(df))
    cols_to_check <- unlist(resolved_cols[!sapply(resolved_cols, is.null)])
    
    if (length(cols_to_check) > 0) {
      # Keep rows that have a non-NA value in at least one of the focal cols
      df <- dplyr::filter(df, dplyr::if_any(tidyr::all_of(cols_to_check), ~ !is.na(.)))
    } else {
      # Filter out all rows if column does not exist or contains only NA
      df <- df[0, ]
    }
  }
  
  # --- On column values ---
  if (!is.null(action$on_column_values)) {
    
    for (filter_item in action$on_column_values) {
      
      col_name <- .resolve_column_name(filter_item$column, names(df))
      
      if (!is.null(col_name)) {
        df <- dplyr::filter(df, .data[[col_name]] %in% filter_item$values)
      } else {
        # If a required column for value filtering is missing, return empty df
        df <- df[0, ]
      }
    }
  }
  
  return(df)
}


#' Format column values using format strings
#'
#' Applies formatting to columns, either overwriting the original or creating
#' new formatted columns. Supports date formatting (strftime) and numeric
#' formatting (sprintf).
#'
#' @param action List containing:
#'   - `formats`: Named list where each element is either:
#'     - A format string (overwrites original column)
#'     - A list with `format` and `output_header` (creates new column)
#' @param df Data frame to format
#'
#' @return Data frame with formatted column(s)
#'
#' @details
#' \itemize{
#'   \item **Date formatting**: Format strings containing `%` are treated as strftime
#'     patterns. Dates are parsed using `lubridate::parse_date_time()` with common
#'     date formats and forced to UTC.
#'   \item **Numeric formatting**: Format strings without `%` are treated as sprintf
#'     patterns applied to numeric values.
#'   \item Missing columns are silently skipped.
#' }
#'
#' @noRd
#' 

.action_format_column <- function(action, df) {
  
  if (!is.null(action$formats)) {
    for (col_name in names(action$formats)) {
      if (col_name %in% names(df)) {
        
        format_details <- action$formats[[col_name]]
        
        # Check if format should overwrite input or create new column
        if (is.list(format_details)) {
          output_col <- format_details$output_header
          format_string <- format_details$format
        } else {
          output_col <- col_name
          format_string <- format_details
        }
        
        # Set specific formatting logic for dates
        if (grepl("%", format_string)) {
          df[[output_col]] <- format(
            lubridate::parse_date_time(
              df[[col_name]],
              orders = c(
                # Date-only formats
                "dmy", "mdy", "ymd", "Ymd", "dmy HM", "dmy HMS",
                "mdy HM", "mdy HMS", "mdy I:M p", "mdy I:M:S p",
                "ymd HM", "ymd HMS", "Ymd HM", "Ymd HMS",
                "b d, Y", "d b Y", "Y b d", "d-b-Y", "Y-b-d",
                "dmy I:M p", "dmy I:M:S p", "Ymd I:M p", "Ymd I:M:S p",
                # ISO 8601 and compact formats
                "Ymd T", "YmdHMS", "YmdHM",
                # 2-digit year formats
                "dmY", "myY", "Ymd", "dmy", "mdy", "Ym", "dmy I p"
              ),
              tz = "UTC"  # Force UTC to avoid timezone issues
            ), format_string
          )
        } else {
          df[[output_col]] <- sprintf(format_string, as.numeric(df[[col_name]]))
        }
      }
    }
  }
  
  return(df)
}


#' Convert column data types
#'
#' Converts one or more columns to specified data types, with special handling
#' for datetime parsing.
#'
#' @param action List containing:
#'   - `columns`: Named list mapping column names (or synonyms) to target types.
#'     Supported types: "datetime", "numeric", "character"
#' @param df Data frame to modify
#'
#' @return Data frame with converted column types
#'
#' @details
#' \itemize{
#'   \item **Datetime conversion**: Parses strings using `lubridate::parse_date_time()`
#'     with common date/datetime formats (YMD, DMY, MDY variants with optional times).
#'     All datetimes are standardized to UTC.
#'   \item **Numeric conversion**: Uses `as.numeric()`, which may produce NA for
#'     non-numeric strings.
#'   \item **Character conversion**: Uses `as.character()`.
#'     Column names are resolved using synonym matching. Missing columns trigger
#'     a warning and are skipped. Missing column map triggers a warning and returns
#'     the data frame unchanged.
#' }
#'
#' @noRd
#' 

.action_convert_data_type <- function(action, df) {
  
  col_map <- action$columns
  if (is.null(col_map)) {
    warning("Action 'convert_data_type' called without a 'columns' map. Skipping.", call. = FALSE)
    return(df)
  }
  
  for (col_name in names(col_map)) {
    target_type <- col_map[[col_name]]
    
    resolved_name <- .resolve_column_name(col_name, names(df))
    if (is.null(resolved_name)) {
      warning(paste0("Column '", col_name, "' not found for data type conversion. Skipping."), call. = FALSE)
      next
    }
    
    # Type conversion logic
    if (target_type == "datetime") {
      df[[resolved_name]] <- parse_date_time(
        df[[resolved_name]],
        orders = c(
          "Ymd HMS",  # 2025-11-16 14:45:30
          "Ymd HM",   # 2025-11-16 14:45
          "Ymd",      # 2025-11-16
          "dmY HMS",  # 16-11-2025 14:45:30
          "dmY HM",   # 16-11-2025 14:45
          "mdY HMS",  # 11-16-2025 14:45:30
          "mdY HM",   # 11-16-2025 14:45
          "Y/m/d H:M:S", # 2025/11/16 14:45:30
          "d/m/Y H:M:S", # 16/11/2025 14:45:30
          "m/d/Y H:M:S"  # 11/16/2025 14:45:30
          #"Ymd GMS z", # ISO 8601 with timezone
          #"c"          # Full ISO 8601 format like "2025-11-16T14:45:30.123Z"
        ),
        tz = "UTC", # Always standardize to UTC
        quiet = TRUE # Prevents printing messages for each parsed format
      )
      
    } else if (target_type == "numeric") {
      df[[resolved_name]] <- as.numeric(df[[resolved_name]])
      
    } else if (target_type == "character") {
      df[[resolved_name]] <- as.character(df[[resolved_name]])
    }
    
  }
  return(df)
}


#' Summarise data by groups
#'
#' Groups data by specified columns and applies aggregation functions to
#' compute summary statistics.
#'
#' @param action List containing:
#'   - `group_by`: Character vector of column names to group by
#'   - `summarise`: List of aggregation operations, each with:
#'     - `fun`: Function name ("max", "min", "sum", "mean", "first", "count_rows")
#'     - `columns`: Column(s) to aggregate (not needed for "count_rows")
#'     - `output_header`: Name for the aggregated column (optional, defaults to
#'       original column name)
#'     - `na_rm`: Whether to remove NAs (optional, defaults to TRUE)
#' @param df Data frame to summarise
#'
#' @return Summarised data frame with one row per group
#'
#' @details
#' Only grouping columns that exist in the data frame are used. If no valid
#' grouping columns are found, a warning is issued and the original data frame
#' is returned unchanged.
#'
#' For "first", the function returns the first non-NA value in each group.
#' For "count_rows", the output name defaults to "row_count" if not specified.
#'
#' @noRd
#'

.action_summarise <- function(action, df) {
  
  # Parse grouping columns directly from the rule
  group_cols <- unlist(action$group_by)
  
  # Get safe columns that actually exist in the data
  safe_group_cols <- intersect(group_cols, names(df))
  
  if (length(safe_group_cols) == 0) {
    warning("Aggregation skipped: no grouping columns found.", call. = FALSE)
    return(df) 
  }
  
  # Rest of the aggregation logic remains the same...
  summarise_list <- list()
  for (op in action$summarise) {
    
    if (op$fun %in% c("max", "min", "sum", "mean", "first")) {
      for (col in op$columns) {
        if (col %in% names(df)) {
          output_name <- op$output_header %||% col
          summarise_list[[output_name]] <- switch(
            op$fun,
            "max" = rlang::expr(max(!!rlang::sym(col), na.rm = !!op$na_rm %||% TRUE)),
            "min" = rlang::expr(min(!!rlang::sym(col), na.rm = !!op$na_rm %||% TRUE)),
            "sum" = rlang::expr(sum(!!rlang::sym(col), na.rm = !!op$na_rm %||% TRUE)),
            "mean" = rlang::expr(mean(!!rlang::sym(col), na.rm = !!op$na_rm %||% TRUE)),
            "first" = rlang::expr(dplyr::first(na.omit(!!rlang::sym(col))))
          )
        }
      }
    } else if (op$fun == "count_rows") {
      output_name <- op$output_name %||% "row_count"
      summarise_list[[output_name]] <- rlang::expr(dplyr::n())
    }
  }
  
  # Run the aggregation
  df <- df %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(safe_group_cols))) %>%
    dplyr::summarise(!!!summarise_list, .groups = "drop")
  
  return(df)
}


#' Replace NA values in columns
#'
#' Replaces NA values in specified columns with a given value.
#'
#' @param action List containing:
#'   - `columns`: Character vector of column names to process
#'   - `with_value`: Value to use in place of NA
#' @param df Data frame to modify
#'
#' @return Data frame with NAs replaced in specified columns
#'
#' @details
#' Only columns that exist in the data frame are processed. Non-existent
#' columns are silently skipped.
#'
#' The replacement value can be of any type compatible with the target columns.
#'
#' @noRd
#' 

.action_replace_na <- function(action, df) {
  cols_to_process <- unlist(action$columns)
  value <- action$with_value
  
  # Find which of the specified columns actually exist in the data frame
  existing_cols <- intersect(cols_to_process, names(df))
  
  if (length(existing_cols) > 0) {
    df <- df %>%
      dplyr::mutate(dplyr::across(
        dplyr::all_of(existing_cols),
        ~ tidyr::replace_na(., value)
      ))
  }
  
  return(df)
}


#' Remove duplicate rows
#'
#' Removes duplicate rows from the data frame, keeping only the first
#' occurrence of each unique row.
#'
#' @param action List (Note: currently unused, included for consistency with
#'   action handler interface)
#' @param df Data frame to deduplicate
#'
#' @return Data frame with duplicate rows removed
#'
#' @details
#' Uses `dplyr::distinct()` to identify and remove duplicates based on all
#' columns. The first occurrence of each unique row combination is retained.
#'
#' @noRd
#' 

.action_deduplicate <- function(action, df) {
  return(dplyr::distinct(df))
}


#' Apply a custom function to create a new column
#'
#' Calls a registered custom function and stores its result in a new column.
#'
#' @param action List containing:
#'   - `function_name`: Name of the custom function (must be registered in
#'     `.custom_functions`)
#'   - `output_header`: Name for the output column
#'   - `params`: Named list of static parameters to pass to the function (optional)
#'   - `input_map`: Named list mapping parameter names to column specifications
#'     (optional)
#' @param df Data frame to process
#'
#' @return Data frame with new column containing function results
#'
#' @details
#' The function must be registered in the `.custom_functions` registry before
#' use. If the function is not found, an error is thrown.
#'
#' Arguments are constructed by combining static parameters (`params`) with
#' resolved column data from `input_map`. If `input_map` resolution fails
#' (e.g., required columns don't exist), the original data frame is returned
#' unchanged.
#'
#' The custom function should return a vector of the same length as the number
#' of rows in the data frame.
#'
#' @noRd
#' 

.action_apply_function <- function(action, df) {
  
  func_name <- action$function_name
  output_col <- action$output_header
  if (is.null(func_name) || is.null(output_col)) {
    stop("Action 'apply_function' requires 'function_name' and 'output_header'.")
  }
  
  # Find the function in custom function registry
  func_to_call <- .custom_functions[[func_name]]
  if (is.null(func_to_call)) {
    stop(paste("Custom function '", func_name, "' not found in registry.", sep=""))
  }
  
  # Prepare the arguments for the function
  args_list <- action$params %||% list()  # Static parameters
  if (!is.null(action$input_map)) {
    # Resolve input map
    resolved_inputs <- .resolve_input_map(action$input_map, df)
    if (is.null(resolved_inputs)) {
      return(df) 
    }
    # Add the resolved data vectors to arg list
    args_list <- c(args_list, resolved_inputs)
  }
  
  # Function call
  result_vector <- do.call(func_to_call, args_list)
  
  df[[output_col]] <- result_vector
  
  return(df)
}


#' Pivot data from long to wide format
#'
#' Transforms data from long to wide format by spreading values from one or
#' more columns into multiple columns based on names from another column.
#'
#' @param action List containing:
#'   - `input_map`: Named list with:
#'     - `names_from`: Column specification for the column whose values will
#'       become new column names
#'     - `values_from`: Column specification(s) for the column(s) whose values
#'       will populate the new columns (can be a list or character vector)
#'   - `keep_only_pivot_cols`: Logical. If TRUE, only the pivot columns are
#'     retained; otherwise all non-pivoted columns are kept as ID columns
#'     (optional, default FALSE)
#' @param df Data frame to pivot
#'
#' @return Data frame in wide format
#'
#' @details
#' Uses `tidyr::pivot_wider()` to reshape the data. The `names_from` column
#' provides the names for new columns, and `values_from` column(s) provide
#' the values to fill them.
#'
#' If `keep_only_pivot_cols` is FALSE (default), all columns not involved in
#' the pivot are retained as ID columns. If TRUE, only the pivoted columns
#' appear in the result.
#'
#' If required columns cannot be resolved, the original data frame is returned
#' unchanged.
#'
#' @noRd
#'

.action_pivot_wider <- function(action, df) {
  
  map <- action$input_map
  
  # --- Resolve 'names_from' ---
  col_names_source <- .resolve_column_name(map$names_from, names(df))
  
  # --- Resolve 'values_from' ---
  if (is.list(map$values_from) || is.character(map$values_from)) {
    resolved_vals <- lapply(map$values_from, function(x) {
      .resolve_column_name(x, names(df))
    })
    col_values_source <- unlist(resolved_vals)
  } else {
    col_values_source <- NULL
  }
  
  # Validation
  if (is.null(col_names_source) || length(col_values_source) == 0) {
    # warning("pivot_wider: could not find specified columns.")
    return(df)
  }
  
  # --- Handle ID columns ---
  keep_only <- isTRUE(action$keep_only_pivot_cols)
  
  if (keep_only) {
    id_cols_arg <- NULL
  } else {
    id_cols_arg <- setdiff(names(df), c(col_names_source, col_values_source))
  }
  
  # --- Perform pivot ---
  df_pivoted <- tidyr::pivot_wider(
    data = df,
    id_cols = if(keep_only) NULL else dplyr::all_of(id_cols_arg),
    names_from = dplyr::all_of(col_names_source),
    values_from = dplyr::all_of(col_values_source)
  )
  
  return(df_pivoted)
}


#' Pivot data from wide to long format
#'
#' Transforms data from wide to long format by gathering multiple columns into
#' two columns: one for the original column names and one for their values.
#'
#' @param action List containing:
#'   - `columns_to_pivot`: Vector of column specifications to pivot into long
#'     format
#'   - `output_map`: Named list with:
#'     - `names_to`: Name for the output column that will contain the original
#'       column names (optional, default "name")
#'     - `values_to`: Name for the output column that will contain the values
#'       (optional, default "value")
#'   - `values_drop_na`: Logical. If TRUE, rows with NA values are dropped
#'     (optional, default FALSE)
#' @param df Data frame to pivot
#'
#' @return Data frame in long format
#'
#' @details
#' Uses `tidyr::pivot_longer()` to reshape the data. Each specified column is
#' converted into multiple rows, with the column name stored in the `names_to`
#' column and the value stored in the `values_to` column.
#'
#' Column specifications are resolved to handle synonyms. If no matching
#' columns are found, a warning is issued and the original data frame is
#' returned unchanged.
#'
#' When `values_drop_na` is TRUE, rows where the value column contains NA are
#' automatically removed from the result.
#'
#' @noRd
#' 

.action_pivot_longer <- function(action, df) {
  
  # --- Resolve columns ---
  targets_raw <- action$columns_to_pivot
  real_cols_to_pivot <- unlist(lapply(targets_raw, function(x) {
    .resolve_column_name(x, names(df))
  }))
  
  if (length(real_cols_to_pivot) == 0) {
    warning("pivot_longer: No matching columns found to pivot.")
    return(df)
  }
  
  # --- Set output names ---
  name_dest <- action$output_map$names_to %||% "name"
  value_dest <- action$output_map$values_to %||% "value"
  
  # --- Enforce NA handling ---
  drop_na <- isTRUE(action$values_drop_na)
  
  df_long <- tidyr::pivot_longer(
    data = df,
    cols = all_of(real_cols_to_pivot),
    names_to = name_dest,
    values_to = value_dest,
    values_drop_na = drop_na
  )
  
  return(df_long)
}
