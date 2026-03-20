#' Apply mapping rules to transform input data to output data model
#'
#' Executes a two-phase transformation process: first applying all data transformation
#' rules in sequence, then applying a final schema rule to select and rename columns
#' according to the target data model.
#'
#' @param input_data A list of data frames in the source data model
#' @param rules A list of mapping rules read from a YAML mapping file, each containing:
#'   \itemize{
#'     \item `mapping_uid`: Unique identifier for the rule (always "999" for schema rule)
#'     \item `source$section`: Name of the source table
#'     \item `target$section`: Name of the target table
#'     \item `actions`: List of transformation actions to apply
#'     \item `order`: Optional execution order (default: 0 for independent rules)
#'   }
#' @param ... Additional arguments passed to `process_actions()`
#'
#' @return A list of data frames in the target data model format, with columns selected
#'   and renamed according to the schema rule (if present).
#'
#' @details
#' **Phase 1: Transformation Rules**
#' \itemize{
#'   \item Rules are sorted by their `order` field (rules without order = 0 run first)
#'   \item Each rule processes its source table through a series of actions listed sequentially in the YAML map
#'   \item Rules can be chained: later rules (order n+1) can use output from earlier rules (order n) as input
#'   \item For lookup operations, both input and intermediate output data are available
#'   \item Results are consolidated into output tables, combining multiple rules if needed
#' }
#'
#' **Phase 2: Schema Application**
#' \itemize{
#'   \item A special rule with `mapping_uid = "999"` defines the final schema
#'   \item Uses `map_headers` actions to select and rename columns
#'   \item Only columns specified in the schema are included in the final output
#'   \item If no schema rule exists, transformed data is returned as-is
#' }
#'
#' @noRd
#'

.apply_mapping_rules <- function(input_data, rules, ...) {
  
  output_data <- list()
  schema_rule_uid <- '999' # Define the UID for the special schema rule
  
  # =================================================================
  # PHASE 1: Execute all transformation rules in order
  # =================================================================
  
  transformation_rules <- rules[sapply(rules, function(r) r$mapping_uid != schema_rule_uid)]
  
  # Sort rules based on order
  orders <- sapply(transformation_rules, function(r) ifelse(is.null(r$order), 0, r$order))
  transformation_rules <- transformation_rules[order(orders)]
  # Note: rules without an 'order' key are assumed to be independent and run first (order = 0).
  
  if (length(transformation_rules) == 0) {
    output_data <- input_data
  } else {
    for (rule in transformation_rules) {
      
      # Debugging helper
      print(rule$mapping_uid)
      
      # If source exists in intermediate output data, use as input (= chained rules)
      if (!is.null(rule$source$section) && rule$source$section %in% names(output_data)) {
        input_df <- output_data[[rule$source$section]]
      } else {
        input_df <- input_data[[rule$source$section]]
      }
      
      if (is.null(input_df)) next
      
      # Merge input and output datasets
      # Note: necessary for chained rules involving lookups
      merged_data <- c(output_data, input_data)
      # Remove duplicate names
      merged_data <- merged_data[!duplicated(names(merged_data))]
      
      # Execute data mapping actions
      transformed_df <- process_actions(
        mapping_uid = rule$mapping_uid,
        actions = rule$actions,
        input_df = input_df,
        dataset = merged_data,
        ...
      )
      if (nrow(transformed_df) == 0) next
      
      # Consolidate output table
      output_data <- .update_output_data(
        mapping_uid = rule$mapping_uid,
        output_data = output_data,
        target_section = rule$target$section,
        data_to_add = transformed_df
      )
    }
  }

  # =================================================================
  # PHASE 2: Final schema application select and rename columns
  # =================================================================
  
  schema_rule <- rules[[which(sapply(rules, function(r) r$mapping_uid == schema_rule_uid))]]
  
  if (!is.null(schema_rule)) {
    final_output_data <- list()
    
    for (action in schema_rule$actions) {
      if (action$type == "map_headers") {
        target_section <- action$target_section
        rename_map <- action$rename_map
        
        # Get the processed data frame from the mapping phase
        df_to_map <- output_data[[target_section]]
        if (is.null(df_to_map)) next
        
        final_cols_list <- list()
        for (new_name in names(rename_map)) {
          old_name_spec <- rename_map[[new_name]]
          found_col_name <- .resolve_column_name(old_name_spec, names(df_to_map))
          
          if (!is.null(found_col_name)) {
            final_cols_list[[new_name]] <- df_to_map[[found_col_name]]
          }
        }
        
        final_df <- tibble::as_tibble(final_cols_list)
        final_output_data[[target_section]] <- final_df %>%
          dplyr::distinct()
      }
    }
    return(final_output_data)
  }
  
  # If no schema rule, return the transformed but unmapped data
  return(output_data)
}


#' Update output data by adding or merging transformed data into a target section
#'
#' Consolidates data from multiple mapping rules into a single output table. If the
#' target section doesn't exist, creates it. If it exists, performs an intelligent
#' merge based on common columns.
#'
#' @param mapping_uid Character string identifying the current mapping rule (used for warnings)
#' @param output_data A list of data frames representing the current output state
#' @param target_section Character string naming the target table to update
#' @param data_to_add A data frame containing the new data to add or merge
#'
#' @return The updated `output_data` list with the target section modified
#'
#' @details
#' The function uses three strategies depending on the state of the target section:
#' \itemize{
#'   \item **New section**: If `target_section` doesn't exist, creates it with `data_to_add`
#'   \item **Merge with common keys**: If common columns exist between the existing data
#'     and new data, performs a `full_join` to combine all rows
#'   \item **Append columns**: If no common columns exist, uses `bind_cols` to add
#'     new columns side-by-side
#' }
#'
#' Many-to-many join warnings are intercepted and replaced with a more informative
#' message indicating the rule and section where this occurred, as this situation is
#' often expected (e.g., when multiple management events relate to the same treatment).
#'
#' @noRd
#'

.update_output_data <- function(mapping_uid, output_data, target_section, data_to_add) {
  if (is.null(output_data[[target_section]])) {
    output_data[[target_section]] <- data_to_add
    return(output_data)
  }
  existing_df <- output_data[[target_section]]
  join_keys <- intersect(names(existing_df), names(data_to_add))
  
  if (length(join_keys) > 0) {
    # Intercept and muffle standard warning message
    output_data[[target_section]] <- withCallingHandlers(
      expr = {
        dplyr::full_join(existing_df, data_to_add, by = join_keys)
      },
      warning = function(w) {
        if (grepl("many-to-many relationship", w$message)) {
          warning(paste0("A many-to-many join occurred in rule '", mapping_uid, "' targeting section: '",
                         target_section, "'.\nThis may be expected of management regimes comprising of multiple events."),
                  call. = FALSE)
          invokeRestart("muffleWarning")
        }
      }
    )
  } else {
    output_data[[target_section]] <- dplyr::bind_cols(existing_df, data_to_add)
  }
  return(output_data)
}