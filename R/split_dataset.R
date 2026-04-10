#' Split a Dataset by a Key Column
#'
#' @description
#' Splits a dataset (a list of data frames) into a nested list, where each sub-list corresponds to a unique value
#' of a specified key column.
#'
#' @details
#' This function is used to split a multi-experiment or multi-year dataset into individual subsets.
#' For tables that do not contain the `split_key`, the entire table is replicated across all subsets
#' (e.g., a single `SOIL` table would be copied into each experiment's subset). The function also cleans the output by
#' removing empty data frames and columns containing only `NA` values.
#'
#' @param dataset The dataset to split, as a list of data frames.
#' @param key The conceptual name of the key to split by (e.g., "experiment", "year"). This is resolved to a specific
#'    column name via the config file.
#' @param data_model The data model of the dataset.
#'
#' @return A named list of lists, where each top-level element is named after a unique value of the split key
#' 
#' @importFrom yaml read_yaml
#' 
#' @export
#'

split_dataset <- function(dataset, key, data_model) { 
  
  key_handlers <- c("experiment", "year", "plot", "treatment")
  unmatched_key <- match.arg(key, key_handlers)
  
  # Load split key from config
  config_path <- system.file("extdata", "datamodels.yaml", package = "csmTools")
  config <- yaml::read_yaml(config_path)
  model_config <- config[[data_model]]
  split_key <- model_config$design_keys[[key]]
  
  # Guard clause if no keys are defined for the focal data model
  if (is.null(split_key)) {
    stop(paste0("Cannot split dataset: the key '", key, "' is not defined in the 'design_keys' for the '", 
                data_model, "' model."),
         call. = FALSE)
  }
  
  # ---- Splitting logic ----
  # Find all unique levels of the split key across all tables that have it.
  all_levels <- unique(unlist(lapply(dataset, function(df) {
    if (split_key %in% names(df)) {
      levels <- unique(df[[split_key]])
      return(levels[!is.na(levels)])
    }
    return(NULL)
  })))
  
  # Handle case where the key is not found anywhere
  if (is.null(all_levels) || length(all_levels) == 0) {
    warning(paste0("Split key '", split_key, "' not found in any data frame."),
            call. = FALSE)
    return(dataset) 
  }
  
  # Initialize the nested output list, with names set to the unique levels.
  output_list <- stats::setNames(lapply(all_levels, function(x) list()), all_levels)
  
  # Loop through each data frame in the original dataset.
  for (table_name in names(dataset)) {
    df <- dataset[[table_name]]
    
    # Case A: The split key EXISTS in this table.
    if (split_key %in% names(df)) {
      # Split the table normally.
      split_df_list <- split(df, f = df[[split_key]], drop = TRUE)
      
      # Add each resulting piece to the correct level in the output list.
      for (level_name in names(split_df_list)) {
        if(level_name %in% names(output_list)) {
          output_list[[level_name]][[table_name]] <- split_df_list[[level_name]]
        }
      }
      
      # Case B: The split key IS MISSING from this table.
    } else {
      # Inform the user about the replication.
      message(paste0("'", split_key, "' not found in '", table_name, "'. Replicating this table across all subsets."))
      
      # Assign the *entire* table to every single subset.
      for (level_name in as.character(all_levels)) {
        output_list[[level_name]][[table_name]] <- df
      }
    }
  }
  
  # ---- Clean-up output
  # Remove empty data frames, e.g., management categories irrelevant for the focal year
  output_list <- lapply(output_list, function(ls) ls[lengths(ls) > 0])
  
  # Remove NA-only columns
  output_list <- lapply(output_list, function(ls){
    ls <- lapply(ls, function(df) {
      df[, colSums(is.na(df)) != nrow(df)]
    })
  })
  
  # Special case: measured data with only structural columns
  # TODO: set with config instead of hard-coding
  output_list <- lapply(output_list, function(ls){
    if (all(colnames(ls[["SUMMARY"]]) %in% c("TRTNO","EXP_YEAR","DATE"))) {
      ls[["SUMMARY"]] <- NULL
    }
    if (all(colnames(ls[["TIME_SERIES"]]) %in% c("TRTNO","EXP_YEAR","DATE"))) {
      ls[["TIME_SERIES"]] <- NULL
    }
    return(ls)
  })
  
  # Reset management IDs fun??
  
  return(output_list)
}
