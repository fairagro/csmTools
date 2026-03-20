#' Process a sequence of data transformation actions
#'
#' Executes a series of actions on a data frame, applying each transformation in order.
#' Actions are dispatched to their corresponding handler functions based on type.
#'
#' @param actions A list of action specifications, each containing at minimum a `type` field
#'   that determines which transformation to apply
#' @param input_df A data frame to be transformed
#' @param dataset A list of data frames representing the complete dataset, used for
#'   lookup operations in join actions
#' @param ... Additional arguments passed to individual action handlers
#'
#' @return A transformed data frame after all actions have been applied sequentially
#'
#' @details
#' Actions are applied in the order they appear in the `actions` list. Each action
#' receives the output of the previous action as its input, creating a transformation
#' pipeline. If any action returns an empty data frame (0 rows), processing stops
#' immediately and the empty frame is returned.
#'
#' **Supported action types include:**
#' \itemize{
#'   \item **Data combination**: `join`, `concatenate_columns`, `coalesce_columns`
#'   \item **Value transformation**: `map_values`, `rowwise_transform`, `apply_function`
#'   \item **Column operations**: `add_column`, `add_column_conditional`, `rename_column`,
#'     `delete_column`, `format_column`, `convert_data_type`, `replace_na`
#'   \item **Row operations**: `filter_rows`, `sort_rows`, `deduplicate`, `summarise`
#'   \item **String operations**: `extract_string`
#'   \item **Reshaping**: `pivot_wider`, `pivot_longer`
#'   \item **Domain-specific**: `define_icasa_management_id`
#' }
#'
#' Each action type is handled by a corresponding internal function (e.g., `.action_join()`,
#' `.action_map_values()`). If an unknown action type is encountered, an error is raised.
#'
#' @noRd
#'

process_actions <- function(actions, input_df, dataset, ...) {
  
  output_df <- input_df
  
  # Pass down optional arguments via a list
  action_args <- list(...)  # FIGURE OUT NA FOR MAP VALUES
  
  for (action in actions) {
    
    # --- Main action dispatcher ---
    output_df <- switch(action$type,
           
           # --- Generic actions (from mapping_actions_generic.R) ---
           "join" = { 
             .action_join(action, output_df, dataset)
           },
           
           "map_values" = {
             .action_map_values(action, output_df)
           },
           
           "add_column" = {
             .action_add_column(action, output_df)
           },
           
           "add_column_conditional" = {
             .action_add_column_conditional(action, output_df)
           },
           
           "rename_column" = {
             .action_rename_column(action, output_df)
           },
           
           "delete_column" = {
             .action_rename_column(action, output_df)
           },
           
           "rowwise_transform" = {
             .action_rowwise_transform(action, output_df)
           },
           
           "concatenate_columns" = {
             .action_concatenate_columns(action, output_df)
           },
           
           "coalesce_columns" = {
             .action_coalesce_columns(action, output_df)
           },
           
           "extract_string" = {
             .action_extract_string(action, output_df)
           },
           
           "sort_rows" = {
             .action_sort_rows(action, output_df)
           },
           
           "filter_rows" = {
             .action_filter_rows(action, output_df)
           },
           
           "deduplicate" = {
             .action_deduplicate(action, output_df)
           },
           
           "format_column" = {
             .action_format_column(action, output_df)
           },
           
           "convert_data_type" = {
             .action_convert_data_type(action, output_df)
           },
           
           "summarise" = {
             .action_summarise(action, output_df)
           },
           
           "replace_na" = {
             .action_replace_na(action, output_df)
           },
           
           'apply_function' = {
             .action_apply_function(action, output_df)
           },
           
           'pivot_wider' = {
             .action_pivot_wider(action, output_df)
           },
           
           'pivot_longer' = {
             .action_pivot_longer(action, output_df)
           },
           
           # --- Specific actions (from mapping_actions_specific.R) ---
           # TODO: move to apply_function?
           "define_icasa_management_id" = {
             .action_define_icasa_management_id(action, output_df)
           },
           
           stop(paste("In rule '", mapping_uid, "', unknown or unhandled action type: ", action$type, sep = ""))
    )
    
    # If the filter rule output an empty table, stop processing further rules
    if (nrow(output_df) == 0) {
      break
    }
  }
  return(output_df)
}


#' Resolve input columns for mapping/transformation actions
#'
#' Extracts column vectors from a data frame based on an input map specification.
#' Used by actions that transform column values (e.g., `map_values`, `apply_function`)
#' to map column specifications to actual data.
#'
#' @param input_map Named list where names identify the role of each input
#'   (e.g., "source", "x", "y") and values are column specifications
#' @param df Data frame containing the columns to extract
#'
#' @return Named list of column vectors matching the input map structure,
#'   or `NULL` if any column cannot be resolved (action should be skipped)
#'
#' @details
#' Uses `.resolve_column_name()` for each column specification. All columns
#' must resolve successfully for the transformation to proceed.
#'
#' @noRd
#'

.resolve_input_map <- function(input_map, df) {
  
  resolved_inputs <- list()
  
  for (local_name in names(input_map)) {
    col_spec <- input_map[[local_name]]
    found_col_name <- .resolve_column_name(col_spec, names(df))
    
    if (is.null(found_col_name)) {
      # Safely get the column name for the warning message
      display_name <- if (is.list(col_spec)) col_spec$header else col_spec
      # warning(paste0("Input column '", display_name, "' not found. Skipping action."), call. = FALSE)
      return(NULL)
    }
    resolved_inputs[[local_name]] <- df[[found_col_name]]
  }
  return(resolved_inputs)
}


#' Resolve a column name specification to an actual column name
#'
#' Matches a column specification against available columns, supporting both
#' direct string matches and specifications with synonym fallbacks.
#'
#' @param name_spec Either a character string or a list with `header` and optional `synonyms`
#' @param available_cols Character vector of available column names
#'
#' @return The first matching column name, or `NULL` if no match found
#'
#' @details
#' When `name_spec` is a list, tries `header` first, then each synonym in order.
#' Returns the first match found.
#'
#' @noRd
#'

.resolve_column_name <- function(name_spec, available_cols) {
  
  all_possible_names <- if (is.list(name_spec)) {
    unlist(c(name_spec$header, name_spec$synonyms))
  } else {
    name_spec
  }
  found_name <- intersect(all_possible_names, available_cols)
  if (length(found_name) > 0) return(found_name[1]) else return(NULL)
}