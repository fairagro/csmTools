#' Assemble dataset from multiple components
#'
#' Combines multiple data components into a unified dataset structure with flexible integration strategies.
#' Handles attribute preservation, key normalization, and different merging approaches.
#'
#' @param components (list) List of data components to assemble. Each component should be a named list
#'   containing data frames/tables.
#' @param keep_all (logical) If `TRUE`, preserves all unique components in the output structure.
#'   If `FALSE` (default), uses the structure of the first component as template.
#' @param action (Character) Integration strategy to apply when combining components with matching names:
#'   \describe{
#'     \item{"merge_properties"}{(default) Merge columns (properties) using specified join type (default)}
#'     \item{"aggregate_records"}{Combine rows (records) with optional grouping and aggregation}
#'     \item{"replace_table"}{Use the most recent component, discarding previous versions}
#'   }
#' @param output_path (character) Optional file path to save the output.
#' @param join_type (character) Join type to use when `action = "merge_properties"`.
#'   Supports: `"full"` (default), `"left"`, `"right"`, `"inner"`.
#' @param groups (character) Vector of column names to group by when `action = "aggregate_records"`.
#'   If `NULL` (default), no grouping is performed.
#' @param agg_fun (character|function) Aggregation function to use when `action = "aggregate_records"`.
#'   Can be a function or name of function like `"coalesce"` (default), `"mean"`, etc.
#'
#' @details
#' The assembly process follows these steps:
#' \enumerate{
#'   \item Resolves and flattens input components
#'   \item Preserves custom attributes from input data
#'   \item Determines output structure based on `keep_all` parameter
#'   \item Normalizes keys to leaf names only (removes path prefixes)
#'   \item Applies the specified integration action to components with matching names
#'   \item Reconstructs the final nested structure
#'   \item Restores custom attributes using object names, and exports if path is provided
#' }
#'
#' @return A structured list containing the assembled dataset
#'
#' @examples
#' \dontrun{
#' # Basic column merging (default)
#' assembled <- assemble_dataset(
#'   components = list(soil_data, weather_data, management_data),
#'   action = "merge_properties"
#' )
#'
#' # Row aggregation with grouping
#' assembled <- assemble_dataset(
#'   components = list(exp1_results, exp2_results),
#'   action = "aggregate_records",
#'   groups = c("treatment", "year"),
#'   agg_fun = "mean"
#' )
#'
#' # Replace strategy (use newest version)
#' assembled <- assemble_dataset(
#'   components = list(old_version, new_version),
#'   action = "replace_table"
#' )
#'
#' # Save directly to file
#' assemble_dataset(
#'   components = my_components,
#'   output_path = "path/to/assembled_dataset.rds"
#' )
#' }
#'
#' @importFrom purrr reduce map
#' 
#' @export
#' 

assemble_dataset <- function(components = list(), keep_all = FALSE, action = "merge_properties", output_path = NULL,
                             join_type = "full", groups = NULL, agg_fun = "coalesce") {
  
  # Resolve input data
  components <- lapply(components, resolve_input)
  # Flatten the component level for downstream processing
  data_list <- do.call(c, components)
  
  # Store custom attributes
  metadata <- .store_custom_attributes(data_list)
  # TODO: merge same named attributes
  
  # Set the output structure
  if (!keep_all) {
    out_str <- components[[1]]
  } else {
    out_str <- data_list[!duplicated(names(data_list))]
  }
  
  # Create data frame pool by flattening the input list
  flat_dataset <- flatten_to_depth(data_list)
  
  # Normalize keys to keep only leaf names
  names(flat_dataset) <- sub(".*\\.", "", names(flat_dataset))
  
  # Apply integration action
  merged_pool <- flat_dataset %>%
    split(names(.)) %>% 
    map(function(sub_list) {
      
      if (action == "merge_properties") {
        return(reduce(sub_list, .merge_properties, join_type = join_type))
        
      } else if (action == "aggregate_records") {
        # Bind rows + aggregate
        return(.aggregate_records(sub_list, groups = groups, agg_fun = agg_fun))
        
      } else if (action == "replace_table") {
        # Return the last item in the list (the most recent component wins)
        return(sub_list[[length(sub_list)]])
      }
      
      stop("assemble_dataset: Unknown action. Use 'merge_properties', 'aggregate_records', or 'replace_table'.")
    })
  
  final_structure <- .repopulate_structure(pool = merged_pool, node = out_str)
  
  # Restore attributes and export
  out <- .restore_custom_attributes(final_structure, metadata)
  out <- export_output(out, output_path)
  
  return(out)
}


#' Merge two data frames with automatic join key detection and conflict resolution
#'
#' Nerges two data frames by detecting common columns, performing the appropriate
#' join operation, and resolving duplicate columns created by the join.
#'
#' @param df1 First data frame
#' @param df2 Second data frame
#' @param join_type Character string specifying join type: `"full"` (default), `"left"`, `"right"`, or `"inner"`
#'
#' @return Single merged data frame with resolved column conflicts
#'
#' @details
#' **Join logic:**
#' \itemize{
#'   \item **No common columns**: Performs Cartesian product (cross join) after
#'         deduplicating `df2`, effectively broadcasting metadata
#'   \item **Common columns exist**: Uses specified `join_type` with common columns as join keys
#' }
#'
#' **Conflict resolution:**
#' When columns exist in both data frames (creating `.x` and `.y` suffixes),
#' values are coalesced using `dplyr::coalesce()`, preferring non-NA values from `df1`.
#' Both suffixed columns are then removed, leaving only the resolved column.
#'
#' @noRd
#' 

.merge_properties <- function(df1, df2, join_type = "full") {
  
  common_cols <- intersect(names(df1), names(df2))
  
  # Cartesian Product (Metadata broadcast)
  if (length(common_cols) == 0) {
    return(dplyr::cross_join(df1, dplyr::distinct(df2)))
  }
  
  # Standard Join
  joined_df <- switch(
    join_type,
    "full"  = dplyr::full_join(df1, df2, by = common_cols),
    "left"  = dplyr::left_join(df1, df2, by = common_cols),
    "right" = dplyr::right_join(df1, df2, by = common_cols),
    "inner" = dplyr::inner_join(df1, df2, by = common_cols),
    stop(paste("Unknown join type:", join_type))
  )
  
  # Coalesce duplication (.x and .y)
  cols_x <- grep("\\.x$", names(joined_df), value = TRUE)
  base_names <- sub("\\.x$", "", cols_x)
  
  for(base in base_names) {
    col_x <- paste0(base, ".x")
    col_y <- paste0(base, ".y")
    if(col_y %in% names(joined_df)) {
      joined_df[[base]] <- dplyr::coalesce(joined_df[[col_x]], joined_df[[col_y]])
      joined_df[[col_x]] <- NULL; joined_df[[col_y]] <- NULL
    }
  }
  return(joined_df)
}


#' Aggregate records from multiple data frames by grouping variables
#'
#' Combines multiple data frames via row-binding, then aggregates duplicate
#' records using specified grouping columns and aggregation function.
#'
#' @param data_list List of data frames to combine and aggregate
#' @param groups Character vector of column names to group by. If `NULL` or
#'   empty, returns row-bound data without aggregation.
#' @param agg_fun Aggregation function name: `"coalesce"` (default, first
#'   non-NA), `"mean"`/`"average"`, `"sum"`, `"max"`, or `"min"`. Numeric
#'   functions apply only to numeric/logical columns; text columns always use
#'   first non-NA value.
#'
#' @return Single aggregated data frame with one row per unique combination of
#'   grouping variables
#'
#' @details
#' **Processing steps:**
#' \enumerate{
#'   \item Row-binds all data frames in `data_list`
#'   \item If no groups specified, returns combined data as-is
#'   \item Groups by specified columns and aggregates remaining columns
#' }
#'
#' **Aggregation behavior:**
#' \itemize{
#'   \item **`"coalesce"`**: Returns first non-NA value (default for all types)
#'   \item **Numeric functions**: Apply only to numeric/logical columns;
#'         non-numeric columns fallback to coalesce behavior
#'   \item **NA handling**: Removed before aggregation; all-NA groups return `NA`
#' }
#'
#' @noRd
#' 

.aggregate_records <- function(data_list, groups = NULL, agg_fun = "coalesce") {

  # Stack all inputs
  combined_df <- dplyr::bind_rows(data_list)

  # Return the stacked data if no groups are defined
  if (is.null(groups) || length(groups) == 0) {
    return(combined_df)
  }

  # Select aggregation logic
  .aggregate_val <- function(x) {
    x_clean <- na.omit(x)
    if(length(x_clean) == 0) return(NA)

    # Text or default -> first value
    if (agg_fun == "coalesce") return(dplyr::first(x_clean))

    if (is.numeric(x_clean) || is.logical(x_clean)) {
      if (agg_fun %in% c("average", "mean")) return(mean(x_clean))
      if (agg_fun == "sum") return(sum(x_clean))
      if (agg_fun == "max") return(max(x_clean))
      if (agg_fun == "min") return(min(x_clean))
    }
    return(dplyr::first(x_clean))
  }

  # Perform the aggregation
  out <- combined_df %>%
    dplyr::group_by(dplyr::across(dplyr::any_of(groups))) %>%
    dplyr::summarise(dplyr::across(dplyr::everything(), .aggregate_val), .groups = "drop")

  return(out)
}


#' Repopulate nested list structure with updated data frames from pool
#'
#' Recursively traverses a nested list structure, replacing data frames with
#' their updated versions from a flat named pool while preserving the original hierarchy.
#'
#' @param pool Named list of data frames (flat structure) containing updated tables
#' @param node Current node being processed (data frame or nested list)
#' @param node_name Character string name of current node, used to lookup
#'   replacement in pool. Default `NULL` for root node.
#'
#' @return Nested list with same structure as `node` but data frames replaced from `pool`
#'   where matches exist. Non-matching data frames and non-list elements returned unchanged.
#'
#' @details
#' **Traversal logic:**
#' \itemize{
#'   \item **Data frame node**: If `node_name` exists in `pool`, return pooled
#'         version; otherwise return original unchanged
#'   \item **List node**: Recursively process each child, passing child name
#'         for pool lookup
#'   \item **Other types**: Return unchanged (pass-through)
#' }
#'
#' Used by `assemble_dataset()` to restore hierarchical structure after flattening and
#' merging operations on component tables.
#'
#' @noRd
#' 

.repopulate_structure <- function(pool, node, node_name = NULL) {
  
  # If data frame, look it up in the data frame pool
  if (is.data.frame(node)) {
    if (!is.null(node_name) && node_name %in% names(pool)) {
      return(pool[[node_name]])
    }
    # If not found return original
    return(node)
  }
  
  # Recurse if list
  if (is.list(node)) {
    out <- lapply(names(node), function(nm) {
      .repopulate_structure(pool, node[[nm]], node_name = nm)
    })
    names(out) <- names(node)
    return(out)
  }
  return(node)
}
