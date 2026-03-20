# =============================================================================
# Library of Complex Built-in Actions for the Mapping Engine
# =============================================================================

# This script contains advanced, multi-step actions that can be called from the
# YAML map. Each function follows the standard signature:
#
# =============================================================================

#' Define crop management regime IDs with treatment-regime mapping
#'
#' Creates unique management regime IDs consistant with ICASA format by analyzing
#' event sequences across plots. Automatically detects whether management regimes
#' align with experimental treatments and assigns IDs accordingly.
#'
#' @param action List containing:
#'   - `plot_keys`: Column names that uniquely identify plots
#'   - `regime_scope`: Column names defining the scope for regime comparison
#'     (e.g., experiment, year)
#'   - `treatment_key`: Column containing treatment identifiers
#'   - `event_key`: Column for unique event-plot identifier to remove (optional)
#'   - `event_cols`: Columns containing event characteristics for signatures
#'   - `order_by`: Column to order events within each plot
#'   - `output_id_col`: Name for the output regime ID column
#' @param df Data frame containing plot-level event data
#'
#' @return Data frame with added regime ID column
#'
#' @details
#' Uses a two-pass algorithm: First, creates regime signatures by concatenating
#' ordered event characteristics within each plot. Second, analyzes the
#' treatment-regime relationship within each scope. If regimes map 1-to-1 with
#' treatments (management IS the treatment), uses treatment IDs. Otherwise,
#' uses global preliminary regime IDs (management is baseline).
#'
#' If `event_key` is specified, that column is removed from the result.
#'
#' @noRd
#' 

# We rename the function to be more descriptive of its new, smarter role.
.action_define_icasa_management_id <- function(action, df) {
  
  # 1. Get parameters from YAML
  plot_keys <- unlist(action$plot_keys)
  regime_scope <- unlist(action$regime_scope)
  treatment_key <- action$treatment_key # New parameter
  event_key <- action$event_key
  event_cols <- unlist(action$event_cols)
  order_col <- action$order_by
  output_col <- action$output_id_col
  
  # --- [Validation checks for parameters would go here] ---
  
  # Ensure the treatment_key is present
  if (!treatment_key %in% names(df)) {
    stop(paste0("treatment_key '", treatment_key, "' not found in the data frame."))
  }
  
  # 2. Pass 1: Create global regime signatures and a preliminary ID
  df_regimes <- df %>%
    # Use only the relevant columns for the signature
    dplyr::select(tidyr::all_of(c(plot_keys, regime_scope, treatment_key, order_col, event_cols))) %>%
    # Arrange events to ensure consistent order
    dplyr::arrange(dplyr::across(tidyr::all_of(c(plot_keys, order_col)))) %>%
    dplyr::mutate(dplyr::across(tidyr::all_of(c(event_cols, order_col)), as.character)) %>%
    # Group by plot to create one signature per plot
    dplyr::group_by(dplyr::across(tidyr::all_of(plot_keys))) %>%
    # Create the signature by pasting all event data together
    # --- THIS IS THE FIX ---
    # Summarise the events into a signature.
    # The grouping keys (plot_keys) are automatically kept.
    # We only need to explicitly carry forward the non-grouping `treatment_key`.
    dplyr::summarise(
      .regime_signature = paste(c_across(tidyr::all_of(c(event_cols, order_col))), collapse = "|"),
      !!treatment_key := first(.data[[treatment_key]]),
      .groups = "drop"
    )
  
  # Create the preliminary global ID
  df_regimes <- df_regimes %>%
    dplyr::group_by(dplyr::across(tidyr::all_of(c(regime_scope, ".regime_signature")))) %>%
    dplyr::mutate(prelim_id = dplyr::cur_group_id()) %>%
    dplyr::ungroup()
  
  # 3. Pass 2: Analyze the relationship between treatment and regime
  final_ids <- df_regimes %>%
    dplyr::group_by(dplyr::across(tidyr::all_of(regime_scope))) %>%
    # For each scope (e.g., experiment-year), perform the check
    dplyr::group_modify(~ {
      .x_scoped <- .x
      
      # Check for 1-to-1 mapping
      regime_to_treatment_map <- .x_scoped %>%
        dplyr::group_by(prelim_id) %>%
        dplyr::summarise(n_treatments = dplyr::n_distinct(.data[[treatment_key]]))
      
      treatment_to_regime_map <- .x_scoped %>%
        dplyr::group_by(.data[[treatment_key]]) %>%
        dplyr::summarise(n_regimes = dplyr::n_distinct(prelim_id))
      
      is_one_to_one <- max(regime_to_treatment_map$n_treatments) == 1 &&
        max(treatment_to_regime_map$n_regimes) == 1
      
      # The Decision: Choose which ID to use
      if (is_one_to_one) {
        # Scenario B: Management IS the treatment. Use the treatment ID.
        .x_scoped %>%
          dplyr::mutate(!!output_col := .data[[treatment_key]])
      } else {
        # Scenario A: Management is baseline. Use the global preliminary ID.
        .x_scoped %>%
          dplyr::mutate(!!output_col := prelim_id)
      }
    }) %>%
    dplyr::ungroup()
  
  # 4. Join the final, chosen ID back to the original dataframe
  # We only need the keys and the new ID column for the join
  final_ids_to_join <- final_ids %>%
    dplyr::select(tidyr::all_of(c(plot_keys, output_col)))
  
  # Remove original unique event-plot identifier
  if (!is.null(event_key)) {
    df <- df %>%
      dplyr::select(-tidyr::all_of(event_key))
  }
  
  result_df <- df %>%
    dplyr::left_join(final_ids_to_join, by = plot_keys)
  
  return(result_df)
}
