#' Convert dataset between data models
#'
#' Transforms a dataset from one data model to another (e.g., BonaRes LTE to ICASA) using mapping rules predefined in a yaml file.
#'
#' @param dataset (list) A list of data frames.
#' @param input_model (character) Name of the source data model (e.g., "bonares-lte_de").
#' @param output_model (character) Name of the target data model (e.g., "icasa").
#' @param output_path (character) Optional file path to save the output.
#' @param unmatched_code (character) How to handle unmatched levels during mapping of categorical variables:
#'  \itemize{
#'   \item `"na"` (default; sets to NA)
#'   \item `"pass_through"` (keeps original value)
#'   \item `"default_value"` (uses default predefined in yaml map)
#'   }
#'
#' @return A list of data frames in the target data model format.
#'
#'
#' @examples
#' # Convert a dataset from BonaRes to ICASA format and write the result in a json file
#' converted_data <- convert_dataset(
#'   dataset = my_bonares_data,
#'   input_model = "bonares-lte_de",
#'   output_model = "icasa",
#'   unmatched_code = "pass_through",
#'   output_path = "path/to/output.json"
#' )
#'
#' @export
#' 

convert_dataset <- function(dataset, input_model, output_model, output_path = NULL, unmatched_code = "na") {
  
  # =================================================================
  # 1- Input resolution
  # =================================================================

  dataset <- resolve_input(dataset)
  
  # =================================================================
  # 1- Mapping configuration
  # =================================================================
  
  # Retrieve data model configuration
  config <- .config_input(dataset, input_model, output_model)
  data_config <- config$data
  data_map <- config$map
  
  rules <- data_map$rules
  
  # Check if mapping is implemented
  if (is.null(data_map)) {
    stop(paste("No mapping defined for '", input_model, "' to '", output_model, "'.", sep = ""))
  }
  
  # =================================================================
  # 2- Mapping execution
  # =================================================================
  
  message("Step 1: Mapping data terms...")
  # Converts columns from source names to canonical target names (e.g., Bonares -> ICASA)
  
  code_handlers <- c("na", "pass_through", "default_value")
  unmatched_code <- match.arg(unmatched_code, code_handlers)
  
  mapped_data <- .apply_mapping_rules(
    input_data = data_config,
    rules = rules,
    unmatched_code = unmatched_code
  )

  # =================================================================
  # 3- Output standardization
  # =================================================================
  
  # Applies model-specific logic like aggregations and calculations.
  message("Step 2: Standardizing output format...")
  
  # --- Drop empty data frames ---
  mapped_data <- lapply(mapped_data, function(df) unique(df))
  
  # --- Apply post-processing logic, if applicable ---
  mapped_data_std <- standardize_data(dataset = mapped_data, data_model = output_model)
  
  # --- Deduplicate and drop NAs ---
  snapshot <- .store_custom_attributes(mapped_data_std)  # Store attributes
  mapped_data_std_clean <- apply_recursive(mapped_data_std, remove_all_na_cols)  # TODO: fix deleting exp_year if empty
  mapped_data_std_clean <- .restore_custom_attributes(mapped_data_std_clean, snapshot)  # Restore attributes
  
  # --- Return output ---
  out <- export_output(mapped_data_std_clean, output_path)
  return(out)
}


#' Load and prepare configuration for data model conversion
#'
#' Retrieves configuration files for both input and output data models, loads the
#' appropriate mapping file, and applies master key logic if defined in the input model.
#'
#' @param dataset A list of data frames to be converted
#' @param input_model Character string naming the source data model (e.g., "bonares-lte_de")
#' @param output_model Character string naming the target data model (e.g., "icasa")
#'
#' @return A list with two elements:
#'   \itemize{
#'     \item `data`: The input dataset, potentially modified with master key applied
#'     \item `map`: The mapping rules loaded from the YAML mapping file
#'   }
#'
#' @details
#' This function performs three main steps:
#' 1. Loads the main data model configurations from `datamodels.yaml`
#' 2. Identifies and loads the appropriate mapping file for the input-to-output conversion
#' 3. If the input model defines a master key, applies it across all tables in the dataset
#'
#' The master key is a column that should be present across all tables in a data model
#' to enable proper relational structure.
#'
#' @noRd
#'

.config_input <- function(dataset, input_model, output_model) {
  
  # Fetch configuration for the focal data model
  config_path <- system.file("extdata", "datamodels.yaml", package = "csmTools")
  config <- yaml::read_yaml(config_path)
  output_model_config <- config[[output_model]]
  input_model_config  <- config[[input_model]]
  
  # Load the map file for the focal data mapping
  map_name <- output_model_config$mappings[[input_model]]
  map_path <- system.file("extdata", map_name, package = "csmTools")
  map <- yaml::read_yaml(map_path)
  
  # Pre-process master keys
  # >> If the input model has a master key, apply to all tables
  has_mk <- !is.null(input_model_config$master_key) &&
    input_model_config$master_key != 'none'
  
  if (has_mk) {
    config_data <- .apply_master_key(dataset, input_model_config)
  } else {
    config_data <- dataset
  }
  out <- list(data = config_data, map = map)
  
  return(out)
}


#' Apply master key across all tables in a dataset
#'
#' Propagates a master key column from a source table to all other tables in the dataset,
#' ensuring relational integrity. Handles both single and multi-experiment scenarios.
#'
#' @param dataset A list of data frames representing the input dataset
#' @param model_config A list containing the data model configuration with elements:
#'   \itemize{
#'     \item `master_key`: Conceptual name of the master key
#'     \item `key_source_table`: Name of the table containing the master key values
#'     \item `design_keys`: Named list mapping conceptual keys to actual column names
#'     \item `shared_tables`: Vector of table names that should be duplicated for each key value
#'   }
#'
#' @return The dataset with the master key propagated to all tables and positioned as
#'   the first column in each table that contains it.
#'
#' @details
#' The function handles three scenarios:
#' \itemize{
#'   \item **Single experiment**: If only one unique key value exists, it's added to all
#'     tables that don't already have it
#'   \item **Multiple experiments with shared tables**: Tables marked as "shared" are
#'     cross-joined with all key values to create copies for each experiment
#'   \item **Tables with existing keys**: Left unchanged to preserve existing relationships
#' }
#'
#' If master key configuration is incomplete or the source table is missing, the function
#' issues a warning and returns the dataset unchanged.
#'
#' @noRd
#'

.apply_master_key <- function(dataset, model_config) {
  
  key_name_conceptual <- model_config$master_key
  key_source_table <- model_config$key_source_table
  key_name_actual <- model_config$design_keys[[key_name_conceptual]]
  shared_tables <- unlist(model_config$shared_tables)
  
  if (is.null(key_name_actual) || is.null(key_source_table) || !key_source_table %in% names(dataset)) {
    warning("Master key details not defined correctly in datamodels.yaml. Skipping linking.", call. = FALSE)
    return(dataset)
  }
  
  unique_keys <- unique(na.omit(dataset[[key_source_table]][[key_name_actual]]))
  
  if (length(unique_keys) == 0) {
    return(dataset)
  }
  
  is_single_experiment <- length(unique_keys) == 1
  
  # Propagate the master key to other tables
  for (table_name in names(dataset)) {

    if (table_name == key_source_table) next
    
    df <- dataset[[table_name]]
    has_key <- key_name_actual %in% names(df)
    is_shared <- table_name %in% shared_tables
    
    if (is_single_experiment && !has_key) {
      df[[key_name_actual]] <- unique_keys[1]
    } else if (!is_single_experiment && !has_key && is_shared) {
      key_df <- tibble::tibble(!!key_name_actual := unique_keys)
      df <- dplyr::cross_join(df, key_df)
    }
    
    dataset[[table_name]] <- df
  }
  
  # Ensure the master key is the first column across all tables
  for (table_name in names(dataset)) {
    df <- dataset[[table_name]]
    
    if (key_name_actual %in% names(df)) {
      df <- dplyr::select(df, dplyr::all_of(key_name_actual), tidyr::everything())
      dataset[[table_name]] <- df
    }
  }
  
  return(dataset)
}
