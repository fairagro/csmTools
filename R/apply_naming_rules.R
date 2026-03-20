#' Apply DSSAT naming conventions to dataset
#'
#' Standardizes a dataset by generating DSSAT-compliant codes for management,
#' soil, and weather components. This function orchestrates the complete naming
#' workflow: extracting components, enriching metadata, generating codes, and
#' reconstructing the dataset.
#'
#' @param dataset List of data frames representing a DSSAT dataset structure
#'
#' @return List of data frames with DSSAT-standardized naming codes applied
#'   and duplicate rows removed
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Splits dataset into METADATA, SOIL, and WEATHER components
#'   \item Enriches metadata to fill gaps needed for DSSAT code generation
#'     (mainly reverse geocoding)
#'   \item Generates DSSAT-compliant codes for each component
#'   \item Reconstructs the original dataset structure with new codes
#'   \item Deduplicates all data frames
#' }
#'
#' @noRd
#' 

apply_naming_rules <- function(dataset) {
  
  # --- Extract data components ---
  dssat_components <- split_dssat_dataset(
    dataset,
    sec = c("METADATA", "SOIL", "WEATHER"),
    merge = TRUE
  )
  
  # --- Fill gaps in metadata for DSSAT code components ---
  data_enriched <- enrich_dssat_metadata(dssat_components)
  
  # --- Generate DSSAT codes ---
  data_nms <- resolve_dssat_codes(data_enriched)
  
  # --- Update input dataset with standardized names ---
  dataset_out <- reconstruct_dssat_dataset(dataset, data_nms)
  
  # --- Data cleaning ---
  # Deduplicate all dataframes
  dataset_out <- apply_recursive(dataset_out, dplyr::distinct)
  
  return(dataset_out)
}
