#' Split DSSAT dataset into component sections
#'
#' Extracts and organizes specific sections from a DSSAT dataset structure,
#' optionally merging related tables within each component.
#'
#' @param dataset List of data frames representing a DSSAT dataset structure
#' @param sec Character vector specifying which sections to extract. Options:
#'   "EXPERIMENT", "METADATA", "MANAGEMENT", "SUMMARY", "TIME_SERIES", "SOIL", "WEATHER".
#'   Default extracts all sections.
#' @param merge Logical indicating whether to merge related tables within each
#'   component (e.g., combining multiple management tables into one). Default TRUE.
#'
#' @return Named list of extracted components. Each element corresponds to a
#'   requested section. Sections not present in the input dataset are omitted.
#'
#' @details
#' Uses specialized extraction functions for each component type:
#' \itemize{
#'   \item EXPERIMENT/METADATA/MANAGEMENT: `.extract_dssat_exp()` with specified section name
#'   \item SUMMARY/TIME_SERIES: `.extract_dssat_obs()` with specified section name
#'   \item SOIL: `.extract_dssat_sol()`
#'   \item WEATHER: `.extract_dssat_wth()`
#' }
#'
#' @noRd
#'

split_dssat_dataset <-  function(dataset,
                                    sec = c("EXPERIMENT", "METADATA", "MANAGEMENT", "SUMMARY", "TIME_SERIES", "SOIL", "WEATHER"),
                                    merge = TRUE) {
  
  sections <- match.arg(sec, several.ok = TRUE)
  
  components <- lapply(sections, function(current_sec) {
    switch(
      current_sec,
      "EXPERIMENT" = .extract_dssat_exp(dataset, sec = c("METADATA", "MANAGEMENT"), merge),
      "METADATA" = .extract_dssat_exp(dataset, sec = "METADATA", merge),
      "MANAGEMENT" = .extract_dssat_exp(dataset, sec = "MANAGEMENT", merge),
      "SUMMARY" = .extract_dssat_obs(dataset, sec = "SUMMARY"),
      "TIME_SERIES" = .extract_dssat_obs(dataset, sec = "TIME_SERIES"),
      "SOIL" = .extract_dssat_sol(dataset, merge),
      "WEATHER" = .extract_dssat_wth(dataset, merge)
    )
  })
  names(components) <- sections
  
  # Drop NULL objects (sections not in input data)
  # TODO: test with missing soil/wth
  components <- purrr::compact(components)
  
  return(components)
}


#' Extract observational data section from DSSAT dataset
#'
#' Retrieves either the SUMMARY or TIME_SERIES section from a DSSAT dataset structure.
#'
#' @param dataset List of data frames representing a DSSAT dataset structure
#' @param sec Character string specifying which observational section to extract.
#'   Must be either "SUMMARY" or "TIME_SERIES".
#'
#' @return Data frame containing the requested observational section, or NULL
#'   if the section is not present in the dataset.
#'
#' @noRd
#' 

.extract_dssat_obs <- function(dataset, sec = c("SUMMARY", "TIME_SERIES")) {
  
  section <- match.arg(sec, several.ok = FALSE)
  
  obs_components <- dataset[names(dataset) %in% section]
  return(obs_components[[section]])
}


#' Extract experiment metadata and management from DSSAT dataset
#'
#' Retrieves experiment-level information (metadata and/or management sections)
#' from a DSSAT dataset structure, with optional merging into a single data frame.
#'
#' @param dataset List of data frames representing a DSSAT dataset structure
#' @param sections Character vector specifying which section types to extract.
#'   Options: "METADATA" (GENERAL, CULTIVARS, FIELDS) and/or "MANAGEMENT"
#'   (TREATMENTS, INITIAL_CONDITIONS, SOIL_ANALYSIS, PLANTING_DETAILS,
#'   IRRIGATION, FERTILIZERS, RESIDUES, CHEMICALS, TILLAGE,
#'   ENVIRONMENT_MODIFICATIONS, HARVEST). Default extracts both.
#' @param merge Logical indicating whether to join all extracted tables into
#'   a single data frame. Default TRUE.
#'
#' @return If `merge = TRUE`, returns a single data frame with all requested
#'   sections joined. If `merge = FALSE`, returns a named list of individual tables.
#'
#' @noRd
#'


.extract_dssat_exp <- function(dataset, sec = c("METADATA", "MANAGEMENT"), merge = TRUE) {
  
  # DSSAT experiment subsections
  meta_secs <- c(
    "GENERAL",
    "CULTIVARS",
    "FIELDS"
  )
  mngt_secs <- c(
    "TREATMENTS",
    "INITIAL_CONDITIONS",
    "SOIL_ANALYSIS",
    "PLANTING_DETAILS",
    "IRRIGATION",
    "FERTILIZERS",
    "RESIDUES",
    "CHEMICALS",
    "TILLAGE",
    "ENVIRONMENT_MODIFICATIONS",
    "HARVEST"
  )

  # Extract target data sections
  target <- c()
  if ("METADATA" %in% sec) {
    target <- c(target, meta_secs)
  }
  if ("MANAGEMENT" %in% sec) {
    target <- c(target, mngt_secs)
  }
  components <- dataset[names(dataset) %in% target]
  
  # HACK! TODO: move to extract template??
  # Format provenance by nested attributes by experiment-year
  if ("GENERAL" %in% names(components)) {
    components[["GENERAL"]] <- components[["GENERAL"]] |>
      dplyr::group_by(
        dplyr::across(
          tidyr::any_of(c("file_name", "EXP_ID", "EXP_YEAR")))) |>
      dplyr::summarise(
        dplyr::across(
          !tidyr::any_of(c("EXP_ID", "EXP_YEAR")),
          ~paste(unique(na.omit(.x)), collapse = "; ")
        ),
        .groups = "drop"
      )
  }

  # Merge into single data frame if required
  if (merge) {
    components <- reduce_by_join(components)
  } 
  
  return(components)
}


#' Extract soil section from DSSAT dataset
#'
#' Retrieves all soil-related tables from a DSSAT dataset structure, with
#' optional merging into a single data frame.
#'
#' @param dataset List of data frames representing a DSSAT dataset structure
#' @param merge Logical indicating whether to join all soil tables into a single
#'   data frame. Default TRUE.
#'
#' @return If `merge = TRUE`, returns a single data frame with soil metadata,
#'   general properties, and layer data joined. If `merge = FALSE`, returns a
#'   named list with elements: meta (SOIL_META), general (SOIL_GENERAL), and
#'   layers (SOIL_LAYERS).
#'
#' @noRd
#'

.extract_dssat_sol <- function(dataset, merge = TRUE) {
  
  soil_components <- list(
    meta = dataset[["SOIL_META"]],
    general = dataset[["SOIL_GENERAL"]],
    layers = dataset[["SOIL_LAYERS"]]
  )
  # TODO: keep? must be handled by PEDON
  soil_notes <- c(
    unlist(soil_components$meta$SL_METHODS_COMMENTS),
    unlist(soil_components$meta$SL_PROF_NOTES)
  )
  
  if (merge) {
    soil_components <- reduce_by_join(soil_components)
  }
  
  return(soil_components)
}


#' Extract weather section from DSSAT dataset
#'
#' Retrieves weather-related tables from a DSSAT dataset structure, with
#' optional merging into a single data frame.
#'
#' @param dataset List of data frames representing a DSSAT dataset structure
#' @param merge Logical indicating whether to join weather metadata and daily
#'   data into a single data frame. Default TRUE.
#'
#' @return If `merge = TRUE`, returns a single data frame with weather metadata
#'   and daily observations joined. If `merge = FALSE`, returns a named list
#'   with elements: meta (WEATHER_METADATA) and data (WEATHER_DAILY).
#'
#' @noRd
#'

.extract_dssat_wth <- function(dataset, merge = TRUE) {
  
  wth_components <- list(
    meta = dataset[["WEATHER_METADATA"]],
    data = dataset[["WEATHER_DAILY"]]
  )
  # TODO: keep? must be handled by WSTA+YEAR
  wth_notes <- unlist(
    lapply(wth_components, function(df) attr(df, "comments")),
    use.names = FALSE
  )
  
  if (merge) {
    wth_components <- reduce_by_join(wth_components)
  }
  
  return(wth_components)
}


#' Reconstruct DSSAT dataset with standardized codes
#'
#' Applies generated DSSAT codes to all dataset components, updating identifiers
#' across management, soil, weather, and field tables while preserving 
#' referential integrity through code mapping attributes.
#'
#' @param dataset List of data frames representing the original DSSAT dataset
#'   structure before code generation.
#' @param data_nms List of data frames with generated DSSAT codes (output from
#'   `resolve_dssat_codes()`). Must contain:
#'   \itemize{
#'     \item **METADATA**: With `code_map` attribute for experiment IDs
#'     \item **WEATHER**: With `code_map` attribute for weather station codes
#'     \item **SOIL**: With `code_map` attribute for soil profile IDs
#'   }
#'
#' @return List of data frames with updated DSSAT codes and restructured tables:
#'   \describe{
#'     \item{**FIELDS**}{Updated with new EXP_ID, ID_FIELD, WSTA, ID_SOIL}
#'     \item{**GENERAL**, **CULTIVARS**}{Management tables with new EXP_ID and file_name}
#'     \item{**SOIL_META**, **SOIL_GENERAL**, **SOIL_LAYERS**}{Soil tables with new PEDON codes}
#'     \item{**WEATHER_DAILY**, **WEATHER_METADATA**}{Weather tables with new INSI codes and file names}
#'     \item{All other tables}{EXP_ID updated globally via code mapping}
#'   }
#'
#' @details
#' **Reconstruction workflow:**
#' \enumerate{
#'   \item **Extract code maps** from attributes of `data_nms` components
#'   \item **Update FIELDS table** via left joins with all three code maps,
#'         replacing ID_FIELD, WSTA, ID_SOIL, and EXP_ID with new values
#'   \item **Reconstruct management tables** (GENERAL, CULTIVARS) by selecting
#'         relevant columns from METADATA plus template columns
#'   \item **Reconstruct soil tables** (SOIL_META, SOIL_GENERAL, SOIL_LAYERS)
#'         from enriched SOIL data, preserving metadata and layer information
#'   \item **Reconstruct weather tables** (WEATHER_DAILY, WEATHER_METADATA)
#'         with updated INSI codes and file names
#'   \item **Apply EXP_ID updates globally** to all remaining tables via vectorized recoding
#' }
#'
#' **Join strategy:**
#' Uses `intersect(colnames(...))` to dynamically determine join keys, ensuring
#' flexibility across varying dataset structures.
#'
#' **Column selection:**
#' \itemize{
#'   \item Prioritizes columns from corresponding templates (e.g., `FIELDS_template`, `SOIL_template`)
#'   \item Preserves `_NOTES` and `_COMMENTS` columns via regex matching
#'   \item Uses `tidyr::any_of()` for safe column selection with missing names
#' }
#'
#' **Code mapping application:**
#' Final step uses `dplyr::recode()` with pre-filtered mapping vectors to avoid recoding errors.
#' Only updates EXP_ID values that exist in each specific table.
#'
#' @section Dependencies:
#' Requires template objects in scope: `FIELDS_template`, `GENERAL_template`,
#' `CULTIVARS_template`, `SOIL_META_template`, `SOIL_GENERAL_template`,
#' `SOIL_template`, `WEATHER_template`, `WEATHER_header_template`.
#'
#' @seealso
#' `resolve_dssat_codes()`, `.resolve_dssat_exp_codes()`, `.resolve_dssat_sol_codes()`, `.resolve_dssat_wth_codes()`
#'
#' @noRd
#'

reconstruct_dssat_dataset <- function(dataset, data_nms) {

  dataset_out <- dataset

  # --- Extract code maps ---
  exp_code_map <- attr(data_nms$METADATA, "code_map")
  wsta_code_map <- attr(data_nms$WEATHER, "code_map")
  soil_code_map <- attr(data_nms$SOIL, "code_map")

  # --- Update soil and weather links in FIELDS table ---
  fields_joined <- dataset[["FIELDS"]] |>
    dplyr::left_join(
      exp_code_map,
      by = intersect(colnames(dataset[["FIELDS"]]), colnames(exp_code_map))
    ) |>
    dplyr::left_join(
      wsta_code_map,
      by = intersect(colnames(dataset[["FIELDS"]]), colnames(wsta_code_map))
    ) |>
    dplyr::left_join(
      soil_code_map,
      by = intersect(colnames(dataset[["FIELDS"]]), colnames(soil_code_map))
    ) |>
    dplyr::mutate(
      ID_FIELD = ID_FIELD_new,
      WSTA = INSI_new,
      ID_SOIL = PEDON_new,
      EXP_ID = EXP_ID_new
    ) |>
    dplyr::left_join(
      data_nms$METADATA,
      by = c("EXP_ID", "L", "XCRD", "YCRD") # Your original keys
    )
  dataset_out[["FIELDS"]] <- fields_joined |>
    dplyr::select(EXP_ID, tidyr::any_of(colnames(FIELDS_template)),
                  tidyr::any_of(grep("_NOTES|_COMMENTS", names(fields_joined), value = TRUE)))

  # --- Reconstruct Management tables ---
  mngt_data_out <- list()
  mngt_metadata_nms <- names(data_nms$METADATA)
  mngt_data_out[["GENERAL"]] <- data_nms$METADATA |>
    dplyr::select(file_name, EXP_ID, INSTITUTION,
                  tidyr::any_of(union(colnames(dataset[["GENERAL"]]),
                                      colnames(GENERAL_template))),
                  dplyr::any_of(grep("_NOTES|_COMMENTS", mngt_metadata_nms, value = TRUE)))
  mngt_data_out[["CULTIVARS"]] <- data_nms$METADATA |>
    dplyr::select(tidyr::any_of(union(colnames(dataset[["CULTIVARS"]]),
                                      colnames(CULTIVARS_template))),
                  dplyr::any_of(grep("_NOTES|_COMMENTS", mngt_metadata_nms, value = TRUE)))
  dataset_out[names(mngt_data_out)] <- mngt_data_out

  # --- Reconstruct Soil profile table ---
  soil_data_out <- list()
  sol_nms <- names(data_nms$SOIL)
  soil_data_out[["SOIL_META"]] <- data_nms$SOIL |>
    dplyr::select(file_name, EXP_ID, INST_NAME, PEDON, YEAR,
                  intersect(colnames(SOIL_META_template), sol_nms),
                  tidyr::any_of(grep("_NOTES|_COMMENTS", sol_nms, value = TRUE))) |>
    dplyr::distinct()
  soil_data_out[["SOIL_GENERAL"]] <- data_nms$SOIL |>
    dplyr::select(EXP_ID, INST_NAME, PEDON, YEAR,
                  intersect(colnames(SOIL_GENERAL_template), sol_nms),
                  tidyr::any_of(grep("_NOTES|_COMMENTS", sol_nms, value = TRUE))) |>
    dplyr::distinct()
  soil_data_out[["SOIL_LAYERS"]] <- data_nms$SOIL |>
    dplyr::select(EXP_ID, INST_NAME, PEDON, YEAR,
                  intersect(colnames(SOIL_template), sol_nms),
                  tidyr::any_of(grep("_NOTES|_COMMENTS", sol_nms, value = TRUE)))
  dataset_out[names(soil_data_out)] <- soil_data_out

  # --- Reconstruct Weather station tables ---
  wth_data_out <- list()
  wth_nms <- names(data_nms$WEATHER)
  wth_data_out[["WEATHER_DAILY"]] <- data_nms$WEATHER |>
    dplyr::select(file_name, WST_NAME, EXP_ID, INSI, YEAR,
                  intersect(colnames(WEATHER_template), wth_nms),
                  tidyr::any_of(grep("_NOTES|_COMMENTS", wth_nms, value = TRUE)))
  wth_data_out[["WEATHER_METADATA"]] <- data_nms$WEATHER |>
    dplyr::select(file_name, WST_NAME, EXP_ID, INSI, YEAR,
                  intersect(colnames(WEATHER_header_template), wth_nms),
                  tidyr::any_of(grep("_NOTES|_COMMENTS", wth_nms, value = TRUE))) |>
    dplyr::distinct()
  dataset_out[names(wth_data_out)] <- wth_data_out


  # --- Update experiment code globally ---
  exp_code_vector <- exp_code_map |>
    dplyr::select(EXP_ID, EXP_ID_new) |>
    tibble::deframe()

  dataset_out <- lapply(dataset_out, function(df) {
    if ("EXP_ID" %in% names(df)) {
      map_filter <- names(exp_code_vector) %in% unique(df$EXP_ID)
      map_filtered <- exp_code_vector[map_filter]

      if (length(map_filtered) > 0) {
        df <- dplyr::mutate(
          df,
          EXP_ID = dplyr::recode(EXP_ID, !!!map_filtered)
        )
      }
    }
    return(df)
  })

  return(dataset_out)
}
