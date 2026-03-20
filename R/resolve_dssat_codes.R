#' Generate DSSAT codes for management, soil, and weather components
#'
#' Orchestrates DSSAT code generation across all major dataset components,
#' producing standardized identifiers for experiments, soils, and weather
#' stations. Attaches code mapping metadata as attributes for traceability.
#'
#' @param dataset List of data frames with components:
#'   \itemize{
#'     \item **MANAGEMENT_CORE**: Experiment metadata
#'     \item **SOIL**: Soil profile data
#'     \item **WEATHER**: Weather station data
#'   }
#'
#' @return Modified dataset list with DSSAT code columns added to each
#'   component. Code mappings (original identifier → DSSAT code) are attached
#'   as attributes to individual components but not returned at the dataset level.
#'
#' @details
#' **Processing sequence:**
#' \enumerate{
#'   \item **Management codes** via `.resolve_dssat_exp_codes()`:
#'         Generates treatment/experiment identifiers (e.g., TRNO, EXNAME)
#'   \item **Soil codes** via `.resolve_dssat_sol_codes()`:
#'         Creates ID_SOIL codes linking profiles to DSSAT soil files
#'   \item **Weather codes** via `.resolve_dssat_wth_codes()`:
#'         Generates WSTA codes for weather station identification
#' }
#'
#' Each resolver function attaches a `code_map` attribute (data frame with
#' original_id → dssat_code mappings) to its output. These are collected
#' internally but not propagated to the final dataset structure.
#'
#' NULL components are skipped without error.
#'
#' @seealso 
#' [.resolve_dssat_exp_codes()], [.resolve_dssat_sol_codes()],
#' [.resolve_dssat_wth_codes()]
#'
#' @noRd
#' 

resolve_dssat_codes <- function(dataset) {
  
  dataset_nms_fmt <- list()
  all_code_maps <- list()
  
  exp_metadata <- dataset$METADATA
  sol_data <- dataset$SOIL
  wth_data <- dataset$WEATHER
  
  # --- Process Management ---
  if (!is.null(exp_metadata)) { 
    exp_dssat_codes <- .resolve_dssat_exp_codes(exp_metadata)
    # Append code map
    exp_id_map <- attr(exp_dssat_codes, "code_map") 
    all_code_maps <- c(all_code_maps, exp_id_map)
    # Format outputs
    dataset$METADATA <- exp_dssat_codes
  }
  
  # --- Process Soil ---
  if (!is.null(sol_data)) {
    sol_dssat_codes <- .resolve_dssat_sol_codes(sol_data)
    # Append code map
    sol_id_map <- attr(sol_dssat_codes, "code_map")
    all_code_maps$ID_SOIL <- sol_id_map
    # Update input data
    dataset$SOIL <- sol_dssat_codes
  }
  
  # --- Process Weather ---
  if (!is.null(wth_data)) {
    wth_dssat_codes <- .resolve_dssat_wth_codes(wth_data)
    # Append code map
    wth_id_map <- attr(wth_dssat_codes, "code_map")
    all_code_maps$WSTA <- wth_id_map
    # Update input data
    dataset$WEATHER <- wth_dssat_codes
  }

  return(dataset)
}


#' Generate DSSAT-compliant codes for experiment metadata
#'
#' Creates or validates DSSAT identifiers for FIELDS, CULTIVARS, and EXPERIMENTS
#' according to DSSAT naming conventions. Generates new codes when existing IDs
#' are invalid or missing, preserving valid codes where present.
#'
#' @param exp_metadata Data frame containing DSSAT GENERAL, FIELDS and CULTIVARS metadata including:
#'   \itemize{
#'     \item **ID_FIELD**: Field identifier (if present, defaults to "NA")
#'     \item **INGENO**: Cultivar/genotype code (if present, defaults to "NA")
#'     \item **EXP_ID**: Experiment identifier (if present, defaults to "NA")
#'     \item **INSTITUTION**: Institution code (required for code generation)
#'     \item **SITE**: Site code (required for code generation)
#'     \item **EXP_YEAR**: Experiment year (if present, defaults to "NNNN")
#'     \item **CR**: Crop code (required for file naming)
#'   }
#'
#' @return Data frame with updated DSSAT codes and additional column:
#'   \itemize{
#'     \item **ID_FIELD**: Validated/generated field code
#'     \item **INGENO**: Validated/generated cultivar code
#'     \item **EXP_ID**: Validated/generated experiment code
#'     \item **file_name**: DSSAT experiment file name (format: `EXP_ID.CRX`)
#'   }
#'   
#'   A `code_map` attribute (data frame) is attached showing original → new
#'   code mappings for ID_FIELD, INGENO, and EXP_ID.
#'
#' @details
#' **Code generation logic:**
#' \enumerate{
#'   \item **Field codes**: Validated via `is_valid_dssat_id()`. Invalid/missing
#'         codes trigger `generate_dssat_id(type = "field")` using institution
#'         and site metadata. One unique code generated per ID_FIELD group.
#'   \item **Cultivar codes**: Similar validation/generation using 
#'         `generate_dssat_id(type = "cultivar")` with institution metadata.
#'   \item **Experiment codes**: Generated per institution-site-year combination
#'         using `generate_dssat_id(type = "experiment")`.
#' }
#'
#' Grouping ensures all rows sharing an original ID receive the same new code.
#' `sequence_no` (group ID) provides uniqueness when generating codes.
#'
#' @seealso 
#' [is_valid_dssat_id()], [generate_dssat_id()], 
#' [.resolve_dssat_soil_codes()], [.resolve_dssat_wth_codes()]
#'
#' @noRd
#'

.resolve_dssat_exp_codes <- function(exp_metadata) {

  # --- Create standard DSSAT soil profile identifiers ---
  exp_dssat_fmt_nms <- exp_metadata %>%
    # 1- FIELD code
    dplyr::mutate(ID_FIELD = if ("ID_FIELD" %in% names(.)) ID_FIELD else "NA") %>%
    dplyr::group_by(ID_FIELD) %>%
    dplyr::mutate(
      ID_FIELD_new = if (!is_valid_dssat_id(dplyr::first(ID_FIELD), "field", "dssat")) {
        # Generate one new ID per group and apply it to all rows in that group
        generate_dssat_id(
          type = "field",
          institution = dplyr::first(INSTITUTION),
          site = dplyr::first(SITE),
          sequence_no = dplyr::cur_group_id()
        )
      } else {
        dplyr::first(ID_FIELD)
      }
    ) %>%
    dplyr::ungroup() %>%
    # 2- CULTIVAR code
    dplyr::mutate(INGENO = if ("INGENO" %in% names(.)) INGENO else "NA") %>%
    dplyr::group_by(INGENO) %>%
    dplyr::mutate(
      INGENO_new = if (!is_valid_dssat_id(dplyr::first(INGENO), "cultivar", "dssat")) {
        # Generate one new ID per group and apply it to all rows in that group
        generate_dssat_id(
          type = "cultivar",
          institution = dplyr::first(INSTITUTION),
          sequence_no = dplyr::cur_group_id()
        )
      } else {
        dplyr::first(INGENO)
      }
    ) %>%
    dplyr::ungroup() %>%
    # 3- EXPERIMENT code
    dplyr::mutate(EXP_ID = if ("EXP_ID" %in% names(.)) EXP_ID else "NA") %>%
    dplyr::mutate(EXP_YEAR = if ("EXP_YEAR" %in% names(.)) EXP_YEAR else "NNNN") %>%
    group_by(INSTITUTION, SITE, EXP_YEAR) %>%
    dplyr::mutate(
      EXP_ID_new = if (!is_valid_dssat_id(dplyr::first(EXP_ID), "experiment", "dssat")) {
        # Generate one new ID per group and apply it to all rows in that group
        generate_dssat_id(
          type = "experiment",
          institution = dplyr::first(INSTITUTION),
          site = dplyr::first(SITE),
          year = dplyr::first(EXP_YEAR),
          sequence_no = dplyr::cur_group_id()
        )
      } else {
        dplyr::first(EXP_ID)
      }
    ) %>%
    dplyr::mutate(
      file_name = paste0(EXP_ID_new, ".", CR, "X")
    ) %>%
    dplyr::ungroup()
  
  # --- Format output ---
  exp_dssat_out <- exp_dssat_fmt_nms |>
    dplyr::mutate(
      ID_FIELD = ID_FIELD_new,
      INGENO = INGENO_new,
      EXP_ID = EXP_ID_new
    )
  
  # ID map for referential integrity checks
  exp_dssat_code_map <- exp_dssat_fmt_nms |>
    dplyr::select(EXP_ID, ID_FIELD, ID_FIELD_new, INGENO, INGENO_new, EXP_ID, EXP_ID_new) |>
    dplyr::distinct()
  attr(exp_dssat_out, "code_map") <- exp_dssat_code_map
  
  return(exp_dssat_out)
}


#' Generate DSSAT-compliant soil profile codes
#'
#' Creates or validates DSSAT soil identifiers (PEDON codes) according to DSSAT
#' naming conventions. Generates new codes when existing identifiers are invalid
#' or missing, preserving valid codes where present.
#'
#' @param sol_data Data frame containing soil profile data with columns:
#'   \itemize{
#'     \item **PEDON**: Soil profile identifier (if present, defaults to "NA")
#'     \item **INST_NAME**: Institution code (required for code generation)
#'     \item **SITE**: Site code (required for code generation)
#'     \item **YEAR**: Year associated with profile (required for code generation)
#'     \item **EXP_ID**: Experiment identifier (required for code mapping)
#'   }
#'
#' @return Data frame with updated DSSAT codes and additional column:
#'   \itemize{
#'     \item **PEDON**: Validated/generated soil profile code (ID_SOIL in DSSAT)
#'     \item **file_name**: DSSAT soil file name (format: `XX.SOL` where XX = 
#'           first 2 characters of PEDON)
#'     \item All original columns from `sol_data`
#'   }
#'   
#'   A `code_map` attribute (data frame) is attached with columns:
#'   \itemize{
#'     \item **EXP_ID**: Experiment identifier for linking
#'     \item **PEDON**: Original soil code
#'     \item **PEDON_new**: Generated/validated DSSAT code
#'   }
#'
#' @details
#' **Code generation logic:**
#' \enumerate{
#'   \item Groups data by existing PEDON values
#'   \item Validates each group's PEDON via `is_valid_dssat_id(type = "soil")`
#'   \item For invalid/missing codes, generates new ID using 
#'         `generate_dssat_id(type = "soil")` with institution, site, year,
#'         and sequence number (group ID) for uniqueness
#'   \item Applies the same new code to all rows within each PEDON group
#'   \item Creates soil file name using first 2 characters of final PEDON code
#' }
#'
#' **File naming:** DSSAT soil files use 2-character prefixes (e.g., "IB.SOL",
#' "AG.SOL") derived from the institution/region code embedded in PEDON.
#'
#' @seealso 
#' [is_valid_dssat_id()], [generate_dssat_id()], 
#' [.resolve_dssat_exp_codes()], [.resolve_dssat_wth_codes()]
#'
#' @noRd
#' 

.resolve_dssat_sol_codes <- function(sol_data) {
  
  # --- Create standard DSSAT soil profile identifiers ---
  sol_dssat_fmt_nms <- sol_data %>%
    dplyr::mutate(PEDON = if ("PEDON" %in% names(.)) PEDON else "NA") %>%
    dplyr::group_by(PEDON) %>%
    dplyr::mutate(
      PEDON_new = if (!is_valid_dssat_id(dplyr::first(PEDON), "soil", "dssat")) {
        # Generate one new ID per group and apply it to all rows in that group
        generate_dssat_id(
          type = "soil",
          institution = dplyr::first(INST_NAME),
          site = dplyr::first(SITE),
          year = dplyr::first(YEAR),
          sequence_no = dplyr::cur_group_id()
        )
      } else {
        dplyr::first(PEDON)
      }
    ) %>%
    dplyr::mutate(
      file_name = paste0(substr(PEDON_new, 1, 2), ".SOL")
    ) %>%
    dplyr::ungroup()
  
  # --- Format output ---
  sol_dssat_out <- sol_dssat_fmt_nms |>
    dplyr::mutate(PEDON = PEDON_new)
    #dplyr::select(EXP_ID, dplyr::any_of(union(colnames(sol_data), colnames(sol_template))))
  
  # ID map for referential integrity checks
  attr(sol_dssat_out, "code_map") <- sol_dssat_fmt_nms |>
    dplyr::select(EXP_ID, PEDON, PEDON_new) |>
    dplyr::distinct()
  
  return(sol_dssat_out)
}


#' Generate DSSAT-compliant weather station codes
#'
#' Creates or validates DSSAT weather station identifiers (INSI codes) according
#' to DSSAT naming conventions. Generates new codes when existing identifiers
#' are invalid or missing, preserving valid codes where present.
#'
#' @param wth_data Data frame containing weather station data with columns:
#'   \itemize{
#'     \item **INSI**: Weather station identifier (if present, defaults to "NA")
#'     \item **WST_NAME**: Weather station name (used for code generation)
#'     \item **YEAR**: Year of weather data (required for file naming)
#'     \item **EXP_ID**: Experiment identifier (required for code mapping)
#'   }
#'
#' @return Data frame with updated DSSAT codes and additional column:
#'   \itemize{
#'     \item **INSI**: Validated/generated weather station code (WSTA in DSSAT)
#'     \item **file_name**: DSSAT weather file name (format: `INSIYYYYNN.WTH`
#'           where INSI = station code, YYYY = year, NN = sequence number)
#'     \item All original columns from `wth_data`
#'   }
#'   
#'   A `code_map` attribute (data frame) is attached with columns:
#'   \itemize{
#'     \item **EXP_ID**: Experiment identifier for linking
#'     \item **INSI**: Original weather station code
#'     \item **INSI_new**: Generated/validated DSSAT code
#'   }
#'
#' @details
#' **Code generation logic:**
#' \enumerate{
#'   \item Groups data by existing INSI and YEAR values
#'   \item Validates each group's INSI via `is_valid_dssat_id(type = "weather_station")`
#'   \item For invalid/missing codes, generates new ID using 
#'         `generate_dssat_id(type = "weather_station")` with institution
#'         (derived from INSI), station name, and year
#'   \item Creates weather file name using INSI + year + sequence number
#' }
#'
#' **Limitations (TODO):**
#' \itemize{
#'   \item `sequence_no` hardcoded to 1 - needs proper counting logic when
#'         multiple stations exist per institution-year
#'   \item File name sequence (`sprintf("%02d", 1)`) similarly hardcoded
#'   \item Institution imputation from INSI may need validation
#' }
#'
#' **File naming:** DSSAT weather files use format `XXXXYYYYNN.WTH` where:
#' \itemize{
#'   \item XXXX = 4-character station code
#'   \item YYYY = 4-digit year
#'   \item NN = 2-digit sequence number (01-99)
#' }
#'
#' @seealso 
#' [is_valid_dssat_id()], [generate_dssat_id()], 
#' [.resolve_dssat_exp_codes()], [.resolve_dssat_sol_codes()]
#'
#' @noRd
#'

.resolve_dssat_wth_codes <- function(wth_data) {
  
  # --- Create standard DSSAT weather station identifiers ---
  wth_dssat_fmt_nms <- wth_data %>%
    dplyr::mutate(INSI = if ("INSI" %in% names(.)) INSI else "NA") %>%
    dplyr::group_by(INSI, YEAR) %>%
    dplyr::mutate(
      INSI_new = if (!is_valid_dssat_id(dplyr::first(INSI), "weather_station", "dssat")) {
        # Generate one new ID per group and apply it to all rows in that group
        generate_dssat_id(
          type = "weather_station",
          institution = dplyr::first(INSI),  #TODO IMPUTATION!
          site = dplyr::first(WST_NAME),
          year = dplyr::first(YEAR),
          sequence_no = 1  # Need another data identifier to do real count!
        )
      } else {
        dplyr::first(INSI)
      }
    ) %>%
    dplyr::mutate(
      file_name = paste0(
        # Need another data identifier to do real count!
        INSI_new, YEAR, sprintf("%02d", 1), ".WTH"
      )
    ) %>%
    dplyr::ungroup()
  
  # --- Format output ---
  wth_dssat_out <- wth_dssat_fmt_nms |>
    dplyr::mutate(INSI = INSI_new)
  
  # ID map for referential integrity checks
  attr(wth_dssat_out, "code_map") <- wth_dssat_fmt_nms |>
    dplyr::select(EXP_ID, INSI, INSI_new) |>
    dplyr::distinct()
  
  return(wth_dssat_out)
}
