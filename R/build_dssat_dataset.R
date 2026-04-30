#' Build DSSAT-formatted file structures from standardized dataset components
#'
#' Converts a standardized dataset into DSSAT-compliant file format structures by routing each
#' component to an appropriate builder function with corresponding templates and formatting specifications.
#'
#' @param dataset Named list of data frames representing DSSAT components:
#'   \itemize{
#'     \item **EXPERIMENT**: Experiment metadata, treatments, and crop management regimes
#'     \item **SUMMARY**: Single-point or summarized measured data (e.g., crop yield)
#'     \item **TIME_SERIES**: Daily time series measured data (e.g., zadoks stage)
#'     \item **SOIL**: Soil profile data
#'     \item **WEATHER**: Weather station data
#'   }
#'
#' @return Named list with same structure as input, where each data frame is replaced by its
#'   DSSAT-formatted equivalent (character vectors or nested lists ready for file writing)
#'
#' @details
#' **Component routing:**
#' \itemize{
#'   \item **EXPERIMENT**: Uses `.build_filex()` for FileX experiment format
#'   \item **SUMMARY/TIME_SERIES**: Uses `.build_dssat_file()` with respective
#'         templates and formatting functions
#'   \item **SOIL**: Uses `.build_dssat_file()` with soil-specific template
#'         and `DSSAT:::sol_v_fmt()` formatter
#'   \item **WEATHER**: Uses `.build_wth()` for weather file format
#' }
#'
#' @noRd
#'

build_dssat_dataset <-  function(dataset) {
  
  data_fmt <- dataset |>
    purrr::imap(~{
      switch(
        .y,
        "EXPERIMENT" = .build_filex(.x),
        "SUMMARY"    = .build_dssat_file(.x, SUMMARY_template, v_fmt_filea),
        "TIME_SERIES"= .build_dssat_file(.x, TIME_SERIES_template, v_fmt_filet),
        "SOIL"       = .build_dssat_file(.x, SOIL_template, DSSAT:::sol_v_fmt()),
        "WEATHER"    = .build_wth(.x),
        
        # Default case if .y matches no other case.
        stop(paste0("Unrecognized DSSAT section name: '", .y, "'. ",
                    "Expected one of 'EXPERIMENT', 'SUMMARY', 'TIME_SERIES', 'SOIL', or 'WEATHER'."))
      )
    })
  
  return(data_fmt)
}


#' Build DSSAT-formatted data frame with print specifications
#'
#' Prepares a data frame for DSSAT file output by applying template structure, filtering to valid
#' DSSAT columns, and attaching print format attributes.
#'
#' @param data Data frame to format for DSSAT output
#' @param template Template data frame defining column structure and metadata
#' @param v_fmt Named list/vector of print format specifications (e.g., `"%6s"`, `"%5.1f"`) for each DSSAT variable
#'
#' @return Data frame with columns from template, filtered to those present in
#'   `v_fmt`, with matched format specs attached as `v_fmt` attribute
#'
#' @details
#' **Processing steps:**
#' \enumerate{
#'   \item Apply template structure via `format_dssat_table()`
#'   \item Filter to columns present in `v_fmt`
#'   \item Attach filtered formats as `v_fmt` attribute
#' }
#'
#' Used by `build_dssat_dataset()` for SUMMARY, TIME_SERIES, and SOIL components.
#'
#' @noRd
#' 

.build_dssat_file <- function(data, template, v_fmt) {
  
  # Apply template format
  data <- format_dssat_table(data, template)
  # Drop non-DSSAT attributes
  data <- dplyr::select(data, intersect(colnames(data), names(v_fmt)))
  # Set print formats
  attr(data, "v_fmt") <- v_fmt[names(v_fmt) %in% names(data)]
  
  return(data)
}


#' Build DSSAT FileX experiment file structure
#'
#' Formats management data into nested list structure for FileX (.X) file
#' writing, applying templates and print formats to each section.
#'
#' @param data Nested list of data frames, one per FileX section (e.g.,
#'   TREATMENTS, CULTIVARS, FIELDS, PLANTING_DETAILS)
#'
#' @return Nested list with same structure, where each data frame is formatted per FILEX_template with
#'   `v_fmt` attribute attached. Missing required sections are filled with template defaults (warns).
#'   Custom attributes from input are preserved.
#'
#' @details
#' **Processing steps:**
#' \enumerate{
#'   \item Preserve custom attributes from input
#'   \item Apply `format_dssat_table()` to each section
#'   \item Filter columns to those in FILEX_template
#'   \item Attach section-specific print formats via `DSSAT:::filex_v_fmt()`
#'   \item Reorder sections per template, drop empty non-required sections
#'   \item Fill missing required sections (TREATMENTS, CULTIVARS, FIELDS,
#'         PLANTING_DETAILS) with template defaults
#'   \item Reattach custom attributes
#' }
#'
#' **Required sections:** TREATMENTS, CULTIVARS, FIELDS, PLANTING_DETAILS must be present or will trigger
#' warnings and use template defaults. GENERAL and SIMULATION_CONTROLS are also handled specially if missing.
#'
#' @noRd
#'

.build_filex <- function(data) {
  
  # Store custom attributes
  attrs <- attributes(data)
  custom_attrs <- attrs[!names(attrs) %in% c("names", "row.names", "class")]
  
  # Get section names to run formatting through each section
  sec_nms <- sort(names(data))
  
  # Apply template
  data <- mapply(
    format_dssat_table,
    data[sec_nms],
    FILEX_template[sec_nms],
    SIMPLIFY = FALSE
  )
  # Drop non-DSSAT attributes
  data <- mapply(
    function(x, y) dplyr::select(x, intersect(colnames(x), names(y))),
    data[sec_nms],
    FILEX_template[sec_nms]
  )
  # Attach print formats
  data <- mapply(function(x, y) {
    attr(x, "v_fmt") <- DSSAT:::filex_v_fmt(y)
    x
  }, data[sec_nms], sec_nms, SIMPLIFY = FALSE)
  
  # Order subsections
  data <- data[match(names(FILEX_template), names(data))]
  data <- data[lengths(data) > 0]
  # data <- lapply(data, function(df) df[order(df[[1]]), ])
  
  # Check file X writing requirements
  # TODO: replace by complete minimum requirement controls
  required_sections <- c("TREATMENTS", "CULTIVARS", "FIELDS", "PLANTING_DETAILS")
  
  for (i in names(FILEX_template)) {
    if (length(data[[i]]) == 0) {
      if(i %in% required_sections) {
        data[[i]] <- FILEX_template[[i]]
        warning(paste0("Required section missing from input data: ", i))
      } else if (i == "GENERAL") {
        data[[i]] <- FILEX_template[[i]]
        warning("Experiment metadata ('GENERAL') is missing.")
      } else if (i == "SIMULATION_CONTROLS") {
        data[[i]] <- DSSAT::as_DSSAT_tbl(FILEX_template[[i]])
        #warning("SIMULATION_CONTROLS section not provided with input data. Controls set to default values.")
      } else {
        data[[i]] <- NULL
      }
    }
  }
  
  # Reattach custom attributes
  attributes(data) <- c(attributes(data), custom_attrs)
  
  return(data)
}


#' Build DSSAT weather file structures
#'
#' Formats weather data into structures ready for .WTH file writing by applying
#' templates and print formats to each station-year combination.
#'
#' @param data Nested list of weather data frames, typically organized by
#'   station and year (e.g., `data[[station_code]][[year]]`)
#'
#' @return Nested list with same structure, where each weather data frame is
#'   formatted per WTH_template with `v_fmt` attribute attached
#'
#' @details
#' Recursively applies `.build_single_wth()` to each leaf data frame in the nested structure,
#' preserving the hierarchical organization. Each weather table receives template formatting and
#' print specifications for DSSAT weather file output.
#'
#' @noRd
#' 

.build_wth <- function(data) {
  apply_recursive(data, .build_single_wth)
}


#' Build single DSSAT weather file structure
#'
#' Formats one weather station-year dataset for .WTH file writing by applying
#' templates and print formats to both daily data and station metadata.
#'
#' @param wth_data Data frame of daily weather observations with station
#'   metadata stored in `GENERAL` attribute
#'
#' @return Data frame formatted per WEATHER_template with `v_fmt` attribute for
#'   daily variables. Station metadata reformatted and stored back in `GENERAL`
#'   attribute with its own `v_fmt`.
#'
#' @details
#' **Processing steps:**
#' \enumerate{
#'   \item **Daily data:** Apply WEATHER_template, filter to valid columns in
#'         `wth_v_fmt("DAILY")`, attach daily print formats
#'   \item **Station metadata:** Extract from `GENERAL` attribute, apply
#'         WEATHER_header_template, filter to valid columns in 
#'         `wth_v_fmt("GENERAL")`, attach metadata print formats
#'   \item Restore formatted metadata to `GENERAL` attribute
#' }
#'
#' **Structure:** Weather files have dual format requirements—daily observations
#' as data frame rows, station info as header attributes. Both components get
#' independent template application and format specs.
#'
#' @noRd
#'

.build_single_wth <- function(wth_data) {
  
  # --- Weather daily ---
  # Apply template format
  wth_data <- format_dssat_table(wth_data, WEATHER_template)
  # Drop non-DSSAT attributes
  v_fmt_wth_daily <- DSSAT:::wth_v_fmt("DAILY")
  wth_data <- dplyr::select(wth_data, intersect(colnames(wth_data), names(v_fmt_wth_daily)))
  # Set print formats
  attr(wth_data, "v_fmt") <- v_fmt_wth_daily
  
  # --- Station metadata ---
  # Apply template format
  wth_metadata <- format_dssat_table(attr(wth_data, "GENERAL"), WEATHER_header_template)
  # Drop non-DSSAT attributes
  v_fmt_metadata <- DSSAT:::wth_v_fmt("GENERAL", old_format = TRUE)
  wth_metadata <- dplyr::select(wth_metadata, intersect(colnames(wth_metadata), names(v_fmt_metadata)))
  # Set print formats
  attr(wth_metadata, "v_fmt") <- v_fmt_metadata
  # Restore metadata as attribute
  attr(wth_data, "GENERAL") <- wth_metadata
  
  return(wth_data)
}
