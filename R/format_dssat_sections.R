#' Format a DSSAT dataset into output-ready section objects
#'
#' Processes a parsed DSSAT dataset list into formatted, output-ready objects for each
#' section (management, observed data, soil, weather). Each section is handled by a
#' dedicated internal formatter, and all outputs are annotated with a processing signature.
#'
#' @param dataset A named list representing a parsed DSSAT experiment with ungrouped data
#'   components (e.g., 'FIELDS', 'SOIL_LAYERS', 'WEATHER_DAILY')
#' @param comments A comments object passed through to each section formatter.
#'
#' @return A compact named list (NULLs removed) with any of the following elements:
#'   \itemize{
#'     \item `EXPERIMENT`: formatted experiment (i.e., metadata + management) file object
#'     \item `SUMMARY`: formatted summary observed data object, or `NULL` if absent
#'     \item `TIME_SERIES`: formatted time-series observed data object, or `NULL` if absent
#'     \item `SOIL`: formatted soil file object
#'     \item `WEATHER`: a named list of formatted weather file objects, one per year
#'   }
#'
#' @noRd
#' 

format_dssat_sections <- function(dataset, comments) {
  
  # Ensure comments is a list, even if empty
  if (is.null(comments)) comments <- list()
  
  # --- Management tables ---
  mngt_list <- split_dssat_dataset(
    dataset,
    sec = "EXPERIMENT",
    merge = FALSE
  )[[1]]
  mngt_fmt <- .format_dssat_exp(mngt_list, comments)
  
  # --- Observed data ---
  sm_df <- dataset[["SUMMARY"]]
  sm_fmt <- .format_dssat_sm(sm_df, comments)
  if (!is.null(sm_fmt)) {
    attr(sm_fmt, "experiment") <- attr(mngt_fmt, "experiment")
    attr(sm_fmt, "file_name") <- sub(".$", "A", attr(mngt_fmt, "file_name"))
  }
  
  ts_df <- dataset[["TIME_SERIES"]]
  ts_fmt <- .format_dssat_ts(ts_df, comments)
  if (!is.null(ts_fmt)) {
    attr(ts_fmt, "experiment") <- attr(mngt_fmt, "experiment")
    attr(ts_fmt, "file_name") <- sub(".$", "T", attr(mngt_fmt, "file_name"))
  }
  
  # --- Soil tables ---
  soil_list <- dataset[names(dataset) %in% c("SOIL_META", "SOIL_GENERAL", "SOIL_LAYERS")]
  soil_fmt <- .format_dssat_soil(soil_list, comments)
  
  # --- Weather tables ---
  wth_list_all <- dataset[grepl("WEATHER", names(dataset))]
  if (length(wth_list_all) > 0) {
    suffixes <- sub(".*_(\\d+)$", "\\1", names(wth_list_all))
    wth_list_by_year <- split(wth_list_all, f = suffixes)
    wth_fmt <- lapply(wth_list_by_year, .format_dssat_wth, comments = comments)
  } else {
    wth_fmt <- NULL
  }
  
  # --- Assemble output ---
  dataset_out <- list(
    EXPERIMENT = mngt_fmt,
    SUMMARY = sm_fmt,
    TIME_SERIES = ts_fmt,
    SOIL = soil_fmt,
    WEATHER = wth_fmt
  )
  dataset_out <- purrr::compact(dataset_out) 
  
  # --- Add package signature ---
  signature <- paste0("Dataset processed on ", Sys.Date(), " with csmtools.")
  
  prepend_comment <- function(x, new_comment) {
    
    # Safely extract existing comments as a character vector
    existing <- attr(x, "comments")
    if (is.null(existing)) {
      existing <- character(0)
    } else {
      existing <- as.character(unlist(existing))
    }
    attr(x, "comments") <- c(new_comment, existing)
    return(x)
  }
  
  # Apply to top-level elements
  dataset_out <- lapply(dataset_out, function(elem) {

    if (is.list(elem) && !is.data.frame(elem) && any(sapply(elem, is.data.frame))) {
      return(lapply(elem, prepend_comment, new_comment = signature))
    }
    return(prepend_comment(elem, new_comment = signature))
  })
  
  return(dataset_out)
}


#' Format a DSSAT experiment section list for output
#'
#' Prepares a parsed DSSAT experiment section list for file output by extracting experiment
#' metadata and attaching file-building attributes.
#'
#' @param exp A named list of experiment sub-section data frames, as returned by
#'   the DSSAT management extraction helpers.
#' @param comments A named list of comments/notes keyed by section name, as output by
#'   `extract_dssat_comments()`
#'    Only entries matching known experiment section names are retained (currently unused in output).
#'
#' @return The input `exp` list with two attributes appended:
#'   \itemize{
#'     \item `"experiment"`: the experiment identifier, taken from `EXP_NAME` if present,
#'       otherwise `EXP_ID`
#'     \item `"file_name"`: the original file name, taken from `GENERAL$file_name`
#'   }
#'
#' @details
#' Comment integration into the formatted output is not yet implemented (see commented-out
#' block). The `comments` argument is filtered to known experiment sections but not
#' currently attached to the return value.
#'
#' @noRd
#'

.format_dssat_exp <- function(exp, comments) {
  
  exp_metadata <- exp[["GENERAL"]]
  mngt_out <- exp
  
  mngt_sections <- c(
    "GENERAL",
    "TREATMENTS",
    "CULTIVARS",
    "FIELDS",
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
  mngt_notes <- comments[names(comments) %in% mngt_sections]
  
  # Generate markdown format with comments
  # if (length(mngt_notes) > 0) {
  #   ## ADD LOGIC
  # }
  
  attr(mngt_out, "experiment") <- ifelse(
    "EXP_NAME" %in% names(exp_metadata),
    unique(exp_metadata$EXP_NAME),
    unique(exp_metadata$EXP_ID)
  )
  attr(mngt_out, "comments") <- unique(mngt_notes)
  attr(mngt_out, "file_name") <- unique(exp_metadata$file_name)
  
  return(mngt_out)
}


#' Format a DSSAT summary observed data table for output
#'
#' Validates and prepares a parsed DSSAT observed data summary (`SUMMARY`) data frame
#' for file output by filtering relevant comments and attaching them as an attribute.
#' Returns `NULL` if the input is absent or degenerate.
#'
#' @param sm A data frame representing the `SUMMARY` observed data section, or `NULL`.
#' @param comments A named list of comments/notes keyed by section name, as output by
#'   `extract_dssat_comments()`
#'
#' @return The input `sm` data frame with a `"comments"` attribute attached, or `NULL`
#'   if `sm` is `NULL` or contains only a single `TRNO` column (a known artefact of
#'   multi-year data splitting).
#'
#' @noRd
#'

.format_dssat_sm <- function(sm, comments) {
  
  if (is.null(sm) || (identical(names(sm), "TRNO") && ncol(sm) == 1)) {
    return(NULL)
  }
  
  sm_notes <- comments[["SUMMARY"]]
  if (!is.null(sm_notes)) {
    sm_notes <- as.character(sm_notes$Content)
  } else {
    sm_notes <- character(0)
  }
  
  sm_out <- sm
  attr(sm_out, "comments") <- unique(sm_notes)
  return(sm_out)
}


#' Format a DSSAT time-series observed data table for output
#'
#' Validates and prepares a parsed DSSAT observed data time-series (`TIME_SERIES`) data frame
#' for file output by filtering relevant comments and attaching them as an attribute. Returns
#' `NULL` if the input is absent or degenerate.
#'
#' @param ts A data frame representing the `TIME_SERIES` observed data section, or `NULL`.
#' @param comments A named list of comments/notes keyed by section name, as output by
#'   `extract_dssat_comments()`
#'
#' @return The input `ts` data frame with a `"comments"` attribute attached, or `NULL`
#'   if `ts` is `NULL` or contains only the identifier columns `TRNO` and/or `DATE`
#'   (a known artefact of multi-year data splitting).
#'
#' @noRd
#' 

.format_dssat_ts <- function(ts, comments) {
  
  if (is.null(ts) || (all(names(ts) %in% c("TRNO", "DATE")) && ncol(ts) <= 2)) {
    return(NULL)
  }
  
  ts_notes <- comments[["TIME_SERIES"]]
  if (!is.null(ts_notes)) {
    ts_notes <- as.character(ts_notes$Content)
  } else {
    ts_notes <- character(0)
  }
  
  ts_out <- ts
  attr(ts_out, "comments") <- unique(ts_notes)
  return(ts_out)
}


#' Format a DSSAT soil section list for output
#'
#' Merges DSSAT soil sub-section data frames, extracts and flattens relevant comments,
#' and attaches file-building attributes to the combined output.
#'
#' @param soil A named list of soil sub-section data frames, expected to contain
#'   any of `"SOIL_META"`, `"SOIL_GENERAL"`, and `"SOIL_LAYERS"`. The `"SOIL_META"`
#'   element must include `PEDON`, `SITE`, and `file_name` columns.
#' @param comments A named list of comments/notes keyed by section name, as output by
#'   `extract_dssat_comments()`
#'
#' @return A single merged data frame (left-joined across all soil sub-sections) with
#'   the following attributes according to the format expected by the DSSAT file parser
#'   (R package `DSSAT`)
#'   \itemize{
#'     \item `"comments"`: a character vector of flattened comment strings across all
#'       soil sub-sections, or an empty list if none are present
#'     \item `"title"`: constructed from `PEDON` and `SITE` metadata
#'     \item `"file_name"`: the original file name, taken from `SOIL_META$file_name`
#'   }
#'
#' @noRd
#' 

.format_dssat_soil <- function(soil, comments) {
  
  # Return NULL if no soil data exists for this experiment
  if (length(soil) == 0) return(NULL)
  
  soil_metadata <- soil[["SOIL_META"]]
  
  # Extract comments
  soil_notes <- comments[names(comments) %in% c("SOIL_META", "SOIL_GENERAL", "SOIL_LAYERS")]
  if (length(soil_notes) > 0) {
    soil_notes <- unlist(lapply(soil_notes, function(df) df$Content), use.names = FALSE)
  } else {
    soil_notes <- character(0)
  }
  
  # Merge data in output
  soil_out <- suppressMessages(
    purrr::reduce(soil, dplyr::left_join)
  )
  
  # Append attributes for file building
  attr(soil_out, "comments") <- unique(soil_notes)
  
  # Guard against missing metadata for the title
  pedon <- if(!is.null(soil_metadata$PEDON)) na.omit(soil_metadata$PEDON) else "Unknown"
  site <- if(!is.null(soil_metadata$SITE)) na.omit(soil_metadata$SITE) else "Unknown"
  
  attr(soil_out, "title") <- ifelse(
    any(grepl("ISRIC", soil_notes)),
    "ISRIC Soil Grids-derived synthetic profile",
    paste(pedon, site, collapse = "; ")
  )
  attr(soil_out, "file_name") <- unique(soil_metadata$file_name)
  
  return(soil_out)
}


#' Format a DSSAT weather section list for output
#'
#' Extracts daily weather data and station metadata from a grouped DSSAT weather list,
#' flattens relevant comments, and attaches file-building attributes to the daily data frame.
#'
#' @param wth A named list containing weather sub-components for a single year group.
#'   Expected to include elements named `"WEATHER_DAILY"` and `"WEATHER_METADATA"`
#'   The metadata element must include `WST_NAME` and `file_name` columns.
#' @param comments A named list of comments/notes keyed by section name, as output by
#'   `extract_dssat_comments()`
#'
#' @return The daily weather data frame with the following attributes according to the
#'   format expected by the DSSAT file parser (R package `DSSAT`):
#'   \itemize{
#'     \item `"GENERAL"`: the station metadata data frame
#'     \item `"location"`: uppercased station name from `WST_NAME`
#'     \item `"comments"`: a character vector of flattened comment strings from all
#'       matching weather comment entries, or an empty list if none are present
#'     \item `"file_name"`: the original file name, taken from `WEATHER_METADATA`
#'   }
#'
#' @noRd
#'

.format_dssat_wth <- function(wth, comments) {
  
  # Check if required components exist
  has_daily <- any(grepl("DAILY", names(wth)))
  has_meta <- any(grepl("METADATA", names(wth)))
  
  if (!has_daily || !has_meta) return(NULL)
  
  wth_data <- wth[grepl("DAILY", names(wth))][[1]]
  wth_metadata <- wth[grepl("METADATA", names(wth))][[1]]
  
  # Extract comments
  wth_notes <- comments[grepl("WEATHER", names(comments))]
  if (length(wth_notes) > 0) {
    wth_notes <- unlist(lapply(wth_notes, function(df) df$Content), use.names = FALSE)
  } else {
    wth_notes <- character(0)
  }
  
  # Append file building attributes
  attr(wth_data, "GENERAL") <- wth_metadata
  attr(wth_data, "location") <- unique(toupper(wth_metadata$WST_NAME))
  attr(wth_data, "comments") <- unique(wth_notes)
  attr(wth_data, "file_name") <- unique(wth_metadata$file_name)
  
  return(wth_data)
}
