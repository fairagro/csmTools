#' Format a DSSAT dataset into output-ready section objects
#'
#' Processes a parsed DSSAT dataset list into formatted, output-ready objects for each
#' section (management, observed data, soil, weather). Each section is handled by a
#' dedicated internal formatter, and all outputs are annotated with a processing signature.
#'
#' @param dataset A named list representing a parsed DSSAT experiment. Expected to contain
#'   some or all of: `"MANAGEMENT"`, `"SUMMARY"`, `"TIME_SERIES"`, `"SOIL_META"`,
#'   `"SOIL_GENERAL"`, `"SOIL_LAYERS"`, and one or more `"WEATHER_*"` tables suffixed
#'   with a 4-digit year.
#' @param comments A comments object passed through to each section formatter.
#'
#' @return A compact named list (NULLs removed) with any of the following elements:
#'   \itemize{
#'     \item `MANAGEMENT`: formatted management file object
#'     \item `SUMMARY`: formatted summary observed data object, or `NULL` if absent
#'     \item `TIME_SERIES`: formatted time-series observed data object, or `NULL` if absent
#'     \item `SOIL`: formatted soil file object
#'     \item `WEATHER`: a named list of formatted weather file objects, one per year
#'   }
#'   The `"experiment"` and `"file_name"` attributes of `SUMMARY` and `TIME_SERIES` are
#'   aligned with those of `MANAGEMENT` (file extension replaced with `"A"` and `"T"`
#'   respectively). All top-level and weather sub-list elements receive a `"comments"`
#'   attribute prepended with a package processing signature.
#'
#' @noRd
#' 

format_dssat_sections <- function(dataset, comments) {
  
  # --- Management tables ---
  mngt_list <- split_dssat_dataset(
    dataset,
    sec = "EXPERIMENT",
    merge = FALSE
  )[[1]]
  mngt_fmt <- .format_dssat_mngt(mngt_list, comments)
  
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
  # Weather components are first grouped by year
  wth_list_all <- dataset[grepl("WEATHER", names(dataset))]
  suffixes <- sub(".*_(\\d+)$", "\\1", names(wth_list_all))
  wth_list_by_year <- split(wth_list_all, f = suffixes)

  wth_fmt <- lapply(wth_list_by_year, .format_dssat_wth, comments =  comments)
  

  # --- Assemble output ---
  dataset_out <- list(
    MANAGEMENT = mngt_fmt,
    SUMMARY = sm_fmt,
    TIME_SERIES = ts_fmt,
    SOIL = soil_fmt,
    WEATHER = wth_fmt
  )
  dataset_out <- purrr::compact(dataset_out)  # Remove NULLs
  
  # --- Add package signature ---
  signature <- paste0("Dataset processed on ", Sys.Date(), " with csmtools.")
  prepend_comment <- function(x, new_comment) {
    comments <- as.character(unlist(attr(x, "comments")))
    attr(x, "comments") <- c(new_comment, comments)
    return(x)
  }
  dataset_out <- lapply(dataset_out, prepend_comment, new_comment = signature)
  # Special case: nested weather dfs
  dataset_out$WEATHER <- lapply(dataset_out$WEATHER, prepend_comment, new_comment = signature)
  
  return(dataset_out)
}


#' Format a DSSAT management section list for output
#'
#' Prepares a parsed DSSAT management section list for file output by extracting experiment
#' metadata and attaching file-building attributes.
#'
#' @param mngt A named list of management sub-section data frames, as returned by
#'   the DSSAT management extraction helpers.
#' @param comments A named list of comments/notes keyed by section name, as output by
#'   `extract_dssat_comments()`
#'    Only entries matching known management section names are retained (currently unused in output).
#'
#' @return The input `mngt` list with two attributes appended:
#'   \itemize{
#'     \item `"experiment"`: the experiment identifier, taken from `EXP_NAME` if present,
#'       otherwise `EXP_ID`
#'     \item `"file_name"`: the original file name, taken from `GENERAL$file_name`
#'   }
#'
#' @details
#' Comment integration into the formatted output is not yet implemented (see commented-out
#' block). The `comments` argument is filtered to known management sections but not
#' currently attached to the return value.
#'
#' @noRd
#'

.format_dssat_mngt <- function(mngt, comments) {
  
  exp_metadata <- mngt[["GENERAL"]]
  mngt_out <- mngt
  
  # Extract comments
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
  
  # Append file building attributes
  attr(mngt_out, "experiment") <- ifelse(
    "EXP_NAME" %in% exp_metadata,
    unique(exp_metadata$EXP_NAME),
    unique(exp_metadata$EXP_ID)
  )
  #attr(mngt_out, "comments") <- mngt_nodes_md
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
  
  # Extract comments
  sm_notes <- comments[names(comments) %in% "SUMMARY"]
  
  # Return NULL if only treatment column is present
  # (possible split artefact in multi-year data)
  if (is.null(sm) || (identical(names(sm), "TRNO") && ncol(sm) == 1)) {
    sm_out <- NULL
  } else {
    sm_out <- sm
    # Append attributes for file building
    attr(sm_out, "comments") <- sm_notes
  }
  
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
  
  # Extract comments
  ts_notes <- comments[names(comments) %in% "TIME_SERIES"]
  
  # Return NULL if only treatment and date column is present
  # (possible split artefact in multi-year data)
  if (is.null(ts) || (all(names(ts) %in% c("TRNO", "DATE")) && ncol(ts) <= 2)) {
    ts_out <- NULL
  } else {
    ts_out <- ts
    # Append attributes for file building
    attr(ts_out, "comments") <- ts_notes
  }
  
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
  
  soil_metadata <- soil[["SOIL_META"]]
  
  # Extract comments
  soil_notes <- comments[names(comments) %in% c("SOIL_META", "SOIL_GENERAL", "SOIL_LAYERS")]
  if (length(soil_notes) > 0) {
    soil_notes <- unlist(lapply(soil_notes, function(df) df$Content), use.names = FALSE)
  }
  
  # Merge data in output
  soil_out <- suppressMessages(
    purrr::reduce(soil, dplyr::left_join)
  )
  
  # Append attributes for file building
  attr(soil_out, "comments") <- soil_notes
  attr(soil_out, "title") <- ifelse(
    any(grepl("ISRIC", soil_notes)),
    "ISRIC Soil Grids-derived synthetic profile",
    paste(na.omit(soil_metadata$PEDON), na.omit(soil_metadata$SITE), collapse = "; ")
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
  
  wth_data <- wth[grepl("DAILY", names(wth))][[1]]
  wth_metadata <- wth[grepl("METADATA", names(wth))][[1]]
  
  # Extract comments
  wth_notes <- comments[grepl("WEATHER", names(comments))]
  if (length(wth_notes) > 0) {
    wth_notes <- unlist(lapply(wth_notes, function(df) df$Content), use.names = FALSE)
  }
  
  # Merge data in output
  # wth_out <- suppressMessages(reduce(wth, left_join))
  
  # Append file building attributes
  attr(wth_data, "GENERAL") <- wth_metadata
  attr(wth_data, "location") <- unique(toupper(wth_metadata$WST_NAME))
  attr(wth_data, "comments") <- wth_notes
  attr(wth_data, "file_name") <- unique(wth_metadata$file_name)
  
  return(wth_data)
}
