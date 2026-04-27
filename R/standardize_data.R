#' Standardise a dataset to a common data model
#'
#' Dispatches to a model-specific standardisation routine and returns the dataset in a normalised form
#' ready for further processing or writing.
#'
#' @param dataset A dataset object as returned by \code{resolve_dataset()} or an equivalent ingestion step.
#' @param data_model A length-one character string selecting the target data model.
#'   One of \code{"icasa"} (no transformation applied) or \code{"dssat"} (delegates to \code{.standardize_dssat_data()}).
#'
#' @return The standardised dataset. For \code{"icasa"} the input is returned unchanged;
#'   for \code{"dssat"} the result of \code{.standardize_dssat_data(dataset)} is returned.
#'
#' @noRd
#' 

standardize_data <- function(dataset, data_model = c("icasa", "dssat")) {
  
  #MATCH ARGS
  
  switch(
    data_model,
    "icasa" = {
      out <- dataset
    },
    "dssat" = {
      out <- .standardize_dssat_data(dataset)
    }
  )
  
  return(out)
}


#' Standardise a dataset to the DSSAT data model
#'
#' Transforms a resolved dataset into the structured format expected by the DSSAT file-writing utilities.
#' The pipeline applies naming conventions, splits the data by experiment, aligns weather records to each cultivation
#' season, extracts section comments, and formats every section for export.
#'
#' @param dataset A named list as returned by \code{resolve_dataset()}, prior to any model-specific transformation.
#'
#' @return If the dataset contains a single experiment, a named list of formatted DSSAT sections.
#'   If it contains multiple experiments, a list of  such named lists, one per experiment.
#'
#' @details
#' The function applies the following steps in order:
#' \enumerate{
#'   \item \code{apply_naming_rules()} maps input field names to DSSAT standard variable codes.
#'   \item \code{split_dataset()} partitions the data by the \code{"experiment"} key following DSSAT experiment conventions.
#'   \item \code{extract_season_weather()} subsets weather data to the cultivation season relevant to each experiment.
#'   \item \code{extract_dssat_notes()} collects any free-text comments embedded in each section.
#'   \item \code{format_dssat_sections()} applies the object structure required by the DSSAT library writing functions.
#' }
#'
#' @seealso \code{standardize_data()}, \code{format_dssat_sections()}
#'
#' @noRd
#' 

# TODO TEST: whole routine should be robust to missing SOIL/WEATHER DATA
.standardize_dssat_data <- function(dataset) {

  # Apply DSSAT standard codes
  dataset_nms <- apply_naming_rules(dataset)
  # TODO: test with single exp + add file names
  
  # Split by experiment (as per DSSAT definition)
  dataset_split <- split_dataset(dataset_nms, key = "experiment", data_model = "dssat")
  # Split weather data to match the cultivation season for each experiment
  dataset_split <- lapply(dataset_split, extract_season_weather)   #TOFIX: DSSAT DATE FORMATTING!
  
  # Extract comments
  comments_split <- lapply(dataset_split, function(ls) {
    comments <- purrr::map(ls, extract_dssat_notes)
    purrr::keep(comments, ~ nrow(.x) > 0)
  })
  
  # Apply required object structure for file export with DSSAT library functions
  dataset_split_fmt <- purrr::map2(
    dataset_split,
    comments_split,
    format_dssat_sections
  )
  
  # Return only first list object if single experiment
  if (is.list(dataset_split_fmt) && length(dataset_split_fmt) == 1) {
    out <- dataset_split_fmt[[1]]  # Single experiment
  } else {
    out <- dataset_split_fmt  # Multiple experiment
  }

  return(out)
}
