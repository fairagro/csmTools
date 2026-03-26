#' Prepare dataset for writing by prepending paths to file_name attributes
#'
#' This is the main orchestrator for path resolution. It determines the correct
#' target directory for each file in the dataset based on user settings
#' and then modifies the 'file_name' attribute on each object.
#'
#' @param dataset The formatted dataset list from `build_dssat_dataset`.
#' @param write_in_dssat_dir Logical. If TRUE, use standard DSSAT subdirs.
#' @param path Character. The target directory (used if `write_in_dssat_dir` is FALSE).
#'
#' @return The `dataset` list with 'file_name' attributes updated with full paths.
#'
#' @noRd
#' 

prepare_dssat_paths <- function(dataset, write_in_dssat_dir, path) {
  
  # --- Map files to path ---
  if (write_in_dssat_dir) {
    
    # Paths are file specific -> simulation-ready setup
    dssat_dir <- .get_dssat_dir()
    crop_dir <- .get_dssat_crop_dir(dssat_dir, dataset$EXPERIMENT)
    path_map <- list(
      EXPERIMENT = crop_dir,
      SUMMARY = crop_dir,
      TIME_SERIES = crop_dir,
      SOIL = file.path(dssat_dir, "Soil"),
      WEATHER = file.path(dssat_dir, "Weather"),
      CULTIVARS = file.path(dssat_dir, "Genotype")  # NOTE: placeholder
    )
  } else {
    
    # All paths are the same user-provided path
    all_names <- names(dataset)
    path_map <- as.list(rep(path, length(all_names)))
    names(path_map) <- all_names
  }
  
  # --- Apply Paths to Dataset ---
  dataset_out <- purrr::imap(dataset, ~{
    obj <- .x
    obj_name <- .y
    target_dir <- path_map[[obj_name]]
    
    # Safety check in case dataset has sections not in path_map
    if (is.null(target_dir)) return(obj)
    
    # Prepend file-specific paths to corresponding file_name
    # Handle WEATHER list special case (2 weather tables due to gs overlapping 2 calendar years)
    if (obj_name == "WEATHER" && is.list(obj) && !is.data.frame(obj)) {
      lapply(obj, .prepend_path_to_filename, dir = target_dir)
    } else {
      .prepend_path_to_filename(obj, dir = target_dir)
    }
  })
  
  return(dataset_out)
}


#' Find the DSSAT CSM directory
#'
#' Checks the 'DSSAT_CSM' environment variable or sets as default DSSAT install location.
#'
#' @return A character string path to the DSSAT directory.
#' 
#' @noRd
#' 

.get_dssat_dir <- function() {
  
  dssat_dir_env <- Sys.getenv("DSSAT_CSM")
  
  if (dssat_dir_env == "") {
    # TODO: check wheather the executable is found
    dssat_exe <- "C:\\DSSAT48\\DSCSM048.EXE"
    options(DSSAT.CSM = dssat_exe)
    message("DSSAT CSM location not specified in global environment and set to the default location: C:\\DSSAT48\\DSCSM048.EXE")
  } else {
    dssat_exe <- dssat_dir_env
    options(DSSAT.CSM = dssat_exe)
    message(paste0("DSSAT CSM location set to:\n", dssat_exe))
  }
  
  return(dirname(dssat_exe))
}


#' Get the DSSAT crop-specific directory
#'
#' @param dssat_dir The main DSSAT directory path from `.get_dssat_dir()`.
#' @param filex The formatted DSSAT 'MANAGEMENT' (FileX) object.
#'
#' @return A path to the crop-specific directory (e.g., .../Maize).
#' 
#' @noRd
#' 

.get_dssat_crop_dir <- function(dssat_dir, filex) {
  
  crop_id <- filex[["CULTIVARS"]]$CR[1]
  # NOTE: index as precaution; should always be unique
  
  # Suppress warnings if lookup file is not found, etc.
  # crop_lookup <- suppressWarnings(get_dssat_terms(key = "crops"))
  dict <- suppressWarnings(fetch_dictionary("dssat"))
  crop_lookup <- dict$DETAIL$`Crop and Weed Species`
  
  crop_name <- crop_lookup |>
    dplyr::filter(CDE == crop_id) |>
    dplyr::pull(DESCRIPTION)
  
  if (length(crop_name) == 0) {
    warning("Could not find crop name for ID: ", crop_id, ". Using main DSSAT directory.", call. = FALSE)
    return(dssat_dir)
  }
  
  return(file.path(dssat_dir, crop_name))
}


#' Prepend a directory path to an object's file_name attribute
#'
#' @param obj The R object (e.g., data.frame) with a 'file_name' attribute.
#' @param dir The directory path to prepend.
#'
#' @return The modified object.
#' 
#' @noRd
#' 

.prepend_path_to_filename <- function(obj, dir) {
  # Check that the object and its file_name attribute exist
  if (!is.null(obj) && !is.null(attr(obj, "file_name"))) {
    # Ensure directory exists
    if (!dir.exists(dir)) dir.create(dir, recursive = TRUE)
    
    attr(obj, "file_name") <- file.path(dir, attr(obj, "file_name"))
  }
  return(obj)
}