#' Write a formatted DSSAT dataset to disk
#'
#' Helper function for writing a fully formatted and path-set dataset to the disk.
#' It maps DSSAT file sections to the corresponding `DSSAT::write_*` functions and run them.
#'
#' @param dataset A named list, formatted by `build_dssat_dataset` and `.prepare_dssat_paths`.
#'   Each element must have a valid `file_name` attribute.
#' @param sol_append Logical; whether to append to the soil file if file already exists in the destination
#'   directory.
#'
#' @noRd
#' 

write_dssat_dataset <- function(dataset, sol_append = FALSE) {

  # Map dataset names to the corresponding DSSAT write function
  write_funs <- list(
    MANAGEMENT = DSSAT::write_filex,
    SUMMARY = DSSAT::write_filea,
    TIME_SERIES = DSSAT::write_filet,
    SOIL = write_sol2,
    WEATHER = write_wth2
  )
  
  # Write files iteratively
  # .x is the data (e.g., dataset$MANAGEMENT)
  # .y is the name (e.g., "MANAGEMENT")
  purrr::iwalk(dataset, ~{

    # --- Set focal write function ---
    write_fun <- write_funs[[.y]]
    
    # Skip if there's no data or no corresponding write function
    if (is.null(.x) || is.null(write_fun)) {
      return(invisible(NULL))
    }
    
    # --- Handle different special write cases ---
    # Set whether soil profile should be appended if files already exists in destination
    if (.y == "SOIL") {
      write_fun(.x, file_name = attr(.x, "file_name"), append = sol_append)
      # Special case for WEATHER (multiple calendar years)
    } else if (.y == "WEATHER" && is.list(.x) && !is.data.frame(.x)) {
      lapply(.x, function(df) {
        write_fun(df, file_name = attr(df, "file_name"))
      })
      # Standard case (MANAGEMENT, SUMMARY, TIME_SERIES, or single WEATHER)
    } else {
      # No 'file_name' arg needed; write_fun finds the attribute
      write_fun(.x, file_name = attr(.x, "file_name"))
    }
  })
  
  # Issue warning if required files are missing
  required_files <- c("MANAGEMENT", "WEATHER", "SOIL")
  missing <- setdiff(required_files, names(dataset))
  
  if (length(missing) > 0) {
    warning("Required file sections for simulation are missing from dataset and not written: ",
            paste(missing, collapse = ", "), call. = FALSE)
  }
  
  return(invisible(NULL))
}
