#' Build, parameterize, and write a DSSAT dataset
#'
#' Prepares a DSSAT dataset for a model run:
#' 1. Formats all raw data sections as proper inputs for DSSAT parser.
#' 2. Writes provided simulation control parameters to MANAGEMENT file.
#' 3. Optionally writes all files to their correct locations in DSSAT subdirectories or a user-specified location.
#'
#' @param dataset (list) A named list of raw DSSAT data containing formatted DSSAT data such as:
#'   `"MANAGEMENT"`, `"SOIL"`, `"WEATHER"`, `"SUMMARY"`, and `"TIME_SERIES"` elements.
#' @param write (logical) If `TRUE`, formatted files are written to disk. Default: `FALSE`.
#' @param sol_append (logical) If `TRUE`, soil profile data is appended as a new record at writing, provided a DSSAT soil file
#'   with the same indentifier (institution) already exists at the write location
#' @param write_in_dssat_dir (logical) If `TRUE`, files are written to standard DSSAT subdirectories.
#'   If `FALSE`, all files are written to the `path` directory. Default: `TRUE`.
#' @param path (character) Target directory for writing files. **Only used if `write_in_dssat_dir = FALSE`**. Default: `getwd()`.
#' @param control_config (character) Path to a JSON/YAML configuration file specifying DSSAT simulation control parameters.
#'   The file should contain key-value pairs where keys are parameter names (e.g., `"NITROGEN"`, `"WATER"`, `"RSEED"`, `"SDATE"`)
#'   and values are the desired settings. These values will override DSSAT's default simulation controls.
#'   To view default values, run `data(dssat_default_simcontrols)`.
#'
#' @details
#' This function is a high-level wrapper that calls several internal helpers in sequence:
#' \enumerate{
#'   \item \code{build_dssat_dataset} formats all raw data sections.
#'   \item \code{set_dssat_controls} modifies the \code{MANAGEMENT} section with parameters from \code{control_args} config file.
#'   \item If \code{write = TRUE}, \code{prepare_dssat_paths} resolves all output file paths
#'  \item if \code{write_dssat_dataset = TRUE} DSSAT input files are written to the adequate location in the DSSAT directory;
#'     if \code{write_dssat_dataset = FALSE}, files are written to the user-specified \code{path}.
#' }
#'
#' @return A named list containing a simulation-ready, formatted, and parameterized DSSAT input data.
#'   The list names will be \code{"MANAGEMENT"}, \code{"SOIL"}, \code{"WEATHER"}, \code{"SUMMARY"}, and \code{"TIME_SERIES"}.
#'
#' @examples
#' \dontrun{
#' # Assuming 'my_raw_dataset' is a list of raw data
#'
#' # Just format and parameterize (here, using config file), without writing
#' formatted_data <- build_simulation_files(
#'   my_raw_dataset,
#'   control_config = "path/to/sim_controls.yaml"
#' )
#'
#' # Format, parameterize, and write to a custom local directory
#' build_simulation_files(
#'   my_raw_dataset,
#'   write = TRUE,
#'   write_in_dssat_dir = FALSE,
#'   path = "./model_run_1",
#'   sol_append = FALSE,
#'   control_config = "path/to/sim_controls.json"
#' )
#'
#' # Format, parameterize, and write to standard DSSAT directories
#' build_simulation_files(
#'   my_raw_dataset,
#'   control_config = "path/to/sim_controls.yaml",
#'   write = TRUE,
#'   write_in_dssat_dir = TRUE
#' )
#' }
#' @importFrom tools file_ext
#' @importFrom yaml read_yaml
#' @importFrom jsonlite fromJSON
#' 
#' @export
#'

build_simulation_files <- function(
    dataset,
    write = FALSE, sol_append = FALSE, write_in_dssat_dir = TRUE, path = getwd(),
    control_config = NULL
) {
  
  # Resolve dataset and simulation configuration (if provided)
  dataset <- resolve_input(dataset)
  control_args <- list()
  if (!is.null(control_config)) {
    control_args <- resolve_input(control_config)
  }
  
  # Check integrity of field-env links
  dataset <- check_ref_integrity(dataset, enforce = TRUE)
  
  # Build parser-ready format (DSSAT_tbl class)
  dataset_fmt <- build_dssat_dataset(dataset)
  # TODO: handle cultivar file
  
  # Parameterize the simulation (overwrite default with provided values)
  dataset_fmt$MANAGEMENT <- set_dssat_controls(
    filex = dataset_fmt$MANAGEMENT, 
    args = control_args
  )
  
  # Handle file writing
  if (write) {
    # Set destination path for each file
    dataset_fmt_nms <- prepare_dssat_paths(
      dataset = dataset_fmt,
      write_in_dssat_dir = write_in_dssat_dir,
      path = path
    )
    # Write the files
    write_dssat_dataset(dataset_fmt_nms, sol_append = sol_append)
  }
  
  return(dataset_fmt)
}


#' Check and enforce referential integrity in DSSAT dataset
#'
#' Validates that soil and weather station references in FIELDS table match existing resources in
#' SOIL and WEATHER components. Optionally enforces integrity by filtering to valid references only.
#'
#' @param dataset List of DSSAT dataset components (SOIL, WEATHER, MANAGEMENT)
#' @param enforce Logical; if `TRUE`, removes FIELDS rows with invalid soil or weather references
#'   and issues warnings. If `FALSE`, performs checks without modification (default: `TRUE`)
#'
#' @return Modified dataset with updated MANAGEMENT$FIELDS table. If
#'   `enforce = TRUE`, invalid references are removed; otherwise unchanged.
#'
#' @details
#' **Reference building:**
#' \enumerate{
#'   \item **Soil profiles:** Extract metadata via `get_soil_meta()`, flatten
#'         nested structure, deduplicate (profiles repeated across layers)
#'   \item **Weather stations:** Extract metadata via `get_weather_meta()`,
#'         flatten, aggregate coordinates by station ID (stations span multiple
#'         years)
#' }
#'
#' **Integrity enforcement:**
#' Uses `enforce_integrity()` to validate FIELDS table against reference tables:
#' \itemize{
#'   \item Checks `ID_SOIL` against soil profile IDs
#'   \item Checks `WSTA` against weather station IDs
#'   \item Issues warnings for orphaned references when enforcing
#' }
#'
#' **Rationale:** DSSAT simulations fail if FIELDS references non-existent soil or weather data.
#' This function ensures all linkages resolve before file writing.
#'
#' @noRd
#'

check_ref_integrity <- function(dataset, enforce = TRUE) {
  
  # =================================================================
  # 1- Build reference tables
  # =================================================================
  
  # --- Soil profile(s) ---
  soil_tree <- apply_recursive(dataset$SOIL, get_soil_meta)
  soil_ref  <- do.call(rbind, flatten_tree(soil_tree))
  # Remove duplicates (metadata repeated when multiple layers)
  soil_ref <- unique(soil_ref)
  
  # --- Weather station(s) ---
  weather_tree <- apply_recursive(dataset$WEATHER, get_weather_meta)
  weather_ref  <- do.call(rbind, flatten_tree(weather_tree))
  # Handle duplicates (same station across multiple years)
  if (!is.null(weather_ref) && nrow(weather_ref) > 0) {
    weather_ref <- aggregate(cbind(LAT, LONG) ~ ID, data = weather_ref, FUN = mean, na.rm = TRUE)
  }
  
  # =================================================================
  # 2- Execute reference checks
  # =================================================================
  
  fields <- dataset$MANAGEMENT[["FIELDS"]]
  
  if (enforce) {
    fields <- enforce_integrity(fields, soil_ref, "ID_SOIL", "Soil")
    fields <- enforce_integrity(fields, weather_ref, "WSTA", "Weather Station")
  }
  dataset$MANAGEMENT[["FIELDS"]] <- fields
  
  return(dataset)
}


#' Enforce referential integrity for soil or weather station IDs
#'
#' Validates and corrects ID mismatches in FIELDS table by assigning valid reference IDs based on
#' availability and proximity. Issues warnings when corrections are applied.
#'
#' @param fields Data frame containing field site information (FIELDS table)
#' @param ref Data frame of valid reference IDs with coordinates (ID, LAT, LONG)
#' @param col Character; name of column in `fields` to validate (e.g., "ID_SOIL" or "WSTA")
#' @param label Character; descriptive label for warning messages (e.g., "Soil" or "Weather Station")
#'
#' @return Modified `fields` data frame with corrected IDs in `col`. Invalid references replaced
#'   using assignment strategy.
#'
#' @details
#' **Assignment strategy when mismatches detected:**
#' \describe{
#'   \item{**Single reference available**}{Assign that ID to all mismatched rows
#'         (forces homogeneous assignment)}
#'   \item{**Multiple references available**}{Use `haversine_dist()` to find nearest reference based on
#'         field coordinates (YCRD, XCRD) vs. reference coordinates (LAT, LONG). Assign closest match per row.}
#'   \item{**Missing coordinates**}{Skip assignment and warn for that specific field row}
#' }
#'
#' **Warning behavior:** Issues warnings for each correction type but allows processing to continue.
#' All invalid references are resolved or flagged.
#'
#' @noRd
#'


enforce_integrity <- function(fields, ref, col, label) {
  
  # Identify indices where the ID is not in the reference ID list
  bad_idx <- which(!fields[[col]] %in% ref$ID)
  
  if (length(bad_idx) > 0) {
    
    # Case 1: only one reference option available -> force assignment
    if (nrow(ref) == 1) {
      warning(
        paste0("Mismatch detected in ", label, " IDs: assigning ", ref$ID[1], "to the 'FIELDS' table."),
        call. = FALSE
      )
      fields[[col]][bad_idx] <- ref$ID[1]
      
      # Case 2: multiple options -> use closest location
    } else if (nrow(ref) > 1) {
      warning(
        paste0("Mismatch detected in ", label, " IDs: assigning closest ", label, " based on coordinates."),
        call. = FALSE
      )
      
      for (i in bad_idx) {
        # Calculate distance from this field to ALL references
        dists <- sapply(1:nrow(ref), function(k) {
          haversine_dist(fields$YCRD[i], fields$XCRD[i], ref$LAT[k], ref$LONG[k])
        })
        
        min_dist <- min(dists, na.rm = TRUE)
        
        if (!is.infinite(min_dist)) {
          # Assign the ID of the closest reference
          fields[[col]][i] <- ref$ID[which.min(dists)]
        } else {
          warning(
            paste0("Cannot assigne closest ", label, "for field row ", i, ": missing geocoordinates."),
            call. = FALSE
            )
        }
      }
    }
  }
  return(fields)
}


#' Flatten nested list structure to simple list of data frames
#'
#' Recursively extracts data frames from arbitrarily nested lists produced by
#' `apply_recursive()`, discarding intermediate list structure.
#'
#' @param x Nested list potentially containing data frames at any depth
#'
#' @return Flat list of data frames, or `NULL` if no data frames found
#'
#' @noRd
#'

flatten_tree <- function(x) {
  if (is.data.frame(x)) return(list(x))
  if (is.list(x)) return(unlist(lapply(x, flatten_tree), recursive = FALSE))
  return(NULL)
}


#' Extract soil profile metadata for referential integrity checks
#'
#' Returns first-row values of PEDON ID and coordinates.
#'
#' @param df Soil profile data frame (typically one profile with multiple layers)
#'
#' @return Single-row data frame with columns ID, LAT, LONG, or `NULL` if required columns missing
#'
#' @noRd
#'

get_soil_meta <- function(df) {
  # Check if required columns exist to avoid errors
  if(!all(c("PEDON", "LAT", "LONG") %in% names(df))) return(NULL)
  data.frame(ID = df$PEDON[1], LAT = df$LAT[1], LONG = df$LONG[1], stringsAsFactors = FALSE)
}

#' Extract weather station metadata for referential integrity checks
#'
#' Retrieves station ID and coordinates from GENERAL attribute. Handles both INSI (legacy) and ID column names.
#'
#' @param df Weather data frame with GENERAL attribute containing station metadata
#'
#' @return Single-row data frame with columns ID, LAT, LONG, or `NULL` if GENERAL attribute missing
#'
#' @noRd
#'

get_weather_meta <- function(df) {
  meta <- attr(df, "GENERAL")
  if (is.null(meta)) return(NULL)
  # Standardize column name INSI -> ID
  id_val <- if("INSI" %in% names(meta)) meta$INSI else meta$ID
  data.frame(ID = id_val, LAT = meta$LAT, LONG = meta$LONG, stringsAsFactors = FALSE)
}
