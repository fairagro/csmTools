#' Edit or add a cultivar entry in a DSSAT genotype file
#'
#' Modifies an existing cultivar entry or adds a new one in the DSSAT `.CUL`
#' file associated with a given crop model. New cultivars are initialised with
#' parameter values equal to the median of the `MINIMA` and `MAXIMA` rows in
#' the file, which can then be overridden via `ppars` or `gpars`.
#'
#' @param cname Character. Name of cultivar to append or edit.
#' @param model Character. Four-letter DSSAT crop model code (e.g. `"MZCER"`),
#'   used to locate the corresponding genotype files.
#' @param ccode Character or `NULL`. Cultivar identifier code (e.g. `"IB0001"`).
#'   Defaults to `"XX9999"` with a warning if not supplied.
#' @param ecode Character or `NULL`. Ecotype code. Defaults to `"DFAULT"` If `NULL` or not found.
#' @param expno Character or `NULL`. Experiment number field. Defaults to `"."` if not supplied.
#' @param ppars Named list. Phenology (or other) cultivar parameters to set or override
#'   e.g. `list(P1 = 200, P2 = 0.5)`.
#' @param gpars Named list. Growth cultivar parameters to set or override.
#' @param write Logical. If `TRUE`, writes the updated `.CUL` file to the DSSAT Genotype directory.
#'   A backup of the original file is created under `<dssat_dir>/0_DataBackUp/Genotype/` if one
#'   does not already exist. Defaults to `FALSE`.
#' @param ... Additional arguments (currently unused).
#'
#' @return A tibble containing the full updated cultivar tabl
#'
#' @details
#' The function resolves the DSSAT installation directory via `.get_dssat_dir()` and expects
#' the `.CUL` file to exist at `<dssat_dir>/Genotype/<model>048.CUL`.
#'
#' When adding a **new** cultivar (i.e. `cname` is not already in the file), default parameter
#' values are computed as the median of the upper and lower bounds set in the `.CUL` file.
#' Any values supplied via `ppars`/`gpars` overwrite these defaults.
#'
#' When `write = TRUE`, the backup is only created once: subsequent calls will not overwrite
#' an existing backup.
#'
#' @seealso [DSSAT::read_eco()], `read_cul2()`, `write_cul2()`
#'
#' @examples
#' \dontrun{
#' # Modify an existing cultivar's parameters
#' edit_cultivar(
#'   cname = "MyVariety",
#'   model = "MZCER",
#'   ppars = list(P1 = 210, P2 = 0.4),
#'   write = FALSE
#' )
#'
#' # Add a new cultivar entry and write to disk
#' edit_cultivar(
#'   cname  = "NewVar",
#'   model  = "MZCER",
#'   ccode  = "IB9999",
#'   ecode  = "LNDET",
#'   ppars  = list(P1 = 190),
#'   write  = TRUE
#' )
#' }
#' 
#' @importFrom DSSAT read_eco
#' @importFrom tibble tibble as_tibble
#' @importFrom dplyr filter summarise across bind_cols rows_update
#' @importFrom tidyselect where
#' @importFrom utils modifyList
#' 
#' @export
#' 

edit_cultivar <- function(cname, model, ccode = NULL, ecode = NULL, expno = NULL,
                          ppars = list(), gpars = list(), write = FALSE, ...) {
  
  # Resolve file location
  dssat_dir <- .get_dssat_dir()
  gen_dir <- file.path(dssat_dir, "Genotype")
  cul_filename <- paste0(model, "048", ".CUL")
  eco_filename <- paste0(model, "048", ".ECO")
  cul_path <- file.path(gen_dir, cul_filename)
  eco_path <- file.path(gen_dir, eco_filename)
  
  # Read genotype files
  if (file.exists(cul_path)) {
    cul0 <- read_cul2(cul_path)
    eco <- read_eco(eco_path)
  } else {
    stop(paste0("Genotype file '", cul_filename, " ' not found at the DSSAT location: ", cul_path))
  }
  
  # Resolve cultivar name
  if (is.null(cname)) stop("Please specify a cultivar name!")
  cname <- toupper(cname)
  if (nchar(cname) > 20) {
    cname <- substr(cname, 1, 20)
  }
  
  if (!cname %in% cul0$VRNAME) {
    # --- Resolve metadata ---
    # Ecotype
    if (!is.null(ecode) && !ecode %in% eco$`ECO#`) {
      warning(
        paste0("No parameters were found for the ecotype '", ecode, "' for the model '", model, "'. ",
               "Ecotype parameters set to default values."),
        call. = FALSE
      )
      ecode <- "DFAULT"
    } else if (is.null(ecode)) {
      warning("Ecotype parameters set to default values.", call. = FALSE)
      ecode <- "DFAULT"
    }
    # Exp no
    if (is.null(expno)) expno <- "."
    # Cultivar code
    if (is.null(ccode)) {
      existing_codes <- cul0$`VAR#`
      i <- 1
      repeat {
        ccode <- paste0("XX", sprintf("%04d", i))
        if (!ccode %in% existing_codes) break
        i <- i + 1
      }
      warning(
        paste0("Cultivar identification code set to '", ccode, "' by default. ",
               "Control identifier consistency across your simulation inputs!"),
        call. = FALSE
      )
    }
    
    # --- Combine metadata and default parameters ---
    cul_metadata <- tibble(
      `VAR#` = as.character(ccode),
      VRNAME = as.character(cname),
      EXPNO = as.character(expno),
      `ECO#` = as.character(ecode)
    )
    # Set default values as the median
    cul_pars <- cul0 |>
      filter(VRNAME %in% c("MINIMA", "MAXIMA")) |>
      summarise(across(where(is.numeric), ~median(.x)))
    cul_data <- bind_cols(cul_metadata, cul_pars)
    
    # --- Set parameters ---
    input_pars <- c(ppars, gpars)
    if (length(input_pars) > 0) {
      cul_data <- modifyList(as.list(cul_data), input_pars) |> as_tibble()
    }
    cul <- bind_rows(cul0, cul_data)

  } else {

    # --- Set parameters ---
    cul_data <- cul0[cul0$VRNAME == cname, ]
    input_pars <- c(ppars, gpars)
    if (length(input_pars) > 0) {
      cul_data <- modifyList(as.list(cul_data), input_pars) |> as_tibble()
      cul <- rows_update(cul0, cul_data, by = "VAR#")
    }
  }

  # --- Export ---
  if (write) {
    backup_dir <- file.path(dssat_dir, "0_DataBackUp/Genotype")
    cul_backup <- file.path(backup_dir, cul_filename)
    dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)
    if (!file.exists(cul_backup)) {
      message(paste0("Overwriting existing cultivar files. Backing up DSSAT default data at: ", cul_backup))
      file.copy(cul_path, cul_backup)
    }
    write_cul2(cul, cul_path)
  }
  
  if (length(input_pars) == 0 && !cname %in% cul0$VRNAME) {
    warning("No input parameters specified: parameters set as median values for the focal model",
            call. = FALSE)
  }
  
  return(cul)
}


#' Restore a DSSAT cultivar file from backup
#'
#' Restores the original cultivar (`.CUL`) file for a specified DSSAT crop model by overwriting
#' the current file with the backup stored in `0_DataBackUp/Genotype/`.
#'
#' @param model Character. DSSAT crop model code (e.g., `"WHCER"`, `"MZCER"`).
#'
#' @return Invisibly `NULL`. Called for its side effect of restoring the file.
#'
#' @examples
#' \dontrun{
#' restore_cultivar("WHCER")
#' }
#' 
#' @export
#' 

restore_cul_defaults <- function(model) {
  
  dssat_dir <- .get_dssat_dir()
  cul_filename <- paste0(model, "048.CUL")
  cul_path <- file.path(dssat_dir, "Genotype", cul_filename)
  cul_backup <- file.path(dssat_dir, "0_DataBackUp/Genotype", cul_filename)
  
  if (!file.exists(cul_backup)) {
    stop(paste0("No backup found for model '", model, "' at: ", cul_backup))
  }
  
  file.copy(cul_backup, cul_path, overwrite = TRUE)
  message(paste0("Cultivar file restored from backup: ", cul_backup))
}



#' Apply simulation controls and parameters to a FileX
#'
#' This internal helper modifies the SIMULATION_CONTROLS and TREATMENTS sections of a formatted DSSAT FileX
#' list. It overwrites defaults with values from the `args` list and set a default SDATE if needed.
#'
#' @param filex A formatted 'MANAGEMENT' (FileX) list generate with a 'build_*' function.
#' @param args A named list of simulation control parameters.
#'
#' @return The modified `filex` list with custom controls.
#' 
#' @noRd
#' 

set_dssat_controls <- function(filex, args) {
  
  # --- SIMULATION_CONTROLS ---
  sim_controls <- filex$SIMULATION_CONTROLS
  # Find args that match names in SIMULATION_CONTROLS
  control_args <- args[names(args) %in% names(sim_controls)]
  # Overwrite defaults with provided args
  if (length(control_args) > 0) {
    sim_controls <- utils::modifyList(sim_controls, control_args)
  }
  filex$SIMULATION_CONTROLS <- sim_controls
  
  # --- TREATMENTS CONTROLS ---
  treatments <- filex$TREATMENTS
  # Set defaults
  treatments <- treatments |>
    dplyr::mutate(
      R = ifelse(is.na(R), 1, R),
      O = ifelse(is.na(O), 0, O),
      C = ifelse(is.na(C), 0, C)
    )
  # Find args that match TREATMENTS columns
  treat_args <- args[names(args) %in% c("R", "O", "C")]
  # Overwrite defaults with provided args
  if (length(treat_args) > 0) {
    for (name in names(treat_args)) {
      treatments[[name]] <- treat_args[[name]]
    }
  }
  filex$TREATMENTS <- treatments
  
  # --- SET DEFAULT STARTING DATE ---
  # Set default start date as first cultivation event
  if (is.na(filex$SIMULATION_CONTROLS$SDATE)) {
    filex$SIMULATION_CONTROLS$SDATE <- .find_min_management_date(filex)
  }
  
  return(filex)
}


#' Find the earliest date in a FileX list
#'
#' Scans all data frames in a FileX list for DATE columns (i.e., names containing "DAT"),
#' and returns the earliest date found (i.e., first cultivation event for the focal season).
#'
#' @param filex A formatted 'MANAGEMENT' (FileX) list generate with a 'build_*' function.
#'
#' @return A date string in DSSAT format ("%y%j").
#' 
#' @noRd
#' 

.find_min_management_date <- function(filex) {
  
  # Get all management subsection data.frames
  all_dfs <- filex[sapply(filex, is.data.frame)]
  
  # Find all DATE columns ("DAT" in colname)
  all_date_cols <- lapply(all_dfs, function(df) {
    df[grepl("DAT", colnames(df))]
  })
  
  # Make a clean date vector
  all_dates_chr <- unlist(all_date_cols, use.names = FALSE)
  all_dates <- suppressWarnings(as.Date(all_dates_chr, format = "%y%j"))
  all_dates <- all_dates[!is.na(all_dates)]
  
  # Return first cultivation event, i.e., earliest managementdate
  if (length(all_dates) == 0) {
    warning("No management dates found to set SDATE. SDATE remains NA.", call. = FALSE)
    return(NA_character_)
  }
  
  min_date <- min(all_dates)
  return(format(min_date, "%y%j"))
}
