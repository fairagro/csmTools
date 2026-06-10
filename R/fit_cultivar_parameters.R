#' Extract a Section from DSSAT management tables (file X)
#'
#' Retrieves the first table from a DSSAT management table list based on section name.
#'
#' @param xtables A named list DSSAT management tables as data frames.
#' @param sec_name Character. The section name or pattern to search for in the names of \code{xtables}.
#'
#' @details
#' The function searches the names of \code{xtables} for entries matching \code{sec_name} using \code{grepl}. It returns the first matching table, or \code{NULL} if no match is found.
#'
#' This is useful for extracting a specific section or table from a list of tables by partial or full name.
#'
#' @return The first table (e.g., data frame) whose name matches \code{sec_name}, or \code{NULL} if no match is found.
#'
#' @examples
#' xtables <- list(SUMMARY = data.frame(a = 1:3), DETAILS = data.frame(b = 4:6))
#' get_xfile_sec(xtables, "SUM")
#' # Returns the SUMMARY data frame
#'

get_xfile_sec <- function(xtables, sec_name) {
  idx <- grep(sec_name, names(xtables))[1]
  if (!is.na(idx)) xtables[[idx]] else NULL
}


#' Retrieve Crop Genotype Data from DSSAT Files
#'
#' Loads crop genotype data (CUL file) for a specified crop and model from DSSAT installation directories.
#'
#' @param crop Character. The crop name or code (e.g., "maize", "wheat").
#' @param model Character. The DSSAT model code (e.g., "APS", "CER", "GRO", "ARO", "CRP", "IXM"). Default includes all.
#'
#' @details
#' The function constructs the file path for the crop genotype file (\code{.CUL}) based on the DSSAT installation path and version, as specified by the \code{DSSAT.CSM} option. It then loads the genotype data using \code{read_cul}. The crop code is constructed from the first two letters of the crop name (uppercased), the model code, and the DSSAT version.
#'
#' The function currently only loads the CUL file, but can be extended to load ECO files as well.
#'
#' @return A data frame containing the crop genotype data, with the crop name attached as an attribute.
#'
#' @examples
#' \dontrun{
#' cdata <- get_cdata("maize", model = "CER")
#' }
#'
#' @export
#' 

get_cdata <- function(crop, model = c("APS","CER","GRO","ARO","CRP","IXM")) {
  model <- match.arg(model)

  dssat_csm  <- gsub("\\", "/", getOption("DSSAT.CSM"), fixed = TRUE)
  dssat_path <- sub("DSCSM.*", "", dssat_csm)
  dssat_vers <- sub(".*DSCSM([^.]+).*", "\\1", dssat_csm)

  crop_code <- paste0(substr(toupper(crop), 1, 2), model, dssat_vers)
  ctable <- read_cul2(file_name = file.path(dssat_path, "Genotype", paste0(crop_code, ".CUL")))

  attr(ctable, "crop") <- crop
  ctable
}


#' Set Column Classes of a Data Frame According to a Reference
#'
#' Coerces the columns of a data frame to specified classes, using a named vector or list of target classes.
#'
#' @param df A data frame whose columns are to be coerced.
#' @param classes A named vector or list specifying the target class for each column (e.g., \code{c(date = "Date", value = "numeric")}).
#'
#' @details
#' The function matches columns in \code{df} to names in \code{classes}, then coerces each column to the specified class using standard R coercion functions (\code{as.Date}, \code{as.numeric}, etc.). If a class is not recognized, the column is returned unchanged.
#'
#' This is useful for ensuring that data frames have the correct column types after import or transformation.
#'
#' @return A data frame with columns coerced to the specified classes.
#'
#' @examples
#' df <- data.frame(date = c("2022-01-01", "2022-01-02"), value = c("1", "2"))
#' classes <- c(date = "Date", value = "numeric")
#' set_class(df, classes)
#' # Returns a data frame with date as Date and value as numeric
#'

set_class <- function(df, classes) {
  
  cols <- intersect(names(df), names(classes))
  df[cols] <- mapply(function(value, cls) {
    switch(cls,
           Date = as.Date(value),
           POSIXct = as.POSIXct(value),
           POSIXt = as.POSIXct(value),
           numeric = as.numeric(value),
           integer = as.integer(value),
           character = as.character(value),
           factor = as.factor(value),
           value)
  }, df[cols], classes[cols], SIMPLIFY = FALSE)

  return(as.data.frame(df))
}


# Static lookup: short section keys → canonical DSSAT section header names.

.xfile_sections <- data.frame(
  sec  = c("general","treatments","cultivars","fields","soil_analysis","initial_conditions",
           "planting","irrigation","fertilizer","organic_amendment","tillage","chemicals",
           "environment_modifications","harvest","simulation_controls"),
  name = c("GENERAL","TREATMENTS                        -------------FACTOR LEVELS------------",
           "CULTIVARS","FIELDS","SOIL ANALYSIS","INITIAL CONDITIONS","PLANTING DETAILS",
           "IRRIGATION AND WATER MANAGEMENT","FERTILIZERS (INORGANIC)","RESIDUES AND ORGANIC FERTILIZER",
           "TILLAGE AND ROTATIONS","CHEMICALS","ENVIRONMENT MODIFICATIONS","HARVEST DETAILS","SIMULATION CONTROLS"),
  stringsAsFactors = FALSE
)


#' Add a New Treatment Row to the treatment matrix of a DSSAT managemeent table list
#'
#' Adds a new treatment to the treatment table within a DSSAT management tale list,
#' using the last row as a template and updating specified columns.
#'
#' @param xtables A named list of tables (e.g., data frames), including a treatment table (name starting with "TREATMENT").
#' @param args A named list of column values to update in the new treatment row.
#'
#' @details
#' The function locates the treatment table in \code{xtables} (the first table whose name starts with "TREATMENT"), copies its last row as a template, and updates the primary key (first column) to a new unique value. Columns specified in \code{args} are updated in the new row. The new row is appended to the treatment table, which is then converted to a DSSAT table using \code{as_DSSAT_tbl}.
#'
#' If a column specified in \code{args} does not exist in the treatment table, a warning is issued.
#'
#' @return The input list with the updated treatment table.
#'
#' @examples
#' \dontrun{
#' xtables <- add_treatment(xtables, args = list(crop = "maize", fertilizer = "NPK"))
#' }
#'
#' @importFrom dplyr bind_rows
#' @export

add_treatment <- function(xtables, args = list()) {

  idx <- which(startsWith(names(xtables), "TREATMENT"))
  trt <- xtables[[idx]]

  new_row      <- trt[nrow(trt), ]
  new_row[[1]] <- max(trt[[1]]) + 1

  for (col in names(args)) {
    if (col %in% colnames(trt)) {
      new_row[[col]] <- args[[col]]
    } else {
      warning(paste("Column", col, "not found in the dataframe"))
    }
  }

  xtables[[idx]] <- as_DSSAT_tbl(bind_rows(trt, new_row))
  xtables
}


#' Add a New evemt to a DSSAT management section Table
#'
#' Adds a new management event (e.g., fertilizer, irrigation, tillage) to the appropriate section table
#' within a DSSAT management table list, creating the section if it does not exist.
#'
#' @param xtables A named list of tables (e.g., data frames), representing DSSAT management sections.
#' @param section Character. The management section to add to (e.g., "fertilizer", "irrigation", "planting", etc.).
#' @param args A named list of column values to update in the new management row.
#'
#' @details
#' The function identifies the appropriate section table in \code{xtables} (by prefix), or creates it if missing using a template. It prepares a new row with the specified arguments, coerces columns to the correct class, and assigns a new unique primary key. For sections with nested (list) columns, it handles class assignment and collapsing as needed. The new row is appended to the section table, which is then converted to a DSSAT table using \code{as_DSSAT_tbl}.
#'
#' If a column specified in \code{args} does not exist in the section table, a warning is issued.
#'
#' @return The input list with the updated management section table.
#'
#' @examples
#' \dontrun{
#' xtables <- add_management(xtables, section = "fertilizer", args = list(FAMN = 50, FDATE = "2022-03-01"))
#' }
#'
#' @importFrom dplyr bind_rows
#' @importFrom tidyr unnest
#' 
#' @export
#' 

# TODO: TEST handle composite section (initial_conditions, irrigation); class list for collapsed vars
#section <- "irrigation"
#args <- list(EFIR = 1, IDATE = c("1981-05-26","1981-06-24"), IRVAL = c(50,50))
#args <- list(FEDATE = c("1981-05-26","1981-06-24"), FMCD = "FE041", FACD = "AP001", FAMN = 120, FAMP = 0, FAMK = 0, FAMC = 0, FDEP = 1)
# TODO: control variable classes/formats?

add_management <- function(xtables,
                           section = c("initial_conditions","planting","tillage","irrigation",
                                       "fertilizer","organic_amendment","chemicals","harvest",
                                       "simulation_controls"),
                           args = list()) {
  
  # Check section input syntax
  section <- match.arg(section, .xfile_sections$sec)
  
  input_nm <- toupper(substr(section, 1, 5))
  sections_pref <- substr(names(xtables), 1, 5)

  if (!(input_nm %in% sections_pref)) {
    nm <- .xfile_sections$name[grepl(input_nm, .xfile_sections$sec, ignore.case = TRUE)]
    xtables[[nm]] <- tibble(FILEX_template[[nm]][0,])
    xtables <- xtables[match(.xfile_sections$name[.xfile_sections$name %in% names(xtables)], names(xtables))]
    sections_pref <- substr(names(xtables), 1, 5)
  }
  
  # Identify focal section
  sec_nm <- names(xtables)[sections_pref == input_nm]  # Section name
  sec <- xtables[[sec_nm]]  # Table
  cols <- names(sec)  # Column names
  classes <- sapply(sec, function(x) class(x)[1])  # Attribute classes

  # Format args as a new row for the specified section
  new_row <- setNames(as.list(rep(NA, length(cols))), cols)
  for (col in names(args)) {
    if (col %in% cols) {
      new_row[[col]] <- args[[col]]
    } else {
      warning(paste("Column", col, "not found in the dataframe"))
    }
  }
  
  # Set each section attribute to appropriate class
  new_row <- set_class(df = new_row, classes = classes)
  # Set new row primary key (1 if table is empty)
  new_row[[1]] <- max(c(0, sec[[1]]), na.rm = TRUE) + 1
  
  # If collapsible section:
  nested_cols <- names(classes[classes %in% "list"])
  
  if (length(nested_cols) > 0){
    classes_nested <- sapply(sec[nested_cols], function(x) sapply(x, function(x) class(x)[1]))
    new_row_lss <- set_class(df = new_row, classes = classes_nested)
    new_row <- cbind(
      new_row[, !names(new_row) %in% nested_cols],
      new_row_lss
    )
  }
  
  # Bind new row to section data
  if (section %in% c("initial_conditions","irrigation","soil_analysis")){  # nested sections
    sec <- unnest(sec, cols = all_of(nested_cols))
    out <- bind_rows(sec, new_row)
    out <- collapse_cols(out, names(new_row_lss))
  } else {
    out <- bind_rows(sec, new_row) 
  }
  
  xtables[[sec_nm]] <- as_DSSAT_tbl(out)

  return(xtables)
}


#' Write a DSSAT Batch File for cultivar parameter calibration
#'
#' Generates and writes a DSSAT batch file for model calibration,
#' formatting the batch table and header according to DSSAT conventions.
#'
#' @param xfile Character vector. The X-file names (one per treatment).
#' @param trtno Integer vector. Treatment numbers.
#' @param rp Integer vector. Replication numbers.
#' @param sq Integer vector. Sequence numbers.
#' @param op Integer vector. Operation numbers.
#' @param co Integer vector. Code numbers.
#' @param cultivar Character. The cultivar name, used to construct the batch file name and header.
#' @param crop_code Character. The two-letter DSSAT crop code, used in the batch file name and header.
#' @param ingeno Character. The cultivar identifier (INGENO), written to the batch file header.
#' @param dir_out Character. Path to the directory where the batch file will be written.
#'
#' @return The name of the batch file written.
#'
#' @examples
#' \dontrun{
#' write_gluebatch(
#'   xfile = c("EX001A.WHX", "EX001B.WHX"),
#'   trtno = 1:2,
#'   rp = c(1, 1),
#'   sq = c(1, 2),
#'   op = c(1, 1),
#'   co = c(1, 1),
#'   cultivar = "Pioneer123",
#'   crop_code = "WH",
#'   ingeno = "IB0001",
#'   dir_out = "C:/DSSAT48/GLWork"
#' )
#' }
#'
#' @importFrom dplyr mutate across
#' @importFrom glue glue_data
#'

write_gluebatch <- function(xfile, trtno, rp, sq, op, co, cultivar, crop_code, ingeno, dir_out){

  # Make batch table
  batch_tbl <- data.frame(FILEX = xfile,
                          TRTNO = trtno,
                          RP = rp,
                          SQ = sq,
                          OP = op,
                          CO = co)

  header_line <- c('%-92s', rep('%7s', 5)) |>
    sprintf(c('@FILEX', 'TRTNO', 'RP', 'SQ', 'OP', 'CO')) |>
    paste0(collapse = '')
  header <- c(paste0('$BATCH(CULTIVAR):', crop_code, ingeno, " ", cultivar), "", header_line)

  column_output <- batch_tbl |>
    mutate(FILEX = sprintf('%-92s', FILEX),
           across(-FILEX, ~sprintf('%7i', .x))) |>
    glue_data('{FILEX}{TRTNO}{RP}{SQ}{OP}{CO}')

  batch_output <- c(header, column_output)
  batch_name <- paste0(cultivar, ".", sprintf("%2s", crop_code), "C")
  batch_path <- file.path(dir_out, batch_name)

  write(batch_output, file = batch_path)  # write batch_output to file
  
  return(batch_name)
}


#' Write a GLUE Simulation Control File for DSSAT calibration Batch Runs
#'
#' Generates and writes a simulation control CSV file for GLUE-based DSSAT calibration batch runs,
#' specifying file paths, flags, and run parameters.
#'
#' @param cfilename Character. The model ID or control file name.
#' @param batchname Character. The name of the cultivar batch file.
#' @param ecocal Character. Whether to calibrate ecotype parameters ("Y" or "N").
#' @param dir_glue Character. Path to the GLUE working directory.
#' @param dir_out Character. Path to the GLUE output directory.
#' @param dir_dssat Character. Path to the DSSAT installation directory.
#' @param flag Integer. GLUE calibration flag (1 = phenology+growth, 2 = phenology, 3 = growth).
#' @param reps Integer. Number of GLUE replicates.
#' @param cores Integer. Number of CPU cores to use.
#' @param dir_genotype Character. Path to the DSSAT Genotype directory.
#'
#' @return The data frame of simulation control variables and values.
#'
#' @examples
#' \dontrun{
#' write_gluectrl(
#'   cfilename  = "CERES048",
#'   batchname  = "Pioneer123.WHC",
#'   ecocal     = "N",
#'   dir_glue   = "C:/DSSAT48/Tools/GLUE",
#'   dir_out    = "C:/DSSAT48/GLWork",
#'   dir_dssat  = "C:/DSSAT48",
#'   flag       = 1,
#'   reps       = 100,
#'   cores      = 4,
#'   dir_genotype = "C:/DSSAT48/Genotype"
#' )
#' }
#'
#' @export
#' 

write_gluectrl <- function(cfilename, batchname, ecocal, dir_glue, dir_out, dir_dssat,
                           flag, reps, cores, dir_genotype){

  controls <- data.frame(
    Variable =
      c("CultivarBatchFile","ModelID","EcotypeCalibration","GLUED","OutputD","DSSATD","GLUEFlag","NumberOfModelRun","Cores","GenotypeD"),
    Value =
      c(batchname, cfilename, ecocal, dir_glue, dir_out, dir_dssat, flag, reps, cores, dir_genotype)
  )

  filepath <- file.path(dir_glue, "SimulationControl.csv")
  write.csv(controls, filepath, row.names = FALSE)

  return(controls)
}


#' Ensure Required DSSAT/GLUE Files Are Present in the GLUE Directory
#'
#' Checks for the presence of required DSSAT/GLUE files in the GLUE directory and copies them from the DSSAT directory if missing.
#'
#' @param dir_dssat Character. Path to the DSSAT installation directory containing required files.
#' @param dir_glue Character. Path to the GLUE working directory where files should be present.
#'
#' @details
#' The function determines the operating system and sets the list of required files accordingly. It checks for each required file in \code{dir_glue}, and if a file is missing, it copies it from \code{dir_dssat}. This ensures that all necessary files for GLUE-based DSSAT simulations are available in the working directory.
#'
#' @return Invisibly returns \code{NULL}. Used for its side effect of copying files if needed.
#'
#' @examples
#' \dontrun{
#' check_glue_files("C:/DSSAT", "C:/GLUE")
#' }
#'
#' @export
#' 

check_glue_files <- function(dir_dssat, dir_glue){

  reqs <- switch(
    Sys.info()[["sysname"]],
    "Windows" = c("MODEL.ERR","SIMULATION.CDE","DSSATPRO.v48","DETAIL.CDE"),
    "Linux"   = c("MODEL.ERR","SIMULATION.CDE","DSSATPRO.v48","DSSATPRO.L48","DETAIL.CDE"),
    "Darwin"  = c("MODEL.ERR","SIMULATION.CDE","DSSATPRO.v48","DETAIL.CDE")  # not tested
  )

  dssat_paths <- file.path(dir_dssat, reqs)
  glue_paths  <- file.path(dir_glue,  reqs)

  missing <- !file.exists(glue_paths)
  file.copy(dssat_paths[missing], glue_paths[missing])
  invisible(NULL)
}


#' Resolve the DSSAT model code and versioned filename stem
#'
#' Uses \code{model} when valid; otherwise falls back to \code{SMODEL} in the
#' X-file's simulation controls section.
#'
#' @param xtables Named list of DSSAT management tables (from \code{read_filex}).
#' @param model Character or \code{NULL}. DSSAT model code (e.g. \code{"CER"}).
#'   \code{NULL} or an unrecognised value triggers inference from \code{xtables}.
#' @return Length-2 character vector: \code{c(model_code, versioned_filename_stem)}.
#' 
#' @noRd
#' 

identify_model <- function(xtables, model) {

  dict <- fetch_dictionary("dssat")
  models <- dict$SIMULATION$`Simulation/Crop Models`
  models_short <- unique(models[[1]])

  if (is.null(model) || !(model %in% models_short)) {
    smodel <- xtables$`SIMULATION CONTROLS`$SMODEL
    if (!is.na(smodel) && smodel != -99) {
      model <- smodel
    } else {
      stop("Error: invalid model. Please specify a model currently implemented in DSSAT: ",
           paste(models_short, collapse = ", "))
    }
  }

  version  <- DSSAT:::get_dssat_version()  #TODO: workflow with DSSAT.CSM not set in options
  cfilename <- paste0(model, sprintf("%03d", as.numeric(version)))

  c(model, cfilename)
}


#' Update a MINIMA or MAXIMA boundary row in a CUL table
#'
#' Overwrites the columns supplied in \code{values}; absent columns retain their
#' existing values. Unrecognised column names in \code{values} trigger a warning.
#'
#' @param ctable Data frame. Cultivar parameter table as read by \code{read_cul}.
#' @param bound Character. Row to update: \code{"MINIMA"} or \code{"MAXIMA"}.
#' @param values Named list. Column-value pairs to write into the boundary row.
#' 
#' @return Updated \code{ctable}.
#' 
#' @noRd
#' 

set_bounds <- function(ctable, bound, values = list()) {

  unknown <- setdiff(names(values), names(ctable))
  if (length(unknown)) warning("Columns not found in CUL table: ", paste(unknown, collapse = ", "))

  idx   <- which(ctable$VRNAME == bound)
  known <- intersect(names(values), names(ctable))
  ctable[idx, known] <- values[known]
  ctable
}


# Validates/defaults a single directory path.
# If `provided` is NULL, returns `default` (stopping if it doesn't exist).
# If `provided` is given, stops if it doesn't exist unless `create = TRUE`.
#' @noRd
#' 

resolve_dir <- function(provided, default, label, create = FALSE) {
  if (is.null(provided)) {
    if (!dir.exists(default)) stop("Error: ", label, " directory not found.")
    default
  } else if (!dir.exists(provided)) {
    if (create) { dir.create(provided, recursive = TRUE); provided }
    else stop("Error: The specified ", label, " directory does not exist: ", provided)
  } else {
    provided
  }
}


#' Calibrate DSSAT Cultivar Parameters Using GLUE
#'
#' Sets up and runs a GLUE-based calibration for DSSAT cultivar parameters, handling file preparation, backup, batch/control file writing, and execution.
#'
#' @param xfile Character. Path to the DSSAT X-file (experiment file).
#' @param cultivar Character. The name of the cultivar to calibrate.
#' @param model Character or NULL. The DSSAT model code (e.g., "CER", "APS"). If NULL, inferred from the X-file.
#' @param trtno Integer or NULL. Treatment number to use for calibration. If NULL, determined automatically.
#' @param pars Character vector. Parameters to calibrate (e.g., \code{c("phenology","growth")}). Default: both.
#' @param method Character. Calibration method. Default is \code{"glue"}.
#' @param minbound, maxbound Named lists. Minimum and maximum bounds for genetic parameters.
#' @param calibrate_ecotype Logical. Whether to calibrate ecotype parameters. Default: FALSE.
#' @param reps Integer. Number of GLUE replicates. Default: 3.
#' @param cores Integer or NULL. Number of CPU cores to use. Default: half of available.
#' @param dir_glue, dir_out, dir_dssat, dir_genotype Character or NULL. Paths to GLUE, output, DSSAT, and genotype directories. If NULL, inferred from DSSAT installation.
#' @param overwrite Logical. Whether to overwrite existing files. Default: FALSE.
#' @param ... Additional arguments passed to internal functions.
#'
#' @details
#' The function prepares all required files and directories, backs up originals, sets up batch and control files, and runs the GLUE calibration script. It updates genetic parameter bounds, disables stress treatments as needed, and writes results back to the genotype directory. The function uses several helper functions for file I/O and management.
#'
#' @return A data frame of the fitted cultivar parameters for the specified cultivar.
#'
#' @examples
#' \dontrun{
#' calibrate(
#'   xfile = "EX001A.WHX",
#'   cultivar = "Pioneer 123",
#'   model = "CER",
#'   reps = 10
#' )
#' }
#' 
#' @importFrom dplyr group_by summarise slice_max pull filter
#' @importFrom tidyr unnest
#' @importFrom DSSAT write_filex
#' 
#' @export
#' 

calibrate <- function(xfile, cultivar, model = NULL, trtno = NULL,
                      pars = c("phenology","growth"), method = "glue", minbound = list(), maxbound = list(), calibrate_ecotype = FALSE,
                      reps = 3, cores = NULL,
                      dir_glue = NULL, dir_out = NULL, dir_dssat = NULL, dir_genotype = NULL,
                      overwrite = FALSE,
                      ...){
  
  setup_calibration <- function(...){
    
    ###------------ Default directories -------------------------------------

    if (is.null(dir_dssat)) {
      dir_dssat <- tryCatch(
        dirname(getOption("DSSAT.CSM")),
        error = function(e) stop(
          "DSSAT-CSM executable not found. ",
          "Set its path with options(DSSAT.CSM = 'C:/path/to/DSCSM048.EXE')"
        )
      )
    } else {
      if (!dir.exists(dir_dssat))
        stop("The specified DSSAT directory does not exist: ", dir_dssat)
      if (is.null(getOption("DSSAT.CSM")))
        options(DSSAT.CSM = dir_dssat)
    }

    dir_genotype <- resolve_dir(dir_genotype, file.path(dir_dssat, "Genotype"),   "Genotype")
    dir_glue     <- resolve_dir(dir_glue,     file.path(dir_dssat, "Tools/GLUE"), "GLUE")
    dir_out      <- resolve_dir(dir_out,      file.path(dir_dssat, "GLWork"),     "Output", create = TRUE)
    
    
    ###------------ Retrieve, load, and backup input files ------------------
    
    # Load file X
    xtables <- read_filex(xfile)
    xfilename <- basename(xfile)
    
    # Retrieve focal cultivar file based on model input (retrieve from file X if NULL)
    model_info <- identify_model(xtables, model)
    model     <- model_info[1]

    # Load file CUL
    modelvers <- model_info[2]
    cfilename <- paste0(modelvers, ".CUL")  # Append extension
    cfile <- file.path(dir_genotype, cfilename)
    ctable <- read_cul(cfile)
    
    # Create backup directory
    dir_date <- format(Sys.Date(), "%Y%m%d")
    backup_dir <- file.path(dir_dssat, "0_BackUp", dir_date)
    if(!dir.exists(backup_dir)){
      dir.create(backup_dir, recursive = TRUE)
    }
    
    # Backup X and C files
    cfile_backup <- file.path(backup_dir, cfilename)
    xfile_backup <- file.path(backup_dir, xfilename)
    write_cul2(ctable, file_name = cfile_backup)
    write_filex(xtables, xfile_backup)
    
    message(sprintf("Original files backed-up in %s.", backup_dir))
    
    
    ###------------ Retrieve crop code and cultivar identifier --------------

    cuTable   <- xtables$CULTIVARS
    ingeno    <- cuTable[cuTable$CNAME == cultivar, "INGENO"]
    crop_code <- cuTable[cuTable$CNAME == cultivar, "CR"]
    
    
    ###------------ Set treatment level for calibration ---------------------
    
    if (is.null(trtno)) {
      
      feTable <- get_xfile_sec(xtables, "FERTILIZER")  # TODO: include OM table too + NITRO/WATER TO Y
      irTable <- get_xfile_sec(xtables, "IRRIGATION")

      # Find the highest nitrogen application
      feMax <- if (!is.null(feTable)) {
        feTable |>
          group_by(dplyr::across(1)) |>
          summarise(FAMN = sum(FAMN)) |>
          slice_max(FAMN) |>
          pull(1)
      } else 0

      # Find the highest irrigation amount
      irMax <- if (!is.null(irTable)) {
        irTable |>
          unnest(cols = c(IDATE, IROP, IRVAL)) |>
          group_by(dplyr::across(1)) |>
          summarise(IRVAL = sum(IRVAL)) |>
          slice_max(IRVAL) |>
          pull(1)
      } else 0

      # Disable stresses absent from the experiment so calibration runs at potential
      stress_types <- character(0)
      if (is.null(feTable)) stress_types <- c(stress_types, "nitrogen")
      if (is.null(irTable)) stress_types <- c(stress_types, "water")
      if (length(stress_types) > 0)
        xtables <- disable_stress(xtables, stress = stress_types)

      trtMat <- get_xfile_sec(xtables, "TREATMENT")
      trtno  <- max(trtMat[[1]])

      if (!is.null(feTable)) trtMat[trtMat[[1]] == trtno, "MF"] <- feMax
      if (!is.null(irTable)) trtMat[trtMat[[1]] == trtno, "MI"] <- irMax

      # When both are present, use the treatment with the highest combined inputs
      if (!is.null(feTable) && !is.null(irTable)) {
        trtno <- trtMat[trtMat$MF == feMax & trtMat$MI == irMax, 1][[1]]
        if (length(trtno) > 1) trtno <- max(trtno)
      }
      
      # Update treatment matrix in xtables accordingly
      xtables[[which(grepl("TREATMENT", names(xtables)))]] <- trtMat
    }
    
    ###------------ Set bounds for genetic parameters -----------------------

    ctable <- set_bounds(ctable, bound = "MINIMA", values = minbound)
    ctable <- set_bounds(ctable, bound = "MAXIMA", values = maxbound)
    
    
    ###------------ Overwrite X and C files with set parameters -------------
    
    write_cul2(ctable, file_name = cfile)
    DSSAT::write_filex(xtables, file_name = xfile)
    
    #message(sprintf("Modified X input saved as %s.\nModified CUL file saved as %s.", xfile, cfile))
    
    
    ###------------ Write batch files for calibration -----------------------
    
    # Write GLUE batch file
    batchname <- write_gluebatch(xfile, trtno, rp = 1, sq = 0, op = 0, co = 0,  #TODO: figure out what these do...
                                 cultivar = cultivar, crop_code = crop_code,
                                 ingeno = ingeno, dir_out = dir_out)

    # Set GLUE flag
    flag <- switch(
      toString(pars),
      "phenology, growth" = 1,
      "phenology"         = 2,
      "growth"            = 3,
      stop("Invalid parameter type")
    )

    # Set input: calibrate ecotype
    ecocal <- if (calibrate_ecotype) "Y" else "N"

    # Set input: number of cores
    cores <- if (is.null(cores)) round(detectCores()/2, 0) else cores

    # Write GLUE control files
    controls <- write_gluectrl(modelvers, batchname,
                               ecocal = ecocal, dir_glue = dir_glue, dir_out = dir_out,
                               dir_dssat = dir_dssat, flag = flag, reps = reps,
                               cores = cores, dir_genotype = dir_genotype)
    
    return(controls)
  } 
  
  controls <- setup_calibration()

  # Run GLUE
  run_calibration <- function(controls, method = "glue", ...){
    
    # Set required directories
    model <- controls[controls$Variable == "ModelID", "Value"]
    dir_dssat <- controls[controls$Variable == "DSSATD", "Value"]
    dir_glue <- controls[controls$Variable == "GLUED", "Value"]
    dir_genotype <- controls[controls$Variable == "GenotypeD", "Value"]
    dir_out <- controls[controls$Variable == "OutputD", "Value"]
    cfilename <- paste0(model, ".CUL")

    # Ensure the working directory is reset on exit
    oldwd <- getwd()
    on.exit(setwd(oldwd))
    # Set work directory to GLUE directory
    setwd(dir_glue)
    
    # Check if all required files are present in GLUE dir (if not, copied from DSSAT dir)
    check_glue_files(dir_dssat, dir_glue)
    # Run GLUE
    system("Rscript GLUE.r")
    
    # Format output
    genpath <- file.path(dir_genotype, cfilename)  # original path
    outpath <- file.path(dir_out, cfilename)
    cfile_fit <- read_cul(outpath)  # new cultivar file  TODO: check if single rec or full file
    
    # Write results
    write_cul2(cfile_fit, genpath)  # overwrite cultivar files in the original location
    message(sprintf("Calibration results written in %s.", genpath))
    
    # Output the fitted parameters for visualization
    out <- dplyr::filter(cfile_fit, VRNAME == cultivar)  
    
    return(out)
  }
  
  glue_out <- run_calibration(controls, method = method)
  
  return(glue_out)
}

###---- TEST ----
#TODO: testnew cultivar (not in original CUL file; set default params and MIN/MAX = default temporarily)
# sequence phenology: (1) VSEN, PPSEN; (2) P5 [FIXED; DEFAULT IF NOT MEASURED: PHINT and P1]
# sequence growth: (1) GRNO, (2) MXFIL [FIXED; DEFAULT IF NOT MEASURED: STMMX, SLAP1]
