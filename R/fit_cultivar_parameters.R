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

get_xfile_sec <- function(xtables, sec_name){
  
  index <- which(grepl(sec_name, names(xtables)))
  tbl <- if(length(index) > 0) xtables[[index]] else NULL
  
  return(tbl)
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

get_cdata <- function(crop, model = c("APS","CER","GRO","ARO","CRP","IXM")) {
  
  # DSSAT path and version
  dssat_csm <- gsub("\\", "/", getOption("DSSAT.CSM"), fixed = TRUE)
  dssat_path <- sub('DSCSM.*', "", dssat_csm)
  dssat_vers <- sub(".*DSCSM", "", sub("\\..*", "", dssat_csm))
  
  #
  crop_code <- paste0(substr(toupper(crop), 1, 2), model, dssat_vers)
  
  # Open genotype files
  ctable <- read_cul(file_name = paste0(dssat_path, "Genotype\\", crop_code, ".CUL"))
  #filee <- read_eco(file_name = paste0(dssat_path, "Genotype\\", crop_code, ".ECO"))
  
  # Output
  out <- ctable
  #out <- list()
  #out$CUL <- ctable
  #out$ECO <- filee
  attr(out, "crop") <- crop
  #attr(out, "model") <- model
  return(out)
  
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
  df <- df[names(df) %in% names(classes)]
  correct_class <- sapply(names(df), function(x) class(x)[1])
  df <- mapply(function(value, correct_class) {
    switch(correct_class,
           Date = as.Date(value),
           POSIXct = as.POSIXct(value),
           POSIXt = as.POSIXct(value),
           numeric = as.numeric(value),
           integer = as.integer(value),
           character = as.character(value),
           factor = as.factor(value),
           value)
  }, df, classes, SIMPLIFY = FALSE)
  
  return(as.data.frame(df))
}


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
  
  trt <- xtables[[which(startsWith(names(xtables), "TREATMENT"))]]
  
  # Keep the last row as default
  new_row <- trt[nrow(trt),]
  # Set new row primary key
  new_row[[1]] <- max(trt[1]) + 1
  
  for (col in names(args)) {
    if (col %in% colnames(trt)) {
      new_row[[col]] <- args[[col]]
    } else {
      warning(paste("Column", arg, "not found in the dataframe"))
    }
  }
  
  out <- bind_rows(trt, new_row)
  xtables[[2]] <- as_DSSAT_tbl(out)
  
  return(xtables)
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
#' xtables <- add_management(xtables, section = "fertilizer", args = list(amount = 50, date = "2022-03-01"))
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

add_management <- function(xtables,
                           section = c("initial_conditions","planting","tillage","irrigation",
                                       "fertilizer","organic_amendment","chemicals","harvest",
                                       "simulation_controls"),
                           args = list()) {
  
  # add to data?
  xsections <- data.frame(
    sec = c("general","treatments","cultivars","fields","soil_analysis","initial_conditions",
            "planting","irrigation","fertilizer","organic_amendment","tillage","chemicals",
            "environment_modifications","harvest","simulation_controls"),
    name = c("GENERAL","TREATMENTS                        -------------FACTOR LEVELS------------",
             "CULTIVARS","FIELDS","SOIL ANALYSIS","INITIAL CONDITIONS","PLANTING DETAILS",
             "IRRIGATION AND WATER MANAGEMENT","FERTILIZERS (INORGANIC)","RESIDUES AND ORGANIC FERTILIZER",
             "TILLAGE AND ROTATIONS","CHEMICALS","ENVIRONMENT MODIFICATIONS","HARVEST DETAILS","SIMULATION CONTROLS")
  )

  # Extract management type prefix
  input_nm <- toupper(substr(section, 1, 5))
  sections_pref <- substr(names(xtables), 1, 5)
  
  # Check if section exists
  if (!(input_nm %in% sections_pref)) {
    
    # Retrieve section name
    nm <- xsections$name[grepl(input_nm, xsections$sec, ignore.case = TRUE)]
    # Add missing section based on DSSAT templates
    xtables[[nm]] <- tibble(FERTILIZERS_template[0,])
    # Set default order
    xtables <- xtables[match(xsections$name[xsections$name %in% names(xtables)], names(xtables))]
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
  
  if (length(nested_cols > 0)){
    classes_nested <- sapply(sec[nested_cols], function(x) sapply(x, function(x) class(x)[1]))
    new_row_lss <- set_class(df = new_row, classes = classes_nested)
    new_row <- cbind(
      new_row[, !(names(new_row) %in% intersect(names(new_row), nested_cols))],
      new_row_lss
    )
  }
  
  # Bind new row to section data
  if (section %in% c("initial_conditions","irrigation","soil_analysis")){  # nested sections
    sec <- unnest(sec, cols = all_of(nested_cols))
    out <- bind_rows(sec, new_row) 
    out <- collapse_cols(new_row, names(new_row_lss))
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
#' @param ... Additional arguments (not used directly).
#'
#' @details
#' The function constructs a batch table with the specified columns, formats the header and data lines according to DSSAT batch file requirements, and writes the result to a file. The batch file name and path are constructed using the \code{cultivar} and \code{crop_code} variables (which must be defined in the environment), and the output directory \code{dir_out}.
#'
#' The function uses the \strong{glue}, \strong{dplyr}, and \strong{stringr} packages for string formatting and data manipulation.
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
#'   co = c(1, 1)
#' )
#' }
#'
#' @importFrom dplyr mutate mutate_at vars
#' @importFrom stringr str_c
#' @importFrom glue glue_data
#'

write_gluebatch <- function(xfile, trtno, rp, sq, op, co, ...){

  # Make batch table
  batch_tbl <- data.frame(FILEX = xfile,
                          TRTNO = trtno,
                          RP = rp,
                          SQ = sq,
                          OP = op,
                          CO = co)

  header_line <- c('%-92s', rep('%7s', 5)) |>  # fixed columns widths
    sprintf(c('@FILEX', 'TRTNO', 'RP', 'SQ', 'OP', 'CO')) |>  # column headers
    str_c(collapse = '')  # collapse into a single column header line
  header <- c(paste0('$BATCH(CULTIVAR):', crop_code, ingeno, " ", cultivar), "", header_line)  # add batch file header label
  
  column_output <- batch_tbl |>
    mutate(FILEX = sprintf('%-92s', FILEX)) |>   # expand filex name to full width
    mutate_at(vars(-FILEX), ~sprintf('%7i', .x)) |>  # write out all other columns
    glue_data('{FILEX}{TRTNO}{RP}{SQ}{OP}{CO}')  # combine columns into character vector
  
  batch_output <- c(header, column_output)  # combine header lines and column data
  batch_name <- paste0(cultivar, ".", sprintf("%2s", crop_code), "C")  # set cultivar batch file name
  batch_path <- paste0(dir_out, "/", batch_name)

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
#' @param ... Additional arguments (not used directly, but required variables must be present in the environment: \code{ecocal}, \code{dir_glue}, \code{dir_out}, \code{dir_dssat}, \code{flag}, \code{reps}, \code{cores}, \code{dir_genotype}).
#'
#' @details
#' The function creates a data frame of simulation control variables and their values, then writes it to \code{SimulationControl.csv} in the GLUE directory. The required variables (e.g., \code{ecocal}, \code{dir_glue}, etc.) must be defined in the environment.
#'
#' This file is used to control GLUE-based DSSAT batch simulations.
#'
#' @return The data frame of simulation control variables and values (invisibly).
#'
#' @examples
#' \dontrun{
#' write_gluectrl(
#'   cfilename = "CERES",
#'   batchname = "BATCH.WHC"
#'   # plus required variables in the environment
#' )
#' }
#'
#' @export

write_gluectrl <- function(cfilename, batchname, ...){

  controls <- data.frame(
    Variable =
      c("CultivarBatchFile","ModelID","EcotypeCalibration","GLUED","OutputD","DSSATD","GLUEFlag","NumberOfModelRun","Cores","GenotypeD"),
    Value =
      c(batchname, cfilename, ecocal, dir_glue, dir_out, dir_dssat, flag, reps, cores, dir_genotype)
  )

  filepath <- paste0(dir_glue, "/SimulationControl.csv")
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

check_glue_files <- function(dir_dssat, dir_glue){
  
  sys <- Sys.info()
  os <- sys[["sysname"]]
  
  reqs <- switch(
    os,
    "Windows" = c("MODEL.ERR","SIMULATION.CDE","DSSATPRO.v48","DETAIL.CDE"),
    "Linux"   = c("MODEL.ERR","SIMULATION.CDE","DSSATPRO.v48","DSSATPRO.L48","DETAIL.CDE"),
    "Darwin"  = c("MODEL.ERR","SIMULATION.CDE","DSSATPRO.v48","DETAIL.CDE")  # not tested
  )

  dssat_paths <- sapply(reqs, function(x) file.path(dir_dssat, x))
  glue_paths <- sapply(reqs, function(x) file.path(dir_glue, x))
  
  for (i in seq_along(glue_paths)){
    if(!file.exists(glue_paths[i])){
      file.copy(dssat_paths[i], glue_paths[i])
    }
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
    
    # Set DSSAT directory
    if(is.null(dir_dssat)){
      dir_dssat <- tryCatch(
        dirname(getOption("DSSAT.CSM")),
        error = function(e) {
          stop("Error: DSSAT-CSM exectuable not found.\n
                  Set the location and file name of the DSSATCSM.EXE using:n\
                  options(DSSAT.CSM = 'C:\ \ path\ to\ executable')")
        }
      )
    } else {
      # Ensure DSSAT.CSM option is set
      if (!dir.exists(dir_dssat)) {
        stop("Error: The specified DSSAT directory does not exist.
             Please provide a valid directory path.")
      }
      # Check if the directory exists
      if(is.null(getOption("DSSAT.CSM"))){
        options(DSSAT.CSM = dir_dssat)
      }
    }
    
    # Set GLUE settings and output subdirectories
    if(is.null(dir_genotype)){
      dir_genotype <- if(dir.exists(paste0(dir_dssat, "/Genotype"))){
        paste0(dir_dssat, "/Genotype")
      } else {
        stop("Error: Genotype directory not found.")
      }
    } else if (!dir.exists(dir_genotype)) {
      stop("Error: The specified Genotype directory does not exist.
           Please provide a valid directory path.")
    }
    
    if(is.null(dir_glue)){
      dir_glue <- if(dir.exists(paste0(dir_dssat, "/Tools/GLUE"))){
        paste0(dir_dssat, "/Tools/GLUE")
      } else {
        stop("Error: GLUE directory not found.")
      }
    } else if (!dir.exists(dir_glue)) {
      stop("Error: The specified GLUE directory does not exist.
           Please provide a valid directory path.")
    }
    
    if(is.null(dir_out)){
      dir_out <- if(dir.exists(paste0(dir_dssat, "/GLWork"))){
        paste0(dir_dssat, "/GLWork")
      } else {
        stop("Error: Output directory not found.")
      }
    } else if (!dir.exists(dir_out)) {
      dir.create(dir_out)
    }##
    
    
    ###------------ Retrieve, load, and backup input files ------------------
    
    # Load file X
    xtables <- read_filex(xfile)
    xfilename <- basename(xfile)
    
    # Retrieve focal cultivar file based on model input (retrieve from file X if NULL)
    identify_model <- function(xtables, model){
      
      # Import DSSAT dict to control input data annotation
      # models <- get_dssat_terms("models")
      dict <- fetch_dictionary("dssat")
      models <- dict$SIMULATION$`Simulation/Crop Models`
      
      # Validate model input annotation
      models_short <- unique(models[[1]])
      if (is.null(model) || !(model %in% models_short)) {
        
        if (!is.na(xtables$`SIMULATION CONTROLS`$SMODEL) || xtables$`SIMULATION CONTROLS`$SMODEL != -99) {
          model <- xtables$`SIMULATION CONTROLS`$SMODEL
        } else {
          stop("Error: invalid model. Please specify a model currently implemented in DSSAT: ", paste(models_short, collapse = ", "))
        }
      }
      
      version <- DSSAT:::get_dssat_version()  #TODO: workflow with DSSAT.CSM not set in options
      cfilename <- paste0(model, sprintf("%03d", as.numeric(version)))
      
      out <- c(model, cfilename)
      
      return(out)
    }
    model <- identify_model(xtables, model)[1]
    
    # Load file CUL
    modelvers <- identify_model(xtables, model)[2]
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
    
    cuTable <- xtables$CULTIVARS  # Cultivar table
    
    ingeno <- cuTable[cuTable$CNAME == cultivar]$INGENO  # Cultivar Identifier
    crop_code <- cuTable[cuTable$CNAME == cultivar]$CR  # Crop code
    
    # Retrieve crop full name
    # crop_nms <- suppressWarnings(get_dssat_terms("crops"))
    dict <- fetch_dictionary("dssat")
    crop_nms <- tmp$DETAIL$`Crop and Weed Species`
    
    crop <- crop_nms[crop_nms$`@CDE` == crop_code,]$DESCRIPTION  # Crop name
    
    
    ###------------ Set treatment level for calibration ---------------------
    
    if (is.null(trtno)) {
      
      feTable <- get_xfile_sec(xtables, "FERTILIZER")  # TODO: include OM table too + NITRO/WATER TO Y
      irTable <- get_xfile_sec(xtables, "IRRIGATION")
      haTable <- get_xfile_sec(xtables, "HARVEST")  # TODO: add to disable_stress new method
      
      # Find the highest nitrogen application
      if (!is.null(feTable)){
        
        feMax <- feTable |>
          group_by(dplyr::across(1)) |>
          summarise(FAMN = sum(FAMN)) |>
          slice_max(FAMN) |>
          pull(1)
      } else {
        feMax = 0
      }
      
      # Find the highest irrigation amount
      if (!is.null(irTable)){
        irMax <- irTable |>
          unnest(cols = c(IDATE, IROP, IRVAL)) |>
          group_by(dplyr::across(1)) |>
          summarise(IRVAL = sum(IRVAL)) |>
          slice_max(IRVAL) |>
          pull(1)
      } else {
        irMax = 0
      }
      
      # Define stress types based on the presence/absence of nitrogen and water management
      stress_types <- c()
      
      if (is.null(feTable)) stress_types <- c(stress_types, "nitrogen")
      if (is.null(irTable)) stress_types <- c(stress_types, "water")
      
      # 1- Drop stress in new treatment if irrigation or fertilization were applied
      if (length(stress_types) > 0) {
        xtables <- disable_stress(xtables, stress = stress_types)
      }
      
      trtMat <- get_xfile_sec(xtables, "TREATMENT")
      trtno <- max(trtMat[1])
      
      # Set treatment number to the highest fertilization level if fertilization was applied
      if (!is.null(feTable)) trtMat[trtno,]$MF <- feMax
      # Set treatment number to the highest irrigation level if irrigation was applied
      if (!is.null(irTable)) trtMat[trtno,]$MI <- irMax
      
      # 2- Both fertilization and irrigation applied: find treatment number with both max values
      if (!is.null(feTable) & !is.null(irTable)) {
        trtno <- trtMat[[which(trtMat$MF == feMax & trtMat$MI == irMax), 1]]
        if (length(trtno) > 1) trtno <- max(trtno)
      }
      
      # Update treatment matrix in xtables accordingly
      xtables[grepl("TREATMENT", names(xtables))][[1]] <- trtMat
    }
    
    ###------------ Set bounds for genetic parameters -----------------------
    
    # Function to create and update boundary rows
    set_bounds <- function(ctable, bound, values = list()) {
      
      cols <- names(ctable)  # Column names
      
      # Initialize new row
      new_row <- setNames(as.list(rep(NA, length(cols))), cols)
      
      # Assign specified values to the new row
      for (col in names(values)) {
        if (col %in% cols) {
          new_row[[col]] <- values[[col]]
        } else {
          warning(paste("Column", col, "not found in the dataframe"))
        }
      }
      
      new_row <- as_tibble(new_row, check.names = FALSE)
      
      # Fill NAs with default row values
      def_row <- ctable[ctable$VRNAME == bound, ]
      for (col in names(new_row)) {
        if (is.na(new_row[[col]]) && col %in% names(def_row)) {
          new_row[[col]] <- def_row[[col]]
        }
      }
      
      # Update the dataframe
      ctable[ctable$VRNAME == bound, ] <- new_row
      
      return(ctable)
    }
    
    ctable <- set_bounds(ctable, bound = "MINIMA", values = min)
    ctable <- set_bounds(ctable, bound = "MAXIMA", values = max)
    
    
    ###------------ Overwrite X and C files with set parameters -------------
    
    write_cul2(ctable, file_name = cfile)
    DSSAT::write_filex(xtables, file_name = xfile)
    
    #message(sprintf("Modified X input saved as %s.\nModified CUL file saved as %s.", xfile, cfile))
    
    
    ###------------ Write batch files for calibration -----------------------
    
    # Write GLUE batch file
    batchname <- write_gluebatch(xfile, trtno, rp = 1, sq = 0, op = 0, co = 0) #TODO: figure out what these do...
    
    # Set GLUE flag
    flag <- switch( 
      toString(pars),
      "phenology, growth" = 1,
      "phenology" = 2,
      "phenology" = 3,
      stop("Invalid parameter type")
    )
    
    # Set input: calibrate ecotype
    ecocal <- if (calibrate_ecotype) "Y" else "N"
    
    # Set input: number of cores
    cores <- if (is.null(cores)) round(detectCores()/2, 0) else cores
    
    # Write GLUE control files
    controls <- write_gluectrl(modelvers, batchname, ecocal,
                               dir_glue, dir_out, dir_dssat, flag, reps, cores, dir_genotype)
    
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
  
  glue_out <- run_calibration(controls, method = "glue")
  
  return(glue_out)
}

###---- TEST ----
#TODO: testnew cultivar (not in original CUL file; set default params and MIN/MAX = default temporarily)
# sequence phenology: (1) VSEN, PPSEN; (2) P5 [FIXED; DEFAULT IF NOT MEASURED: PHINT and P1]
# sequence growth: (1) GRNO, (2) MXFIL [FIXED; DEFAULT IF NOT MEASURED: STMMX, SLAP1]
