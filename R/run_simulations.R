#' Run DSSAT simulations
#'
#' Executes DSSAT simulations for specified treatments and returns output data.
#' Automatically handles DSSAT executable location, batch file creation, and output collection.
#'
#' @param filex_path (character) Path to the DSSAT experiment file (file X).
#' @param treatments (character) Vector of treatment numbers to simulate, as defined in file X.
#' @param framework (character) Simulation framework to use. Currently only supports "dssat".
#' @param dssat_dir (character) Path to DSSAT installation directory. If `NULL` (default), attempts to:
#'   1. Use the `DSSAT_CSM` environment variable, or
#'   2. Default to `"C:/DSSAT48"` on Windows or `~/dssat/` on Unix.
#' @param sim_dir (character) Directory for simulation files and outputs. If `NULL` (default), defaults to:
#'   1. `dssat_dir` if specified, or
#'   2. `"C:/DSSAT48"` on Windows or `~/dssat/` on Unix.
#' @param args (list) Named list of additional simulation parameters to override defaults.
#'   Supported parameters: `RP`, `SQ`, `OP`, `CO` (each should be a vector matching `treatments` length).
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Locates the DSSAT executable and sets up directory paths
#'   \item Reads the experiment (.WHX) and treatment (.WHA) files
#'   \item Creates a batch file with specified treatments and parameters
#'   \item Executes the simulations in batch mode
#'   \item Collects and returns all output files (.OUT) as a named list
#' }
#'
#' Environment variables:
#' \describe{
#'   \item{DSSAT_CSM}{Path to DSSAT executable (optional)}
#' }
#'
#' @return A named list of simulation outputs, where:
#'   \describe{
#'     \item{Names}{Treatment identifiers (without .OUT extension)}
#'     \item{Values}{Data frames containing simulation output for each treatment}
#'   }
#'
#' @examples
#' \dontrun{
#' # Basic usage with default DSSAT location
#' results <- run_simulations(
#'   filex_path = "path/to/Experiment.WHX",
#'   treatments = c("T1", "T2", "T3")
#' )
#'
#' # With custom parameters
#' results <- run_simulations(
#'   filex_path = "path/to/Experiment.WHX",
#'   treatments = c("T1", "T2"),
#'   dssat_dir = "D:/DSSAT48",
#'   sim_dir = "D:/simulations/run1",
#'   args = list(RP = c(1, 2), SQ = c(0, 1))
#' )
#'
#' # Using environment variable
#' Sys.setenv(DSSAT_CSM = "path/to/DSCSM048.EXE")
#' results <- run_simulations("Experiment.WHX", c("T1", "T2"))
#' }
#'
#' @importFrom utils modifyList
#' @importFrom tools file_path_sans_ext
#' @importFrom DSSAT read_filex write_dssbatch run_dssat read_output
#'
#' @export
#' 
#' 

run_simulations <- function(filex_path, treatments, framework = "dssat", dssat_dir = NULL, sim_dir = NULL, args = list()) {
  
  # Read file X
  filex <- DSSAT::read_filex(filex_path)
  
  # Specify the location of the DSSAT CSM executable (can be passed as environment variable) and the location of input/output files
  if (is.null(dssat_dir)) {
    if (Sys.getenv("DSSAT_CSM") != "") {
      dssat_exe <- Sys.getenv("DSSAT_CSM")
      dssat_dir <- dirname(dssat_exe)
      sim_dir <- ifelse(is.null(sim_dir), dssat_dir, sim_dir)
      options(DSSAT.CSM = dssat_exe)
    } else {
      # use default windows dir
      options(DSSAT.CSM = "C:\\DSSAT48\\DSCSM048.EXE")
      sim_dir <- ifelse(is.null(sim_dir), "C:/DSSAT48", sim_dir)  # specify dir for simulations: input files, batch files and simulations all stored there
      dssat_dir <- "C:/DSSAT48"
    }
  } else {
    options(DSSAT.CSM = paste0(dssat_dir, "/DSCSM048.EXE"))
    sim_dir <- ifelse(is.null(sim_dir), "C:/DSSAT48", sim_dir)
  }
  
  # For Unix systems use home dir as default
  if(.Platform$OS.type == "unix"){
    options(DSSAT.CSM = paste0(Sys.getenv("HOME"), "/dssat/dscsm048")) # still hardcoded :(
    sim_dir <- paste0(Sys.getenv("HOME"), "/dssat/")
  }

  old_wd <- paste0(getwd(), "/") # trailing slash to avoid path issues
  setwd(sim_dir)

  # Default parameters
  default <- list(
    TRTNO = treatments,
    RP = rep(1, length(treatments)),
    SQ = rep(0, length(treatments)),
    OP = rep(0, length(treatments)),
    CO = rep(0, length(treatments))
  )
  params <- modifyList(default, args)  # set simulation parameters
  
  # Simulations
  batch_tbl <- data.frame(
    FILEX = rep(filex_path, length(params$TRTNO)),  # TODO: retrieve filename is not associated as attr (read through regular filex call)
    TRTNO = params$TRTNO,
    RP = params$RP,
    SQ = params$SQ,
    OP = params$OP,
    CO = params$CO
  )
  
  write_dssbatch(batch_tbl)  # write batch file
  run_dssat(run_mode = "B")  # run simulations
  
  # Return all "readable" simulation outputs
  setwd(old_wd)  # reset wd

  output_files <- list.files(path = sim_dir, pattern = "\\.OUT$", full.names = TRUE)
  output_files <- output_files[!grepl("INFO|RunList|WARNING", output_files)]
  out <- lapply(output_files, read_output)
  out <- setNames(
    out,
    tools::file_path_sans_ext(basename(output_files))
  )
  # TODO: add non-readable files
  
  return(out)
}
