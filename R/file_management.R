#' Retrieve Template Version from Excel File
#'
#' Loads the Excel template workbook and extracts the version number.
#'
#' @param path Character string. The file path to the Excel workbook from which to retrieve the template version.
#'
#' @return A string version identifier.
#'
#' @details
#' This function uses `wb_load` to load the workbook and `wb_read` to read the value in a specific location in the workbook.
#' It assumes that the version information is always stored in this specific location.
#'
#' @examples
#' # Example usage:
#' # version <- get_template_version("path/to/template.xlsm")
#'
#' @importFrom openxlsx2 wb_load wb_read
#'
#' @export

get_template_version <- function(path) {
  
  wb <- wb_load(path)
  # Read the value in cell A1 from the "Menu" sheet
  version <- wb_read(wb, sheet = "Menu", cols = 1, rows = 1, colNames = FALSE)[[1,1]]
  return(version)
}


#' Compare User and Package Template Versions
#'
#' Checks and compares the version of the user's template file with the package's template file for ICASA VBA.
#'
#' @details
#' This function locates the user's template at `~/.csmTools/template_icasa_vba.xlsm` and the package template within the installed package's `extdata` directory.
#' It retrieves the version from each template using `get_template_version()`, splits the version strings into major, minor, and patch components, and prints a comparison table.
#' If the user template does not exist, a message is displayed and the function exits silently.
#'
#' @return
#' Invisibly returns a data frame with columns: \code{file}, \code{major}, \code{minor}, and \code{patch}, showing the version breakdown for both user and package templates.
#'
#' @examples
#' # Compare template versions (will print a table if both files exist)
#' check_template_version()
#'
#' @export

check_template_version <- function() {
  
  user_template <- file.path(Sys.getenv("HOME"), ".csmTools/template_icasa_vba.xlsm")
  pkg_template <- system.file("extdata", "template_icasa_vba.xlsm", package = "csmTools")
  
  # Check if user template exists
  if (!file.exists(user_template)) {
    message("User template not found.")
    return(invisible(NULL))
  }
  
  # Get version strings
  user_ver <- get_template_version(user_template)
  pkg_ver  <- get_template_version(pkg_template)
  
  # Breakdown version strings
  vers <- lapply(list(user_ver, pkg_ver), function(x) strsplit(x, "\\.")[[1]])
  vers <- as.data.frame(do.call(rbind, vers), stringsAsFactors = FALSE)
  colnames(vers) <- c("major", "minor", "patch")
  vers[] <- lapply(vers, as.numeric)
  
  # Add file labels
  vers$file <- c("user", "package")
  vers <- vers[, c("file", "major", "minor", "patch")]
  
  print(vers)
  invisible(vers)
}


#' Ensure User Has the Latest ICASA VBA Template
#'
#' Copies the latest version of the ICASA VBA template to the user's directory, updating and backing up as needed.
#'
#' @param force Logical. If \code{TRUE}, forces an update of the user template even if an older version exists, backing up the previous file. Default is \code{FALSE}.
#'
#' @details
#' This function checks for the presence of the user template at \code{~/.csmTools/template_icasa_vba.xlsm}. If it does not exist, it is copied from the package's \code{extdata} directory.
#' If the user template exists, its version is compared to the package template using \code{check_template_version()}. If the package template is newer, the user is prompted to update.
#' If \code{force = TRUE}, the user template is backed up and replaced with the latest version from the package.
#'
#' @return
#' Invisibly returns a list with elements:
#' \describe{
#'   \item{path}{The normalized path to the user template.}
#'   \item{created}{Logical, \code{TRUE} if the template was newly created.}
#'   \item{updated}{Logical, \code{TRUE} if the template was updated.}
#' }
#'
#' @examples
#' # Ensure the user has the latest template (will prompt if update is available)
#' fetch_template()
#'
#' # Force update and backup the old template
#' fetch_template(force = TRUE)
#'
#' @export

fetch_template <- function(force = FALSE) {
  
  # Set user and package template paths
  user_dir <- file.path(Sys.getenv("HOME"), ".csmTools")
  if (!dir.exists(user_dir)) dir.create(user_dir, recursive = TRUE)
  
  user_template <- file.path(user_dir, "template_icasa_vba.xlsm")  # User file
  pkg_template <- system.file("extdata", "template_icasa_vba.xlsm", package = "csmTools")  # Default file
  
  # Check if package template exists
  if (!file.exists(pkg_template)) {
    stop("Package template not found: ", pkg_template)
  }
  
  # If user template does not exist, copy from package
  if (!file.exists(user_template)) {
    file.copy(pkg_template, user_template, overwrite = TRUE)
    message("User template created at: ", normalizePath(user_template))
    return(invisible(list(path = normalizePath(user_template), created = TRUE, updated = FALSE)))
  }
  
  # Compare versions
  vers <- check_template_version()
  ver_value <- vers$major * 1e6 + vers$minor * 1e3 + vers$patch
  idx <- which.max(ver_value)
  most_recent <- vers[idx, "file"]
  
  outdated <- most_recent != "user"
  
  if (!outdated) {
    message("User template is the most recent version.")
    return(invisible(list(path = normalizePath(user_template), created = FALSE, updated = FALSE)))
  }
  
  message("A new template version is available.")
  
  if (force) {
    backup <- file.path(
      user_dir,
      paste0("template_icasa_vba_backup_", format(Sys.Date(), "%y%m%d"), ".xlsm")
    )
    file.copy(user_template, backup, overwrite = TRUE)
    file.copy(pkg_template, user_template, overwrite = TRUE)
    message("User template was updated. Old version backed up as: ", normalizePath(backup))
    return(invisible(list(path = normalizePath(user_template), created = FALSE, updated = TRUE)))
  } else {
    message("Run fetch_template(force = TRUE) to update. Your data will be backed up.")
    return(invisible(list(path = normalizePath(user_template), created = FALSE, updated = FALSE)))
  }
}


#'
#'
#' @export
#'

fetch_dictionary <- function(data_model = "icasa") {
  
  # --- 1. Load ICASA Dictionary Configuration ---
  config_path <- system.file("extdata", "datamodels.yaml", package = "csmTools")
  config <- yaml::read_yaml(config_path)
  dict_source <- config[[data_model]]$dict_source$path
  
  if (is.null(dict_source)) {
    message(paste0("No queriable source is provide for ", data_model, ". Check datamodels.yaml."))
    return(invisible(NULL))
  }
  
  switch(data_model,
         "icasa" = {
           dict <- .fetch_icasa_dictionary(path = dict_source)
         },
         "dssat" = {
           dict <- .fetch_dssat_dictionary(path = dict_source) 
         },
         {
           stop("Unsupported data model: ", data_model)
         }
  )
  
  return(dict)
}


#'
#'
#' @noRd
#'

.fetch_icasa_dictionary <- function(path) {
  
  # Use GitHub API URL to list the repository contents
  api_url <- "https://api.github.com/repos/DSSAT/ICASA-Dictionary/contents/CSV?ref=main"
  repo_contents <- jsonlite::fromJSON(api_url)
  
  # List of CSV filenames
  csv_files <- repo_contents[grepl("\\.csv$", repo_contents$name, ignore.case = TRUE), ]
  
  # Read all CSVs into a named list
  dict <- lapply(csv_files$download_url, function(file) {
    read.csv(file)
  })
  names(dict) <- gsub(".csv", "", csv_files$name)

  return(dict)
}


#'
#'
#' @noRd
#'

.fetch_dssat_dictionary <- function(path) {
  
  # Use GitHub API URL to list the repository contents
  api_url <- "https://api.github.com/repos/DSSAT/dssat-csm-os/contents/Data?ref=develop"
  repo_contents <- jsonlite::fromJSON(api_url)
  
  # Filter file keys (*.CDE files)
  cde_files <- repo_contents[grepl("\\.CDE$", repo_contents$name, ignore.case = TRUE), ]
  
  # Download files
  dict <- lapply(cde_files$download_url, function(url) {
    lines <- readLines(url)
    .parse_dssat_keys(lines)
  })
  names(dict) <- gsub(".CDE", "", cde_files$name)

  return(dict)
}


#'
#'
#' @noRd
#'

.parse_dssat_keys <- function(lines) {
  
  # Convert to UTF-8 to prevent encoding errors (e.g., special characters like \xb7 in latin1)
  lines <- iconv(lines, from = "latin1", to = "UTF-8")
  # Skip completely empty lines and NA lines generated by iconv
  lines <- lines[!is.na(lines) & trimws(lines) != ""]
  
  # Initialize state variables
  result_list <- list()
  
  current_section <- NULL
  current_comments <- character()
  current_headers <- character()
  header_starts <- numeric()
  current_data <- character() 
  
  global_title <- NULL
  global_comments <- character()
  is_first_section <- TRUE
  
  # Store current section everytime new section is encountered (*)
  save_current_section <- function() {
    if (!is.null(current_section)) {
      
      # Case A: the section has no column headers (no '@' line was found)
      if (length(current_headers) == 0) {
        if (is_first_section) {
          # First instance corresponds to the file name and description
          global_title <<- current_section
          global_comments <<- current_comments
          is_first_section <<- FALSE
        } else {
          # Subsequent instances kept as raw lines
          attr(current_data, "comments") <- current_comments
          result_list[[current_section]] <<- current_data
        }
      } 
      # Case B: the section has column headers (standard data table)
      else {
        is_first_section <<- FALSE
        
        if (length(current_data) > 0) {
          # Define start and end indices for fixed-width extraction
          starts <- header_starts
          ends <- c(starts[-1] - 1, 10000)
          
          # Cut each data row into columns based on the calculated starts/ends
          parsed_rows <- lapply(current_data, function(row) {
            trimws(substring(row, starts, ends))
          })
          
          # Bind the list of rows into a dataframe
          sec_df <- as.data.frame(do.call(rbind, parsed_rows), stringsAsFactors = FALSE)
          colnames(sec_df) <- current_headers
          
        } else {
          # Empty dataframe if headers without data
          sec_df <- data.frame(matrix(ncol = length(current_headers), nrow = 0))
          colnames(sec_df) <- current_headers
        }
        
        # Section comments attached as attributes
        attr(sec_df, "comments") <- current_comments
        result_list[[current_section]] <<- sec_df
      }
    }
  }
  
  # Regex pattern matching all valid/expected column names in DSSAT .CDE files
  header_pattern <- "CDE|LABEL|DESCRIPTION\\.*|Description|SO|SYNONYMS|NAME|ALIAS|MODEL|CROP"
  
  # Iterate over every line
  for (line in lines) {
    trimmed_line <- trimws(line)
    
    # Check the leading character to determine the line type:
    
    # Comments ('!')
    if (startsWith(trimmed_line, "!")) {
      comment_text <- sub("^!\\s*", "", trimmed_line) # Remove the '!' and leading spaces
      current_comments <- c(current_comments, comment_text)
      
      # Section headers ('*')
    } else if (startsWith(trimmed_line, "*")) {
      
      # Save section and reset state variables for the new section
      save_current_section()
      current_section <- sub("^\\*\\s*", "", trimmed_line) # Remove the '*'
      current_comments <- character() 
      current_headers <- character()  
      header_starts <- numeric()
      current_data <- character() 
      
      # Column headers ('@')
    } else if (startsWith(trimmed_line, "@")) {
      # Replace '@' with a space to avoid shifting the character indices
      raw_header <- sub("^@", " ", line)
      
      # Find the starting positions of the recognized column names
      matches <- gregexpr(header_pattern, raw_header)
      
      if (matches[[1]][1] != -1) {
        header_starts <- as.numeric(matches[[1]])

        # Fix to catch 1-character data values sitting underneath the '@' symbol.
        header_starts[1] <- 1 
        
        # Extract the column names
        raw_names <- regmatches(raw_header, matches)[[1]]
        # Clean up trailing dots for 'DESCRIPTION' cols
        current_headers <- sub("\\.+$", "", raw_names)
      }
      
      # Data lines (no leading char)
    } else {
      # Keep the un-trimmed line so the fixed-width spacing is preserved
      current_data <- c(current_data, line)
    }
  }
  
  # Finalize
  save_current_section()
  
  # Attach the global title and comments to output list
  if (!is.null(global_title)) {
    attr(result_list, "title") <- global_title
  }
  if (length(global_comments) > 0) {
    attr(result_list, "comments") <- global_comments
  }
  
  return(result_list)
}
