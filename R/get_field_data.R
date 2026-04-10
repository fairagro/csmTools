#' Read field book (meta)data from the ICASA Excel template
#'
#' Imports agronomic management and observation data from the ICASA-Agro Excel template.
#' It handles template versioning, file path discovery, and conversion to the standard ICASA data model.
#'
#' @param path Character string. Path to the template file. If \code{NULL} (default),
#'   the function automatically locates or creates the managed template in the
#'   user's home directory (\code{~/.csmTools/}) using \code{\link{fetch_template}}.
#' @param raw Logical. If \code{FALSE} (default), the data is automatically converted
#'   from the entry-oriented \code{"icasa-agro"} model to the standard \code{"icasa"}
#'   model. Set to \code{TRUE} to retrieve the data exactly as it appears in the spreadsheet.
#' @param keep_empty Logical. Whether to retain empty rows or fields during the
#'   import process. Defaults to \code{FALSE}.
#'
#' @details
#' The function follows a specific workflow to ensure data integrity:
#' \enumerate{
#'   \item If no path is provided, it attempts to use the template in the user's home directory.
#'   \item If that template was just created (first-time use), the function stops to allow the user to input data.
#'   \item If a custom path is provided, a warning is issued noting that this specific file will
#'   not receive automatic version updates from the package.
#' }
#'
#' @section Data Models:
#' \itemize{
#'   \item \strong{icasa-agro}: The human-readable, entry-friendly format used inside the Excel template.
#'   \item \strong{icasa}: The standardized data model used by \code{csmTools}  processing functions
#'   (e.g., for DSSAT export).
#' }
#'
#' @return A list-based dataset containing the parsed field data, structured according to the requested data model.
#'
#' @seealso \code{\link{fetch_template}}, \code{\link{read_template}}, \code{\link{convert_dataset}}
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Read from the default system-managed template
#' field_data <- get_field_data()
#'
#' # Read from a custom file path and keep raw entry format
#' raw_data <- get_field_data(path = "my_experiment_data.xlsm", raw = TRUE)
#' }
#' 

get_field_data <- function(path = NULL, raw = FALSE, keep_empty = FALSE) {
  
  # Fetch template
  if (is.null(path)) {
    tpl <- fetch_template()
    if (tpl$created) {
      stop("Newly created template contains no data! Please fill it in and rerun.")
    }
    path <- tpl$path
  } else {
    user_template <- file.path(Sys.getenv("HOME"), ".csmTools/template_icasa_vba.xlsm")
    warning(
      "You have supplied a custom template file path. ",
      "Be aware that this file will not be updated automatically with package updates. ",
      "For the latest supported version, use the default user template at: ", user_template 
    )
  }
  
  # Read field data from template
  field_data <- read_template(path, keep_empty = keep_empty)
  
  # Map to ICASA
  if (!raw) {
    field_data <- convert_dataset(
      field_data,
      input_model = "icasa-agro",
      output_model = "icasa"
    )
  }
  
  return(field_data)
}


#'
#'
#' @noRd
#'

read_template <- function(path, keep_empty = FALSE) {
  
  # Load workbook and set name criterion for sheets to be loaded (capitalized sheet names)
  wb <- suppressWarnings(openxlsx2::wb_load(path))
  nms <- openxlsx2::wb_get_sheet_names(wb)
  nms_data <- nms[grepl("^[A-Z_]+$", nms)]
  dict <- wb_to_df(wb, sheet = "Dictionary", startRow = 1)
  dict <- dict[which(dict$var_order != -99), ]  # Drop unused terms
  
  # Extract all sections and section names
  dfs <- list()
  for (i in 1:length(nms_data)){
    
    df <- suppressWarnings(
      openxlsx2::wb_to_df(wb, sheet = nms_data[i], startRow = 4, detect_dates = TRUE)
    )
    
    # Drop artefacts if any
    df <- remove_artefacts(df)
    
    # Drop input helpers if any
    df <- remove_helpers(df)
    
    # Enforce data formats
    df_dict <- dict[which(dict$sheet == nms_data[i]), ]
    for (nm in names(df)) {
      
      class <- df_dict$class[df_dict$var_name == nm]
      
      # Skip if no class defined
      if (length(class) == 0 || is.na(class)) next
      
      df[[nm]] <- withCallingHandlers({
        switch(
          class,
          "text" = ,
          "code" = as.character(df[[nm]]),
          "numeric" = as.numeric(df[[nm]]),
          "integer" = as.integer(df[[nm]]),
          "date" = as.Date(lubridate::parse_date_time(
            df[[nm]],
            orders = c("ymd", "dmy", "mdy", "bdY", "dbY", "ymd HMS", "dmy HMS"),
            quiet = TRUE
          )),
          df[[nm]]  # Default: keep as is
        )
      }, warning = function(w) {
        # Check if the warning is about coercion
        if (grepl("coercion", w$message, ignore.case = TRUE) || grepl("parse", w$message, ignore.case = TRUE)) {
          
          # For easy debugging of mis-specified data class
          message(sprintf(
            "Warning in sheet '%s', column '%s': %s", 
            nms_data[i], 
            nm, 
            w$message
          ))
          
          # Muffle the standard R warning to avoid duplicates
          invokeRestart("muffleWarning")
        }
      })
    }
    
    # Drop empty data frames if required
    if (nrow(df) > 0 || keep_empty) {
      dfs[[i]] <- df
      names(dfs)[i] <- nms_data[i]
    }
  }
  
  # Delete empty dataframes
  if (!keep_empty) {
    dfs <- Filter(function(df) !is.null(df) || !all(dim(df) == c(0, 0)) || !all(is.na(df)), dfs)
  }
  
  return(dfs)
}


#'
#'
#' @noRd
#'

remove_helpers <- function(df) {
  
  # Identify column with input helpers
  helper_columns <- sapply(df, function(col) any(grepl("^[0-9]+\\|", as.character(col))))
  
  # Remove helper names, keeping only unique identifiers
  df[helper_columns] <- lapply(df[helper_columns], function(col) {
    x <- as.character(col)
    # Extract numeric part before first "|" for matching rows
    x <- ifelse(grepl("^[0-9]+\\|", x),
                sub("^([0-9]+)\\|.*", "\\1", x),
                x)
    # Convert back to numeric
    x <- as.numeric(x)
  })
  
  return(df)
}


#'
#'
#' @noRd
#'

remove_artefacts <- function(df) {
  
  # Identify artefact columns
  colnames(df) <- ifelse(is.na(colnames(df)), "unnamed", colnames(df))
  
  # Ensure uniqueness without sanitizing special characters (in some ICASA names; alternative to make.names)
  colnames(df) <- make.unique(colnames(df))
  
  # Remove unnammed and full NA columns
  df <- df[ , !grepl("^unnamed", colnames(df))]
  df <- df %>% dplyr::filter(rowSums(is.na(.)) != ncol(.))
  
  return(df)
}
