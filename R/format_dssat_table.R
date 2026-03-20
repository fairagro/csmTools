#' Apply a DSSAT template to a data.frame
#'
#' Internal helper to format a DSSAT-terminology data.frame based on a DSSAT template.
#'
#' This function performs several key formatting steps:
#' 1.  Collapses list-columns (e.g., for composite DSSAT sections) as defined in the template.
#' 2  Coerces all matching columns to the data types specified in the template.
#' 3.  Orders columns according to the template, leaving any extra (non-DSSAT) attributes at the end.
#' 4.  Fills `NULL` list-columns with `NA` for consistent structure.
#' 5.  Converts the final object to a 'DSSAT_tbl' and reattaches original custom attributes.
#'
#' @param df A data.frame to be formatted.
#' @param template A template data.frame specifying the desired structure and column types.
#'
#' @return A formatted 'DSSAT_tbl'.
#'
#' @noRd
#' 

format_dssat_table <- function(df, template) {
  
  # Store custom attributes before transformation (exclude core attributes)
  attrs <- attributes(df)
  custom_attrs <- attrs[!names(attrs) %in% c("names", "row.names", "class")]
  
  # Collapse columns of composite sections
  has_lists <- purrr::map_lgl(template, is.list)
  is_list <- has_lists[has_lists]
  if (length(is_list) > 0) {
    df <- collapse_cols(df, intersect(names(is_list), names(df)))
  }
  
  # Apply template format to the input table
  col_names <- intersect(names(df), names(template))
  for (col_name in col_names) {
    df[[col_name]] <- as(df[[col_name]], class(template[[col_name]])[1])
  }
  
  # Populate the template with the input data
  template_cols <- colnames(template)
  extra_cols <- setdiff(colnames(df), template_cols)
  out <- template %>%
    dplyr::bind_rows(df) %>%
    dplyr::select(tidyr::all_of(template_cols), tidyr::all_of(extra_cols)) %>%
    dplyr::slice(-1) %>%
    dplyr::arrange(row.names(template))
  
  # Format NULL lists for composite tables (e.g., IRRIGATION)
  has_lists <- purrr::map_lgl(out, is.list)
  out[has_lists] <- purrr::map(out[has_lists], ~ ifelse(is.null(.x[[1]]), rep(NA, length(out[[1]])), .x))
  
  # Format output and reattach custom attributes
  out <- DSSAT::as_DSSAT_tbl(out)
  attributes(out) <- c(attributes(out), custom_attrs)
  
  return(out)
}
