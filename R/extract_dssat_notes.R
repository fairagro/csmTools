#' Extract notes and comments from a DSSAT data section
#'
#' Collects all `_NOTES` and `_COMMENTS` columns from a DSSAT data frame, reshaping them
#' into a long-format tibble grouped by the first two identifier columns. General dataset-level
#' comments are preserved as an attribute on the result.
#'
#' @param data A data frame or tibble representing a DSSAT data section. Expected to have
#'   at least two identifier columns and optionally one or more `_NOTES`/`_COMMENTS` columns.
#'
#' @return A tibble in long format with columns:
#' \itemize{
#'   \item The first two columns of `data` (used as grouping identifiers)
#'   \item `Column`: name of the source `_NOTES`/`_COMMENTS` column
#'   \item `Content`: the extracted note/comment string
#' }
#' If no `_NOTES`/`_COMMENTS` columns exist, returns an empty tibble with the same structure.
#' The `"general"` attribute is set to the `comments` attribute of `data` in all cases.
#'
#' @details
#' Within each group, non-empty, non-NA values are collapsed with `"; "` before pivoting.
#' After pivoting, entries matching an R `c("...", "...")` vector string (as may be stored
#' when comments were previously serialized) are parsed and unnested into individual rows.
#' Escaped quotes within extracted strings are unescaped.
#'
#' @noRd
#'

extract_dssat_notes <- function(data) {
  
  # Extract general comments
  general <- attr(data, "comments")
  
  # Get names of first 2 columns for grouping
  group_cols <- names(data)[1:2]
  
  # Check for comment/note columns
  notes_cols_exist <- any(grepl("_NOTES|_COMMENTS", names(data)))
  
  if (!notes_cols_exist) {
    # Return an empty tibble with the final column structure.
    empty_df <- data[0, group_cols, drop = FALSE] 
    empty_df$Notes_Column <- character(0)
    empty_df$Notes_Content <- character(0)
    
    return(empty_df)
  }

  # Format comment map per section
  comment_map <- data |>
    dplyr::group_by(dplyr::across(tidyr::all_of(group_cols))) |>
    dplyr::summarise(
      dplyr::across(
        tidyr::matches("_NOTES|_COMMENTS"),
        ~ paste(unique(na.omit(.x[.x != ""])), collapse = "; "),
        .names = "{col}"
      ),
      .groups = "drop"
    ) |>
    tidyr::pivot_longer(
      cols = tidyr::matches("_NOTES|_COMMENTS"), # This is now safe
      names_to = "Column",
      values_to = "Content"
    ) |>
    # Split nested comments
    dplyr::mutate(
      Content_List = dplyr::if_else(
        # Regex to fetch vector structures
        stringr::str_detect(trimws(Content), "^c\\(.*\\)$"),
        # Extract all quoted string
        stringr::str_extract_all(Content, "\"(.*?)\""),
        list(Content)
      )
    ) |>
    tidyr::unnest(Content_List) |>
    # Overwrite original
    dplyr::mutate(
      # Remove leading/trailing quotes
      Content = stringr::str_replace_all(Content_List, "^\"|\"$", ""),
      # Clean up any escaped quotes that are now literal
      Content = stringr::str_replace_all(Content, "\\\\\"", "\"")
    ) |>
    dplyr::select(-Content_List) |>
    dplyr::filter(!is.na(Content) & Content != "")
  
  # Append general comments to output
  attr(comment_map, "general") <- general
  
  return(comment_map)
}
