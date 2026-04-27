#' Recursively apply a function to data frames in a list
#'
#' Traverses a list, including nested lists, and applies function `f` to every element that is a data frame.
#'
#' @param x A list, possibly nested, containing data frames.
#' @param f The function to apply to each data frame.
#' @param ... Additional arguments passed on to `f`.
#'
#' @return A list with the same structure as `x`.
#' 
#' @noRd
#' 

apply_recursive <- function(x, f, ...) {
  if (is.data.frame(x)) {
    return(f(x, ...))
  }
  if (is.list(x)) {
    return(lapply(x, apply_recursive, f = f, ...))
  }
  return(x)
}


#' Transpose a list of named lists
#'
#' Converts a list of row-like lists (e.g., `list(list(a=1, b=2), list(a=3, b=4))`)
#' into a list of column-like lists (e.g., `list(a=list(1, 3), b=list(2, 4))`).
#'
#' @param ls A list of named lists, all with the same names.
#'
#' @return A transposed list.
#' 
#' @noRd
#' 

revert_list_str <- function(ls) {
  x <- lapply(ls, `[`, names(ls[[1]]))
  apply(do.call(rbind, x), 2, as.list)
}


#' Get the name of a data frame within a list
#'
#' Returns the name(s) of elements in \code{ls} that are identical to
#' \code{df}.
#'
#' @param ls A named list of data frames.
#' @param df A data frame to search for.
#'
#' @return A character vector of names of elements in \code{ls} that are
#'   identical to \code{df}. Returns \code{character(0)} if no match is
#'   found. Returns multiple names if duplicates exist in \code{ls}.
#'
#' @noRd
#' 

get_df_name <- function(ls, df) {
  names(ls)[sapply(ls, function(x) identical(x, df))]
}


#' Remove duplicate data frames from a list
#'
#' Returns a copy of \code{ls} with duplicate data frames removed, where equality is determined
#' by value via \code{dput()} serialisation.
#'
#' @param ls A list of data frames.
#'
#' @return A subset of \code{ls} containing only the first occurrence of each
#'   unique data frame. Names are preserved.
#' 
#' @noRd
#' 

drop_duplicate_dfs <- function(ls) {
  
  # Convert each data frame to a string representation
  df_strings <- sapply(ls, function(df) paste(capture.output(dput(df)), collapse = ""))
  # Find unique indices
  unique_indices <- !duplicated(df_strings)
  
  # Return only unique data frames
  ls[unique_indices]
}


#' Find data frames common to two lists by value
#'
#' Returns every data frame in \code{list1} that also appears in \code{list2}.
#' Each data frame is serialised to a canonical string with \code{dput()} and the intersection is
#' found on those strings.
#'
#' @param list1,list2 Lists of data frames to compare.
#'
#' @return A subset of \code{list1} containing only those data frames whose content matches at least
#'   one data frame in \code{list2}. Names are preserved. If no common data frames exist an empty list is returned.
#'
#' @details
#' Serialisation via \code{dput()} / \code{capture.output()} is exact but can be slow for large data frames.
#' Column order, row order, attributes, and class are all reflected in the string, so two data frames that differ only
#' in row order or an attribute will not be considered equal.
#'
#' @noRd

intersect_dfs <- function(list1, list2) {
  
  # Convert each data frame to a string representation
  str1 <- sapply(list1, function(df) paste(capture.output(dput(df)), collapse = ""))
  str2 <- sapply(list2, function(df) paste(capture.output(dput(df)), collapse = ""))
  
  # Find common string representations
  common_strs <- intersect(str1, str2)
  
  # Return the data frames from list1 that are in the intersection
  list1[which(str1 %in% common_strs)]
}


#' Reduce a list of data frames to one by successive natural joins
#'
#' Iteratively left-joins each data frame in \code{df_list} to the accumulated result,
#' using the intersection of column names as the join key at each step.
#' 
#' @param df_list A list of data frames.
#'
#' @return A single data frame produced by left-joining all non-\code{NULL}
#'   elements of \code{df_list} in order.
#'
#' @details
#' At each reduction step the join keys are recomputed as the intersection of column names between the
#' current accumulator and the next data frame. If that intersection is empty the function stops
#' with an informative error.
#'
#' @noRd

reduce_by_join <- function(df_list) {
  
  .join_by_intersect <- function(.x, .y) {
    join_keys <- intersect(names(.x), names(.y))
    
    if (length(join_keys) == 0) {
      stop("Failed to reduce components. No common column names found across all input data frames.")
    }
    
    dplyr::left_join(.x, .y, by = join_keys)
  }
  
  df_list |>
    purrr::compact() |>
    purrr::reduce(.join_by_intersect)
}


#' Flatten a nested list down to its data-frame leaves
#'
#' Recursively traverses a nested list and collects every data frame found at any depth,
#' returning them as a flat list. Non-list, non-data-frame elements are silently discarded.
#'
#' @param x A data frame, a (possibly nested) list, or any other R object.
#'
#' @return A flat (depth-one) list of data frames. Returns \code{NULL} if
#'   \code{x} contains no data frames.
#'
#' @details
#' Traversal follows these rules at each node:
#' \itemize{
#'   \item A data frame is returned as a one-element list and recursion stops.
#'   \item A non-data-frame list is expanded and each child is processed recursively;
#'     \code{NULL} results are dropped before the children are concatenated with \code{do.call(c, ...)}.
#'   \item Any other object (atomic vector, \code{NULL}, etc.) is discarded.
#' }
#'
#' @noRd
#' 

flatten_to_depth <- function(x) {
  
  if (is.data.frame(x)) return(list(x))
  
  if (is.list(x)) {
    children <- lapply(x, flatten_to_depth)
    # Remove NULLs and combine
    children <- children[!sapply(children, is.null)]
    if (length(children) == 0) return(NULL)
    
    return(do.call(c, children))
  }
  return(NULL)
}
