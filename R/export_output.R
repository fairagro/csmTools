#' Export dataset to output path or return in memory
#'
#' Writes a dataset to a JSON file if an output path is provided, or returns the dataset invisibly
#'  if writing was performed. If `output_path` is `NULL`, the dataset is returned as-is for in-memory use.
#'
#' @param dataset A dataset object to be exported.
#' @param output_path Either `NULL` to return the dataset in memory, or a single character string
#' specifying the output file path. Only `.json` file extensions are supported.
#'
#' @return The input `dataset`, returned visibly if `output_path` is `NULL`,
#'   or invisibly if written to file.
#'
#' @details
#' Throws an error if:
#' \itemize{
#'   \item `output_path` is a non-JSON file extension
#'   \item The JSON file fails to write (propagated from `write_json_dataset()`)
#' }
#'
#' @noRd
#' 

export_output <- function(dataset, output_path) {
  
  if (is.null(output_path)) {
    return(dataset)
  }
  
  # Write output if not null
  if (is.character(output_path) && length(output_path) == 1) {
    file_extension <- tools::file_ext(output_path)
    if (tolower(file_extension) == "json") {
      tryCatch({
        write_json_dataset(dataset, file = output_path)
      }, error = function(e) {
        stop(paste("Failed to write JSON file from", dataset, "Error:", e$message))
      })
    } else {
      stop(paste("Output must be a JSON file:", output_path))
    }
  }
  
  return(invisible(dataset))
}


#' Write an R list dataset to a JSON file
#'
#' Serializes an R list to JSON, preserving R object attributes (e.g., class, custom attributes)
#' that would otherwise be lost. Recursively processes nested lists, data frames, and atomic
#' vectors, wrapping objects with attributes into a `{data, attributes}` structure in JSON.
#'
#' @param dataset A list representing the dataset to serialize. Throws an error if not a list.
#' @param file A single character string specifying the output JSON file path.
#' @param pretty Logical. Whether to pretty-print the JSON output. Default: `TRUE`.
#' @param auto_unbox Logical. Whether to automatically unbox single-element vectors in JSON
#'   output. Passed to `jsonlite::write_json()`. Default: `TRUE`.
#' @param ... Additional arguments passed to `jsonlite::write_json()`.
#'
#' @return Called for its side effect of writing a JSON file. Returns `NULL` invisibly.
#'
#' @details
#' **Wrapping logic** — an object is wrapped into `list(data = ..., attributes = ...)` if it is:
#' \itemize{
#'   \item A data frame or tibble
#'   \item A list or atomic vector with non-standard attributes (i.e., beyond `names`,
#'     `row.names`, `class`, `reference`)
#'   \item A named atomic vector
#' }
#'
#' **Preserved attributes** written under `attributes`:
#' \itemize{
#'   \item `_class_attribute`: the object's class
#'   \item `_names_attribute`: names of atomic vectors
#'   \item Any other custom attributes, processed recursively
#' }
#'
#' Data frames are coerced to lists via `as.list()` without recursing into columns.
#' Objects already of class `json` are treated as leaf nodes and written as-is.
#'
#' @noRd
#'

write_json_dataset <- function(dataset, file, pretty = TRUE, auto_unbox = TRUE, ...) {
  
  if (!is.list(dataset)) {
    stop("Input 'dataset' must be an R list.")
  }
  
  # Recursive helper to process individual R objects for JSON serialization
  object_to_json <- function(obj) {
    obj_attrs <- attributes(obj)
    
    # Check if the object should be wrapped
    should_wrap <- (is.data.frame(obj) || tibble::is_tibble(obj)) ||
      (length(obj_attrs) > 0 &&
         !all(names(obj_attrs) %in% c("names", "row.names", "class", "reference"))) ||
      (is.atomic(obj) && !is.null(names(obj)) && !inherits(obj, "json"))
    
    if (should_wrap) {
      
      # --- Handle attributes ---
      filtered_attrs_list <- list()
      
      # Handle class
      if ("class" %in% names(obj_attrs)) {
        filtered_attrs_list[["_class_attribute"]] <- obj_attrs[["class"]]
      }
      # Handle atomic names
      if (is.atomic(obj) && !is.null(names(obj))) {
        filtered_attrs_list[["_names_attribute"]] <- names(obj)
      }
      
      # Handle custom attributes (recursive)
      attrs_to_exclude <- c("names", "row.names", "class", "reference")
      for (attr_name in setdiff(names(obj_attrs), attrs_to_exclude)) {
        filtered_attrs_list[[attr_name]] <- object_to_json(obj_attrs[[attr_name]])
      }
      
      # --- Handle data ---
      if (is.data.frame(obj) || tibble::is_tibble(obj)) {
        # No recursion for data frames (no capture of column-level attributes)
        data_for_json <- as.list(obj)
      } else if (is.list(obj)) {
        # It's a list with attributes. Process its children recursively.
        data_for_json <- lapply(obj, object_to_json)
      } else {
        # Atomic vector / other leaves
        data_for_json <- obj
      }
      
      return(list(
        data = data_for_json,
        attributes = if (length(filtered_attrs_list) > 0) filtered_attrs_list else list()
      ))
      
    } else if (is.list(obj) && !inherits(obj, "json")) {
      
      # --- Standard list (no attributes) ---
      lapply(obj, object_to_json)
      
    } else {
      # --- Leaf nodes ---
      obj
    }
  }
  
  # Start recursion
  json_data <- object_to_json(dataset)
  
  # Write to file
  jsonlite::write_json(json_data, path = file, pretty = pretty, auto_unbox = auto_unbox, ...)
}

