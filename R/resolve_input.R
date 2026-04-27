#' Resolve and normalise a dataset input
#'
#' Accepts a dataset in one of several forms — a file path or an in-memory R object — and returns a consistently
#' structured R object ready for further processing.
#'
#' @param x One of the following:
#' \describe{
#'   \item{Character string}{Path to a \code{.json}, \code{.csv}, or \code{.yaml} file to be read from disk.}
#'   \item{Named list}{An in-memory dataset, optionally in a nested DSSAT write-ready format. Nested columns are unnested
#'           (recursively) and any custom attributes are preserved.}
#' }
#'
#' @return A data frame (for single-table file inputs) or a named list of data frames (for JSON or list inputs),
#'   with custom attributes restored.
#'
#' @details
#' For list inputs the function:
#' \enumerate{
#'   \item Captures any custom attributes attached to each element.
#'   \item Identifies nested columns via \code{apply_recursive()} and \code{identify_nested_cols()}.
#'   \item Recursively unnests those columns with \code{tidyr::unnest()}, walking the list tree depth-first.
#'   \item Restores the saved custom attributes on the resulting data frames.
#' }
#'
#' @noRd
#'

resolve_input <- function(x) {
  
  resolved_data <- NULL
  
  if (is.character(x) && length(x) == 1) {
    if (file.exists(x)) {
      file_extension <- tools::file_ext(x)
      if (tolower(file_extension) == "json") {
        tryCatch({
          resolved_data <- read_json_dataset(file = x)
        }, error = function(e) {
          stop(paste("Failed to load JSON file from", x, "Error:", e$message))
        })
      }
      else if (tolower(file_extension) == "csv") {
        resolved_data <- read.csv(file = x)
      }
      else if (tolower(file_extension) == "yaml") {
        resolved_data <- yaml::read_yaml(file = x)
      }
      else {
        stop(paste("Input format not handled", x))
      }
    } else {
      stop(paste("Input file not found:", x))
    }
  }

  else if (!is.data.frame(x) && is.list(x)) {
    
    # Preserve custom attributes
    attr <- lapply(x, function(obj) {
      attributes(obj)[!names(attributes(obj)) %in% c("names", "row.names", "class")]
    })
    
    # Unwrap nested structure (DSSAT write-ready formats)
    nested_cols <- apply_recursive(x, identify_nested_cols)
  
    
    .unnest_recursive <- function(data_node, names_node) {
      
      # CASE 1: We reached a DataFrame (The "Leaf")
      if (is.data.frame(data_node)) {
        # Check if there are columns to unnest for this specific dataframe
        if (length(names_node) > 0) {
          # Use all_of() to ensure safety if names_node is a character vector
          data_unnest <- tidyr::unnest(data_node, cols = dplyr::all_of(names_node))
          return(data_unnest)
        } else {
          return(data_node)
        }
      }
      
      # CASE 2: We are at a List (The "Branch")
      if (is.list(data_node)) {
        # Call this function again on the children
        # This walks down both lists simultaneously
        return(purrr::map2(data_node, names_node, .unnest_recursive))
      }
      
      # Fallback (e.g. if data_node is just a loose number or NULL)
      return(data_node)
    }
    
    # --- Usage ---
    x <- .unnest_recursive(x, nested_cols)
    # x <- purrr::map2(x, nested_cols, ~ {
    #   if (length(.y) > 0) {
    #     tidyr::unnest(.x, cols = .y)
    #   } else {
    #     .x
    #   }
    # })
    
    # Restore attributes
    x <- purrr::map2(x, attr, ~{
      attributes(.x) <- c(attributes(.x), .y)
      return(.x)
    })
    
    resolved_data <- x
  }
  else {
    stop("Unsupported input type. Input must be a character string (JSON filepath) or an R list.")
  }
  
  return(resolved_data)
}


#' Remove named attributes from an R object
#'
#' Strips one or more named attributes from any R object that supports \code{attributes()},
#' leaving all other attributes intact.
#'
#' @param obj Any R object.
#' @param names_to_remove A character vector of attribute names to remove.
#'
#' @return \code{obj} with the specified attributes removed. If \code{obj} has no attributes, it is returned unchanged.
#'
#' @noRd
#' 

remove_named_attributes <- function(obj, names_to_remove) {

  if (!is.null(attributes(obj))) {
    attrs <- attributes(obj)
    attrs <- attrs[!names(attrs) %in% names_to_remove]
    attributes(obj) <- attrs
  }
  return(obj)
}


#' Identify columns containing nested vectors in a JSON-derived object
#'
#' Recursively traverses a nested list or environment (typically parsed from JSON) and returns the names of any elements
#' that are matrices or higher-dimensional arrays with a single leading dimension, indicating a nested vector that may
#' require special handling during unnesting.
#'
#' @param json_obj A list, environment, or other R object to inspect, typically the output of a JSON parser.
#'
#' @return A character vector of unique column names (leaf-level keys) whose values are nested vectors.
#'   Returns \code{character(0)} if none are found.
#'
#' @noRd
#' 

.identify_nested_vector_cols <- function(json_obj) {
  # Recursive helper to traverse the object
  find_nested <- function(x, parent_path = "") {
    if (is.null(x) || (is.atomic(x) && length(x) <= 1)) {
      return(NULL)
    }
    
    # Check if this is a nested vector (matrix/array with dim > 1)
    if ((is.matrix(x) || is.array(x)) && length(dim(x)) > 1 && dim(x)[1] == 1) {
      return(tail(strsplit(parent_path, "\\$")[[1]], 1))  # Extract last part of path
    }
    
    # Recursively check list/environment children
    if (is.list(x) || is.environment(x)) {
      results <- c()
      for (nm in names(x)) {
        child_path <- if (parent_path == "") nm else paste0(parent_path, "$", nm)
        child_result <- find_nested(x[[nm]], child_path)
        if (!is.null(child_result)) {
          results <- c(results, child_result)
        }
      }
      return(results)
    }
    
    return(NULL)
  }
  
  # Flatten results and extract unique column names
  nested_names <- find_nested(json_obj)
  if (is.null(nested_names)) {
    return(character(0))  # Return empty vector if nothing found
  } else {
    return(unique(unlist(nested_names)))
  }
}


#' Read a DSSAT-compatible dataset from a JSON file
#'
#' Parses a JSON file and reconstructs the original R object hierarchy, restoring data frames
#' (including tibbles), list-columns, custom class attributes, and any other metadata that was serialised alongside the data.
#'
#' @param file_path A length-one character string giving the path to the \code{.json} file to read.
#'
#' @return A named list mirroring the top-level structure of the JSON file. Each element is reconstructed as its original R class
#'   (\code{data.frame}, \code{tbl_df}, etc.) with custom attributes reattached. List-columns that were stored as nested JSON arrays are
#'   rewrapped as R list-columns.
#'
#' @details
#' The function assumes the JSON was produced by a paired writer that stores each R object as \code{\{data: ..., attributes: ...\}}.
#' The reconstruction logic follows these steps:
#' \enumerate{
#'   \item \code{jsonlite::fromJSON()} reads the file with
#'     \code{simplifyVector = TRUE} and \code{simplifyDataFrame = FALSE} to retain control over the structure.
#'   \item \code{.identify_nested_vector_cols()} scans the raw parse tree for columns that must be rewrapped as list-columns.
#'   \item An internal recursive function \code{object_from_json()} walks the tree, dispatching on the presence of \code{data}/\code{attributes} keys
#'     to rebuild data frames, or falling through to process generic lists and
#'     atomic values.
#'   \item The original R class vector (stored under
#'     \code{_class_attribute}) is restored, followed by any remaining custom
#'     attributes.
#' }
#'
#' @seealso \code{\link{.identify_nested_vector_cols}}
#' 
#' @importFrom tibble tibble as_tibble
#' @importFrom jsonlite fromJSON
#'
#' @export
#' 

read_json_dataset <- function(file_path) {
  # Read the entire JSON file as a nested list.
  # simplifyVector = TRUE: Tries to simplify JSON arrays into R atomic vectors.
  # simplifyDataFrame = FALSE: Prevents jsonlite from creating data.frames immediately,
  #                            allowing more control over structure.
  # flatten = FALSE: Maintains the nested structure.
  raw_json_data <- jsonlite::fromJSON(file_path, simplifyVector = TRUE, simplifyDataFrame = FALSE, flatten = FALSE)
  
  # Identify nested vector columns in the whole dataset for downstream wrapping
  nested_cols <- .identify_nested_vector_cols(raw_json_data)
  
  # Recursive function to reconstruct R objects from the parsed JSON
  object_from_json <- function(json_obj) {
    if (is.list(json_obj) && "data" %in% names(json_obj) && "attributes" %in% names(json_obj)) {
      # This block handles data frames / tibbles based on the "data" and "attributes" convention
      
      data_part <- json_obj$data
      attrs_part <- json_obj$attributes
      
      class_attr <- NULL
      reconstructed_attrs <- list()
      
      # Attempt to extract original R class attribute
      if (is.character(attrs_part) && length(attrs_part) > 0 &&
          any(c("data.frame", "tbl_df", "tibble") %in% attrs_part)) {
        class_attr <- attrs_part
      } else if (is.list(attrs_part) && "_class_attribute" %in% names(attrs_part)) {
        class_attr <- attrs_part[["_class_attribute"]]
        attrs_part[["_class_attribute"]] <- NULL # Remove handled attribute
      }
      
      # Handle other attributes (if any, like names or custom ones)
      if (is.list(attrs_part)) {
        # Recursively reconstruct other custom attributes
        reconstructed_attrs <- lapply(attrs_part, object_from_json)
      }
      
      # Process data_part (the actual column data)
      if (!is.null(class_attr) && any(c("data.frame", "tbl_df", "tibble") %in% class_attr)) {
        if (!is.null(data_part)) {
          processed_columns <- lapply(names(data_part), function(col_name) {
            col <- data_part[[col_name]]
            
            # Convert JSON 'null' values to NA_character_ (Note: potentially problematic!)
            if (is.null(col)) {
              return(NA_character_)
            }
            
            # Conditionally wrap atomic vectors into a list to create list-columns
            if (is.matrix(col) && nrow(col) == 1 && is.atomic(col)) {
              # Flatten 1-row matrices (e.g., chr [1,1:2]) to vectors (e.g., chr [1:2])
              col <- as.vector(col)
            }
            if (col_name %in% nested_cols && is.atomic(col) && !is.list(col)) {
              # Ensure we only wrap non-empty atomic vectors designated as list-columns
              if (length(col) > 0 || !is.null(col)) { # Ensure it's not trying to wrap a truly empty NULL
                return(list(col))
              }
            }
            
            return(col)
          })
          names(processed_columns) <- names(data_part) # Re-apply original column names
          
          # Convert to tibble
          reconstructed_obj <- tibble::as_tibble(processed_columns)
          
        } else {
          # Empty tibble if data is NULL
          reconstructed_obj <- tibble::tibble()
        }
        # Apply the original classes
        class(reconstructed_obj) <- class_attr
      } else {
        # If it's not a data.frame/tibble structure, just return the data_part,
        # after recursively processing it if it's a list
        reconstructed_obj <- lapply(data_part, object_from_json)
        # Apply generic attributes if available
        if (!is.null(attrs_part)) {
          current_attrs <- attributes(reconstructed_obj)
          attributes(reconstructed_obj) <- c(current_attrs, reconstructed_attrs)
        }
      }
      
      # Apply any remaining custom attributes that were recursively reconstructed
      if (length(reconstructed_attrs) > 0) {
        current_attrs <- attributes(reconstructed_obj)
        # Filter out NULL attributes that might have resulted from stripping _class_attribute etc.
        reconstructed_attrs <- reconstructed_attrs[!sapply(reconstructed_attrs, is.null)]
        if(length(reconstructed_attrs) > 0) {
          attributes(reconstructed_obj) <- c(current_attrs, reconstructed_attrs)
        }
      }
      
      return(reconstructed_obj)
      
    } else if (is.list(json_obj) && !inherits(json_obj, c("data.frame", "tbl_df", "tibble", "json"))) {
      # Generic R list (recursive processing for its elements)
      lapply(json_obj, object_from_json)
    } else {
      # Atomic objects, unwrapped data frames (if jsonlite simplified directly), or already processed JSON.
      return(json_obj)
    }
  }
  
  # Apply the recursive reconstruction to the parsed JSON data
  dataset_dssat <- lapply(raw_json_data, object_from_json)
  
  return(dataset_dssat)
}
