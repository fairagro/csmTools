#' Extract custom attributes into a flat list using local-level names
#'
#' Recursively traverses a nested list object and collects non-structural attributes at each level
#' into a single flat list, keyed by local node name.
#'
#' @param obj A list object (or any R object) to inspect for custom attributes.
#' @param node_name A string giving the key to assign to the current level's attributes in the output list.
#'   Defaults to \code{"root"}.
#'
#' @details Structural attributes (\code{names}, \code{row.names}, \code{dim}, \code{dimnames}, \code{class},
#'   \code{levels}) are ignored at every level. Recursion proceeds into list elements but stops at data frames.
#'   Unnamed list elements are keyed by their integer index (as a character string). If multiple nodes share the
#'   same local name, later entries will overwrite earlier ones in the flat output.
#'
#' @return A named flat list where each element is itself a named list of custom attributes belonging to that node.
#'   Nodes without custom attributes are omitted.
#'
#' @noRd
#' 

.store_custom_attributes <- function(obj, node_name = "root") {
  
  # Get attributes and remove structural attributes
  attrs <- attributes(obj)
  metadata <- setdiff(names(attrs), c("names", "row.names", "dim", "dimnames", "class", "levels"))
  
  out <- list()
  # Store custom attributes
  if (length(metadata) > 0) {
    out[[node_name]] <- attrs[metadata]
  }
  
  # Recurse if it is a list, but stop at data frames
  if (is.list(obj) && !is.data.frame(obj)) {
    
    obj_names <- names(obj)
    
    for (i in seq_along(obj)) {
      
      if (!is.null(obj_names) && !is.na(obj_names[i]) && obj_names[i] != "") {
        child_name <- obj_names[i]
      } else {
        # Use index if unnamed
        child_name <- as.character(i)
      }
      
      # Recurse and flatten
      out <- c(out, .store_custom_attributes(obj[[i]], node_name = child_name))
    }
  }
  
  return(out)
}


#' Restore metadata from a flat list via Name Matching
#' 
#' Traverses an object. If a node's name matches a key in the metadata list,
#' the attributes are merged.
#' 
#' @param obj The target object to restore attributes to.
#' @param metadata The flat list of attributes (output of get_flat_metadata).
#' @param node_name The name of the current node (default "root").
#' @return The object with metadata restored.
#'
#' @noRd
#' 

.restore_custom_attributes <- function(obj, metadata, node_name = "root") {
  
  # Restore metadata to current level
  if (node_name %in% names(metadata)) {
    # Note: grabs the first match if duplicate keys
    attrs <- metadata[[node_name]]
    
    for (attr_name in names(attrs)) {
      attr(obj, attr_name) <- attrs[[attr_name]]
    }
  }
  
  # Recurse to children, treating data frames as leaves
  if (is.list(obj) && !is.data.frame(obj)) {
    
    obj_names <- names(obj)
    
    for (i in seq_along(obj)) {
      
      if (!is.null(obj_names) && !is.na(obj_names[i]) && obj_names[i] != "") {
        curr_child_name <- obj_names[i]
      } else {
        # Use index if unnamed
        curr_child_name <- as.character(i)
      }
      
      # Recursive call
      obj[[i]] <- .restore_custom_attributes(obj[[i]], metadata, node_name = curr_child_name)
    }
  }
  
  return(obj)
}
