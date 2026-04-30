#' Format ICASA Column Headers
#'
#' @description
#' Swaps column headers between ICASA long 'variable name' format and short 'display code' format.
#'
#' @details
#' This utility function fetches the official ICASA dictionary from the URL specified in the configuration.
#' It then builds a lookup table to rename columns in the dataset to the desired `header_type`.
#'
#' @param dataset The ICASA dataset as a named list of data frames.
#' @param header_type The target header style, either `"short"` or `"long"`.
#'
#' @return The dataset with renamed headers.
#' 
#' @importFrom yaml read_yaml
#' @importFrom dplyr mutate select
#' 
#' @export
#'

map_icasa_headers <- function(dataset, header_type = "short") {
  
  # Fetch ICASA dictionary
  dict <- fetch_dictionary("icasa")
  
  # Keep only 'header' sections and filter
  header_sec <- c("Metadata", "Management_info", "Soils_data", "Weather_data", "Measured_data")
  header_dict <- dict[names(dict) %in% header_sec]
  header_dict <- do.call(bind_rows, header_dict) |>
    mutate(long_name = Variable_Name, short_name = Code_Display) |>
    select(long_name, short_name)

  # --- 3. Create Name Lookup and Rename ---
  name_lookup <- if (header_type == "short") {
    setNames(header_dict$short_name, header_dict$long_name)
  } else {
    setNames(header_dict$long_name, header_dict$short_name)
  }
  
  for (i in seq_along(dataset)) {
    current_names <- names(dataset[[i]])
    # Find which of the current column names exist in our lookup table
    names_to_swap <- intersect(current_names, names(name_lookup))
    
    if (length(names_to_swap) > 0) {
      # Replace only the names that were found
      match_indices <- match(names_to_swap, current_names)
      current_names[match_indices] <- name_lookup[names_to_swap]
      names(dataset[[i]]) <- current_names
    }
  }
  
  return(dataset)
}
