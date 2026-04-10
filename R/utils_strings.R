#' Find the longest common prefix in a character vector
#'
#' @param strings A character vector.
#'
#' @return A character string of the longest common prefix.
#' 
#' @noRd
#' 

find_common_prefix <- function(strings) {
  
  common_prefix <- strings[1]
  for (str in strings) {
    while (!startsWith(str, common_prefix) && nchar(common_prefix) > 0) {
      common_prefix <- substr(common_prefix, 1, nchar(common_prefix) - 1)
    }
  }
  return(common_prefix)
}


#' Abbreviate strings with strict length enforcement
#'
#' Creates a standard abbreviation that transliterate non-ASCII to ASCII equivalent and
#' strictly enforce minimum length by force truncating the output
#'
#' @param string Character vector to abbreviate.
#' @param minlength Integer. The desired length of the abbreviation.
#' @param na_replace Character. The value used to replace `NA` entries in the original vector.
#'   Defaults to "XX".
#'
#' @return A character vector of the same length as `string`.
#' 
#' @note Because this function force-truncates the output to `minlength`, the resulting abbreviations
#'   are not guaranteed to be unique if multiple input strings share the same starting characters.
#'
#' 
#' @noRd
#' 

strict_abbreviate <- function(string, minlength = 2, na_replace = "XX") {
  
  out <- string
  is_missing <- is.na(out)
  
  # Transliterate and remove punctuation
  out <- stringi::stri_trans_general(out, "Latin-ASCII")
  out <- gsub("[[:punct:]]", "", out)
  
  # Abbreviate
  out <- abbreviate(out, minlength = minlength, strict = TRUE)
  
  # Truncate in case abbrev > minlength despite 'strict'
  # Handles multiple institution cases (only first one is kept)
  out <- substr(out, 1, minlength)
  
  # NA handling
  out[is_missing] <- na_replace
  
  return(unname(out))
}

