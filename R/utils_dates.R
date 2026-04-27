#' Test whether a vector contains date or datetime values
#'
#' Applies a series of heuristic checks to determine whether \code{x} can be interpreted as a date or
#' datetime vector. Returns \code{TRUE} as soon as a format matches at least \code{threshold} of the sampled values.
#'
#' @param x A vector to test. Only character, factor, \code{Date}, \code{POSIXct},
#'   and \code{POSIXlt} vectors can return \code{TRUE}; all other types return \code{FALSE}
#' @param dssat_fmt Logical. If \code{TRUE}, also recognises the DSSAT five-digit
#'   \code{YYDDD} Julian date format (\code{"\%y\%j"}). Defaults to \code{FALSE}.
#' @param n_check Integer. Maximum number of non-missing values sampled for format testing.
#'   Defaults to \code{5}.
#' @param threshold Numeric in \code{[0, 1]}. Minimum proportion of sampled values that must parse successfully
#'   for a format to be accepted.  Defaults to \code{0.8}.
#'
#' @return A single logical value: \code{TRUE} if \code{x} is date-like, \code{FALSE} otherwise.
#'
#' @details
#' The function short-circuits at several points before attempting format parsing:
#' \enumerate{
#'   \item Non-character/factor/date types return \code{FALSE} immediately.
#'   \item After coercion to character and removal of \code{NA}/empty strings, an empty vector returns \code{FALSE}.
#'   \item Without \code{dssat_fmt}, strings lacking \code{-}, \code{/}, or \code{T} separators return \code{FALSE}.
#'   \item With \code{dssat_fmt}, an all-numeric five-character string is treated as a candidate DSSAT Julian date
#'     and proceeds to format testing.
#' }
#' Only the first \code{n_check} non-missing values are passed to \code{as.POSIXct()} to limit computation on large vectors.
#'
#' @noRd
#' 

is_date <- function(x, dssat_fmt = FALSE, n_check = 5, threshold = 0.8) {
  
  # Date-like heuristics
  if (!is.character(x) && !is.factor(x) &&
      !lubridate::is.Date(x) && !lubridate::is.POSIXct(x) && !lubridate::is.POSIXlt(x)) {
    return(FALSE)
  }
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)] # remove NAs
  if (length(x) == 0) return(FALSE)
 
  # Check for standard separators
  has_separators <- any(grepl("[-/T]", x))
  # Check if it could be a DSSAT date (5 digits, all numeric)
  is_dssat_string <- FALSE
  if (dssat_fmt && !has_separators) {
    x_non_na <- x[!is.na(x) & nzchar(x)]
    if (length(x_non_na) > 0) {
      # Check if all non-NA values are 5-digit numeric-like strings
      is_dssat_string <- all(nchar(x_non_na) == 5) && 
        !anyNA(suppressWarnings(as.numeric(x_non_na)))
    }
  }
  if (!has_separators && !is_dssat_string) {
    return(FALSE)
  }
  
  x <- head(x, n_check)
  
  formats <- c(
    "%Y-%m-%dT%H:%M:%S",  # ISO 8601 with T
    "%Y-%m-%dT%H:%M",     # ISO 8601 with T, no seconds
    "%Y-%m-%d %H:%M:%S",  # Datetime with space
    "%Y-%m-%d %H:%M",     # Datetime with space, no seconds
    "%Y-%m-%d",           # Date only
    "%m/%d/%Y",           # US format
    "%d/%m/%Y",           # European format
    "%Y/%m/%d",           # Year first with slashes
    "%Y%m%d",             # Compact YMD
    "%m%d%Y",             # Compact MDY
    "%d%m%Y",             # Compact DMY
    "%Y-%m-%dT%H:%M:%OS", # ISO 8601 with fractional seconds
    "%Y-%m-%dT%H:%M:%OSZ" # ISO 8601 with Zulu time
  )
  if (dssat_fmt) {
    formats <- c(formats, "%y%j")
  }
  
  n_valid <- 0
  for(fmt in formats) {
    res <- try(as.POSIXct(x, format = fmt, tz = "UTC"), silent = TRUE)
    if(!inherits(res, "try-error")) {
      n_valid <- sum(!is.na(res))
      if (n_valid / length(x) >= threshold) return(TRUE)
    }
  }
  FALSE
}


#' Standardise a date or datetime string to a common format
#'
#' Attempts to parse a date or datetime character string by trying a predefined set of formats 
#' and returns the value reformatted according to \code{output_format}.
#' If no format matches, the original input is returned unchanged.
#'
#' @param x A character string (or vector) representing a date or datetime.
#' @param output_format A character string passed to \code{format()} that controls the output representation,
#'   or the special value \code{"Date"} to return an R \code{Date} object. Defaults to \code{"\%Y-\%m-\%d"}.
#'
#' @return A character vector in \code{output_format}, a \code{Date} vector when \code{output_format = "Date"},
#'   or the original \code{x} if no format could be matched.
#'
#' @details
#' Parsing is attempted with \code{as.POSIXct()} under \code{tz = "UTC"} for each of the following formats, in order:
#' \itemize{
#'   \item ISO 8601 with and without seconds (\code{\%Y-\%m-\%dT\%H:\%M:\%S}, \code{\%Y-\%m-\%dT\%H:\%M})
#'   \item Space-separated datetime variants
#'   \item Date-only (\code{\%Y-\%m-\%d})
#'   \item US (\code{\%m/\%d/\%Y}), European (\code{\%d/\%m/\%Y}), and year-first (\code{\%Y/\%m/\%d}) slash formats
#'   \item Compact numeric forms (\code{\%Y\%m\%d}, \code{\%m\%d\%Y}, \code{\%d\%m\%Y})
#'   \item ISO 8601 with fractional seconds and Zulu suffix
#' }
#' The first format that produces at least one non-\code{NA} result is used.
#'
#' @noRd
#' 

standardize_date <- function(x, output_format = "%Y-%m-%d") {
  
  formats <- c(
    "%Y-%m-%dT%H:%M:%S",  # ISO 8601 with T
    "%Y-%m-%dT%H:%M",     # ISO 8601 with T, no seconds
    "%Y-%m-%d %H:%M:%S",  # Datetime with space
    "%Y-%m-%d %H:%M",     # Datetime with space, no seconds
    "%Y-%m-%d",           # Date only
    "%m/%d/%Y",           # US format
    "%d/%m/%Y",           # European format
    "%Y/%m/%d",           # Year first with slashes
    "%Y%m%d",             # Compact YMD
    "%m%d%Y",             # Compact MDY
    "%d%m%Y",             # Compact DMY
    "%Y-%m-%dT%H:%M:%OS", # ISO 8601 with fractional seconds
    "%Y-%m-%dT%H:%M:%OSZ" # ISO 8601 with Zulu time
  )
  
  for (fmt in formats) {
    res <- suppressWarnings(as.POSIXct(x, format = fmt, tz = "UTC"))
    if (any(!is.na(res))) {
      # Output as Date or as character in desired format
      if (output_format == "Date") {
        return(as.Date(res))
      } else {
        return(format(res, output_format))
      }
    }
  }

  return(x)  # If nothing worked, return original
}
