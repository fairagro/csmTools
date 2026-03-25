#' Validate DSSAT identifier format
#'
#' Checks whether a given identifier conforms to DSSAT naming conventions for
#' experiments, cultivars, fields, soil profiles, or weather stations.
#'
#' @param x Character string to validate. Returns `FALSE` if `NULL` or `NA`.
#' @param item Character string specifying identifier type. One of:
#'   \itemize{
#'     \item `"experiment"`: Experiment ID (e.g., "IBSA0101")
#'     \item `"cultivar"`: Cultivar/genotype code (e.g., "IB0001")
#'     \item `"field"`: Field identifier (e.g., "IBSA0001")
#'     \item `"soil"`: Soil profile ID (e.g., "IBSA000001" or "IB00000001")
#'     \item `"weather_station"`: Weather station code (e.g., "IBSA")
#'   }
#' @param framework Character string. Currently only `"dssat"` supported (reserved
#'   for future extension to other frameworks).
#'
#' @return Logical. `TRUE` if `x` matches the DSSAT pattern for `item`, 
#'   `FALSE` otherwise (including for `NULL` or `NA` inputs).
#'
#' @details
#' **DSSAT naming patterns:**
#' \describe{
#'   \item{**Experiment**}{4 uppercase letters + 4 digits (e.g., "UFGA8301")}
#'   \item{**Cultivar**}{2 uppercase letters + 4 digits (e.g., "IB0001")}
#'   \item{**Field**}{4 uppercase letters + 4 digits (e.g., "IBSN0001")}
#'   \item{**Soil**}{Either:
#'     \itemize{
#'       \item 4 letters + 6 digits (e.g., "IBSA000001")
#'       \item 2 letters + 8 digits (e.g., "IB00000001")
#'     }
#'   }
#'   \item{**Weather station**}{4 uppercase letters (e.g., "IBSA")}
#' }
#'
#' Patterns enforce strict formatting: exact character counts, uppercase letters,
#' and numeric digits in prescribed positions.
#'
#' @examples
#' \dontrun{
#' is_valid_dssat_id("UFGA8301", "experiment")  # TRUE
#' is_valid_dssat_id("ufga8301", "experiment")  # FALSE (lowercase)
#' is_valid_dssat_id("IB0001", "cultivar")      # TRUE
#' is_valid_dssat_id("IBSA", "weather_station") # TRUE
#' is_valid_dssat_id(NA, "field")               # FALSE
#' }
#'
#' @seealso `generate_dssat_id()`, `.resolve_dssat_exp_codes()`
#'
#' @noRd
#'

is_valid_dssat_id <- function(x, item, framework = "dssat"){
  
  if (is.null(x)) {
    return(FALSE)
  }
  
  args <- c("experiment", "cultivar", "field", "soil", "weather_station")
  item <- match.arg(item, args)
  
  pattern <- switch(
    item,
    "experiment" = "^[A-Z]{4}[0-9]{4}$",
    "cultivar" = "^[A-Z]{2}[0-9]{4}$",
    "field" = "^[A-Z]{4}[0-9]{4}$",
    "soil" = "(^[A-Z]{4}[0-9]{6}$)|(^[A-Z]{2}[0-9]{8}$)",
    "weather_station" = "^[A-Z]{4}$"
  )
  is_valid <- grepl(pattern, x)
  is_valid[is.na(is_valid)] <- FALSE
  
  return(is_valid)
}


#' Generate DSSAT-compliant identifier codes
#'
#' Creates standardized DSSAT identifiers for experiments, cultivars, fields,
#' soil profiles, or weather stations based on metadata components.
#'
#' @param type Character string specifying identifier type. One of:
#'   \itemize{
#'     \item `"experiment"`: Experiment ID (8 characters; e.g., "IBSA0101")
#'     \item `"field"`: Field identifier (8 characters; e.g., "IBSA0001")
#'     \item `"cultivar"`: Cultivar/genotype code (6 characters; e.g., "IB0001")
#'     \item `"soil"`: Soil profile ID (10 characters; e.g., "IBSA000001" or "IB00000001")
#'     \item `"weather_station"`: Weather station code (4 characters; e.g., "IBSA")
#'   }
#' @param institution Character string. Institution name or code (defaults to 
#'   "XX" if `NA`). Abbreviated to 2 uppercase letters.
#' @param site Character string. Site name or code (defaults to "XX" if `NA`).
#'   First word extracted and abbreviated to 2 uppercase letters. Not used for cultivar codes.
#' @param year Numeric or character. Year associated with identifier (defaults
#'   to "XX" if `NA`). Last 2 digits used for experiment and soil codes.
#' @param sequence_no Numeric. Sequential number for uniqueness within groups.
#'   Used for experiment, field, cultivar, and soil codes.
#'
#' @return Character string containing the generated DSSAT identifier in uppercase.
#'
#' @details
#' **Generated ID formats:**
#' \describe{
#'   \item{**Experiment**}{`IISSYYSS` - Institution (2) + Site (2) + Year (2) + 
#'         Sequence (2), e.g., "IBSA2301"}
#'   \item{**Field**}{`IISSSSSS` - Institution (2) + Site (2) + Sequence (4), e.g., "IBSA0001"}
#'   \item{**Cultivar**}{`IISSSS` - Institution (2) + Sequence (4), e.g., "IB0001"}
#'   \item{**Soil**}{`IISSYYSSSS` - Institution (2) + Site (2) + Year (2) + 
#'         Sequence (4), e.g., "IBSA230001"}
#'   \item{**Weather station**}{`IISS` - Institution (2) + Site (2), e.g., "IBSA"}
#' }
#'
#' **Abbreviation logic:**
#' \itemize{
#'   \item Institution/site names abbreviated to 2 characters via `strict_abbreviate()`
#'   \item Site names preprocessed to extract first word (`sub(" .*", "")`)
#'   \item All output converted to uppercase
#'   \item Missing values replaced with "XX" placeholders
#' }
#'
#' **Sequence numbering:**
#' \itemize{
#'   \item Experiment: 2-digit sequence (01-99)
#'   \item Field: 4-digit sequence (0001-9999)
#'   \item Cultivar: 4-digit sequence (0001-9999)
#'   \item Soil: 4-digit sequence (0001-9999)
#' }
#'
#' @examples
#' \dontrun{
#' generate_dssat_id("experiment", "ICRISAT", "Bambey", 2023, 1)
#' # Returns: "ICBA2301"
#'
#' generate_dssat_id("cultivar", "AgResults", sequence_no = 42)
#' # Returns: "AG0042"
#'
#' generate_dssat_id("weather_station", "ICRISAT", "Samanko")
#' # Returns: "ICSA"
#' }
#'
#' @seealso `is_valid_dssat_id()`, `strict_abbreviate()`, `.resolve_dssat_exp_codes()`
#'
#' @noRd
#'

generate_dssat_id <- function(type, institution, site = NA, year = NA, sequence_no = NA) {
  
  institution <- ifelse(is.na(institution), "XX", institution)
  site <- ifelse(is.na(site), "XX", site)
  year <- ifelse(is.na(year), "XX", as.character(year)) # Ensure year is character
  
  inst_abbr <- strict_abbreviate(institution, 2)
  site_abbr <- ifelse(!is.na(site), strict_abbreviate(sub(" .*", "", site), 2), NA_character_)
  
  ids <- case_when(
    type == "experiment" ~ paste0(inst_abbr, site_abbr, substr(year, 3, 4), sprintf("%02d", sequence_no)),
    type == "field" ~ paste0(inst_abbr, site_abbr, sprintf("%04d", sequence_no)),
    type == "cultivar" ~ paste0(inst_abbr, sprintf("%04d", sequence_no)),
    type == "soil" ~ paste0(inst_abbr, site_abbr, substr(year, 3, 4), sprintf("%04d", sequence_no)),
    type == "weather_station" ~ paste0(inst_abbr, site_abbr),
    TRUE ~ NA_character_
  )
  
  toupper(ids)
}
