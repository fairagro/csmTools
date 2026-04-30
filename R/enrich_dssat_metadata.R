#' Enrich DSSAT components with complete metadata
#'
#' Fills gaps in soil and weather metadata by imputation from experiment data,
#' then enriches all components with spatial information from external APIs.
#'
#' @param dssat_components Named list containing METADATA, SOIL, and WEATHER
#'   data frames extracted from a DSSAT dataset
#'
#' @return Named list with the same structure as input, containing enriched
#'   data frames with complete metadata and spatial information formatted for
#'   DSSAT code generation
#'
#' @details
#' Processing pipeline:
#' \enumerate{
#'   \item Imputes missing soil metadata from experiment data
#'   \item Imputes missing weather metadata from experiment data
#'   \item Enriches all components with spatial data via API calls
#'   \item Formats spatial information into DSSAT-compliant fields
#' }
#'
#' Spatial enrichment uses latitude/longitude columns: YCRD/XCRD (management),
#' LAT/LONG (soil and weather).
#'
#' @noRd
#'

enrich_dssat_metadata <- function(dssat_components) {
  
  # TODO: apply workaround when LAT and LONG from FIELDS are missing
  exp <- dssat_components$METADATA
  sol <- dssat_components$SOIL
  wth <- dssat_components$WEATHER

  # --- Impute missing metadata ---
  sol_imputed <- .impute_sol_metadata(sol, exp)
  wth_imputed <- .impute_wth_metadata(wth, exp)
  
  # --- Enrich with Spatial API Data ---
  # TODO: wrap in if statements?
  exp_locations <- enrich_geodata(exp, lat_col = "YCRD", lon_col = "XCRD")
  sol_locations <- enrich_geodata(sol_imputed, lat_col = "LAT", lon_col = "LONG")
  wth_locations <- enrich_geodata(wth_imputed, lat_col = "LAT", lon_col = "LONG")
  
  # --- Format Spatial Data into DSSAT Fields ---
  exp_dssat_fmt <- .format_exp_locations(exp_locations)
  sol_dssat_fmt <- .format_sol_locations(sol_locations)
  wth_dssat_fmt <- .format_wth_locations(wth_locations)
  
  # --- Return Final List ---
  out <- list(
    METADATA = exp_dssat_fmt,
    SOIL = sol_dssat_fmt,
    WEATHER = wth_dssat_fmt
  )
  
  return(out)
}


#' Impute missing soil metadata from experiment data
#'
#' Fills gaps in soil data by joining with experiment metadata and computing
#' derived fields where direct values are unavailable.
#'
#' @param sol Data frame containing soil profile data (SOIL component)
#' @param exp Data frame containing experiment metadata (METADATA)
#'
#' @return Soil data frame with imputed metadata fields, reordered to place
#'   key identifiers/attributes (EXP_ID, PEDON, INST_NAME, YEAR, TEXTURE) first
#'
#' @details
#' Imputation logic:
#' \itemize{
#'   \item **INST_NAME**: Uses INSTITUTION from experiment data if missing
#'   \item **YEAR**: Derived from DATE field (format: yyddd), falls back to
#'         EXP_YEAR if unavailable
#'   \item **LAT/LONG**: Uses experiment coordinates (YCRD/XCRD) if missing
#'   \item **TEXTURE**: Computed from clay/silt fractions (SLCL, SLSI) using
#'         `get_soil_texture()` helper if not provided
#' }
#'
#' Joins soil and experiment data on all common columns to preserve
#' relationships while avoiding duplication.
#'
#' @noRd
#'

.impute_sol_metadata <- function(sol, exp) {

  joined <- dplyr::left_join(sol, exp, by = intersect(names(sol), names(exp)))
  nms <- names(joined)

  joined |>
    dplyr::mutate(
      INST_NAME = if ("INST_NAME" %in% nms) INST_NAME else NA_character_,
      SOL_INSTITUTION = if ("SOL_INSTITUTION" %in% nms) SOL_INSTITUTION else "XX",
      INSTITUTION = if ("INSTITUTION" %in% nms) INSTITUTION else "XX",
      INST_NAME = dplyr::coalesce(INST_NAME, INSTITUTION),
      YEAR_FROM_DATE = if ("DATE" %in% nms) {
        suppressWarnings(
          as.character(lubridate::year(as.Date(as.character(DATE), format = "%y%j")))
        )
      } else {
        NA_character_
      },
      YEAR = if ("YEAR" %in% nms) YEAR else NA_character_,
      EXP_YEAR = if ("EXP_YEAR" %in% nms) EXP_YEAR else "XXXX",
      YEAR = dplyr::coalesce(YEAR, YEAR_FROM_DATE, EXP_YEAR),
      LAT = if ("LAT" %in% nms) LAT else NA_real_,
      YCRD = if ("YCRD" %in% nms) YCRD else NA_real_,
      LAT = dplyr::coalesce(LAT, YCRD),
      LONG = if ("LONG" %in% nms) LONG else NA_real_,
      XCRD = if ("XCRD" %in% nms) XCRD else NA_real_,
      LONG = dplyr::coalesce(LONG, XCRD),
      TEXTURE = if ("TEXTURE" %in% nms) {
        TEXTURE
      } else if (all(c("SLCL", "SLSI") %in% nms) &&
                 !any(is.null(SLCL), is.null(SLSI)) &&
                 !any(is.na(SLCL), is.na(SLSI)) &&
                 !any(SLCL == -99, SLSI == -99)) {
        get_soil_texture(
          clay_frac = SLCL,
          silt_frac = SLSI,
          sand_frac = 1.0 - SLCL - SLSI
        )
      } else {
        NA_character_
      }
    ) |>
    dplyr::select(
      dplyr::all_of(intersect(c("EXP_ID", "PEDON", "INST_NAME", "YEAR", "LAT", "LONG", "TEXTURE"), nms)),
      dplyr::all_of(setdiff(names(sol), c("EXP_ID", "PEDON", "INST_NAME", "YEAR", "LAT", "LONG", "TEXTURE"))),
    )
}


#' Impute missing weather metadata from experiment datas
#'
#' Fills gaps in weather data by joining with experiment metadata and computing
#' derived fields where direct values are unavailable.
#'
#' @param wth Data frame containing weather station data (WEATHER component)
#' @param exp Data frame containing experiment metadata (METADATA)
#'
#' @return Weather data frame with imputed metadata fields, reordered to place
#'   key identifiers (EXP_ID, INSI, LAT, LONG, YEAR) first
#'
#' @details
#' Imputation logic:
#' \itemize{
#'   \item **INSI**: Uses INSTITUTION from experiment data if missing
#'   \item **LAT/LONG**: Uses experiment coordinates (YCRD/XCRD) if missing
#'   \item **YEAR**: Extracted from first 2 characters of DATE field (format: yyddd)
#' }
#'
#' Joins weather and experiment data on all common columns to preserve
#' relationships while avoiding duplication.
#'
#' @noRd
#'

.impute_wth_metadata <- function(wth, exp) {

  joined <- dplyr::left_join(wth, exp, by = intersect(names(wth), names(exp)))
  nms <- names(joined)

  joined |>
    dplyr::mutate(
      INSI = if ("INSI" %in% nms) INSI else NA_character_,
      WTH_INSTITUTION = if ("WTH_INSTITUTION" %in% nms) WTH_INSTITUTION else "XX",
      INSTITUTION = if ("INSTITUTION" %in% nms) INSTITUTION else "XX",
      INSI = dplyr::coalesce(INSI, INSTITUTION),
      LAT = if ("LAT" %in% nms) LAT else NA_real_,
      YCRD = if ("YCRD" %in% nms) YCRD else NA_real_,
      LAT = dplyr::coalesce(LAT, YCRD),
      LONG = if ("LONG" %in% nms) LONG else NA_real_,
      XCRD = if ("XCRD" %in% nms) XCRD else NA_real_,
      LONG = dplyr::coalesce(LONG, XCRD),
      YEAR = if ("YEAR" %in% nms) YEAR else "XXXX",
      YEAR = substr(DATE, 1, 2),
    ) |>
    dplyr::select(
      dplyr::all_of(intersect(c("EXP_ID", "INSI", "LAT", "LONG", "YEAR"), nms)),
      dplyr::all_of(setdiff(names(wth), c("EXP_ID", "INSI", "LAT", "LONG", "YEAR"))),
    )
}


#' Format experiment location data for DSSAT
#'
#' Constructs DSSAT-compliant location fields by combining spatial enrichment
#' data with existing experiment metadata.
#'
#' @param exp_locations Data frame containing experiment metadata enriched with
#'   spatial data from `enrich_geodata()`
#'
#' @return Data frame with formatted location fields suitable for DSSAT file
#'   generation
#'
#' @details
#' Field construction:
#' \itemize{
#'   \item **FLNAME**: Uses existing field name, falls back to uppercase
#'         ShortLabel from spatial API
#'   \item **SITE**: Composite field combining:
#'         \itemize{
#'           \item Address (FLNAME, City, Region, CntryName) if SITE missing
#'           \item Rounded coordinates (XCRD, YCRD to 2 decimal places)
#'           \item Rounded elevation (ELEV to 2 decimal places)
#'         }
#'         All components separated by semicolons, NA values excluded
#'   \item **ELEV**: Uses existing elevation, falls back to API-provided value
#' }
#'
#' The SITE field format ensures DSSAT files contain complete, human-readable
#' location information while preserving precise coordinates.
#'
#' @noRd
#'

.format_exp_locations <- function(exp_locations) {

  nms <- names(exp_locations)

  exp_locations |>
    dplyr::mutate(
      INSTITUTION = if ("INSTITUTION" %in% nms) INSTITUTION else "XX",
      FLNAME = if ("FLNAME" %in% nms) FLNAME else NA_character_,
      FLNAME = ifelse(is.na(FLNAME), toupper(ShortLabel), FLNAME),
      # Fill site as address if missing
      SITE = if ("SITE" %in% nms) SITE else NA_character_,
      SITE_addr = tidyr::unite(data.frame(FLNAME, City, Region, CntryName),
                               "SITE", sep = ", ", na.rm = TRUE)$SITE,
      SITE = ifelse(is.na(SITE), SITE_addr, SITE),
      ELEV = if ("ELEV" %in% nms) ELEV else NA_real_,
      ELEV = dplyr::coalesce(ELEV, elevation)
    ) |>
    dplyr::mutate(
      XCRD_tmp = round(XCRD, 2),
      YCRD_tmp = round(YCRD, 2),
      ELEV_tmp = round(ELEV, 2)
    ) |>
    tidyr::unite("SITE", SITE, XCRD_tmp, YCRD_tmp, ELEV_tmp, sep = "; ", na.rm = TRUE, remove = TRUE)
}


#' Format soil location data for DSSAT
#'
#' Constructs DSSAT-compliant location fields for soil profiles by combining
#' spatial enrichment data with existing soil metadata.
#'
#' @param sol_locations Data frame containing soil data enriched with spatial
#'   data from `enrich_geodata()`
#'
#' @return Data frame with formatted location fields suitable for DSSAT soil
#'   file generation, grouped by PEDON
#'
#' @details
#' Field construction (per PEDON):
#' \itemize{
#'   \item **COUNTRY**: Uses existing country code, falls back to API-provided
#'         CountryCode. Converts ISO3 codes to ISO2 format using
#'         `countrycode::countrycode()` if COUNTRY field doesn't exist
#'   \item **SITE**: Uses existing site name, falls back to "City, Region"
#'         from spatial API. Comma-separated format for consistency
#'   \item **ELEV**: Uses existing elevation, falls back to API-provided value
#' }
#'
#' Groups output by PEDON to ensure profile-level aggregation for subsequent
#' DSSAT soil file operations.
#'
#' @noRd
#' 

.format_sol_locations <- function(sol_locations) {

  nms <- names(sol_locations)
  has_country <- "COUNTRY" %in% nms
  has_site <- "SITE" %in% nms

  sol_locations |>
    dplyr::group_by(PEDON) |>
    dplyr::mutate(
      COUNTRY = if (has_country) COUNTRY else NA_character_,
      COUNTRY = if (has_country)
        dplyr::coalesce(COUNTRY, CountryCode)
      else
        countrycode::countrycode(CountryCode, origin = "iso3c", destination = "iso2c"),
      SITE = if (has_site) SITE else NA_character_,
      SITE = if (has_site)
        dplyr::coalesce(SITE, paste(City, Region, sep = ","))
      else
        paste(City, Region, sep = ", "),
      ELEV = if ("ELEV" %in% nms) ELEV else NA_real_,
      ELEV = dplyr::coalesce(ELEV, elevation)
    )
}


#' Format weather station location data for DSSAT
#'
#' Constructs DSSAT-compliant location fields for weather stations by combining
#' spatial enrichment data with existing weather metadata.
#'
#' @param wth_locations Data frame containing weather data enriched with spatial
#'   data from `enrich_geodata()`
#'
#' @return Data frame with formatted location fields suitable for DSSAT weather
#'   file generation, grouped by station (INSI, LAT, LONG)
#'
#' @details
#' Field construction (per weather station):
#' \itemize{
#'   \item **WST_NAME**: Uses existing weather station name, falls back to
#'         concatenated location string (ShortLabel, City, Region, CntryName)
#'         from spatial API, comma-separated with NA values excluded
#'   \item **ELEV**: Uses existing elevation, falls back to API-provided value
#'   \item **REFHT**: Reference height for temperature/humidity measurements.
#'         Initialized as NA if not present (user must specify)
#'   \item **WNDHT**: Wind measurement height. Initialized as NA if not present
#'         (user must specify)
#' }
#'
#' Groups output by station identifiers (INSI, LAT, LONG) to ensure proper
#' aggregation for DSSAT weather file metadata sections.
#'
#' @noRd
#'

.format_wth_locations <- function(wth_locations) {

  nms <- names(wth_locations)

  wth_locations |>
    dplyr::group_by(INSI, LAT, LONG) |>
    dplyr::mutate(
      WST_NAME = if ("WST_NAME" %in% nms) WST_NAME else NA_character_,
      WST_NAME = dplyr::coalesce(WST_NAME,
                                 tidyr::unite(data.frame(ShortLabel, City, Region, CntryName),
                                              "WST_SITE", sep = ", ", na.rm = TRUE)$WST_SITE),
      ELEV = if ("ELEV" %in% nms) ELEV else NA_real_,
      ELEV = dplyr::coalesce(ELEV, elevation),
      REFHT = if ("REFHT" %in% nms) REFHT else NA_real_,
      WNDHT = if ("WNDHT" %in% nms) WNDHT else NA_real_
    )
}
