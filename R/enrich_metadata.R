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

enrich_metadata <- function(dssat_components) {
  
  exp <- dssat_components$METADATA
  sol <- dssat_components$SOIL
  wth <- dssat_components$WEATHER

  # --- Impute missing metadata ---
  sol_imputed <- .impute_sol_metadata(sol, exp)
  wth_imputed <- .impute_wth_metadata(wth, exp)
  
  # --- Enrich with Spatial API Data ---
  # TODO: wrap in if statements?
  exp_locations <- enrich_spatial_data(exp, lat_col = "YCRD", lon_col = "XCRD")
  sol_locations <- enrich_spatial_data(sol_imputed, lat_col = "LAT", lon_col = "LONG")
  wth_locations <- enrich_spatial_data(wth_imputed, lat_col = "LAT", lon_col = "LONG")
  
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
#'         `get_sol_texture()` helper if not provided
#' }
#'
#' Joins soil and experiment data on all common columns to preserve
#' relationships while avoiding duplication.
#'
#' @noRd
#'

.impute_sol_metadata <- function(sol, exp) {
  
  sol %>%
    dplyr::left_join(exp, by = intersect(names(sol), names(exp))) %>%
    dplyr::mutate(
      INST_NAME = if ("INST_NAME" %in% names(.)) INST_NAME else NA_character_,
      INST_NAME = dplyr::coalesce(INST_NAME, INSTITUTION),
      YEAR_FROM_DATE = if ("DATE" %in% names(.)) {
        suppressWarnings(
          as.Date(as.character(DATE), format = "%y%j") %>%
            lubridate::year() |>
            as.character()
        )
      } else {
        NA_character_
      },
      YEAR = if ("YEAR" %in% names(.)) YEAR else NA_character_,
      YEAR = dplyr::coalesce(YEAR, YEAR_FROM_DATE, EXP_YEAR),
      LAT = if ("LAT" %in% names(.)) LAT else NA_real_,
      LAT = dplyr::coalesce(LAT, YCRD),
      LONG = if ("LONG" %in% names(.)) LONG else NA_real_,
      LONG = dplyr::coalesce(LONG, XCRD),
      TEXTURE = if ("TEXTURE" %in% names(.)) {
        TEXTURE
      } else {
        # (Assuming get_sol_texture is a helper)
        get_sol_texture(
          clay_frac = SLCL,
          silt_frac = SLSI,
          sand_frac = 1.0 - SLCL - SLSI
        )
      }
    ) %>%
    dplyr::select(
      dplyr::all_of(intersect(c("EXP_ID", "PEDON", "INST_NAME", "YEAR", "TEXTURE"), names(.))),
      dplyr::all_of(setdiff(names(sol), c("EXP_ID", "PEDON", "INST_NAME", "YEAR", "TEXTURE"))),
    )
}


#' Impute missing weather metadata from experiment data
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
  
  wth %>%
    dplyr::left_join(exp, by = intersect(names(wth), names(exp))) %>%
    dplyr::mutate(
      INSI = if ("INSI" %in% names(.)) INSI else NA_character_,
      INSI = dplyr::coalesce(INSI, INSTITUTION),
      LAT = if ("LAT" %in% names(.)) LAT else NA_real_,
      LAT = dplyr::coalesce(LAT, YCRD),
      LONG = if ("LONG" %in% names(.)) LONG else NA_real_,
      LONG = dplyr::coalesce(LONG, XCRD),
      YEAR = if ("YEAR" %in% names(.)) YEAR else NA_character_,
      YEAR = substr(DATE, 1, 2),
    ) %>%
    dplyr::select(
      dplyr::all_of(intersect(c("EXP_ID", "INSI", "LAT", "LONG", "YEAR"), names(.))),
      dplyr::all_of(setdiff(names(wth), c("EXP_ID", "INSI", "LAT", "LONG", "YEAR"))),
    )
}


#' Format experiment location data for DSSAT
#'
#' Constructs DSSAT-compliant location fields by combining spatial enrichment
#' data with existing experiment metadata.
#'
#' @param exp_locations Data frame containing experiment metadata enriched with
#'   spatial data from `enrich_spatial_data()`
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
  
  exp_locations %>%
    dplyr::mutate(
      FLNAME = if ("FLNAME" %in% names(.)) FLNAME else NA_character_,
      FLNAME = ifelse(is.na(FLNAME), toupper(ShortLabel), FLNAME),
      # Fill site as address if missing
      SITE = if ("SITE" %in% names(.)) SITE else NA_character_,
      SITE_addr = tidyr::unite(data.frame(FLNAME, City, Region, CntryName),
                              "SITE", sep = ", ", na.rm = TRUE)$SITE,
      SITE = ifelse(is.na(SITE), SITE_addr, SITE),
      ELEV = if ("ELEV" %in% names(.)) ELEV else NA_real_,
      ELEV = dplyr::coalesce(ELEV, elevation)
    ) %>%
    dplyr::mutate(
      XCRD_tmp = round(XCRD, 2), 
      YCRD_tmp = round(YCRD, 2), 
      ELEV_tmp = round(ELEV, 2)
    ) %>%
    tidyr::unite("SITE", SITE, XCRD_tmp, YCRD_tmp, ELEV_tmp, sep = "; ", na.rm = TRUE, remove = TRUE)
}


#' Format soil location data for DSSAT
#'
#' Constructs DSSAT-compliant location fields for soil profiles by combining
#' spatial enrichment data with existing soil metadata.
#'
#' @param sol_locations Data frame containing soil data enriched with spatial
#'   data from `enrich_spatial_data()`
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
  
  sol_locations %>%
    dplyr::group_by(PEDON) %>%
    dplyr::mutate(
      COUNTRY = if ("COUNTRY" %in% names(.)) COUNTRY else NA_character_,
      COUNTRY = ifelse("COUNTRY" %in% names(.),
                       dplyr::coalesce(COUNTRY, CountryCode),
                       countrycode::countrycode(CountryCode, origin = "iso3c", destination = "iso2c")),
      SITE = if ("SITE" %in% names(.)) SITE else NA_character_,
      SITE = ifelse("SITE" %in% names(.),
                    dplyr::coalesce(SITE, paste(City, Region, sep = ",")),
                    paste(City, Region, sep = ", ")),
      ELEV = if ("ELEV" %in% names(.)) ELEV else NA_real_,
      ELEV = dplyr::coalesce(ELEV, elevation)
    )
}


#' Format weather station location data for DSSAT
#'
#' Constructs DSSAT-compliant location fields for weather stations by combining
#' spatial enrichment data with existing weather metadata.
#'
#' @param wth_locations Data frame containing weather data enriched with spatial
#'   data from `enrich_spatial_data()`
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
  
  wth_locations %>%
    dplyr::group_by(INSI, LAT, LONG) %>%
    dplyr::mutate(
      WST_NAME = if ("WST_NAME" %in% names(.)) WST_NAME else NA_character_,
      WST_NAME = dplyr::coalesce(WST_NAME, 
                                 tidyr::unite(data.frame(ShortLabel, City, Region, CntryName),
                                              "WST_SITE", sep = ", ", na.rm = TRUE)$WST_SITE),
      ELEV = if ("ELEV" %in% names(.)) ELEV else NA_real_,
      ELEV = dplyr::coalesce(ELEV, elevation),
      REFHT = if ("REFHT" %in% names(.)) REFHT else NA_real_,
      WNDHT = if ("WNDHT" %in% names(.)) WNDHT else NA_real_
    )
}


#' Enrich dataset with spatial metadata from API
#'
#' Queries spatial API for location information (elevation, administrative
#' boundaries, etc.) for unique coordinates in a dataset and joins results
#' back to the original data.
#'
#' @param data Data frame containing coordinate columns
#' @param lat_col Character string specifying the latitude column name
#' @param lon_col Character string specifying the longitude column name
#'
#' @return Data frame with original data plus additional spatial metadata
#'   columns from API response
#'
#' @details
#' Processing steps:
#' \enumerate{
#'   \item Extracts unique, non-missing coordinate pairs from input data
#'   \item Queries spatial API via `.enrich_location_data()` for each unique location
#'   \item Joins enriched metadata back to original data using coordinate columns as keys
#' }
#'
#' Only unique coordinates are queried to minimize API calls. All rows with
#' matching coordinates receive the same enriched metadata.
#'
#' @seealso [.enrich_location_data()] for API query implementation
#'
#' @noRd
#'

enrich_spatial_data <- function(data, lat_col, lon_col) {

  # Get unique coordinates
  coord_cols <- c(lat_col, lon_col)
  coords <- data |>
    dplyr::filter(!is.na(.data[[lat_col]]) & !is.na(.data[[lon_col]])) |>
    dplyr::distinct(across(all_of(coord_cols)))
  
  # Enrich spatial metadata
  message(paste0("Enriching ", nrow(coords), " unique locations..."))
  api_data <- .enrich_location_data(coords, lat_col = lat_col, long_col = lon_col)
  geocode_data <- dplyr::bind_cols(coords, api_data)
  
  # Join back to the original data frame
  out <- dplyr::left_join(data, geocode_data, by = coord_cols)
  
  return(out)
}


#' Query spatial APIs for location metadata
#'
#' Retrieves elevation and reverse geocoding data for coordinate pairs using
#' external APIs (elevatr for elevation, tidygeocoder/ArcGIS for addresses).
#'
#' @param coords Data frame containing coordinate columns
#' @param lat_col Character string specifying the latitude column name in `coords`
#' @param long_col Character string specifying the longitude column name in `coords`
#'
#' @return Data frame with spatial metadata columns:
#'   \itemize{
#'     \item **elevation**: Meters above sea level (from AWS terrain tiles)
#'     \item **ShortLabel**: Short location identifier
#'     \item **City**: Municipality/city name
#'     \item **Region**: State/province/administrative level 1
#'     \item **CntryName**: Full country name
#'     \item **CountryCode**: ISO3 country code
#'   }
#'
#' @details
#' **API Services:**
#' \itemize{
#'   \item Elevation: `elevatr::get_elev_point()` with AWS terrain tiles (EPSG:4326)
#'   \item Reverse geocoding: `tidygeocoder::reverse_geocode()` via ArcGIS service
#' }
#'
#' **Error Handling:**
#' \itemize{
#'   \item Elevation failures return NA for all rows with a warning
#'   \item Reverse geocoding failures omit address columns with a warning
#'   \item Partial failures (some fields missing) are handled gracefully
#' }
#'
#' Column names defined in `name_map` provide ICASA/DSSAT mappings for future
#' standardization (currently returns raw API field names).
#'
#' @seealso [elevatr::get_elev_point()], [tidygeocoder::reverse_geocode()]
#'
#' @noRd
#'

.enrich_location_data <- function(coords, lat_col, long_col) {
  
  # --- Define output terms mappings ---
  name_map <- list(
    # Raw API Name = list(icasa = "...", dssat = "...")
    elevation = list(icasa = "field_elevation", dssat = "ELEV"),
    ShortLabel = list(icasa = "site_name", dssat = "SITE"),
    City = list(icasa = "field_sub_sub_country", dssat = "FLL3"),
    Region = list(icasa = "field_sub_country", dssat = "FLL2"),
    CntryName = list(icasa = "field_country", dssat = "FLL1"),
    CountryCode = list(icasa = "tmp_country_code", dssat = "TMP_COUNTRY_CODE")
  )
  raw_geo_names <- c("elevation", "ShortLabel", "City", "Region", "CntryName", "CountryCode")
  
  # --- Standardize input coords for APIs ---
  std_coords <- as.data.frame(coords[, c(long_col, lat_col)])
  names(std_coords) <- c("x", "y")
  
  # --- Get elevation (elevatr) ---
  elev_data <- tryCatch({
    elevatr::get_elev_point(std_coords, prj = 4326, src = "aws")
  }, error = function(e) {
    warning("Elevation API call (elevatr) failed.", call. = FALSE)
    # Return a compliant NA data frame
    return(data.frame(elevation = rep(NA_real_, nrow(std_coords))))
  })
  
  # --- Get address information via reverse geocoding (tidygeocoder) ---
  addr_data <- tryCatch({
    tidygeocoder::reverse_geocode(
      std_coords,
      long = "x", lat = "y", 
      method = 'arcgis', 
      full_results = TRUE,
      quiet = TRUE
    )
  }, error = function(e) {
    warning("Reverse geocode API call (tidygeocoder) failed.", call. = FALSE)
    return(NULL) # Handled below
  })
  
  # --- Combine results ---
  api_results <- elev_data
  if (!is.null(addr_data)) {
    available_geo_names <- intersect(names(addr_data), raw_geo_names)
    api_results <- dplyr::bind_cols(api_results, addr_data[, available_geo_names])
  }
  
  return(api_results)
}
