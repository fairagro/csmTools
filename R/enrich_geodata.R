#' Enrich dataset with spatial metadata from external APIs
#'
#' Queries spatial APIs for location information (elevation, administrative
#' boundaries, etc.) for unique coordinates in a dataset and joins results
#' back to the original data.
#'
#' @param data Data frame containing coordinate columns
#' @param lat_col Character string specifying the latitude column name
#' @param lon_col Character string specifying the longitude column name
#' @param ... Additional arguments passed to `.query_spatial_apis()`, including:
#'   \itemize{
#'     \item `crs`: ESPG code for coordinate reference system (default `4326`).
#'     \item `elev_api`: Elevation data source (default `"aws"`). See 
#'           [elevatr::get_elev_point()] for options.
#'     \item `revgeocode_api`: Reverse geocoding service (default `"arcgis"`). 
#'           See [tidygeocoder::reverse_geocode()] for options.
#'   }
#'
#' @return Data frame with original data plus additional spatial metadata
#'   columns from API responses:
#'   \itemize{
#'     \item **elevation**: Meters above sea level
#'     \item **ShortLabel**: Short location identifier
#'     \item **City**: Municipality/city name
#'     \item **Region**: State/province/administrative level 1
#'     \item **CntryName**: Full country name
#'     \item **CountryCode**: ISO3 country code
#'   }
#'   
#'   Rows with missing coordinates remain unchanged. Multiple rows with 
#'   identical coordinates receive the same enriched metadata.
#'
#' @details
#' **Processing steps:**
#' \enumerate{
#'   \item Extracts unique, non-missing coordinate pairs from input data
#'   \item Queries spatial APIs via `.query_spatial_apis()` for each unique 
#'         location (displays progress message)
#'   \item Joins enriched metadata back to original data using coordinate 
#'         columns as keys
#' }
#'
#' **Notes:**
#' Only unique coordinates are queried to minimize API calls.
#' Warnings are issued for failed API calls. Missing elevation data returns 
#' `NA`; missing geocoding data omits those columns entirely.
#'
#' @seealso 
#' \itemize{
#'   \item [.query_spatial_apis()] - Internal function handling API calls
#'   \item [elevatr::get_elev_point()] - Elevation API documentation
#'   \item [tidygeocoder::reverse_geocode()] - Geocoding API documentation
#' }
#'
#' @examples
#' \dontrun{
#' # Basic usage with defaults (AWS elevation, ArcGIS geocoding)
#' enriched <- enrich_geodata(my_data, lat_col = "latitude", lon_col = "longitude")
#'
#' # Use different APIs
#' enriched <- enrich_geodata(
#'   my_data, 
#'   lat_col = "lat", 
#'   lon_col = "lon",
#'   elev_api = "epqs",        # USGS Elevation Point Query Service
#'   revgeocode_api = "osm"    # OpenStreetMap Nominatim
#' )
#' }
#'
#' @importFrom dplyr filter distinct bind_cols across left_join
#' @importFrom tidyr all_of
#'
#' @export
#'

enrich_geodata <- function(data, lat_col, lon_col, ...) {

  # Get unique coordinates
  coord_cols <- c(lat_col, lon_col)
  coords <- data |>
    filter(!is.na(.data[[lat_col]]) & !is.na(.data[[lon_col]])) |>
    distinct(across(all_of(coord_cols)))
  
  # Enrich spatial metadata
  message(paste0("Enriching ", nrow(coords), " unique locations..."))
  api_data <- .query_spatial_apis(coords, lat_col = lat_col, long_col = lon_col, ...)
  geocode_data <- bind_cols(coords, api_data)
  
  # Join back to the original data frame
  out <- left_join(data, geocode_data, by = coord_cols)
  
  return(out)
}


#' Query spatial APIs for location metadata
#'
#' Internal helper that retrieves elevation and reverse geocoding data for 
#' coordinate pairs using external APIs. Called by `enrich_spatial_data()` to
#' fetch spatial metadata for unique location coordinates.
#'
#' @param coords Data frame containing coordinate columns
#' @param lat_col Character string specifying the latitude column name in `coords`
#' @param long_col Character string specifying the longitude column name in `coords`
#' @param crs Integer specifying the ESPG code for the coordinate reference system
#'   Default is '4326' (ESPG:4326 -> WGS84 crs)
#' @param elev_api Character string specifying the elevation data source. 
#'   Default is `"aws"` (Amazon Web Services terrain tiles). See 
#'   [elevatr::get_elev_point()] for other options.
#' @param revgeocode_api Character string specifying the reverse geocoding service.
#'   Default is `"arcgis"`. See [tidygeocoder::reverse_geocode()] for other options.
#'
#' @return Data frame with spatial metadata columns (raw API field names):
#'   \itemize{
#'     \item **elevation**: Meters above sea level (from elevation API)
#'     \item **ShortLabel**: Short location identifier (from geocoding API)
#'     \item **City**: Municipality/city name
#'     \item **Region**: State/province/administrative level 1
#'     \item **CntryName**: Full country name
#'     \item **CountryCode**: ISO3 country code
#'   }
#'   
#'   Returns one row per input coordinate pair. Unavailable fields are omitted.
#'
#' @details
#' **API Services:**
#' \itemize{
#'   \item Elevation: `elevatr::get_elev_point()` with configurable data source (EPSG:4326)
#'   \item Reverse geocoding: `tidygeocoder::reverse_geocode()` with configurable service
#' }
#'
#' **Error Handling:**
#' \itemize{
#'   \item Elevation failures return NA for all rows with a warning
#'   \item Reverse geocoding failures omit address columns with a warning
#'   \item Partial failures (some fields missing from API) are handled gracefully
#' }
#'
#' **Column Naming:**
#' Returns raw API field names. The internal `name_map` defines ICASA/DSSAT 
#' mappings for downstream standardization by the parent function.
#'
#' @seealso 
#' \itemize{
#'   \item [elevatr::get_elev_point()] for elevation API details
#'   \item [tidygeocoder::reverse_geocode()] for geocoding API details
#'   \item `enrich_spatial_data()` - parent function that calls this helper
#' }
#'
#' @noRd
#'

.query_spatial_apis <- function(coords, lat_col, long_col, crs = 4326, elev_api = "aws", revgeocode_api = "arcgis") {
  
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
    elevatr::get_elev_point(std_coords, prj = crs, src = elev_api)
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
      method = revgeocode_api, 
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
