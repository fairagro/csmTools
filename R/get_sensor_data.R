#' Download and format sensor data from OGC SensorThings API endpoint
#'
#' Retrieves, merges, and formats observational data for specified variables, location, and time range from an OGC SensorThings API (STA) endpoint.
#'
#' @param url (character) The base URL of the OGC SensorThings API endpoint (should end with a slash).
#' @param creds (list) A named list containing Keycloak authentication details (see Details).
#' @param var (character vector) The names of observed properties (variables) to extract, as defined ObservedProperties on the focal STA endpoint.
#' @param lon (numeric) Longitude (x coordinate) of the target location (decimal degrees E/W, range 6:15)
#' @param lat (numeric) Latitude (y coordinate) of the target location (decimal degrees N/S, range 47:55)
#' @param radius (numeric) Search radius around the focal coordinates, in meters.
#' @param from (character or date) Start date of the desired time range as YYYY-MM-DD (inclusive).
#' @param to (character or date) End date of the desired time range as YYYY-MM-DD (inclusive).
#' @param aggregate (character) Method for consolidating data (e.g., \code{"median"}).
#' @param output_path (character) Optional file path to save the output.
#'
#' @details
#' \strong{Credentials Structure:}
#' The \code{creds} argument must be a named list containing exactly these five keys:
#' \itemize{
#'   \item \code{url}: The Keycloak token endpoint URL.
#'   \item \code{client_id}: The registered Client ID.
#'   \item \code{client_secret}: The Client Secret.
#'   \item \code{username}: User's username.
#'   \item \code{password}: User's password.
#' }
#' 
#' This function orchestrates the retrieval of datastream metadata and observations. It automatically obtains a token using the provided credentials.
#'
#' @return A named list with one entry per device, each containing \code{DATASTREAM} (a wide data
#'   frame of observations) and \code{METADATA} (device metadata). Returns \code{NULL} if no data
#'   is found. If \code{output_path} is provided the list is also written to a JSON file.
#'
#' @examples
#' \dontrun{
#' # 1. Define credentials object
#' my_creds <- list(
#'   url = "https://auth.example.com/realms/main/protocol/openid-connect/token",
#'   client_id = "my_app_id",
#'   client_secret = "xyz_secret_123",
#'   username = "jdoe",
#'   password = "supersecretpassword"
#' )
#' 
#' # 2. Run extraction
#' data <- get_sensor_data(
#'   url = "https://example.com/SensorThings/v1.0/",
#'   creds = my_creds,
#'   var = c("air_temperature"),
#'   lon = 12.34, lat = 56.78, radius = 500,
#'   from = "2022-01-01", to = "2022-01-02"
#' )
#' }
#'
#' @importFrom rlang !! :=
#' @importFrom dplyr mutate select
#' @importFrom lubridate ymd_hms
#' 
#' @export
#' 

get_sensor_data <- function(url, creds = NULL, var, lon, lat, radius, from, to, aggregate = "median", output_path = NULL) {
   
  aggregate <- match.arg(aggregate, c("mean", "median", "none"))
  
  # --- Credential validation and authentication ---
  creds <- .read_creds(creds)
  token <- tryCatch({
    get_kc_token(
      url = creds$url,
      client_id = creds$client_id,
      client_secret = creds$client_secret,
      username = creds$username,
      password = creds$password
    )
  }, error = function(e) {
    stop("Authentication Failed: ", e$message)
  })
  
  if(is.null(token)) {
    stop("Authentication failed: the Keycloak server returned no token (check credentials).")
  }

  # --- Identify datastreams macthing the input ---
  message("Retrieving data streams...")
  
  ds_metadata <- .locate_sta_datastreams(
    url = url,
    token = token,
    var = var,
    lon = lon,
    lat = lat,
    radius = radius,
    from = from,
    to = to
  )
  
  if (nrow(ds_metadata) == 0) {
    return(NULL)
  }
  
  # --- Download all observations for the focal datastreams ---
  message("Downloading observations ...")
  
  urls_obs <- paste0(
    ds_metadata$Datastream.link,
    "?$expand=Observations($select=phenomenonTime,result;$skip=0),ObservedProperty($select=name)"
  )
  obs <- lapply(urls_obs, function(url) .get_all_obs(url, token))
  
  # Attach device metadata to each datastream
  dataset <- list()
  for (i in seq_along(obs)) {
    if (is.null(obs[[i]]) || nrow(obs[[i]]) == 0) next
    obs_df <- obs[[i]] |>
      mutate(phenomenonTime = ymd_hms(phenomenonTime)) |>
      mutate(!!ds_metadata$ObservedProperty.name[i] := as.numeric(result)) |>
      select(-result)
    nm <- paste(ds_metadata$Thing.name[i], ds_metadata$Datastream.id[i], sep = "_DS")
    dataset[[nm]] <- list(DATASTREAM = obs_df, METADATA = ds_metadata[i, ])
  }

  if (length(dataset) == 0) {
    warning("Datastreams found, but no observations retrieved.")
    return(NULL)
  }
  
  # --- Group by device ---
  device_nms <- sub("_[^_]+$", "", names(dataset))
  ds_device_list <- split(dataset, device_nms)
  
  # Consolidate
  ds_consolidated <- .consolidate_datastreams(ds_device_list, aggregation = aggregate)
  
  # Resolve output
  out <- export_output(ds_consolidated, output_path)
  
  return(out)
}


#' Retrieve all devices (Things) with their locations from an STA endpoint
#'
#' @param url Character. Base URL of the STA endpoint.
#' @param token Character or NULL. Bearer token for authentication.
#'
#' @return A data frame of Things with longitude and latitude columns.
#'
#' @noRd

.locate_sta_devices <- function(url, token = NULL) {
  url_locs <- paste0(url, "/Things?$expand=Locations")
  response <- httr::GET(url_locs, .sta_auth(token))
  httr::stop_for_status(response)
  parsed <- .parse_sta_response(response)

  parsed$value |>
    tidyr::unnest(Locations, names_sep = "_") |>
    tidyr::unnest_wider(Locations_location, names_sep = "_") |>
    tidyr::unnest(Locations_location_coordinates) |>
    dplyr::mutate(
      longitude = purrr::map_dbl(Locations_location_coordinates, ~ .x[1]),
      latitude  = purrr::map_dbl(Locations_location_coordinates, ~ .x[2])
    ) |>
    dplyr::select(-Locations_location_coordinates)
}


#' Locate OGC SensorThings Datastreams by Location, Variable, and Timeframe
#'
#' Queries an OGC SensorThings API endpoint to find datastreams at a specified longitude and latitude, for selected observed properties, and within a given time range.
#'
#' @param url Character. The base URL of the OGC SensorThings API endpoint (should end with a slash).
#' @param token Character or NULL. The Bearer token for authentication, or NULL if not required.
#' @param var Character vector. The names of observed properties (variables) to search for (e.g., \code{c("air_temperature", "solar_radiation", "rainfall")}).
#' @param lon Numeric. Longitude of the target location.
#' @param lat Numeric. Latitude of the target location.
#' @param from Character or Date. Start date of the desired time range (inclusive).
#' @param to Character or Date. End date of the desired time range (inclusive).
#' @param ... Additional arguments passed to internal functions.
#'
#' @details
#' Issues a single paginated request using \code{$expand} to fetch all Things with their
#' Locations and Datastreams (including ObservedProperty) in one round-trip, then filters
#' the flat result by spatial radius, variable name, and time overlap.
#'
#' @return A data frame of matching datastreams, or an empty data frame (with a \code{warning}) if no suitable data is found.
#'
#' @examples
#' \dontrun{
#' .locate_sta_datastreams(
#'   url = "https://example.com/SensorThings/v1.0/",
#'   token = "your_access_token",
#'   var = c("air_temperature", "rainfall"),
#'   lon = 12.34,
#'   lat = 56.78,
#'   from = "2022-01-01",
#'   to = "2022-12-31"
#' )
#' }
#'
#' @noRd

.locate_sta_datastreams <- function(url, token = NULL, device_id = NULL, var, lon, lat, radius = 0, from, to, ...) {

  auth <- .sta_auth(token)

  # --- Identify datastreams per location and observed property ---
  # Single batched request: all Things with Locations + Datastreams + ObservedProperty
  all_things <- list()
  next_url <- paste0(url, "Things?$expand=Locations,Datastreams($expand=ObservedProperty)")
  while (!is.null(next_url)) {
    response <- httr::GET(next_url, auth)
    httr::stop_for_status(response)
    parsed <- .parse_sta_response(response, simplify = FALSE)
    all_things <- c(all_things, parsed$value)
    next_url <- parsed$`@iot.nextLink`
  }

  if (length(all_things) == 0) {
    warning("No Things found on the server.")
    return(data.frame())
  }

  # --- Compile metadata ---
  # Build a flat dataframe of all datastreams tagged with their parent Thing name
  datastreams <- dplyr::bind_rows(purrr::map(all_things, function(thing) {
    ds_list <- thing$Datastreams
    if (length(ds_list) == 0) return(NULL)

    dplyr::bind_rows(purrr::map(ds_list, function(ds) {
      coords <- na.omit(as.numeric(ds$observedArea$coordinates))
      if (length(coords) < 2) return(NULL)

      phtime <- ds$phenomenonTime
      if (is.null(phtime) || is.na(phtime)) return(NULL)
      dates <- strsplit(phtime, "/")[[1]]
      if (length(dates) < 2) return(NULL)

      tibble::tibble(
        Thing.name = thing$name,
        Datastream.id = ds$`@iot.id`,
        Datastream.link = ds$`@iot.selfLink`,
        Datastream.name = ds$name,
        Datastream.description = ds$description,
        observationType = ds$observationType,
        ObservedProperty.id = ds$ObservedProperty$`@iot.id`,
        ObservedProperty.name = ds$ObservedProperty$name,
        unitOfMeasurement.name = ds$unitOfMeasurement$name,
        unitOfMeasurement.symbol = ds$unitOfMeasurement$symbol,
        longitude = coords[1],
        latitude = coords[2],
        start_date = as.Date(dates[1]),
        end_date = as.Date(dates[2])
      )
    }))
  }))

  if (is.null(datastreams) || nrow(datastreams) == 0) {
    warning("No valid datastreams could be retrieved from the server.")
    return(data.frame())
  }

  # --- Filter based on input arguments ---
  # Find focal datastreams based on coordinate and radius inputs
  focal_datastreams_list <- lapply(seq_along(lon), function(i) {
    current_lon <- lon[i]
    current_lat <- lat[i]

    distances <- haversine_dist(current_lat, current_lon, datastreams$latitude, datastreams$longitude)
    in_radius_idx <- which(distances <= radius)
    datastreams_in_radius <- datastreams[in_radius_idx, ]

    if (nrow(datastreams_in_radius) > 0) {
      datastreams_in_radius$input_lon <- current_lon
      datastreams_in_radius$input_lat <- current_lat
      datastreams_in_radius$distance_m <- distances[in_radius_idx]
    }
    return(datastreams_in_radius)
  })
  out <- dplyr::bind_rows(focal_datastreams_list) |>
    dplyr::distinct()

  if (nrow(out) == 0) {
    warning("No data was measured within the specified radius of the given location(s).")
    return(data.frame())
  }

  out <- dplyr::filter(out, ObservedProperty.name %in% var)
  if (nrow(out) == 0) {
    warning("No data for the focal variable could be retrieved at the specified location(s).")
    return(data.frame())
  }

  # Overlap check: keep datastreams whose measurement period overlaps the requested range
  out <- dplyr::filter(out, start_date <= as.Date(to) & end_date >= as.Date(from))
  if (nrow(out) == 0) {
    warning("No datastream covers the requested timeframe.")
    return(data.frame())
  }

  return(out)
}


#' Retrieve All Observations from a SensorThings Datastream
#'
#' Fetches all observations from a specified OGC SensorThings API datastream, handling server-side pagination as needed.
#'
#' @param url Character. The URL of the datastream endpoint (should include \code{?$expand=Observations} or similar).
#' @param token Character. The Bearer token for authentication.
#'
#' @details
#' This function repeatedly queries the SensorThings API for observations associated with a datastream, following pagination links if present. It accumulates all observations into a single data frame, including the \code{phenomenonTime} and \code{result} fields.
#'
#' The function uses the \strong{httr} and \strong{jsonlite} packages for HTTP requests and JSON parsing.
#'
#' @return A data frame containing all observations for the specified datastream, with columns \code{phenomenonTime} and \code{result}.
#'
#' @examples
#' \dontrun{
#' .get_all_obs(
#'   url = "https://example.com/SensorThings/v1.0/Datastreams(1)?$expand=Observations",
#'   token = "your_access_token"
#' )
#' }
#' 
#' @noRd
#' 

.get_all_obs <- function(url, token) {

  auth <- .sta_auth(token)
  pages <- list()

  # First page: Datastream $expand response — observations live at parsed$Observations
  response <- httr::GET(url, auth)
  httr::stop_for_status(response)
  parsed <- .parse_sta_response(response)
  pages <- c(pages, list(parsed$Observations))
  next_url <- parsed$`Observations@iot.nextLink`

  # Subsequent pages: following nextLink returns a bare collection — observations at parsed$value
  while (!is.null(next_url)) {
    response <- httr::GET(next_url, auth)
    httr::stop_for_status(response)
    parsed <- .parse_sta_response(response)
    pages <- c(pages, list(parsed$value))
    next_url <- parsed$`@iot.nextLink`
  }

  dplyr::bind_rows(pages)
}


#' Process a nested list of device datastreams
#'
#' Cleans, aggregates, and joins datastreams for each device in a nested list.
#'
#' @param device_list The nested list.
#' @param aggregation How to handle duplicate variable columns (e.g., two
#'   'air_temperature' streams for one device).
#'   - "mean" (default): Average the values.
#'   - "median": Take the median of the values.
#'   - "none": Keep all variables, appending .x, .y suffixes.
#'
#' @return A named list with one entry per device, each containing `DATASTREAM`
#'   (a single wide data frame) and `METADATA` (a combined tibble).
#'
#' @noRd

.consolidate_datastreams <- function(device_list, aggregation = "mean") {

  aggregation <- match.arg(aggregation, c("mean", "median", "none"))

  agg_func <- switch(aggregation,
                     "mean"   = mean,
                     "median" = median)

  .nan_to_na <- function(df, col) {
    dplyr::mutate(df, !!col := dplyr::if_else(
      is.nan(!!rlang::sym(col)), NA_real_, !!rlang::sym(col)
    ))
  }

  data_processed <- purrr::map(device_list, ~ {

    # Remove duplicate timestamps within each datastream
    data_clean <- purrr::map(.x, ~ {
      df <- .x$DATASTREAM
      col <- setdiff(names(df), "phenomenonTime")
      df |>
        dplyr::group_by(phenomenonTime) |>
        dplyr::summarise(!!col := mean(!!rlang::sym(col), na.rm = TRUE)) |>
        dplyr::ungroup() |>
        .nan_to_na(col)
    })

    # Aggregate same-named variables across streams
    if (aggregation == "none") {
      data_agg <- data_clean
    } else {
      col_names <- purrr::map_chr(data_clean, ~ setdiff(names(.), "phenomenonTime"))
      data_agg <- purrr::map(split(data_clean, col_names), ~ {
        col <- setdiff(names(.x[[1]]), "phenomenonTime")
        dplyr::bind_rows(.x) |>
          dplyr::group_by(phenomenonTime) |>
          dplyr::summarise(!!col := agg_func(!!rlang::sym(col), na.rm = TRUE)) |>
          dplyr::ungroup() |>
          .nan_to_na(col)
      })
    }

    list(
      DATASTREAM = if (length(data_agg) > 0) purrr::reduce(data_agg, dplyr::full_join, by = "phenomenonTime") else NULL,
      METADATA   = dplyr::bind_rows(purrr::map(.x, "METADATA"))
    )
  })

  return(data_processed)
}
