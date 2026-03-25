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
#' @return A consolidated data frame or output file depending on configuration.
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
#' @importFrom tools file_ext
#' @importFrom yaml read_yaml
#' @importFrom jsonlite fromJSON
#' @importFrom magrittr %>%
#' @importFrom rlang !! :=
#' @importFrom dplyr mutate select
#' @importFrom lubridate ymd_hms
#' 
#' @export
#' 

get_sensor_data <- function(url, creds = NULL, var, lon, lat, radius, from, to, aggregate = "median", output_path = NULL) {
  
  
  # --- Credential validation ---
  if(is.null(creds)) {
    stop("Access denied. Please provide the 'creds' list containing your authentication details.")
  }
  if (is.character(creds)) {
    
    if (!file.exists(creds)) {
      stop("Credential file not found at: ", creds)
    }
    
    creds_ext <- tolower(file_ext(creds))
    if (creds_ext == "yaml" || creds_ext == "yml") {
      creds <- read_yaml(creds)
    } else if (creds_ext == "json") {
      creds <- fromJSON(txt = readLines(creds))
    } else {
      stop("Unsupported configuration file format. Use YAML (.yaml/.yml) or JSON (.json)")
    }
  }
  
  required_keys <- c("url", "client_id", "client_secret", "username", "password")
  missing_keys <- setdiff(required_keys, names(creds))
  
  # Stop if any keys are missing
  if(length(missing_keys) > 0) {
    stop(
      "Invalid 'creds' structure.\n",
      "The following keys are missing from your credentials list: ", 
      paste(missing_keys, collapse = ", "), "\n",
      "Please ensure 'creds' is a list with keys: url, client_id, client_secret, username, password."
    )
  }
  
  # --- Authentication ---
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
  
  # TODO: fix, returns a message, not a df when no ds is found
  if(nrow(ds_metadata) == 0) {
    warning("No datastreams found at the specified location.")
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
  for (i in seq_along(obs)){
    
    # Skip empty results
    if(is.null(obs[[i]]) || nrow(obs[[i]]) == 0) next 
    
    obs_df <- obs[[i]] %>%
      mutate(phenomenonTime = ymd_hms(phenomenonTime)) %>%
      mutate(!!ds_metadata$ObservedProperty.name[i] := as.numeric(result)) %>%
      select(-result)
    dataset[[i]] <- list(
      DATASTREAM = obs_df,
      METADATA = ds_metadata[i, ]
    )
  }
  
  # Handle case where observations were found but empty
  if(length(dataset) == 0) {
    warning("Datastreams found, but no observations retrieved.")
    return(NULL)
  }
  
  names(dataset) <- paste(ds_metadata$Thing.name, ds_metadata$Datastream.id, sep = "_DS")
  
  # --- Group by device ---
  device_nms <- sub("_[^_]+$", "", names(dataset))
  ds_device_list <- split(dataset, device_nms)
  
  # Consolidate
  ds_consolidated <- .consolidate_datastreams(ds_device_list, aggregation = aggregate)
  
  # Resolve output
  out <- export_output(ds_consolidated, output_path)
  
  return(out)
}


#' Obtain an Access Token from a Keycloak Server Using Resource Owner Password Credentials
#'
#' Requests an OAuth2 access token from a Keycloak authentication server using the Resource Owner Password Credentials Grant (password grant).
#'
#' @param url Character. The token endpoint URL of the Keycloak server (typically ends with \code{/protocol/openid-connect/token}).
#' @param client_id Character. The client ID registered in Keycloak.
#' @param client_secret Character. The client secret associated with the client ID.
#' @param username Character. The username of the Keycloak user.
#' @param password Character. The password of the Keycloak user.
#'
#' @details
#' This function sends a POST request to the Keycloak token endpoint with the provided credentials and client information, using the OAuth2 Resource Owner Password Credentials Grant. The function expects a successful response to contain an \code{access_token} field.
#'
#' The function uses the \strong{httr} package for HTTP requests. The token is extracted from the response and returned as a character string.
#'
#' @return A character string containing the access token, or \code{NULL} if the request fails or the token is not found.
#'
#' @examples
#' \dontrun{
#' token <- get_kc_token(
#'   url = "https://my-keycloak-server/auth/realms/myrealm/protocol/openid-connect/token",
#'   client_id = "myclient",
#'   client_secret = "mysecret",
#'   username = "myuser",
#'   password = "mypassword"
#' )
#' }
#'
#' @importFrom httr POST content add_headers verbose
#' 
#' @export
#' 

get_kc_token <- function(url, client_id, client_secret, username, password) {
  
  response <- POST(
    url = url,
    body = list(
      grant_type = "password",
      client_id = client_id,
      client_secret = client_secret,
      username = username,
      password = password
    ),
    encode = "form",
    add_headers(`Content-Type` = "application/x-www-form-urlencoded"),
    verbose()
  )
  
  token <- content(response)$access_token
  
  return(token)
}


#' POST Data to an OGC SensorThings API Endpoint
#'
#' Sends a POST request to an OGC SensorThings API endpoint to create a new resource (e.g., Thing, Sensor, ObservedProperty, Datastream, or Observation).
#'
#' @param object Character. The type of resource to create. Must be one of \code{c("Things", "Sensors", "ObservedProperties", "Datastreams", "Observations")}.
#' @param body A list representing the JSON body to be sent in the request.
#' @param url Character. The base URL of the OGC SensorThings API endpoint (should end with a slash).
#' @param token Character. The Bearer token for authentication.
#'
#' @details
#' This function converts the provided \code{body} to JSON and sends it as a POST request to the specified OGC SensorThings API endpoint, appending the resource type (\code{object}) to the base URL. The request includes the provided Bearer token for authentication.
#'
#' The function uses the \strong{httr} package for HTTP requests and the \strong{jsonlite} package for JSON conversion.
#'
#' @return The response object from the POST request (an \code{httr::response} object).
#'
#' @examples
#' \dontrun{
#' post_ogc_iot(
#'   object = "Things",
#'   body = list(name = "MyThing", description = "A test thing"),
#'   url = "https://example.com/SensorThings/v1.0/",
#'   token = "your_access_token"
#' )
#' }
#'
#' @importFrom httr POST add_headers verbose
#' @importFrom jsonlite toJSON
#' 
#' @export

post_ogc_iot <- function(object = c("Things","Sensors","ObservedProperties","Datastreams","Observations"), body, url, token){
  
  body_json <- toJSON(body, auto_unbox = TRUE)
  
  url <- paste0(url, object)
  response <- POST(url, body = body_json, encode = "json",
                   add_headers(
                     `Content-Type` = "application/json",
                     `Authorization` = paste("Bearer", token)
                   ),
                   verbose())
}


#' Delete a Resource from an OGC SensorThings API Endpoint
#'
#' Sends a DELETE request to an OGC SensorThings API endpoint to remove a specified resource (e.g., Thing, Sensor, ObservedProperty, Datastream, or Observation) by its ID.
#'
#' @param object Character. The type of resource to delete. Must be one of \code{c("Things", "Sensors", "ObservedProperties", "Datastreams", "Observations")}.
#' @param object_id The unique identifier of the resource to delete.
#' @param url Character. The base URL of the OGC SensorThings API endpoint (should end with a slash).
#' @param token Character. The Bearer token for authentication.
#'
#' @details
#' This function constructs the full URL for the resource by appending the resource type (\code{object}) and the resource ID (\code{object_id}) in OData format to the base URL. It then sends a DELETE request to this URL, including the provided Bearer token for authentication.
#'
#' The function checks that the URL starts with \code{http://} or \code{https://} and stops with an error if not.
#'
#' The function uses the \strong{httr} package for HTTP requests.
#'
#' @return The response object from the DELETE request (an \code{httr::response} object).
#'
#' @examples
#' \dontrun{
#' delete_ogc_iot(
#'   object = "Things",
#'   object_id = 123,
#'   url = "https://example.com/SensorThings/v1.0/",
#'   token = "your_access_token"
#' )
#' }
#'
#' @importFrom httr DELETE add_headers
#' 
#' @export

delete_ogc_iot <- function(object = c("Things","Sensors","ObservedProperties","Datastreams","Observations"), object_id, url, token){
  
  if (!grepl("^http[s]?://", url)) {
    stop("Invalid URL: Must start with http:// or https://")
  }
  
  url <- paste0(url, object, "(", object_id, ")")
  response <- DELETE(url,
                     add_headers(
                       `Content-Type` = "application/json",
                       `Authorization` = paste("Bearer", token)
                     ))
}


#' Update a Resource on an OGC SensorThings API Endpoint (PATCH)
#'
#' Sends a PATCH request to an OGC SensorThings API endpoint to update a specified resource (e.g., Thing, Sensor, ObservedProperty, Datastream, or Observation) by its ID.
#'
#' @param object Character. The type of resource to update. Must be one of \code{c("Things", "Sensors", "ObservedProperties", "Datastreams", "Observations")}.
#' @param object_id The unique identifier of the resource to update.
#' @param url Character. The base URL of the OGC SensorThings API endpoint (should end with a slash).
#' @param token Character. The Bearer token for authentication.
#' @param body A list representing the JSON body with the fields to update.
#'
#' @details
#' This function constructs the full URL for the resource by appending the resource type (\code{object}) and the resource ID (\code{object_id}) in OData format to the base URL. It converts the \code{body} to JSON and sends a PATCH request to this URL, including the provided Bearer token for authentication.
#'
#' The function checks that the URL starts with \code{http://} or \code{https://} and stops with an error if not.
#'
#' The function uses the \strong{httr} package for HTTP requests and the \strong{jsonlite} package for JSON conversion.
#'
#' @return The response object from the PATCH request (an \code{httr::response} object).
#'
#' @examples
#' \dontrun{
#' patch_ogc_iot(
#'   object = "Things",
#'   object_id = 123,
#'   url = "https://example.com/SensorThings/v1.0/",
#'   token = "your_access_token",
#'   body = list(name = "Updated Thing Name")
#' )
#' }
#'
#' @importFrom httr PATCH add_headers
#' @importFrom jsonlite toJSON
#' 
#' @export
#' 

patch_ogc_iot <- function(object = c("Things","Sensors","ObservedProperties","Datastreams","Observations"), object_id, url, token, body){
  
  if (!grepl("^http[s]?://", url)) {
    stop("Invalid URL: Must start with http:// or https://")
  }
  body_json <- toJSON(body, auto_unbox = TRUE)
  
  url <- paste0(url, object, "(", object_id, ")")
  response <- PATCH(url, body = body_json, encode = "json",
                    add_headers(
                      `Content-Type` = "application/json",
                      `Authorization` = paste("Bearer", token)
                    ))
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
#' The function first identifies all devices (Things) from the SensorThings API, extracting their locations. It then retrieves all datastreams for devices at the specified coordinates, including metadata about observed properties and measurement periods. It filters datastreams to those matching the requested observed properties (\code{var}) and checks if their measurement periods encompass the requested time range (\code{from} to \code{to}).
#'
#' The function returns a data frame of matching datastreams, or a message if no suitable data is found.
#'
#' The function uses the \strong{httr}, \strong{jsonlite}, \strong{dplyr}, \strong{tidyr}, and \strong{tibble} packages.
#'
#' @return A data frame of matching datastreams, or a character message if no data is found for the specified criteria.
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
#' 

# TODO: optimize sequence:
# Right now expanding locations > download all within radius > filter wanter properties
# Should be: expanding locations and properties > only download target records
.locate_sta_datastreams <- function(url, token = NULL, device_id = NULL, var, lon, lat,  radius = 0, from, to,...){
  
  # --- Identify all devices from the target server ---
  .locate_sta_devices <- function(...) {
    
    url_locs <- paste0(url, "Things?$expand=Locations")
    response <- httr::GET(
      url_locs,
      httr::add_headers(`Authorization` = paste("Bearer", token))
    )
    
    devices <- jsonlite::fromJSON(
      httr::content(response, as = "text", encoding = "UTF-8")
    )
    
    devices <- devices$value %>%
      # Unnest the 'Locations' list-column.
      tidyr::unnest(Locations, names_sep = "_") %>%
      # Unnest 'Locations_location' data frame column; creates new columns for 'type' and 'coordinates'.
      tidyr::unnest_wider(Locations_location, names_sep = "_") %>%
      tidyr::unnest(Locations_location_coordinates) %>%  # simplify
      # Split coordinates into separate vectors
      dplyr::mutate(
        longitude = purrr::map_dbl(Locations_location_coordinates, ~ .x[1]),
        latitude  = purrr::map_dbl(Locations_location_coordinates, ~ .x[2])
      ) %>%
      dplyr::select(-Locations_location_coordinates)
    
    return(devices)
  }
  
  # --- Data retrieval ---
  devices <- .locate_sta_devices(url, token)
  device_nms <- devices$name
  
  # Retrieve all datastreams for the selected devices
  # CHECK: here could add logic to retrieve sensor type info and structure data model
  url_dev_ds <- paste0(devices$`@iot.selfLink`, "?$expand=Datastreams")
  response <- lapply(url_dev_ds, function(url) {
    httr::GET(
      url,
      httr::add_headers(`Authorization` = paste("Bearer", token))
    )
  })
  
  url_ds_with_nulls <- lapply(response, function(x) {
    tryCatch({
      jsonlite::fromJSON(
        httr::content(x, as = "text", encoding = "UTF-8")
      )$Datastreams %>%
        dplyr::pull(`@iot.selfLink`)
    }, error = function(e) {
      iot_id <- gsub(".*Things\\((\\d+)\\).*", "\\1", x$url)
      warning(paste("Could not retrieve datastreams for Thing with @iot.id:", iot_id), 
              call. = FALSE)
      return(NULL)
    })
  })
  
  # Create a logical index of which devices were successful
  is_valid <- !sapply(url_ds_with_nulls, is.null)
  device_nms <- device_nms[is_valid]
  url_ds <- url_ds_with_nulls[is_valid]
  
  # Get datastreams metadata incl. observed properties and device identification
  url_ds_prop <- lapply(url_ds, function(url) paste0(url, "?$expand=ObservedProperty"))
  response <- lapply(url_ds_prop, function(urls) {
    lapply(urls, function(url) {
      httr::GET(
        url,
        httr::add_headers(`Authorization` = paste("Bearer", token))
      )
    })
  })
  
  ds_ls <- lapply(response, function(dev) {
    lapply(dev, function(x) {
      tryCatch({
        jsonlite::fromJSON(
          httr::content(x, as = "text", encoding = "UTF-8")
          )
      }, error = function(e) {
        datastream_id <- gsub(".*Datastreams\\((\\d+)\\).*", "\\1", x$url)  # TODO: fix regex
        warning(
          paste("Could not parse content for Datastream with @iot.id:", datastream_id),
          call. = FALSE
        )
        return(NULL)
      })
    })
  })
  names(ds_ls) <- device_nms
  
  # Compile output [FIXED NESTED DEVICES [multiple sensors per device]]
  ds_out <- list()
  for (i in seq_along(ds_ls)) {
    
    ds_out[[i]] <- 
      dplyr::bind_rows(
        lapply(ds_ls[[i]], function(ds) {
          
          coords <- na.omit(as.numeric(ds$observedArea$coordinates))
          if (length(coords) < 2) return(NULL)  # Skip if coordinates are invalid
          
          tibble::tibble(
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
            phenomenonTime = ds$phenomenonTime
          ) %>%
            tidyr::separate(phenomenonTime, into = c("start_date","end_date"), sep = "/") %>%
            dplyr::mutate(dplyr::across(start_date:end_date, ~ as.Date(.x)))
        })
      )
  }
  
  # Re-assign names  
  is_valid <- !sapply(ds_out, is.null)  # not null (has 2 geocoordinates)
  ds_out_filtered <- ds_out[is_valid]
  ds_names <- names(ds_ls)[is_valid]
  names(ds_out_filtered) <- ds_names
  ds_out_filtered <- ds_out_filtered[sapply(ds_out_filtered, function(df) length(df) > 0)]
  
  # Combine datastreams in one dataframe
  datastreams <- dplyr::bind_rows(ds_out_filtered, .id = "Thing.name")
  if (is.null(datastreams) || nrow(datastreams) == 0) {
    return("No valid datastreams could be retrieved from the server.")
  }
  
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
  out <- dplyr::bind_rows(focal_datastreams_list) %>%
    dplyr::distinct()
  
  if (nrow(out) == 0) {
    return("No data was measured within the specified radius of the given location(s).")
  }
  
  out <- out %>% filter(ObservedProperty.name %in% var)
  if (nrow(out) == 0) {
    return("No data for the focal property could be retrieved at the specified location(s).")
  }
  
  out <- out %>%
    mutate(is_contained = start_date >= as.Date(from) & end_date <= as.Date(to)) %>%
    filter(is_contained)
  
  if (nrow(out) == 0) {
    return("Measured data does not encompass the requested timeframe.")
  } else {
    return(out %>% select(-is_contained)) # Remove the temporary column
  }
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
  
  url_ds <- sub("\\?.*", "", url)  # datastream url
  
  all_obs <- data.frame(phenomenonTime = character(0), result = numeric(0))
  i = 0
  repeat {
    response <- httr::GET(
      url,
      httr::add_headers(`Authorization` = paste("Bearer", token))
    )
    content <- jsonlite::fromJSON(
      httr::content(response, as = "text", encoding = "UTF-8")
    )
    all_obs <- rbind(all_obs, content$Observations)
    if (is.null(content$`Observations@iot.nextLink`)) break
    i <- i+100
    url <- paste0(url_ds, "?$expand=Observations($select=phenomenonTime,result;$skip=",i,"),ObservedProperty($select=name)")
  }
  return(all_obs)
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
#' @return A processed list with the same top-level names, but each element
#'   contains only two items: `DATASTREAM` (a single, wide data frame) and
#'   `METADATA` (a single, combined tibble).
#'   
#' @noRd
#'   

.consolidate_datastreams <- function(device_list, aggregation = "mean") {
  
  aggregation <- match.arg(aggregation, c("mean", "median", "none"))
  
  # Map aggregation functions
  agg_func <- switch(aggregation,
                     "mean" = mean,
                     "median" = median)
  
  # Iterate over each device
  data_processed <- purrr::map(device_list, ~ {
    
    # --- Data cleaning ---
    # Remove artefacts (duplicate time stamps)
    data_clean <- purrr::map(.x, ~ {
      df <- .x$DATASTREAM
      value_col_name <- setdiff(names(df), "phenomenonTime")
      
      df %>%
        dplyr::group_by(phenomenonTime) %>%
        dplyr::summarise(
          !!value_col_name := mean(!!sym(value_col_name), na.rm = TRUE)
        ) %>%
        dplyr::ungroup() %>%
        dplyr::mutate(
          !!value_col_name := dplyr::if_else(
            is.nan(!!rlang::sym(value_col_name)), NA_real_, !!rlang::sym(value_col_name)
          )
        )
    })
    
    data_agg <- NULL
    
    # --- Aggregate same-named attributes ---
    if (aggregation == "none") {
      data_agg <- data_clean
    } else {
      
      # Group dataframes by variable names
      val_col_names <- purrr::map_chr(data_clean, ~ setdiff(names(.), "phenomenonTime"))
      data_clean_grp <- split(data_clean, val_col_names)
      
      # Apply the selected aggregation function
      data_agg <- purrr::map(data_clean_grp, ~ {
        value_col_name <- setdiff(names(.x[[1]]), "phenomenonTime")
        # Bind all dfs in the group, then group by time, and aggregate
        dplyr::bind_rows(.x) %>%
          dplyr::group_by(phenomenonTime) %>%
          dplyr::summarise(
            !!value_col_name := agg_func(!!rlang::sym(value_col_name), na.rm = TRUE)
          ) %>%
          dplyr::ungroup() %>%
          dplyr::mutate(
            # Convert NaNs to NAs
            !!value_col_name := dplyr::if_else(
              is.nan(!!rlang::sym(value_col_name)), NA_real_, !!rlang::sym(value_col_name)
            )
          )
      })
    }
    
    # --- Join datastreams by device ---
    joined_data <- if (length(data_agg) > 0) {
      purrr::reduce(data_agg, dplyr::full_join, by = "phenomenonTime")
    } else {
      NULL
    }
    
    # --- Combine datastream metadata by device ---
    metadata_clean <- purrr::map(.x, "METADATA")
    combined_metadata <- dplyr::bind_rows(metadata_clean)
    
    # --- Output
    list(
      DATASTREAM = joined_data,
      METADATA = combined_metadata
    )
  })
  
  return(data_processed[[1]])
}
