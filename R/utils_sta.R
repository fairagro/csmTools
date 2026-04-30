#' Obtain an access token from a Keycloak server using Resource Owner Password Credentials
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
#' This function sends a POST request to the Keycloak token endpoint with the provided credentials and client information,
#' using the OAuth2 Resource Owner Password Credentials Grant. The function expects a successful response to contain an \code{access_token} field.
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
#' @importFrom httr POST content add_headers
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
    add_headers(`Content-Type` = "application/x-www-form-urlencoded")
  )
  httr::stop_for_status(response)

  token <- content(response)$access_token

  return(token)
}


#' POST data to an OGC SensorThings API endpoint
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
#' post_sta(
#'   object = "Things",
#'   body = list(name = "MyThing", description = "A test thing"),
#'   url = "https://example.com/SensorThings/v1.0/",
#'   token = "your_access_token"
#' )
#' }
#'
#' @importFrom httr POST add_headers
#' @importFrom jsonlite toJSON
#'
#' @export

post_sta <- function(object = c("Things","Sensors","ObservedProperties","Datastreams","Observations"), body, url, token){

  url <- .validate_sta_url(url)
  body_json <- toJSON(body, auto_unbox = TRUE)

  endpoint <- paste0(url, object)
  response <- POST(endpoint, body = body_json, encode = "json",
                   httr::add_headers(`Content-Type` = "application/json"),
                   .sta_auth(token))
  httr::stop_for_status(response)

  return(invisible(response))
}


#' Delete a resource from an OGC SensorThings API endpoint
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
#' The function uses the \strong{httr} package for HTTP requests.
#'
#' @return The response object from the DELETE request (an \code{httr::response} object).
#'
#' @examples
#' \dontrun{
#' delete_sta(
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

delete_sta <- function(object = c("Things","Sensors","ObservedProperties","Datastreams","Observations"), object_id, url, token){

  url <- .validate_sta_url(url)

  endpoint <- paste0(url, object, "(", object_id, ")")
  response <- DELETE(endpoint,
                     httr::add_headers(`Content-Type` = "application/json"),
                     .sta_auth(token))
  httr::stop_for_status(response)

  return(invisible(response))
}


#' Update a resource on an OGC SensorThings API endpoint (PATCH)
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
#' The function uses the \strong{httr} package for HTTP requests and the \strong{jsonlite} package for JSON conversion.
#'
#' @return The response object from the PATCH request (an \code{httr::response} object).
#'
#' @examples
#' \dontrun{
#' patch_sta(
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

patch_sta <- function(object = c("Things","Sensors","ObservedProperties","Datastreams","Observations"), object_id, url, token, body){

  url <- .validate_sta_url(url)
  body_json <- toJSON(body, auto_unbox = TRUE)

  endpoint <- paste0(url, object, "(", object_id, ")")
  response <- PATCH(endpoint, body = body_json, encode = "json",
                    httr::add_headers(`Content-Type` = "application/json"),
                    .sta_auth(token))
  httr::stop_for_status(response)

  return(invisible(response))
}


#' Construct a Bearer token authorization header
#'
#' @param token Character. The Bearer token.
#' @return An \code{httr} config object with the Authorization header set.
#' 
#' @noRd
#' 

.sta_auth <- function(token) {
  httr::add_headers(`Authorization` = paste("Bearer", token))
}


#' Parse an httr response body as JSON
#'
#' @param response An \code{httr::response} object.
#' @param simplify Logical. Passed to \code{simplifyVector} in \code{jsonlite::fromJSON}.
#'   Default \code{TRUE}.
#' 
#' @return A parsed R object.
#' 
#' @noRd
#' 

.parse_sta_response <- function(response, simplify = TRUE) {
  jsonlite::fromJSON(
    httr::content(response, as = "text", encoding = "UTF-8"),
    simplifyVector = simplify
  )
}


#' Load and validate Keycloak credentials
#'
#' Accepts a named list, or a path to a YAML/JSON file, and validates that all required Keycloak
#' fields are present.
#'
#' @param creds Named list or character path to a YAML/JSON credentials file.
#' @return A validated named list with keys \code{url}, \code{client_id}, \code{client_secret},
#'   \code{username}, \code{password}.
#'
#' @noRd
#' 

.read_creds <- function(creds) {

  if (is.null(creds)) {
    stop("Access denied. Please provide the 'creds' list containing your authentication details.")
  }
  if (is.character(creds)) {
    if (!file.exists(creds)) {
      stop("Credential file not found at: ", creds)
    }
    creds_ext <- tolower(tools::file_ext(creds))
    if (creds_ext %in% c("yaml", "yml")) {
      creds <- yaml::read_yaml(creds)
    } else if (creds_ext == "json") {
      creds <- jsonlite::fromJSON(txt = readLines(creds))
    } else {
      stop("Unsupported configuration file format. Use YAML (.yaml/.yml) or JSON (.json)")
    }
  }
  required_keys <- c("url", "client_id", "client_secret", "username", "password")
  missing_keys <- setdiff(required_keys, names(creds))
  if (length(missing_keys) > 0) {
    stop(
      "Invalid 'creds' structure.\n",
      "The following keys are missing from your credentials list: ",
      paste(missing_keys, collapse = ", "), "\n",
      "Please ensure 'creds' is a list with keys: url, client_id, client_secret, username, password."
    )
  }
  creds
}


#' Validate and normalise an OGC SensorThings API service root URL
#'
#' Checks that \code{url} contains an OGC STA version segment (\code{/v1.0} or
#' \code{/v1.1}), appending a trailing slash if absent. The trailing slash is
#' required because all resource paths are built with
#' \code{paste0(url, "Things")}.
#'
#' @param url Character. The candidate service root URL.
#' @return The normalised URL (character), guaranteed to end with a slash.
#'
#' @noRd

.validate_sta_url <- function(url) {
  if (!grepl("/v1\\.\\d+/?$", url)) {
    stop(
      "'url' does not look like a valid OGC SensorThings API service root. ",
      "Expected a versioned path, e.g. 'https://host/path/v1.0' or '.../v1.1/'. ",
      "Got: ", url
    )
  }
  if (!grepl("/$", url)) url <- paste0(url, "/")
  url
}

