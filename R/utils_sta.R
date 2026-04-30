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
  
  body_json <- toJSON(body, auto_unbox = TRUE)
  
  url <- paste0(url, object)
  response <- POST(url, body = body_json, encode = "json",
                   add_headers(
                     `Content-Type` = "application/json",
                     `Authorization` = paste("Bearer", token)
                   ))
  
  return(invisible(response))
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
  
  url <- paste0(url, object, "(", object_id, ")")
  response <- DELETE(url,
                     add_headers(
                       `Content-Type` = "application/json",
                       `Authorization` = paste("Bearer", token)
                     ))
  
  return(invisible(response))
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
  
  body_json <- toJSON(body, auto_unbox = TRUE)
  
  url <- paste0(url, object, "(", object_id, ")")
  response <- PATCH(url, body = body_json, encode = "json",
                    add_headers(
                      `Content-Type` = "application/json",
                      `Authorization` = paste("Bearer", token)
                    ))
  
  return(invisible(response))
}
