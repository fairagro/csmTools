#' Calculate Haversine Distance
#'
#' @description
#' Calculates the great-circle distance between two geographic points on Earth using the Haversine formula.
#'
#' @param lat1 Latitude of the first point, in decimal degrees.
#' @param lon1 Longitude of the first point, in decimal degrees.
#' @param lat2 Latitude of the second point, in decimal degrees.
#' @param lon2 Longitude of the second point, in decimal degrees.
#'
#' @return The distance between the two points, in meters.
#'
#' @examples
#' # Calculate the distance between Paris and Lyon
#' haversine_dist(48.8566, 2.3522, 45.7640, 4.8357)
#'
#' @noRd
#' 

haversine_dist <- function(lat1, lon1, lat2, lon2) {
  
  R <- 6371000 # Earth radius in meters
  dLat <- (lat2 - lat1) * pi / 180
  dLon <- (lon2 - lon1) * pi / 180
  a <- sin(dLat/2)^2 + cos(lat1 * pi / 180) * cos(lat2 * pi / 180) * sin(dLon/2)^2
  c <- 2 * atan2(sqrt(a), sqrt(1-a))
  d <- R * c
  return(d)
}
