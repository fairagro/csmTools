#' Download, format, and impute daily weather time series
#'
#' Fetches weather data for a target location and time range from a supported source repository
#' and returns it as a named list of data frames.
#'
#' @param lon Numeric. Longitude of the target location in decimal degrees.
#' @param lat Numeric. Latitude of the target location in decimal degrees.
#' @param from Character or Date. Start date of the requested time range in
#'   \code{"YYYY-MM-DD"} format (inclusive).
#' @param to Character or Date. End date of the requested time range in
#'   \code{"YYYY-MM-DD"} format (inclusive).
#' @param pars Character vector. Target weather variables to retrieve. One or more of
#'   \code{"air_temperature"}, \code{"precipitation"}, \code{"solar_radiation"}, \code{"dewpoint"},
#'   \code{"relative_humidity"}, \code{"wind_speed"}, \code{"par"}, or \code{"evaporation"}.
#' @param res Character. Temporal resolution. One of \code{"daily"} or \code{"hourly"}.
#' @param src Character. Source repository. One of \code{"dwd"} or \code{"nasa_power"}.
#' @param output_path Character. Optional file path to save the output.
#'
#' @details
#' Validates \code{src}, \code{pars}, and \code{res} against supported values, then dispatches to
#' the appropriate download handler. For \code{"nasa_power"}, each element of \code{pars} is mapped to its
#' corresponding NASA POWER parameter code and fetched via \code{nasapower::get_power()}.
#' The \code{"dwd"} source is currently a placeholder.
#'
#' @return A named list containing a \code{WEATHER_DAILY} data frame with the requested variables
#'   for the specified location and time range.
#'
#' @importFrom nasapower get_power
#'
#' @export
#'

get_weather_data <- function(lon, lat, from, to, pars, res, src, output_path = NULL){
  
  # Check arguments
  src_handlers <- c("dwd", "nasa_power")
  src <- match.arg(src, src_handlers)
  pars_handlers <- c("air_temperature", "precipitation", "solar_radiation", "dewpoint",
                     "relative_humidity", "wind_speed", "par", "evaporation")
  pars <- match.arg(pars, pars_handlers, several.ok = TRUE)
  res_handlers <- c("daily", "hourly")
  res <- match.arg(res, res_handlers)
  
  # --- Fetch data from source ---
  switch(src,
         "dwd" = {
           return(message("dwd currently just a placeholder..."))
         },
         "nasa_power" = {
           
           # --- Define parameters ---
           params <- unlist(
             lapply(pars, function(x) {
               switch(x,
                      air_temperature = c("T2M", "T2M_MAX", "T2M_MIN"), 
                      precipitation = "PRECTOTCORR",
                      solar_radiation = "ALLSKY_SFC_SW_DWN",
                      wind_speed = "WS2M",
                      dewpoint = "T2MDEW",
                      relative_humidity = "RH2M",
                      par = "ALLSKY_SFC_PAR_TOT",
                      evaporation = "EVLAND",
                      x)
             })
           )
           # --- Downdload data ---
           wth_raw <- get_power(
             community = "ag",
             pars = params,
             temporal_api = res,
             lonlat = c(lon, lat),
             dates = c(as.character(from), as.character(to))
           )
         })
  
  # Remove external pointer attributes (not serialized at output resolution)
  attr(wth_raw, "problems") <- NULL
  # Format as a named list for mapping
  wth_out <- list(
    WEATHER_DAILY = wth_raw
  )
  
  wth_out <- export_output(wth_out, output_path = output_path)
  
  return(wth_out)
}
