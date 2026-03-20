#' Calculate annual temperature statistics from weather data
#'
#' Computes TAV (annual mean temperature) and AMP (half-amplitude of monthly mean temperatures)
#' following DSSAT conventions. Handles DSSAT date format (YYDDD) and supports both station-level (INSI)
#' and experiment-level (EXP_ID) groupings.
#'
#' @param wth_data Data frame with columns DATE, TMAX, TMIN, and optionally EXP_ID/INSI for grouping
#'
#' @return Data frame with columns YEAR, AMP, TAV, plus any grouping columns (EXP_ID/INSI) present
#'   in input. Returns single row of NAs if required columns missing or data empty.
#'
#' @details
#' **Processing steps:**
#' \enumerate{
#'   \item Validate required columns (DATE, TMAX, TMIN) exist
#'   \item Calculate daily average temperature: (TMAX + TMIN) / 2
#'   \item Parse DSSAT date format (YYDDD):
#'         - Extract 2-digit year (YY > 50 → 1900s, else 2000s)
#'         - Extract day-of-year (DDD)
#'         - Convert to calendar month
#'   \item Group by experiment/station, year, and month
#'   \item Calculate monthly mean temperatures
#'   \item Group by experiment/station and year
#'   \item Calculate annual statistics:
#'         - AMP = (max monthly mean - min monthly mean) / 2
#'         - TAV = mean of all daily average temperatures
#' }
#'
#' **DSSAT conventions:**
#' \itemize{
#'   \item **TAV**: Annual average of daily mean temperatures
#'   \item **AMP**: Half the difference between warmest and coldest monthly mean temperatures
#'         (used for soil temperature calculations)
#'   \item **Date format**: YYDDD where YY is 2-digit year, DDD is day-of-year
#' }
#'
#' @noRd
#'

calculate_wth_stats <- function(wth_data) {
  
  if (nrow(wth_data) == 0 ||
      !all(c("DATE", "TMAX", "TMIN") %in% names(wth_data))) {
    out_null <- data.frame(
      YEAR = NA_character_,
      AMP = NA_real_,
      TAV = NA_real_
    )
    return(out_null)
  }
  
  wth_tstats <- wth_data |>
    dplyr::mutate(
      TAVG = (TMAX + TMIN) / 2,
      # Breakdown DSSAT dates to extract year and month
      yr_2digit = as.numeric(substr(DATE, 1, 2)),
      doy = as.numeric(substr(DATE, 3, 5)),
      FULL_YEAR = ifelse(yr_2digit > 50, 1900 + yr_2digit, 2000 + yr_2digit),
      MO = lubridate::month(as.Date(paste0(FULL_YEAR, "-01-01")) + lubridate::days(doy - 1))
    ) |>
    dplyr::group_by(across(any_of(c("EXP_ID", "INSI"))), FULL_YEAR, MO) |>
    dplyr::mutate(MO_TAV = mean(TAVG, na.rm = TRUE)) |>
    dplyr::ungroup() |>
    dplyr::group_by(across(any_of(c("EXP_ID", "INSI"))), YEAR) |>
    dplyr::summarise(
      AMP = (max(MO_TAV) - min(MO_TAV)) / 2,
      TAV = mean(TAVG, na.rm = TRUE), .groups = "drop"
    ) |>
    dplyr::distinct()
  
  return(wth_tstats)
}
