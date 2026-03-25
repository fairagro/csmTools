#' Extract and split weather data for the cultivation season
#'
#' Filters weather tables in a DSSAT experiment list to the years spanned by the
#' cultivation season, splits them by year, computes yearly weather statistics, and
#' merges those statistics into the metadata tables. The original weather tables are
#' replaced by the split, annotated versions in the returned list.
#'
#' @param dssat_exp A named list representing a DSSAT experiment, containing one or more
#'   tables whose names include `"WEATHER"` (expected to contain `"DAILY"` and `"METADATA"`
#'   sub-tables) alongside other experiment components.
#'
#' @return A named list structured as `dssat_exp`, with the original `"WEATHER"` tables
#'   removed and replaced by:
#'   \itemize{
#'     \item Per-year `METADATA` tables (with yearly weather statistics joined in)
#'     \item Per-year `DAILY` tables
#'   }
#'   Table names are suffixed with the full 4-digit year (e.g., `"WEATHER_DAILY_2012"`).
#'
#' @details
#' The cultivation season bounds are determined by [identify_production_season()] using
#' `period = "cultivation_season"`. Weather data is filtered to years overlapping that
#' season using the 2-digit `YEAR` column present in DSSAT weather tables.
#'
#' Yearly statistics are computed by `calculate_wth_stats()` and joined to each
#' corresponding metadata table on all shared columns.
#'
#' @noRd
#' 

extract_season_weather <- function(dssat_exp) {
  
  # Find weather tables
  wth_tbls <- dssat_exp[grepl("WEATHER", names(dssat_exp))]
  
  # Identify bounds of the cultivation season
  cs_dates <- identify_production_season(
    dssat_exp,
    period = "cultivation_season",
    dssat_date_fmt = TRUE
  )
  cs_years <- unique(lubridate::year(cs_dates))
  cs_years_short <- substr(cs_years, 3, 4)
  year_map <- setNames(as.character(cs_years), cs_years_short)
  
  wth_split <- list()
  
  for (wth_name in names(wth_tbls)) {
    df <- wth_tbls[[wth_name]]
    
    # Filter focal years
    df_filtered <- filter(df, YEAR %in% cs_years_short)
    
    # Split into year-level data frames
    split_list <- split(df_filtered, f = df_filtered$YEAR)
    
    # Return as flat list with years as suffix
    for (short_year in names(split_list)) {
      full_year <- year_map[short_year]
      nm <- paste(wth_name, full_year, sep = "_")
      wth_split[[nm]] <- split_list[[short_year]]
    }
  }
  
  # Calculate yearly statistics
  wth_data <- wth_split[grepl("DAILY", names(wth_split))]
  wth_metadata <- wth_split[grepl("METADATA", names(wth_split))]
  wth_tstats <- lapply(wth_data, calculate_wth_stats)
  wth_metadata <- mapply(
    function(x, y) left_join(x, y, by = intersect(colnames(x), colnames(y))),
    wth_metadata, wth_tstats,
    SIMPLIFY = FALSE
  )
  
  # Update dataset
  dssat_exp[grepl("WEATHER", names(dssat_exp))] <- NULL
  out <- c(dssat_exp, wth_metadata, wth_data)
  
  return(out)
}
