#' Lookup cereal growth stage dates
#'
#' Extracts and standardizes growth stage dates from phenological data using specified growth stage
#' scales (e.g., BBCH) and selection rules.
#'
#' @param data Data frame containing phenological observations output from the `phenocover` tool,
#'   i.e., daily time series where each day is labeled with a cereal growth stage derived from NDVI
#'   time-series of a focal cereal canopy 
#'   \itemize{
#'     \item `Date` (character or Date) - Observation dates in DD/MM/YYYY format
#'     \item `Growth_Stage` (character) - Growth stage names. See data(growth_stages) for terminology.
#'   }
#' @param gs_scale Character. Growth stage scale to use.
#'   \itemize{
#'     \item `bbch` BBCH scale for cereals
#'     \item `zadoks` Zadoks scale for cereals
#'   }
#' @param gs_codes Numeric. Optional vector of specific growth stage codes to extract.
#'   If `NULL` (default), all available codes are used.
#' @param date_select_rule Character. Rule for selecting dates when multiple observations exist
#'   for a growth stage. Options:
#'   \itemize{
#'     \item `"median"` (default) - Use median date
#'     \item `"first"` - Use earliest date
#'     \item `"last"` - Use latest date
#'     \item `"all"` - Return all dates
#'   }
#' @param output_path Character. Optional path to save output. If `NULL` (default), returns data as a list.
#'
#' @details This function performs the following steps:
#' \itemize{
#'   \item Maps growth stages to standardized codes using reference scales
#'   \item Calculates scaled codes within each growth stage range
#'   \item Applies selected rule to handle multiple observations
#'   \item Returns dates in both original and day-of-year formats
#' }
#'
#' Scaled codes are calculated by linearly interpolating between the minimum and maximum codes for
#' each growth stage based on the relative position of each observation date within the stage's date range.
#'
#' @return
#' A named list containing:
#' \itemize{
#'   \item `CEREAL_PHENOLOGY`: Data frame with columns:
#'     \itemize{
#'       \item `scale` - Growth stage scale used
#'       \item `growth_stage` - Growth stage name
#'       \item `scaled_code` - Standardized growth stage code
#'       \item `doy` - Day of year
#'       \item `Date` - Original date
#'       \item `date` - Processed date (when applicable)
#'       \item `year` - Year (when applicable)
#'     }
#' }
#' If `output_path` is specified, saves to files in addition to returning the list
#'
#' @examples
#' # Using default BBCH scale with median dates
#' gs_dates <- lookup_gs_dates(phenology_data)
#'
#' # Get first observation dates for specific codes
#' gs_dates <- lookup_gs_dates(
#'   phenology_data,
#'   gs_codes = c(30, 65, 89),
#'   date_select_rule = "first"
#' )
#'
#' # Save all observations to file
#' lookup_gs_dates(phenology_data, date_select_rule = "all",
#'                output_path = "gs_dates_output/")
#'
#' @importFrom dplyr filter group_by summarise mutate left_join select distinct n rename
#' @importFrom lubridate ymd year yday
#' 
#' @export
#' 

# Get all unique growth stages and their code ranges
lookup_gs_dates <- function(data, gs_scale = "bbch", gs_codes = NULL,
                            date_select_rule = c("median", "first", "last", "all"), output_path = NULL) {
  
  data <- resolve_input(data)
  
  # Validate date_select_rule input
  date_select_rule <- match.arg(date_select_rule)
  
  scales <- growth_stages
  
  stage_info <- scales %>%
    filter(scale == gs_scale) %>%
    group_by(scale, growth_stage) %>%
    summarise(
      min_code = min(code),
      max_code = max(code),
      code_count = n(),
      .groups = "drop"
    )
  
  # Then scale the dates
  scaled_df <- data %>%
    mutate(Date = as.Date(Date, format = "%d/%m/%Y")) %>%
    group_by(Growth_Stage) %>%
    mutate(
      min_date = min(Date),
      max_date = max(Date),
      position = as.numeric(Date - min_date) / as.numeric(max_date - min_date)
    ) %>%
    left_join(stage_info, by = c("Growth_Stage" = "growth_stage")) %>%
    rename(growth_stage = Growth_Stage) %>%
    mutate(
      # Calculate scaled code
      scaled_code = min_code + position * (max_code - min_code),
      # Round appropriately based on how many codes exist for this stage
      scaled_code = ifelse(code_count > 10, round(scaled_code, 1),
                           ifelse(code_count > 5, round(scaled_code, 0),
                                  round(scaled_code)))
    ) %>%
    ungroup() %>%
    select(scale, growth_stage, Date, scaled_code) %>%
    distinct()
  
  if (is.null(gs_codes)) {
    gs_codes <- unique(scaled_df$scaled_code)
  }
  
  date_lookup <- scaled_df %>%
    mutate(
      date = ymd(Date),
      year = year(Date),
      doy = yday(Date)
    ) %>%
    select(date, year, doy) %>%
    unique()
  
  # Handle different selection rules
  gs_single_dates <- scaled_df %>%
    filter(scaled_code %in% gs_codes) %>%
    mutate(doy = yday(Date), date = ymd(Date))
  
  if (date_select_rule == "all") {
    gs_single_dates <- gs_single_dates %>%
      select(scale, growth_stage, scaled_code, doy, Date)
  } else {
    # For first/last/median, we need to group and summarize
    gs_single_dates <- gs_single_dates %>%
      group_by(scale, growth_stage, scaled_code) %>%
      summarise(
        doy = case_when(
          date_select_rule == "median" ~ round(median(doy, na.rm = TRUE)),
          date_select_rule == "first" ~ min(doy, na.rm = TRUE),
          date_select_rule == "last" ~ max(doy, na.rm = TRUE)
        ),
        date = case_when(
          date_select_rule == "median" ~ as.Date(median(as.numeric(Date)), origin = "1970-01-01"),
          date_select_rule == "first" ~ min(Date, na.rm = TRUE),
          date_select_rule == "last" ~ max(Date, na.rm = TRUE)
        ),
        .groups = "drop"
      ) %>%
      # Ensure we have the Date column in the output
      mutate(Date = date) %>%
      select(scale, growth_stage, scaled_code, doy, Date)
  }
  
  # Join with date lookup if needed
  if ("doy" %in% names(gs_single_dates)) {
    gs_single_dates <- gs_single_dates %>%
      left_join(date_lookup, by = "doy")
  }
  
  out <- list(CEREAL_PHENOLOGY = gs_single_dates)
  out <- export_output(out, output_path = output_path)
  
  return(out)
}

