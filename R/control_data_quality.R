#'
#'
#'
#'
#'
#'
#' @export
#'


flag_missing_weather <- function(wdata, coverage = NULL) {
  
  # Convert DATE column to Date format
  wdata[["DATE"]] <- as.Date(wdata[["DATE"]], format = "%y%j")
  wthDates <- wdata[["DATE"]]
  wthYears <- unique(year(wthDates))
  
  # Define coverage period if not specified
  if (is.null(coverage)) {
    coverage <- c(
      as.Date(paste(min(wthYears), "01-01", sep = "-")),
      as.Date(paste(max(wthYears), "12-31", sep = "-"))
    )
  } else {
    coverage <- as.Date(coverage, format = "%Y-%m-%d")
  }
  
  # Generate full date sequence for the coverage period
  gs_seq <- data.frame(DATE = seq(coverage[1], coverage[2], by = "day"))
  
  # Merge weather data with full date sequence & ensure correct ordering
  wdata <- full_join(wdata, gs_seq, by = "DATE") |> arrange(DATE)
  
  # Identify columns with at least one non-NA value
  vars <- setdiff(colnames(wdata)[colSums(!is.na(wdata)) > 0], "DATE")
  
  # Find missing dates for each variable
  wnas <- lapply(vars, function(var) {
    naRow <- wdata$DATE[which(is.na(wdata[[var]]))]
    if (length(naRow) == 0) return(NULL)
    return(naRow)
  })
  
  # Remove empty entries
  vars <- vars[!sapply(wnas, is.null)]
  wnas <- wnas[!sapply(wnas, is.null)]
  names(wnas) <- vars
  
  # Output messages based on results
  if (length(wnas) == 0) {
    message(sprintf("Daily weather coverage complete for the period %s - %s", coverage[1], coverage[2]))
  } else {
    allNas <- unlist(wnas)
    fromNas <- as.Date(min(allNas))
    toNas <- as.Date(max(allNas))
    nNas <- length(allNas)
    warning(paste("Missing daily weather data detected at", nNas, "locations between", fromNas, "and", toNas),
            call. = FALSE)
  }
  wnas <- if (length(wnas) == 0) list() else wnas
  
  return(invisible(wnas))
}

#'
#'
#'
#'
#'
#'
#' @export
#'

check_weather_coverage <- function(mngt, wth, period = c("growing season", "year")){
  
  # Init vectors
  gsNas <- yrNas <- c()
  
  # Retrieve and classify data
  plSec <- xtables[["PLANTING_DETAILS"]]
  # TODO: what if no harvest date is specified?
  haSec <- xtables[["HARVEST"]]
  wthDates <- as.Date(wth[["DATE"]], format = "%y%j")
  
  plDate <- as.Date(plSec[["PDATE"]], format = "%y%j")
  haDate <- as.Date(haSec[["HDATE"]], format = "%y%j")
  
  ### ---- Check coverage -------------------
  
  # Check if the weather coverage includes the whole growing season
  if (plDate <= min(wthDates)) {
    warning(
      "The weather data does not cover planting.\nPlease ensure daily weather is available across the entire growing season or edit management event sequence.",
      call. = FALSE
    )
  } else if (haDate >= max(wthDates)){
    warning(
      "The weather data does not cover harvest.\nPlease ensure daily weather is available across the entire growing season or edit management event sequence.",
      call. = FALSE
    )
  } else if (plDate <= min(wthDates) && haDate >= max(wthDates)) {
    warning("The weather data does not cover planting and harvest.\nPlease ensure weather data so it cover the entire growing season or edit management event sequence.",
            call. = FALSE
    )
  }
  
  ### ---- Check missing data ---------------
  
  # Check if the weather file covers the entire year (requirement for crop calendarss)
  if (period == "growing season") {
    nas <- flag_missing_weather(wth, coverage = c(plDate, haDate))
  } else if (period == "year") {
    from = as.Date(paste(
      year(plDate),
      "01-01",
      sep = "-"))
    to = as.Date(paste(
      year(haDate),
      "01-01",
      sep = "-"))
    nas <- flag_missing_weather(wth, coverage = c(from, to))
  }
  
  return(nas)
}

#' TMP?
#' 
#' 
#' @importFrom rlang !! :=
#' @importFrom dplyr select all_of mutate anti_join bind_rows arrange
#' @importFrom lubridate leap_year

impute_missing_days <- function(data, variable, method = "loess") {
  
  #TMP
  if (!("doy" %in% colnames(data)) || !(variable %in% colnames(data))) {
    stop("Data must contain 'doy' and the specified variable.")
  }
  
  # Ensure doy is numeric
  data <- data |>
    select(doy, year, month, all_of(variable)) |>
    mutate(doy = as.numeric(doy))
  
  # Generate full sequence of days per year
  years <- as.numeric(unique(data$year))
  expected_days <- do.call(rbind, lapply(years, function(x) {
    data.frame(year = x, doy = if (leap_year(x)) seq(1, 366) else seq(1, 365))
  }))
  
  # Identify missing days
  missing_days <- expected_days |> anti_join(data, by = c("year", "doy"))
  
  if (nrow(missing_days) == 0) {
    return(data)  # No missing days, return original data
  }
  
  # Fit LOESS model for interpolation
  loess_model <- loess(as.formula(paste(variable, "~ doy")), data = data, span = 0.2)
  predicted_values <- predict(loess_model, newdata = missing_days)
  
  # Create interpolated dataset
  interpolated_data <- missing_days |>
    mutate(!!variable := predicted_values) |>
    select(doy, year, !!variable)

  # Merge interpolated data back into the original dataset
  data_filled <- bind_rows(
    data |> select(year, doy, !!variable),
    interpolated_data
  ) |>
    arrange(year, doy)
  
  return(data_filled)
}

#'
#'
#'
#'

# Quality check minimum data
check_data_requirements <- function(mngt, soil, obs, wth,
                                    framework = "dssat",
                                    std = c("minimum", "optimal")){
  
  # Convert all data inputs to list
  alldat <- list(mngt, obs, soil, weather)
  alldat <- lapply(alldat, function(d){
    if (is.data.frame(d)){
      d <- list(d)
    }
    return(d)
  })
  
  # Get map 
  map <- load_map()  # tmp! set map path internal to package
  
  map <- map[map$data_model == framework,]  #TODO: CHECK IF IN SUBFUNCTIONS
  
  if (std == "minimum") {
    map <- map[map$DQ_mindat == 2,]
  } else if (std == "optimal") {
    map <- map[map$DQ_mindat > 0,]
  }
  
  
  ###------------ Check for minimum data ----------------------------------
  
  check_minimum_data <- function(df, sec, map = load_map(),
                                 framework = "dssat",
                                 std = c("minimum", "optimal")) {
    
    # df = mngt[["FIELDS"]]
    # sec = "FIELDS"
    # map = load_map()
    # framework = "dssat"
    # std = "minimum"
    print(sec)
    
    # Identify the expected variables for this section
    if (std == "minimum") {
      map <- map[map$DQ_mindat == 2,]
    } else if (std == "optimal") {
      map <- map[map$DQ_mindat > 0,]
    }
    expected_vars <- map$header[map$section %in% sec]    
    
    
    # Find which expected variables are missing in the actual dataframe
    missing_vars <- setdiff(expected_vars, names(df))
    warn_missing <- if (length(missing_vars) > 0){
      paste("  ->", str_to_title(std), "requirement variable",
            missing_vars, "is missing")
    }
    
    # Find if expected vars contain only NAs
    is_all_na <- apply(df, 2, function(x) all(is.na(x)))
    all_na_vars <- names(df[, is_all_na])
    all_na_vars <- intersect(expected_vars, all_na_vars)
    warn_allna <- if (length(all_na_vars) > 0){
      paste("  ->", str_to_title(std), "requirement variable",
            all_na_vars, "contains no data")
    }
    
    # Find if expected vars contain at least one missing values
    has_na <- apply(df, 2, function(x) any(is.na(x)))
    any_na_vars <- names(df[, is_all_na])
    any_na_vars <- intersect(expected_vars, any_na_vars)
    any_na_vars <- setdiff(any_na_vars, all_na_vars)
    warn_anyna <- if (length(any_na_vars) > 0){
      paste("  ->", str_to_title(std), "requirement variable",
            all_na_vars, "contains missing values")
    }
    
    # Generate warning messages
    msg_section <- paste0("Section ", sec, ":")
    warn_messages <- c(msg_section, warn_missing, warn_allna, warn_anyna)
    
    # Print warnings
    if (length(warn_messages) > 1) {
      message(paste(warn_messages, collapse = "\n"))
    }
    
    # Create markdown report with only relevant sections
    report_sections <- list()
    if (length(missing_vars) > 0)
      report_sections$missing_vars <- glue(
        "#### Missing columns: {paste(glue('`{missing_vars}`'), collapse = ', ')}"
      )
    if (length(all_na_vars) > 0)
      report_sections$all_na_vars <- glue(
        "#### Variables containing no data: {paste(glue('`{all_na_vars}`'), collapse = ', ')}"
      )
    if (length(any_na_vars) > 0) 
      report_sections$any_na_vars <- glue(
        "#### Variables containing missing values: {paste(glue('`{any_na_vars}`'), collapse = ', ')}"
      )
    
    # If all sections are empty, return NULL (so it's ignored in lapply)
    if (length(report_sections) == 0) {
      message(paste("Minimum requirements met for section", sec))
      return(NULL)
    }
    
    # Compile final report with dynamic title
    report <- glue("### Section {sec}\n\n{paste(report_sections, collapse = '\n\n')}")
    
    
    # Print markdown report
    return(report)
  }
  
  reports <- lapply(names(mngt), function(sec) {
    df <- mngt[[sec]]
    check_minimum_data(df, sec, map, framework = "dssat", std = "minimum")
  })
  
  soil_secs <- unique(map$section[grepl("SOIL", map$section)])
  
  soil_reports <- lapply(soil_secs, function(sec) {
    check_minimum_data(soil, sec, map, framework = "dssat", std = "minimum")
  })
  
  wth_secs <- unique(map$section[grepl("WEATHER", map$section)])
  
  wth_reports <- lapply(wth_secs, function(sec) {
    check_minimum_data(wth, sec, map, framework = "dssat", std = "minimum")
  })
  
  check_minimum_data(soil_dssat, sec = ,
                     map, framework = "dssat", std = "minimum")
  
  # Drop NULL (sections with no issue detected)
  reports <- Filter(Negate(is.null), reports)
  
  # Bind reports
  mngt_report <- paste(reports, collapse = "\n")
  
  writeLines(mngt_report, "tmp.md")  # Save to file
  
  check_req_sec <- function(mngt){
    
    
  }
  
  
  
  
  check_minimum_data(soil_dssat, map, framework = "dssat", std = "minimum")
  
  
  
  
  names(missing_vars) <- names(mngt)  # Label the output with section names
  
  
  
  
  
  
  return(tmp)
}


#### ---- 2025-10-23 IMPLEMENT SECTION CHECKS BASED ON MINIMUM REQUIREMENTS

