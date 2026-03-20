#'
#'
#'

split_dssat_components <-  function(dataset,
                                    sec = c("MANAGEMENT_CORE", "MANAGEMENT", "SUMMARY", "TIME_SERIES", "SOIL", "WEATHER"),
                                    merge = TRUE) {
  
  sections <- match.arg(sec, several.ok = TRUE)
  
  components <- lapply(sections, function(current_sec) {
    switch(
      current_sec,
      "MANAGEMENT_CORE" = .extract_dssat_mngt_core(dataset, merge),
      "MANAGEMENT" = .extract_dssat_mngt(dataset, merge),
      "SUMMARY" = .extract_dssat_obs(dataset, "SUMMARY"),
      "TIME_SERIES" = .extract_dssat_obs(dataset, "TIME_SERIES"),
      "SOIL" = .extract_dssat_soil(dataset, merge),
      "WEATHER" = .extract_dssat_wth(dataset, merge)
    )
  })
  names(components) <- sections
  
  # Drop NULL objects (sections not in input data)
  # TODO: test with missing soil/wth
  components <- compact(components)
  
  return(components)
}


#'
#'
#' @noRd
#'

.extract_dssat_obs <- function(dataset, sec = c("SUMMARY", "TIME_SERIES")) {
  
  section <- match.arg(sec, several.ok = FALSE)

  obs_components <- dataset[names(dataset) %in% section]
  return(obs_components[[section]])
}


#'
#'
#' @noRd
#'

.extract_dssat_mngt <- function(dataset, merge = TRUE) {
  
  mngt_components <- dataset[
    !grepl("SOIL_META|SOIL_GENERAL|SOIL_LAYERS|WEATHER|SUMMARY|TIME_SERIES", names(dataset))
  ]
  
  # HACK! TODO: move to extract template??
  # Format provenance by nested attributes by experiment-year
  mngt_components[["GENERAL"]] <- mngt_components[["GENERAL"]] %>%
    group_by(across(any_of(c("EXP_ID", "EXP_YEAR")))) %>%
    summarise(
      across(
        !any_of(c("EXP_ID", "EXP_YEAR")),
        ~paste(unique(na.omit(.x)), collapse = "; ")
      ),
      .groups = "drop"
    )
  
  if (merge) {
    mngt_components <- reduce_by_join(mngt_str_components)
  } 
  
  return(mngt_components)
}


#'
#'
#' @noRd
#'

.extract_dssat_mngt_core <- function(dataset, merge = TRUE) {
  
  # Keep only structural components of management
  # i.e., those with standard identifiers: EXPERIMENT (metadata), FIELDS, CULTIVARS
  mngt_str_components <- dataset[names(dataset) %in% c("GENERAL","FIELDS","CULTIVARS")]
  
  # HACK! TODO: move to extract template??
  # Format provenance by nested attributes by experiment-year
  mngt_str_components[["GENERAL"]] <- mngt_str_components[["GENERAL"]] %>%
    group_by(across(any_of(c("EXP_ID", "EXP_YEAR")))) %>%
    summarise(
      across(
        !any_of(c("EXP_ID", "EXP_YEAR")),
        ~paste(unique(na.omit(.x)), collapse = "; ")
      ),
      .groups = "drop"
    )
  
  if (merge) {
    mngt_str_components <- reduce_by_join(mngt_str_components)
  }
  
  return(mngt_str_components)
}


#'
#'
#' @noRd
#'

.extract_dssat_soil <- function(dataset, merge = TRUE) {
  
  soil_components <- list(
    meta = dataset[["SOIL_META"]],
    general = dataset[["SOIL_GENERAL"]],
    layers = dataset[["SOIL_LAYERS"]]
  )
  # TODO: keep? must be handled by PEDON
  soil_notes <- c(
    unlist(soil_components$meta$SL_METHODS_COMMENTS),
    unlist(soil_components$meta$SL_PROF_NOTES)
  )
  
  if (merge) {
    soil_components <- reduce_by_join(soil_components)
  }
  
  return(soil_components)
}

#'
#'
#' @noRd
#'

.extract_dssat_wth <- function(dataset, merge = TRUE) {
  
  wth_components <- list(
    meta = dataset[["WEATHER_METADATA"]],
    data = dataset[["WEATHER_DAILY"]]
  )
  # TODO: keep? must be handled by WSTA+YEAR
  wth_notes <- unlist(
    lapply(wth_components, function(df) attr(df, "comments")),
    use.names = FALSE
  )
  
  if (merge) {
    wth_components <- reduce_by_join(wth_components)
  }
  
  return(wth_components)
}