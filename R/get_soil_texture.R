#' Classify soil texture using USDA Triangle
#'
#' This function determines the USDA soil texture class (e.g., "Loam", "Silty Clay")
#' based on the particle size fractions of clay, silt, and sand.
#'
#' @param clay_frac Numeric. Clay fraction of the soil sample (0-1).
#' @param silt_frac Numeric. Silt fraction of the soil sample (0-1).
#' @param sand_frac Numeric. Sand fraction of the soil sample (0-1).
#' 
#' @return A character vector of USDA soil texture classes.
#'
#' @noRd
#' 

.get_texture_scalar <- function(clay_frac, silt_frac, sand_frac, system = "usda") {
  
  # Check for NA/NULL inputs
  if (is.na(clay_frac) || is.na(silt_frac) || is.na(sand_frac)) {
    warning("Particle size distribution data incomplete. Soil texture cannot be estimated.")
    return(NA_character_)
  }
  
  # Convert fractions to percentages to align with classification logic
  clay <- clay_frac * 100
  silt <- silt_frac * 100
  sand <- sand_frac * 100
  
  # Check for valid sum, allowing for small rounding errors
  total <- clay + silt + sand
  if (abs(total - 100) > 1) {
    warning(paste0("Inputs sum to ", round(total, 2), ", not 100. Review particle size input data."))
  }

  # USDA Classification Logic
  if (silt + 1.5 * clay < 15) {
    texture <- "S"     # Sand
  } else if (silt + 1.5 * clay >= 15 & silt + 2 * clay < 30) {
    texture <- "LS"    # Loamy sand
  } else if ((clay >= 7 & clay < 20 & sand > 52 & silt + 2 * clay >= 30) | (clay < 7 & silt < 50 & silt + 2 * clay >= 30)) {
    texture <- "SL"    # Sandy loam
  } else if (clay >= 7 & clay < 27 & silt >= 28 & silt < 50 & sand <= 52) {
    texture <- "L"     # Loam
  } else if ((silt >= 50 & clay >= 12 & clay < 27) | (silt >= 50 & silt < 80 & clay < 12)) {
    texture <- "SIL"   # Silty loam
  } else if (silt >= 80 & clay < 12) {
    texture <- "SI"    # Silt
  } else if (clay >= 20 & clay < 35 & silt < 28 & sand > 45) {
    texture <- "SCL"   # Sandy clay loam
  } else if (clay >= 27 & clay < 40 & sand > 20 & sand <= 45) {
    texture <- "CL"    # Clay loam
  } else if (clay >= 27 & clay < 40 & sand <= 20) {
    texture <- "SICL"  # Silty clay loam
  } else if (clay >= 35 & sand > 45) {
    texture <- "SC"    # Sandy clay
  } else if (clay >= 40 & silt >= 40) {
    texture <- "SIC"   # Silty clay
  } else if (clay >= 40 & sand <= 45 & silt < 40) {
    texture <- "C"     # Clay
  } else {
    texture <- NA_character_ # Fallback for unclassified
  }
  
  return(texture)
}


#' Classify soil texture from particle size fractions
#'
#' A vectorized wrapper around \code{.get_texture_scalar()} for classifying soil texture
#' from clay, silt, and sand fractions.
#'
#' @param clay_frac Numeric. Clay fraction of the soil sample (0-1).
#' @param silt_frac Numeric. Silt fraction of the soil sample (0-1).
#' @param sand_frac Numeric. Sand fraction of the soil sample (0-1).
#'
#' @details
#' Applies \code{.get_texture_scalar()} element-wise across vectors of particle size fractions.
#' All three arguments are vectorized.
#'
#' @return A character vector of soil texture classifications.
#'
#' @examples
#' \dontrun{
#' get_soil_texture(clay_frac = c(0.2, 0.4), silt_frac = c(0.3, 0.3), sand_frac = c(0.5, 0.3))
#' }
#'
#' @export
#' 

# Vectorize the scalar function to accept vectors
get_soil_texture <- Vectorize(
  .get_texture_scalar, 
  vectorize.args = c("clay_frac", "silt_frac", "sand_frac")
)
