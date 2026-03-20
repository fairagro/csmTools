#' Disable water and/or nitrogen stress by adding management treatments
#'
#' Creates new treatment(s) with automatic irrigation and/or nitrogen saturation fertilization to
#' remove corresponding stress limitations from simulations.
#'
#' @param xtables Experiment file tables (X-file structure)
#' @param stress Character vector specifying stress(es) to disable: `"water"`, `"nitrogen"`, or both.
#'   Default is `c("water", "nitrogen")`.
#'
#' @return Modified `xtables` with added treatment(s) and associated management records (simulation
#'   controls for water, fertilizer applications for nitrogen)
#'
#' @details
#' This function adds management practices that eliminate specified stress
#' factors from simulations:
#'
#' \itemize{
#'   \item **Water stress**: Enables automatic irrigation (IRRIG = "A") that maintains optimal
#'         soil moisture throughout the growing season
#'   \item **Nitrogen stress**: Adds three fertilizer applications totaling 120 kg N/ha at different
#'         dates to ensure non-limiting N availability
#'   \item **Both**: Creates a single treatment combining both practices for fully non-limiting growth conditions
#' }
#'
#' A new treatment is automatically created with a descriptive name ("Auto-irrigation", "N saturation",
#' or "Auto-irrigation + N saturation") and linked to the appropriate management records.
#'
#' @examples
#' \dontrun{
#' # Disable both water and nitrogen stress (default)
#' xtables <- disable_stress(xtables)
#'
#' # Disable only water stress
#' xtables <- disable_stress(xtables, stress = "water")
#'
#' # Disable only nitrogen stress
#' xtables <- disable_stress(xtables, stress = "nitrogen")
#' }
#'
#' @export
#'

disable_stress <- function(xtables, stress = c("water", "nitrogen")) {
  
  if ("water" %in% stress) {
    sm <- get_xfile_sec(xtables, "SIMULATION")
    sm <- sm[max(nrow(sm), 1), ]  # Keep last row if multiple levels exist
    sm_list <- lapply(sm, identity)
    sm_list$WATER <- "Y" 
    sm_list$IRRIG <- "A"
    
    xtables <- add_management(xtables, section = "simulation_controls", args = sm_list)
    
    sm_index <- max(get_xfile_sec(xtables, "SIMULATION")[1])
  }
  
  if ("nitrogen" %in% stress) {
    # TODO: change to add 5-10 kg every 2-3 days from first to last day of growing season
    fe_list <- list(
      FDATE = c("1981-10-29", "1982-03-24", "1981-06-10"),
      FMCD = rep("FE041", 3), FACD = rep("AP001", 3), FDEP = 1,
      FAMN = 120, FAMP = 0, FAMK = 0, FAMC = 0, FAMO = 0
    )
    
    xtables <- add_management(xtables, section = "fertilizer", args = fe_list)
    
    fe_index <- max(get_xfile_sec(xtables, "FERTILIZER")[1])
  }
  
  if (identical(sort(stress), c("nitrogen", "water"))) {
    attrs <- list(TNAME = "Auto-irrigation + N saturation", SM = sm_index, MF = fe_index)
  } else if ("water" %in% stress) {
    attrs <- list(TNAME = "Auto-irrigation", SM = sm_index)
  } else if ("nitrogen" %in% stress) {
    attrs <- list(TNAME = "N saturation", MF = fe_index)
  }
  
  out <- add_treatment(xtables, args = attrs)
  return(out)
}
