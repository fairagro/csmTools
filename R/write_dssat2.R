#' Writes data to a single DSSAT weather file
#'
#' @export
#'
#' @param wth a tibble containing the data to write to a DSSAT
#' weather file
#'
#' @param file_name a character vector of length one that contains the name
#' of a single DSSAT file into which `wth` will be written
#'
#' @param force_std_fmt a logical value indicating whether to override the
#'   variable format stored within the `wth` object with standard DSSAT formatting
#'
#' @param location a character value that gives the location for the
#'   weather file header line
#'
#' @param comments a character vector containing any comments to be
#'   included in the weather file
#'
#' @param INSI Institute and site code (four-digit character code)
#'
#' @param LAT Latitude in decimal degrees
#'
#' @param LONG Longitude in decimal degrees
#'
#' @param ELEV Elevation in meters
#'
#' @param TAV Long-term average air temperature at reference
#'   height (typically 2 meters)
#'
#' @param AMP Long-term monthly air temperature amplitude at
#'   reference height (typically 2 meters)
#'
#' @param REFHT reference height for air temperature measurements
#'
#' @param WNDHT reference height for wind speed measurements
#'
#' @param CO2 carbon dioxide concentration in parts per million
#'
#' @return NULL
#'
#' @importFrom tibble tibble
#' @importFrom dplyr "%>%"
#' @importFrom stringr str_c
#' @importFrom purrr map
#'

write_wth2 <- function(wth, file_name, force_std_fmt = FALSE,
                      location = NULL, comments = NULL,
                      INSI = NULL, LAT = NULL, LONG = NULL,
                      ELEV = NULL, TAV = NULL, AMP = NULL,
                      REFHT = NULL, WNDHT = NULL, CO2 = NULL){
  
  if(is.null(location)){
    location <- attr(wth, "location")
  }
  
  location <- paste0('*WEATHER DATA : ', location)
  
  if(is.null(comments)){
    comments <- .format_comments(wth)
  }else{
    comments <- .format_comments(comments)
  }
  
  # Determine if wth was read from old or new file format
  general <- attr(wth,'GENERAL')
  
  if(is.null(general)){
    general <- data.frame(INSI = if(is.null(INSI)) NA_character_ else INSI,
                          LAT = if(is.null(LAT)) NA_real_ else LAT,
                          LONG = if(is.null(LONG)) NA_real_ else LONG,
                          ELEV = if(is.null(ELEV)) NA_real_ else ELEV,
                          TAV = if(is.null(TAV)) NA_real_ else TAV,
                          AMP = if(is.null(AMP)) NA_real_ else AMP,
                          REFHT = if(is.null(REFHT)) NA_real_ else REFHT,
                          WNDHT = if(is.null(WNDHT)) NA_real_ else WNDHT)
  }
  
  old_format <- is.data.frame(general)
  
  if(old_format){
    g_v_fmt <- attr(general,"v_fmt")
  }else{
    g_v_fmt <- do.call(
      c,
      lapply(general,
             function(.x){
               attr(.x,"v_fmt")
             })
    )
  }
  
  if(old_format){
    if(force_std_fmt | is.null(g_v_fmt)){
      g_v_fmt <- wth_v_fmt2("GENERAL", old_format = old_format)
    }
    
    # Replace columns in general data.frame with non-null function arguments
    # the double exclamation "bang-bang" operator (!!) forces assignment
    # using the function argument of the same name
    if(!is.null(INSI)) general$INSI <- INSI
    if(!is.null(LAT)) general$LAT <- LAT
    if(!is.null(LONG)) general$LONG <- LONG
    if(!is.null(ELEV)) general$ELEV <- ELEV
    if(!is.null(TAV)) general$TAV <- TAV
    if(!is.null(AMP)) general$AMP <- AMP
    if(!is.null(REFHT)) general$REFHT <- REFHT
    if(!is.null(WNDHT)) general$WNDHT <- WNDHT
    if(!is.null(CO2)) general$CO2 <- CO2
    
    attr(general, "v_fmt") <- g_v_fmt
    
    gen_out <-  c(
      DSSAT::write_tier(general,
                 drop_na_rows = FALSE),
      "")
  }else{
    if(force_std_fmt | is.null(g_v_fmt)){
      g_v_fmt <- wth_v_fmt2("GENERAL", old_format = old_format)
    }
    gen_out <- c(
      "*GENERAL",
      unlist(
        lapply(
          lapply(general,
                 function(.x){
                   attr(.x, "v_fmt") <- g_v_fmt
                   return(.x)
                 }),
          function(.y){
            c(
              DSSAT::write_tier(.y, drop_na_rows=FALSE),
              "")
          })
      ),
      "*DAILY DATA"
    )
  }
  
  d_v_fmt <- attr(wth,"v_fmt")
  
  if(force_std_fmt | is.null(d_v_fmt)){
    d_v_fmt <- wth_v_fmt2("DAILY")
  }
  
  attr(wth, "v_fmt") <- d_v_fmt
  
  tier_output <- c(
    location,
    "",
    comments,
    "",
    gen_out,
    DSSAT::write_tier(wth, drop_na_rows = FALSE)
  )
  
  write(tier_output, file_name)
  
  return(invisible())
}

#'
#'
#' @noRd
#'

wth_v_fmt2 <- function(section, old_format = FALSE) {
  
  if(section == "DAILY"){
    v_fmt <- c(DATE = "%5s", # instead of %7s
               SRAD = "%6.1f", TMAX = "%6.1f",
               TMIN = "%6.1f", RAIN = "%6.1f", WIND = "%6.0f",
               RHUM = "%6.1f", DEWP = "%6.1f", PAR = "%6.1f",
               EVAP = "%6.1f", VAPR = "%6.2f", SUNH = "%6.1f")
  } else if(section == "GENERAL") {
    
    if(old_format){
      
      v_fmt <- c(INSI = "%6s", LAT = "%9.3f", LONG = "%9.3f", ELEV = "%6.0f",
                 TAV = "%6.1f", AMP = "%6.1f", REFHT = "%6.1f",
                 WNDHT = "%6.1f", CO2 = "%6f")
    } else {
      
      v_fmt <- c(Latitude = "%9.1f", Longitud = "%9.2f", Elev = "%6.0f",
                 Zone = "%5f", TAV = "%7f", TAMP = "%6.1f", REFHT = "%6f",
                 WNDHT = "%6f", SITE = "%-s", WYR = "%4.0f",
                 WFIRST = "%8.0f", WLAST = "%8.0f", PEOPLE = "%-s",
                 ADDRESS = "%-s", METHODS = "%-s", INSTRUMENTS = "%-s",
                 PROBLEMS = "%-s", PUBLICATIONS = "%-s",
                 DISTRIBUTION = "%-s", NOTES = "%-s")
    }
  } else {
    
    v_fmt <- NULL
  }
}




#' Writes soil parameters to a single DSSAT soil parameter file (*.SOL)
#'
#' @export
#'
#' @inheritParams DSSAT::read_dssat
#'
#' @param sol a tibble of soil profiles that have been read in by read_sol()
#'
#' @param append TRUE or FALSE indicating whether soil profile should
#' be appended to file_name. If FALSE, the soil profile will be written
#' to a new file and will overwrite file_name (if it exists).
#'
#' @param title a length-one character vector that contains the title of the soil file
#'
#' @param force_std_fmt a logical value indicating whether to override the
#' variable format stored within the FileX object with standard DSSAT formatting
#'
#' @return Invisibly returns NULL
#'
#' @examples
#'
#' # Extract file path for sample soil file
#' sample_sol <- system.file('extdata','SAMPLE.SOL',package='DSSAT')
#'
#' # Read sample soil file
#' sol <- read_sol(sample_sol)
#'
#' # Create example soil file path
#' sample_sol2 <- paste0(tempdir(),'/SAMPLE.SOL')
#'
#' # Write example soil file
#' write_sol2(sol,sample_sol2)
#'

write_sol2 <- function(sol, file_name, title = NULL, append = TRUE, force_std_fmt = TRUE){
  
  if(is.null(title))  title <- attr(sol,'title')
  if(is.null(title))  title <- 'General DSSAT Soil Input File'
  
  comments <- .format_comments(sol)
  
  if(force_std_fmt | is.null(attr(sol,'v_fmt'))){
    attr(sol,'v_fmt') <- DSSAT::sol_v_fmt()
  }
  if(is.null(attr(sol,'tier_info'))){
    tier_info <- DSSAT::sol_tier_info()
    info_index <- unlist(
      lapply(tier_info,
             function(.x){
               any(.x != "SLB" & .x %in% names(sol))
             })
    )
    attr(sol,'tier_info') <- tier_info[info_index]
  }
  
  sol_out <- unlist(
    lapply(1:nrow(sol),
           function(.x){
             DSSAT::write_soil_profile(sol[.x,])
           })
  )
  
  # Insert comments
  # TODO: move inside write_sol_profile?
  if(is.list(comments)) {
    
    sol_out <- .insert_post_comments(sol_out, post_comments = comments$pedon)
    
    if(!append){
      sol_out <- c(
        paste0('*SOILS: ', title),
        comments$general,
        sol_out)
    }
  } else {
    
    if(!append){
      sol_out <- c(
        paste0('*SOILS: ', title),
        comments,
        sol_out)
    }
  }
  
  write(sol_out, file_name, append = append)
  
  return(invisible())
}


#' Reads parameters from a single DSSAT cultivar parameter file (*.CUL)
#'
#' @export
#'
#' @inheritParams DSSAT::read_dssat
#'
#' @param cul a DSSAT_tbl containing the contents of a DSSAT cultivar parameter file
#'
#' @return a tibble containing the data from the raw DSSAT output
#'
#' @examples
#'
#' # Extract file path for sample cultivar file path
#' sample_cul_file <- system.file('extdata','SAMPLE.CUL',package='DSSAT')
#'
#' # Read sample cultivar file
#' cul <- read_cul(sample_cul_file)
#'
#' # Create example cultivar file path
#' sample_cul_file2 <- paste0(tempdir(),'/SAMPLE.CUL')
#'
#' # Write out sample cultivar file
#' write_cul2(cul,sample_cul_file2)
#'

write_cul2 <- function(cul, file_name){
  
  first_line <- attr(cul,'first_line')
  
  comments <- .format_comments(cul)
  
  switches <- attr(cul,'switches')
  switches <- paste0("!", switches)
  
  ctable <- DSSAT::write_tier(cul,
                       pad_name = c('VAR-NAME','VRNAME'))
  
  tier_output <- c(
    first_line,
    '',
    comments,
    "",
    ctable[1],
    switches,
    ctable[-1])
  
  write(tier_output, file_name)
  
  return(invisible(NULL))
}




#'
#'
#' @noRd
#'

.format_comments <- function(x_in){
  comments <- attr(x_in, "comments")
  
  if(is.null(comments) & is.character(x_in)) comments <- x_in
  
  if(is.list(comments)){
    gen_comments <- purrr::reduce(comments, intersect)
    post_comments <- lapply(comments, function(x) setdiff(x, gen_comments))
    post_comments <- Filter(function(x) length(x) > 0, post_comments)  # drop empty comments
    
    
    if(!is.null(gen_comments)){
      gen_comments <- gsub(" +$", "", # remove trailing spaces
                           gsub("^[ ]{0,1}", "! ", # add initial !
                                gsub("^ *!", "", gen_comments))) # strip off any initial !
    }
    if(!is.null(post_comments) | length(post_comments) == 0){
      post_comments <- lapply(post_comments, function(lines) {
        gsub(" +$", "",
             gsub("^[ ]{0,1}", "! ",
                  gsub("^ *!", "", lines)))
      })
    }
    comments <- list(general = gen_comments, pedon = post_comments)
  } else {
    if(!is.null(comments)){
      comments <- gsub(" +$", "", # remove trailing spaces
                       gsub("^[ ]{0,1}", "! ", # add initial !
                            gsub("^ *!", "", comments))) # strip off any initial !
    }
  }
  
  return(comments)
}


#'
#'
#' @noRd
#'

.insert_post_comments <- function(raw_lines, post_comments) {
  
  new_lines <- raw_lines
  pedon_comments <- post_comments
  
  # Iterating backwards to ensure line indices remain stable
  pedon_names_to_insert <- rev(names(pedon_comments))
  
  for (p_name in pedon_names_to_insert) {
    comments_to_insert <- pedon_comments[[p_name]]
    # Find the focal profile based on pedon
    pattern <- paste0("^\\*", p_name)
    insert_index <- grep(pattern, new_lines)
    
    # Check that we found exactly one line to insert after
    if (length(insert_index) == 1) {
      new_lines <- append(new_lines,
                          values = comments_to_insert,
                          after = insert_index)
    } else if (length(insert_index) == 0) {
      warning(paste("Could not find line for PEDON:", p_name))
    } else {
      # TODO: insert multiple times?
      warning(paste("Found multiple lines for PEDON:", p_name, ". No comments inserted."))
    }
  }
  
  return(new_lines)
}
