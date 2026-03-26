#' Reads parameters from a single DSSAT cultivar parameter file (*.CUL)
#'
#' @export
#'
#' @inheritParams DSSAT::read_dssat
#'
#' @inheritParams DSSAT::read_filex
#'
#' @return a tibble containing the data from the raw DSSAT output
#'
#' @importFrom dplyr "%>%" first
#' @importFrom readr cols col_character
#' @importFrom stringr str_detect str_subset str_which
#' @importFrom purrr map reduce
#'
#' @examples
#'
#' # Extract file path for sample cultivar file path
#' sample_cul_file <- system.file('extdata','SAMPLE.CUL',package='DSSAT')
#'
#' # Read sample cultivar file
#' cul <- read_cul(sample_cul_file)
#'
#'

read_cul2 <- function(file_name, col_types=NULL, col_names=NULL,
                     left_justified=c('VAR#', 'VARNAME\\.*', 'VAR-NAME\\.*','VRNAME\\.*'),
                     use_std_fmt = TRUE){
  
  cul_col_types <- cols(`VAR#`=col_character(),
                        `VARNAME\\.*`=col_character(),
                        `VAR-NAME\\.*`=col_character(),
                        `VRNAME\\.*`=col_character(),
                        `  ECO#`=col_character(),
                        ` EXPNO`=col_character(),
                        `  EXP#`=col_character())
  
  if(str_detect(file_name,'SCCSP')){
    col_names <- col_names %>%
      c(.,'Stalk','Sucro','Null1',
        'TB(1)','TO1(1)','TO2(1)',
        'TB(2)','TO1(2)','TO2(2)',
        ' *TM(1)',' *TM(2)',
        'StHrv','RTNFAC','Null7',
        'RES30C','RLF30C') %>%
      unique()
  }
  
  if(!is.null(col_types)){
    col_types$cols <- c(cul_col_types$cols, col_types$cols)
  }else{
    col_types <- cul_col_types
  }
  
  # Read in raw data from file
  raw_lines <- readLines(file_name, warn = FALSE)
  
  first_line <- raw_lines %>%
    head(1)
  
  comments <- DSSAT:::extract_comments(raw_lines)
  
  switches_index <- c(grep("^Coeff", comments), grep("^Calibr", comments))
  switches <- comments[switches_index]
  comments <- comments[-switches_index]
  
  begin <- raw_lines %>%
    str_which('^@')
  
  end <- begin %>%
    tail(-1) %>%
    {. - 1} %>%
    c(.,length(raw_lines))
  
  if(use_std_fmt){
    tier_fmt <- DSSAT:::cul_v_fmt(file_name)
  }else{
    tier_fmt <- NULL
  }
  
  cul <- map(1:length(begin),
             ~ DSSAT::read_tier_data(raw_lines[begin[.]:end[.]],
                             col_types = cul_col_types,
                             col_names = col_names,
                             left_justified = left_justified,
                             tier_fmt = tier_fmt,
                             convert_date_cols = FALSE,
                             # Workaround for current issue leading to incorrect v_fmt
                             # being linked to the cul table. V_fmt input used only
                             # to set column width and data type. Recalculated with
                             # 'construct_variable_format' regardless of whether it
                             # was provided as an arg or not. In the case of CUL file this
                             # can produce deviation from the actual cul_v_fmt template
                             # For now wporkaround rather than direct correction in
                             # read_tier_data, uncertain what impact this may have in other instances
                             store_v_fmt = FALSE)) %>%
    reduce(DSSAT:::combine_tiers)
  
  attr(cul,'v_fmt') <- tier_fmt
  attr(cul,'first_line') <- first_line
  attr(cul,'switches') <- switches
  attr(cul,'comments') <- comments
  
  cul <- DSSAT::as_DSSAT_tbl(cul)
  
  return(cul)
}


#' Reads soil parameters from a single DSSAT soil parameter file (*.SOL)
#'
#' @export
#'
#' @inheritParams DSSAT::read_dssat
#'
#' @param id_soil a length-one character vector containing the soil ID code for a
#' single soil profile
#' @param nested a logical to set whether layer data should be nested in the output
#' (one row per soil profile)
#'
#' @return a tibble containing the data from the raw DSSAT file
#'
#' @importFrom dplyr "%>%" first
#' @importFrom tidyr unnest all_of
#' @importFrom stringr str_subset str_replace str_extract str_which str_c
#' @importFrom purrr map reduce
#' @importFrom readr cols col_double col_character
#'
#' @examples
#'
#' # Extract file path for sample soil file
#' sample_sol <- system.file('extdata','SAMPLE.SOL',package='DSSAT')
#'
#' sol <- read_sol(sample_sol)
#'

read_sol2 <- function(file_name, id_soil = NULL, nested = TRUE){
  
  # Read in raw data from file
  # exclude lines that are all spaces or lines with EOF in initial position
  raw_lines <- grep("^(?!\032) *([^ ]+)",
                    readLines(file_name, warn = FALSE),
                    perl = TRUE,
                    value = TRUE)
  
  # Specify left-justified columns
  left_justified <- c('SITE','COUNTRY',' SCS FAMILY', ' SCS Family')
  
  # Specify column types
  col_types <- readr::cols(
    `      LAT`=col_double(),
    `     LONG`=col_double(),
    SSAT=col_double(),
    ` SCS FAMILY`=col_character(),
    ` SCS Family`=col_character(),
    SCOM=col_character(),
    COUNTRY=col_character(),
    SITE=col_character(),
    SMHB=col_character(),
    SMPX=col_character(),
    SMKE=col_character(),
    SLMH=col_character(),
    SLB=col_double()
  )
  # {.$cols <- c(.$cols,col_types$cols);.}
  
  # Store title and comments
  basename <- basename(file_name)
  title <- gsub("\\*SOILS", "", raw_lines[1])
  title <- trimws(gsub(":", "", title))
  
  # Find start/end positions for each soil profile (PEDON)
  pedon_raw_start_end <- DSSAT:::find_pedon(raw_lines)
  comments_lines <- find_comments(raw_lines)
  comments <- link_soil_comments(comments_lines, pedon_raw_start_end)
  
  # Drop comments and empty lines
  clean_lines <- DSSAT:::drop_empty_lines( drop_comments(raw_lines) )
  pedon_clean_start_end <- DSSAT:::find_pedon(clean_lines)
  
  # Filter profiles based on id_soil
  if(!is.null(id_soil)){
    pedon_clean_start_end <- pedon_clean_start_end[pedon_clean_start_end$PEDON %in% id_soil, ]
    comments <- comments[[id_soil]]
  }
  
  # Extract general information for each PEDON
  gen_info <- DSSAT:::read_sol_gen_info(clean_lines[pedon_clean_start_end$start])
  
  # Strip out and concatenate the lines for each PEDON
  pedon_lines <- DSSAT:::concat_lines(clean_lines, pedon_clean_start_end)
  
  # Find start/end positions for each soil data tier within each PEDON
  tier_start_end <- DSSAT:::find_tier(pedon_lines)
  
  # Strip out and concatenate the lines for each soil data tier
  tier_lines <- DSSAT:::concat_lines(pedon_lines$lines, tier_start_end)
  
  # Remove empty lines
  tier_lines <- with(tier_lines,
                     tier_lines[lines != "", ])
  
  # Read all lines by the same header
  tiers_out <- lapply(unique(tier_lines$header),
                      function(h){
                        raw_lines <- c(h,
                                       with(tier_lines, lines[header == h]))
                        
                        pedon <- with(tier_lines, PEDON[header == h])
                        
                        tier_data <- DSSAT::read_tier_data(raw_lines,
                                                    left_justified = left_justified,
                                                    col_types = col_types,
                                                    tier_fmt = DSSAT:::sol_v_fmt(),
                                                    convert_date_cols = FALSE)
                        
                        tier_data$PEDON <- pedon
                        
                        colnames(tier_data) <- toupper(colnames(tier_data))
                        
                        return(tier_data)
                      })
  
  layer_ind <- sapply(tiers_out, is_sol_layer)
  
  # Create layer-specific data frame with rows nested by PEDON
  layer_data <- DSSAT:::nest_rows(
    # Recursively merge layer-specific data
    DSSAT:::recursive_merge(
      # Subset for list elements with layer-specific data
      tiers_out[layer_ind],
      by = c("PEDON", "SLB")),
    by = "PEDON")
  
  # Create whole profile data frame with one row per PEDON
  profile_data <- DSSAT:::recursive_merge(
    c(list(gen_info),
      tiers_out[!layer_ind]),
    by = c("PEDON"))
  
  # Merge whole-profile and layer-specific data
  tiers_out <- DSSAT:::coalesce_merge(profile_data, layer_data)
  
  # Return layers as nested list if nest set to TRUE
  if (!nested) {
    list_cols <- names(tiers_out)[sapply(tiers_out, is.list)]
    tiers_out <- as.data.frame(
      unnest(tiers_out, cols = all_of(list_cols))
    )
  }
  
  # Attach metadata
  attr(tiers_out, "file_name") <- basename
  attr(tiers_out, "title") <- title
  attr(tiers_out, "comments") <- comments
  
  return(tiers_out)
}


#' Find comment lines
#'
#' Scans raw lines for comments (lines beginning with "!") and returns them
#' along with their line numbers.
#'
#' @param raw A character vector where each element is a line of text.
#'
#' @return A data.frame with two columns: `line_number` (the original index)
#'   and `comment_text` (the full text of the comment line).
#'
#' @export
#'

find_comments <- function(raw) {
  
  comment_indices <- grep("^!", raw)
  
  if (length(comment_indices) == 0) {
    return(data.frame(line_number = integer(0), comment_text = character(0)))
  }
  comment_text <- raw[comment_indices]
  
  return(data.frame(line_number = comment_indices, comment_text = comment_text))
}


#' Helper function to link comments to pedon
#'
#'
#' @noRd
#'

link_soil_comments <- function(comments_lines, pedon_start_end) {
  
  if (nrow(comments_lines) == 0 || nrow(pedon_start_end) == 0) {
    return(list())
  }
  
  first_pedon_start <- min(pedon_start_end$start)
  all_pedon_names <- pedon_start_end$PEDON
  
  # Separate general and post comments
  is_general <- comments_lines$line_number < first_pedon_start
  gen_comments <- comments_lines[is_general, ]
  post_comments <- comments_lines[!is_general, ]
  
  # Assign post comments to closest pedon
  if (nrow(post_comments) > 0) {
    pedon_starts <- pedon_start_end$start
    pedon_names <- pedon_start_end$PEDON
    
    pedon_ind <- sapply(post_comments$line_number, function(comment_line) {
      distances <- abs(comment_line - pedon_starts)
      which.min(distances)
    })
    
    post_comments$PEDON <- pedon_names[pedon_ind]
  } else {
    post_comments$PEDON <- character(0)
  }
  
  # Build output list
  names(all_pedon_names) <- all_pedon_names
  
  out <- lapply(all_pedon_names, function(current_pedon) {
    
    specific_comments_df <- post_comments[post_comments$PEDON == current_pedon, ]
    
    combined_df <- rbind(
      gen_comments[, c("line_number", "comment_text")],
      specific_comments_df[, c("line_number", "comment_text")]
    )
    combined_df_sorted <- combined_df[order(combined_df$line_number), ]
    
    return(combined_df_sorted$comment_text)
  })
  
  return(out)
}
