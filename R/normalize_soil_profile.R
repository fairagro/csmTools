#' Normalize soil profile depths
#'
#' Standardizes DSSAT soil profile data to a predefined sequence of depths using interpolation.
#' Handles numeric soil properties while preserving metadata and categorical variables.
#'
#' @param data (list/dataframe) Input soil profile data in DSSAT format or as a list with SOIL element.
#' @param depth_seq (numeric) Target depth sequence for normalization (in cm).
#'   Default: `c(5,10,20,30,40,50,60,70,90,110,130,150,170,190,210)`.
#' @param method (character) Interpolation method. Options: `"linear"` (default) or `"constant"`.
#' @param diagnostics (logical) Whether to generate diagnostic plots. Default: `TRUE`.
#' @param output_path (character) Optional file path to save the output.
#'
#' @return A list containing the normalized soil profile data frame (element named "SOIL").
#'
#' @examples
#' # Normalize a soil profile to standard depths and save to file
#' normalized_soil <- normalize_soil_profile(
#'   data = my_soil_data,
#'   depth_seq = c(0, 10, 30, 60, 100, 200),
#'   method = "linear",
#'   diagnostics = TRUE,
#'   output_path = "path/to/normalized_soil.csv"
#' )
#'
#' @note
#' - Only numeric columns are interpolated; categorical variables are preserved.
#' - If a depth in `depth_seq` already exists in the input, the original value is retained.
#' - Diagnostic plots are saved as PNG files when `output_path` is provided.
#'
#' @importFrom tidyr everything replace_na gather
#' @importFrom dplyr mutate relocate arrange right_join across filter select
#' @importFrom ggplot2 ggplot aes geom_point geom_path facet_wrap theme_bw ggsave
#'
#' @export
#' 

normalize_soil_profile <- function(data,
                                   depth_seq = c(5,10,20,30,40,50,60,70,90,110,130,150,170,190,210),
                                   method = "linear",
                                   diagnostics = TRUE,
                                   output_path = NULL) {
  
  # Resolve input data
  if (is.data.frame(data)) {
    data <- list(SOIL = data)
  }
  data_list <- resolve_input(data)
  data <- data_list[["SOIL"]]
  
  # Preserve additional attributes
  attr <- attributes(data)[!names(attributes(data)) %in% c("names", "row.names", "class")]
  
  depth_col <- which(colnames(data) == "SLB")
  headers <- data[, 1:depth_col]
  profile <- data[, depth_col:ncol(data)]  # profile table starting with depth col in standard DSSAT format
  profile_nas <- profile[, colSums(is.na(profile)) == nrow(profile)]  # stored only NAs
  profile_fct <- profile[, sapply(profile, is.character) & (colSums(is.na(profile)) < nrow(profile))]  # stored character variables
  profile <- profile[, colSums(is.na(profile)) < nrow(profile)]  # remove missing variables (only NAs)
  profile <- profile[, sapply(profile, is.numeric)]  # remove categorical variables
  
  interp <- data.frame(SLB = setdiff(depth_seq, profile$SLB))
  for (i in 2:ncol(profile)){
    approx <- approx(x = profile$SLB, y = profile[[i]], xout = setdiff(depth_seq, profile$SLB), method = method, rule = 2)
    interp <- cbind(interp, approx$y)
  }
  names(interp) <- names(profile)
  
  std_profile <- rbind(profile, interp)
  
  # Make diagnostic plots
  if (diagnostics) {
    
    plot_df <- std_profile %>%
      mutate(src = ifelse(SLB %in% setdiff(depth_seq, profile$SLB), "interpolated", "input")) %>%
      relocate(src, .before = SLB) %>%
      gather("var", "value", 3:ncol(.)) %>%
      arrange(var, -SLB)
    
    plot <- ggplot(plot_df, aes(x = value, y = -SLB)) +
      geom_point(aes(colour = src)) +
      geom_path() +
      facet_wrap(~var, ncol = 4, scales = "free_x") +
      theme_bw()
    
    if (!is.null(output_path)) {
      plot_file_name <- file.path(dirname(output_path), "soil_normalization_diagnostics.png")
      ggsave(
        filename = plot_file_name,
        plot,
        width = 15, height = 12, units = "cm",
        dpi = 600,
        bg = "white"
      )
    }
    print(plot)
  }
  
  # Format normalized soil profile
  normalized_profile <- cbind(headers, profile_nas, profile_fct) %>%
    right_join(std_profile, by = "SLB") %>%
    mutate(across(everything(), ~ {
      unique_vals <- unique(na.omit(.))
      if (length(unique_vals) == 1) {
        replace_na(., unique_vals)
      } else {
        .
      }
    })) %>%  # duplicate all header values in interpolated layers
    mutate(DEPTH = max(SLB)) %>%  # update max depth in header if relevant
    filter(SLB %in% depth_seq) %>%
    arrange(SLB) %>%
    select(colnames(data))
  
  # Restore preserved attributes
  attributes(normalized_profile) <- c(attributes(normalized_profile), attr)
  out <- list(SOIL = normalized_profile)
  out <- export_output(out, output_path = output_path)

  return(out)
}
