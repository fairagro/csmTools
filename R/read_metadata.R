#' Read and format experiment metadata from the BonaRes repository metadata XML file
#'
#' This function reads a metadata XML file based on the BonaRes metadata schema and extracts key fields for use as input
#' arguments in crop experiment data wrangling functions. It parses spatial, temporal, provenance, contact, legal, and funding
#' information, and returns a tidy one-row tibble suitable for downstream processing.
#'
#' @param path Character. Path to the metadata XML file.
#' @param schema Character. Name of the metadata schema (default: "bonares"). Currently not used in the function logic,
#'   but reserved for future extensions.
#'
#' @return A tibble (one row) with the following columns:
#' \describe{
#'   \item{Experiment_name}{Experiment name(s) as a single string.}
#'   \item{Experiment_title}{Experiment title(s) as a single string.}
#'   \item{Experiment_doi}{Experiment DOI(s) as a single string.}
#'   \item{Contact_persons}{Contact person(s), separated by "; ".}
#'   \item{Institutions}{Institution(s), separated by "; ".}
#'   \item{Contact_email}{Contact email(s), separated by "; ".}
#'   \item{Legal_constraints}{Legal constraints, separated by "; ".}
#'   \item{Funding}{Funding information, separated by "; ".}
#'   \item{Date_published}{Earliest date found in the metadata (as.Date).}
#'   \item{Date_revised}{Most recent date found in the metadata (as.Date).}
#'   \item{Coordinate_system}{Coordinate system as a string (e.g., "EPSG:4326 v8.9").}
#'   \item{Longitude}{Mean longitude (numeric) from west/east bounds.}
#'   \item{Latitude}{Mean latitude (numeric) from north/south bounds.}
#' }
#'
#' @details
#' The function is designed for BonaRes and similar metadata standards, but can be adapted for other ISO 19139-based schemas. It uses XPath
#' queries to extract relevant fields and collapses multiple values with "; " where appropriate.
#' Dates are parsed as `Date` objects and the earliest/latest are used for publication and revision, respectively.
#' 
#' @seealso [xml2::read_xml()], [tibble::tibble()]
#'
#' @examples
#' \dontrun{
#' # Read metadata from XML file
#' meta <- read_metadata("path/to/xml")
#' print(meta)
#' }
#'
#' @importFrom xml2 read_xml xml_ns xml_text xml_find_all xml_find_first
#' @importFrom tibble tibble as_tibble
#'
#' @export
#' 

read_metadata <- function(mother_path, schema = "bonares") {
  
  # Helper to extract, filter, and collapse text
  extract_collapse <- function(doc, xpath, ns, collapse = TRUE) {
    vals <- xml2::xml_text(xml2::xml_find_all(doc, xpath, ns))
    vals <- vals[nzchar(vals)]
    if (collapse) {
      vals <- paste(vals, collapse = "; ")
    }
    vals
  }
  extract_first <- function(doc, xpath, ns) {
    val <- xml2::xml_text(xml2::xml_find_first(doc, xpath, ns))
    ifelse(nzchar(val), val, NA_character_)
  }
  
  # Read metadata file
  metadata <- xml2::read_xml(mother_path)
  # Get namespace mapping
  ns <- xml2::xml_ns(metadata)
  
  
  ##---- Spatial coverage ----
  coord_system <- paste0(
    extract_first(metadata, ".//gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:codeSpace/gco:CharacterString", ns), ":",
    extract_first(metadata, ".//gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:code/gco:CharacterString", ns), " v",
    extract_first(metadata, ".//gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:version/gco:CharacterString", ns)
  )
  west_bound <- as.numeric(extract_first(metadata, ".//gmd:westBoundLongitude/gco:Decimal", ns))
  east_bound <- as.numeric(extract_first(metadata, ".//gmd:eastBoundLongitude/gco:Decimal", ns))
  north_bound <- as.numeric(extract_first(metadata, ".//gmd:northBoundLatitude/gco:Decimal", ns))
  south_bound <- as.numeric(extract_first(metadata, ".//gmd:southBoundLatitude/gco:Decimal", ns))
  
  
  ##---- Temporal coverage ----
  # NOTE 2025-07-14: not in schema anymore!
  # exp_start <- as.numeric(extract_first(metadata, ".//gml:beginPosition", ns))
  # exp_end <- as.numeric(extract_first(metadata, ".//gml:endPosition", ns))
  
  
  ##---- Provenance metadata ----
  # Experiment metadata
  exp_name  <- extract_collapse(metadata, ".//gmd:hierarchyLevelName/gco:CharacterString", ns)
  exp_title <- extract_collapse(metadata, ".//gmd:identificationInfo/bnr:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString", ns)
  exp_doi   <- extract_collapse(metadata, ".//gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString", ns)
  # Contacts
  persons <- extract_collapse(metadata, ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:individualName/gco:CharacterString", ns, collapse = FALSE)
  institutions <- extract_collapse(metadata, ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString", ns)
  emails <- extract_collapse(metadata, ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString",
                             ns, collapse = FALSE)
  # Legal constraints
  legalConstraints <- extract_collapse(metadata, ".//gmd:identificationInfo/bnr:MD_DataIdentification/gmd:resourceConstraints", ns)
  # Funding
  funding <- c(
    extract_first(metadata, ".//bnr:funderName/gco:CharacterString", ns),
    extract_first(metadata, ".//bnr:MD_FundingReference/bnr:identifier/bnr:MD_ReferenceIdentifier/bnr:identifier/gco:CharacterString", ns),
    extract_first(metadata, ".//bnr:awardNumber/gco:CharacterString", ns),
    extract_first(metadata, ".//bnr:awardTitle/gco:CharacterString", ns)
  )
  funding <- paste(funding[nzchar(funding)], collapse = "; ")
  
  
  ##---- Dates ----
  dates <- xml2::xml_text(
    xml2::xml_find_all(
      metadata,
      ".//gmd:identificationInfo/bnr:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gco:Date",
      ns))
  dates <- as.Date(dates)
  dates <- dates[!is.na(dates)]
  date_published <- if (length(dates) > 0) min(dates) else NA
  date_revised   <- if (length(dates) > 0) max(dates) else NA
  
  
  ##---- Variable key ----
  var_key <- tryCatch({
    as_tibble(
      get_dataset_varkeys(mother_path, schema = "bonares")
    )
  }, error = function(e) {
    warning("Failed to retrieve variable keys. Server may be down. Original error: ", e$message)
    return(NULL)
  })
  
  
  ##---- Output ----
  exp_metadata <- tibble(
    Experiment_name = exp_name,
    Experiment_title = exp_title,
    Experiment_doi = exp_doi,
    Legal_constraints = legalConstraints,
    Funding = funding,
    Date_published = date_published,
    Date_revised = date_revised,
    Coordinate_system = coord_system,
    Longitude = mean(c(west_bound, east_bound), na.rm = TRUE),
    Latitude = mean(c(north_bound, south_bound), na.rm = TRUE)
  )
  persons <- tibble(
    Contact_persons = persons,
    Contact_email = emails
  )
  institutions <- tibble(
    Institutions = institutions
  )
  
  list(
    metadata = list(METADATA = exp_metadata,
                    PERSONS = persons,
                    INSTITUTIONS = institutions),
    variable_key = var_key
  )
}


#' Extract leaf table identifiers from a BonaRes metadata XML file
#'
#' Parses a mother-table XML metadata file and retrieves information about all linked child (leaf) tables via
#' their foreign key declarations.
#'
#' @param mother_path Path to the mother table's XML metadata file.
#' @param schema A string indicating the metadata schema (e.g. \code{"bonares"}) (placeholder).
#'
#' @return A data frame (one row per foreign key) with columns:
#' \describe{
#'   \item{identifier}{Unique identifier of the linked leaf table.}
#'   \item{tbl_name}{Name of the leaf table.}
#'   \item{foreign_key}{Column name of the foreign key in the leaf table.}
#' }
#'
#' @noRd
#' 

get_leaf_ids <- function(mother_path, schema = "bonares") {
  
  metadata <- xml2::read_xml(mother_path)
  ns <- xml2::xml_ns(metadata)  # Namespace mapping
  
  # Find all variable nodes linked to mother table's foreign keys
  var_nodes <- xml2::xml_find_all(metadata, "//bnr:MD_ForeignKey")
  
  leaves <- purrr::map(var_nodes, function(x) {
    id <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:identifier"))
    tbl_name <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:table"))
    fkey <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:column"))
    # Group into a list
    list(identifier = id, tbl_name = tbl_name, foreign_key = fkey)
  })
  dplyr::bind_rows(leaves)
}


#' Extract variable metadata from a BonaRes XML metadata file
#'
#' Parses a table's XML metadata file and returns a data frame describing all columns, including their names,
#' descriptions, units, and data types.
#'
#' @param path Path to the XML metadata file.
#' @param schema A string indicating the metadata schema (e.g. \code{"bonares"}). Currently unused but reserved
#'   for future schema support.
#'
#' @return A data frame (one row per column) with columns:
#' \describe{
#'   \item{file_name}{Name of the source table.}
#'   \item{name}{Column name.}
#'   \item{description}{Human-readable description of the column.}
#'   \item{methods}{Measurement or derivation methods.}
#'   \item{unit}{Unit of measurement.}
#'   \item{data_type}{Data type of the column.}
#'   \item{missing_vals}{Symbol or value used to represent missing data.}
#' }
#'
#' @noRd
#' 

get_varkey <- function(path, schema = "bonares") {
  
  metadata <- xml2::read_xml(path)
  ns <- xml2::xml_ns(metadata)  # Namespace mapping
  
  # Find all nodes macthing table column descriptions
  var_nodes <- xml2::xml_find_all(metadata, "//bnr:MD_Column")
  # Extract the data for each variable as a named vector
  vars <- purrr::map(var_nodes, function(x) {
    file_name <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:tableName"))
    name <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:name"))
    descript <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:description"))
    methods <- xml2::xml_text(xml2::xml_find_first(x, "//bnr:methods"))
    unit <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:unit"))
    data_type <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:dataType"))
    nas <- xml2::xml_text(xml2::xml_find_first(x, "//bnr:missingValue"))
    # Group into a list
    list(file_name = file_name,
         name = name,
         description = descript,
         methods = methods,
         unit = unit,
         data_type = data_type,
         missing_vals = nas)
  })
  # Combine the list of named vectors into a data frame
  key <- dplyr::bind_rows(vars)
  
  return(key)
}

#' Build a combined variable key for an entire dataset
#'
#' Retrieves and merges column-level metadata for a mother table and all its linked leaf tables,
#' producing a single data frame describing every variable across the dataset.
#'
#' @param mother_path Path or URL to the mother table's XML metadata file.
#' @param schema A string indicating the metadata schema (e.g.\code{"bonares"}). (placeholder)
#'
#' @details Leaf table identifiers are resolved via \code{get_leaf_ids()}, and their metadata XML files
#'    are fetched from the BonaRes REST endpoint. Table names are simplified by stripping the longest common filename
#' prefix, file extensions, and optional leading table-number patterns.
#'   Duplicate rows are removed before returning.
#'
#' @return A data frame with one row per unique variable across all tables,
#'   with columns:
#' \describe{
#'   \item{file_name}{Original filename with \code{.csv} extension.}
#'   \item{tbl_name}{Simplified table name.}
#'   \item{name}{Column name.}
#'   \item{description}{Human-readable description.}
#'   \item{methods}{Measurement or derivation methods.}
#'   \item{unit}{Unit of measurement.}
#'   \item{data_type}{Data type.}
#'   \item{missing_vals}{Missing value indicator.}
#' }
#'
#' @noRd

get_dataset_varkeys <- function(mother_path, schema = "bonares") {
  
  # Build URL for all table metadata xml
  leaves <- get_leaf_ids(mother_path)
  paths <- paste0("https://maps.bonares.de/finder/resources/dataform/xml/", leaves$identifier)
  paths <- c(mother_path, paths)
  
  keys <- lapply(paths, function(id) get_varkey(path = id, schema = "bonares"))
  names(keys) <- sapply(keys, function(df) as.character(df$file_name[1]))
  keys <- do.call(rbind, keys)
  
  # Simplify table names
  prefix <- find_common_prefix(keys$file_name)  # find common prefix
  tbl_name <- sub(paste0("^", prefix), "", keys$file_name)  # drop common prefix
  tbl_name <- sub("\\..*$", "", tbl_name)  # drop file extension
  tbl_name <- sub("^\\d+_V\\d+_\\d+_", "", tbl_name)  # (optional) table number
  file_name <- paste0(keys$file_name, ".csv")
  
  out <- cbind(file_name, tbl_name, keys[,2:ncol(keys)])
  out[!duplicated(out),]
}
