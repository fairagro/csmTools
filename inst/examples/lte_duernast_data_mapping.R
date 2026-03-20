## ------------------------------------------------------------------------------------------------------------------
## Script name: lte_duernast_data_mapping.R
## Purpose of script: data processing pipeline for crop modeling project based on the Duernast experiment and the 
## NWheat model in DSSAT
##
## Author: Benjamin Leroy
## Date Created: 2025-08-01
## Email: benjamin.leroy@tum.de
## ------------------------------------------------------------------------------------------------------------------
## Notes:
## 2025-08-04:
## - Data available on the BonaRes repository only covers a fraction of the time span of the experiment (2015-2022;
## the experiment has been running since 1978)
## - Coordinates reported in the metadata are not accurate and were manually replaced by actual coordinates until
## a workaround is implemented
## 2025-09-19:
## - Script updated with the complete map engine overhaul of the csmTools package. You must first update the csmTools
## package for the script to work properly!
## 
## ------------------------------------------------------------------------------------------------------------------

# ---- Set-up -------------------------------------------------------------------------------------------------------

#install.packages("devtools")  # Run once; helper to install functions from GitHub
library(devtools)
install_github("leroy-bml/csmTools", force = TRUE)  # Run only to install or update the package (install all dependencies when prompted)
library(csmTools)
#install.packages("dplyr")
library(dplyr)  # for extra tweaks
library(openxlsx2)  # to write key as xlsx
library(sf)  # coordinate transformations

# library(dplyr)
# library(stringi)
# library(purrr)
# library(lubridate)
# library(tibble)
# library(tidyr)

# ---- Read data ----------------------------------------------------------------------------------------------------

# --- Raw data tables ---
#duernast_raw <- read_exp_data(dir = "./data/0_raw", extension = "csv")
duernast_raw <- read_exp_data(dir = "./inst/extdata/test_data/lte_duernast/0_raw", extension = "csv")


# --- Metadata and raw data variable key ---
duernast_metadata_url <- "https://maps.bonares.de/finder/resources/dataform/xml/7e526e38-4bf1-492b-b903-d8dbcfd36b6d"
# duernast_metadata_url_local <- "./data/0_raw/7e526e38-4bf1-492b-b903-d8dbcfd36b6d.xml"
# --> to use the local copy of the xml; in case the BonaRes server is down
meta <- read_metadata(duernast_metadata_url, "bonares-lte_de")

# Provenance metadata
duernast_metadata <- meta$metadata
# Variable key from the BonaRes data model, in case you need to check raw data
duernast_key <- meta$variable_key

# Write variable key
# NB: xlsx format to prevent interference with csv file (raw data) in the same folder
wb <- wb_workbook() |> 
  wb_add_worksheet("Sheet1") |> 
  wb_add_data(sheet = "Sheet1", x = duernast_key)
#wb_save(wb, "./data/0_raw/lte_duernast_variableKey.xlsx")

# --- Combine data and metadata ---

duernast_raw <- c(
  duernast_raw,
  list(METADATA = duernast_metadata$METADATA,
       PERSONS = duernast_metadata$PERSONS,
       INSTITUTIONS = duernast_metadata$INSTITUTIONS)
)

# ---- Format data to ICASA -----------------------------------------------------------------------------------------

duernast_icasa <- convert_dataset(
  dataset = duernast_raw,
  input_model = "bonares-lte_de",
  output_model = "icasa",
  unmatched_code = "default_value",
)

# To set short instead of long headers
duernast_icasa_short <- duernast_icasa %>%
  format_icasa_headers(header_type = "short")


# ---- Get soil grids profile data ----------------------------------------------------------------------------------

# Convert coordinates to sf object with Gauss-Krüger CRS
coords <- data.frame(
  x = unique(duernast_icasa$FIELDS$field_longitude),
  y = unique(duernast_icasa$FIELDS$field_latitude)
)
# GK Zone 4: EPSG 31468
sf_point <- st_as_sf(coords, coords = c("x", "y"), crs = 31468)
sf_wgs84 <- st_transform(sf_point, crs = 4326)  # WGS84 = ESPG:4326

lat <- st_coordinates(sf_wgs84)[2]
lon <- st_coordinates(sf_wgs84)[1]

# Update FIELDS
duernast_icasa$FIELDS$field_latitude <- lat
duernast_icasa$FIELDS$field_longitude <- lon

# Download soil profile data
duernast_soilgrids_profile <- get_soilGrids_profile(lat, lon)

# Append soil profile to ICASA data
duernast_icasa <- c(duernast_icasa, duernast_soilgrids_profile)


# ---- Format data to DSSAT -----------------------------------------------------------------------------------------

duernast_dssat <- convert_dataset(
  dataset = duernast_icasa,
  input_model = "icasa",
  output_model = "dssat",
  unmatched_code = "default_value"
)


# ---- Manual edits for simulation ----------------------------------------------------------------------------------

df <- duernast_dssat$`2015`$SOIL_LAYERS
soil_normalized <- normalize_soil_profile(duernast_dssat$`2015`$SOIL_LAYERS)
tmp <- 
  build_dssat_file(soil_normalized$data, SOIL_template, DSSAT:::sol_v_fmt())
ma_path <-
  "C:/Users/bmlle/Documents/0_DATA/TUM/HEF/FAIRagro/2-UseCases/UC6_IntegratedModeling/Resources/Students/MScThesis_Muhammad Arslan/duernast_exp_modeling/data/2_dssat/TU.SOL"
write_sol(tmp, ma_path)

##SIMULATE LAYAERS



# ---- Write data files ---------------------------------------------------------------------------------------------

# --- ICASA format ---
outpath <- "./data/1_icasa"

# HACK: unlist nested comments to write files
duernast_icasa_files <- duernast_icasa
duernast_icasa_files[["PROFILE_METADATA"]]$soil_profile_methods <- 
  paste(unlist(duernast_icasa_files[["PROFILE_METADATA"]]$soil_profile_methods), collapse = "; ")

for (nm in names(duernast_icasa_files)) {
  write.csv(
    duernast_icasa_files[[nm]], 
    file = file.path(outpath, paste0(nm, ".csv")), 
    row.names = FALSE
  )
}

# --- DSSAT input files ---

# HACK: DSSAT data format not well handling multiple-crop in the same growing season
# --> Skipping 2018 (cover crop + Maize)
duernast_dssat <- duernast_dssat[names(duernast_dssat) != "TUDU1801"]

duernast_dssat_fmt <- lapply(duernast_dssat, function(dataset) {
  compile_dssat_dataset(
    dataset,
    sol_append = FALSE,
    write = TRUE, path = "./data/2_dssat",
    control_args = list(RSEED = 1234)
  )
})
