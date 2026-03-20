## -----------------------------------------------------------------------------------
## Script name: example_pipeline_experiment.R
## Purpose of script: showcasing etl pipeline prototype of FAIRagro 
## UC6 as an example of application of FAIR RDM
##
## Author: Benjamin Leroy
## Date Created: 2026-01-31
## Copyright (c) Benjamin Leroy, 2026
## Email: benjamin.leroy@tum.de
## -----------------------------------------------------------------------------------
## Notes:
## 
## -----------------------------------------------------------------------------------

###----- Install/load package --------------------------------------------------------

# Install devtools if you haven't already
install.packages("devtools")
# Install the package
devtools::install_github("leroy-bml/csmTools", force = TRUE)
# Load the package
library(csmTools)
library(dplyr)  #tmp
library(tidyr)  #tmp
library(DSSAT)
# template: %LocalAppData%\R\

###----- Crop management/manually input data (template) ------------------------------
## -----------------------------------------------------------------------------------
## This section demonstrates the import of ICASA-compliant field (meta)data via the
## manual csmTemplate
##
## -----------------------------------------------------------------------------------

template_path <-
  "C:/Users/bmlle/Documents/0_DATA/TUM/HEF/FAIRagro/2-UseCases/UC6_IntegratedModeling/Workflows/csmTools/inst/extdata/template_icasa_vba.xlsm"

# Extract template data
mngt_obs_icasa <- get_field_data(
  path = template_path,
  exp_id = "HWOC2501",
  headers = "long",
  keep_null_events = FALSE
)

# Retrieve cultivation season temporal coverage to identify weather requirements
mngt_tables <-
  mngt_obs_icasa[!names(mngt_obs_icasa) %in% c("GENERAL", "PERSONS", "INSTITUTIONS")]
cseason <- identify_production_season(
  mngt_tables,
  period = "cultivation_season",
  output = "bounds"
)
print(cseason)  # Note: the cultivation season overlaps 2024 and 2025


###----- IoT weather stations data ---------------------------------------------------
## -----------------------------------------------------------------------------------
## This section showcase the acquisition of IoT sensor data (weather ## time series)
## from a weather station operating on the study field in Triesdorf and stored on
## FROST server (OGC SensorThings API). The steps include (1) pulling the data;
## (2) transform to DSSAT input format.The 'extract' routine requires FROST server
## credentials.
##
## -----------------------------------------------------------------------------------

# Set keycloack and FROST credentials
uc6_creds <- list(
  url = "https://keycloak.hef.tum.de/realms/master/protocol/openid-connect/token",
  client_id = Sys.getenv("FROST_CLIENT_ID"),
  client_secret = Sys.getenv("FROST_CLIENT_SECRET"),
  username = Sys.getenv("FROST_USERNAME"),
  password = Sys.getenv("FROST_PASSWORD")
)
## Note: here you can also use the path to a YAML or JSON credentials file:
# uc6_creds <- "./data/frost_credentials.yaml"
# However these needs to be safely specified (currently just dummy credentials, will not work as is!)

# Data extraction and mapping
wth_sensor_raw <- get_sensor_data(
  url = Sys.getenv("FROST_USER_URL"),
  creds = uc6_creds,
  var = c("air_temperature", "solar_radiation", "volume_of_hydrological_precipitation"),
  lon = 10.64506,
  lat = 49.20901,
  radius = 10,
  from = "2025-01-01",
  to = "2025-08-31"
)

# --- Map raw weather data to ICASA ---
wth_sensor_icasa <- convert_dataset(
  dataset = wth_sensor_raw,
  input_model = "user",
  output_model = "icasa",
  unmatched_code = "na"
  # output_path = "./data/ochsenwasen_weather_sensor_icasa.json"
)


###----- Complementary weather data --------------------------------------------------
## -----------------------------------------------------------------------------------
## This section showcase the extraction of standardization of weather data from a
## third-party databases (NASA POWER) to complement the field-measured weather data
## (i.e., fill data gaps).
##
## -----------------------------------------------------------------------------------

# --- Download complementary weather data ---
wth_model_nasapower <- get_weather_data(
  lon = 10.64506,
  lat = 49.20901,
  pars = c("air_temperature", "precipitation", "solar_radiation"),
  res = "daily",
  from = "2024-01-01",
  to = "2025-08-09",
  src = "nasa_power"
  # output_path = "./data/ochsenwasen_weather_nasapower.json"
)

# --- Map complementary weather data to ICASA ---
wth_model_icasa <- convert_dataset(
  dataset = wth_model_nasapower,
  input_model = "nasa-power",
  output_model = "icasa"
  # output_path = "./data/ochsenwasen_weather_nasapower_icasa.json"
)

# --- Combine with field data ---

# Here we use the 'assemble_data_wrapper' function to complement field data with the
# modeled data. We want a single metadata record but keep all records, hence the
# different merging steps.

wth_icasa <- c(
  assemble_dataset(
    components = list(
      wth_sensor_icasa["WEATHER_METADATA"],
      wth_model_icasa["WEATHER_METADATA"]
    ),
    keep_all = FALSE,
    action = "merge_properties",
    join_type = "left"
  ),
  assemble_dataset(
    components = list(
      wth_sensor_icasa["WEATHER_DAILY"],
      wth_model_icasa["WEATHER_DAILY"]
    ),
    keep_all = FALSE,
    action = "merge_properties",
    join_type = "full"
  )
)


###----- Soil profile data - external database --------------------------------------
## ----------------------------------------------------------------------------------
## This section demonstrates the sourcing of soil profile data from a a published
## dataset (Global 10-km Soil Grids DSSAT profiles). The data is first downloaded
## with the Dataverse API, and the closest profile to the set of input coordinates
## extracted. As the dataset already provides ready-to-use DSSAT formats, no
## mapping step is necesary here. 
## Data reference: https://doi.org/10.7910/DVN/1PEEY0
##
## ----------------------------------------------------------------------------------

#####----- Extract profile data from SoilGrids --------------------------------------
soil_icasa <- get_soil_profile(
  lon = 10.64506,
  lat = 49.20901,
  src = "soil_grids",
  dir = NULL,
  delete_raw_files = FALSE
  # output_path = "./data/ochsenwasen_soil_icasa.json"
)


###----- Phenology data - GDD model and drone orthophotos ---------------------------
## ----------------------------------------------------------------------------------
## This section links to the UAV image processing workflow (raster2sensor + phenology
## estimation). The function takes the process output and pick one date for the focal
## growth stages by scaling the estimated growth stage date sequences to the selected
## growth stage scale.
## ----------------------------------------------------------------------------------

gs_raw <- lookup_gs_dates(
  data = "./data/wheat_phenology_results.csv",
  gs_scale = "zadoks",
  gs_codes = c(65, 87),  # Zadoks codes for anthesis and maturity stages
  date_select_rule = "median"
  # output_path = "./data/ochsenwasen_phenology.json"
)

gs_icasa <- convert_dataset(
  dataset = gs_raw,
  input_model = "user",
  output_model = "icasa"
  # output_path = "./data/ochsenwasen_phenology_icasa.json"
)


###----- Data integration and mapping to DSSAT --------------------------------------
## ----------------------------------------------------------------------------------
## Combine all ICASA datasets, either by merging or aggregating same-named data frames
## or appending new dataframes to a single dataset object. The object is then mapped
## to DSSAT
## ----------------------------------------------------------------------------------

# Integrate all data sources
dataset_icasa <- assemble_dataset(
  components = list(
    mngt_obs_icasa,    # Field/crop management data
    gs_icasa,          # Growth stages data 
    soil_icasa,        # SoilGrids data
    wth_icasa          # Merged weather data
  ),
  keep_all = TRUE,
  action = "merge_properties",
  join_type = "full"
  # output_path = "./data/ochsenwasen_icasa.json"
)

# Map from ICASA to DSSAT (write-ready)
dataset_dssat <- convert_dataset(
  dataset = dataset_icasa,
  input_model = "icasa",
  output_model = "dssat"
  # output_path = "./data/ochsenwasen_dssat.json"
)


###----- Perform simulations --------------------------------------------------------
## ----------------------------------------------------------------------------------
## Compiles DSSAT input files from all the different data sections (weather, soil,
## crop management, cultivar parameters, measured data) and set controls for the
## simulations (DSSAT documentation ###).
## Simulation are performed using the written inputs
## 
## NB: this part require a local installation of DSSATCSM
## (default installation dir: "C:/DSSAT48")
##
## ----------------------------------------------------------------------------------

###----- Adjust data ----------------------------------------------------------------

# --- Test new soil profiles ---
# Here it is important to set 'nested = FALSE' to load the soil profile as an unnested
# rows (one row per layer), to allow further editing
generic_loam_slp <- read_sol(
  file_name = "C:/DSSAT48/Soil/SOIL.SOL",
  id_soil = "IB00000007",
  nested = FALSE
)
# Editing of the water retention profiles
generic_loam_slp$SDUL - generic_loam_slp$SLLL  # Not very high...
generic_loam_slp$SDUL <- generic_loam_slp$SDUL * 1.2
generic_loam_slp$SSAT <- generic_loam_slp$SSAT * 1.2


# --- Normalize soil profile ---
soil_dssat_std <- normalize_soil_profile(
  # data = "./data/ochsenwasen_dssat.json",
  data = generic_loam_slp,
  depth_seq = c(5,10,20,30,40,50,60,70,90,110,130,150,170,190,210),
  diagnostics = TRUE,
  method = "linear"
  # output_path = "./data/ochsenwasen_soil_dssat_normalized.json"
)

# Update dataset with normalized soil profile ('replace')
dataset_dssat <- assemble_dataset(
  components = list(
    dataset_dssat,
    soil_dssat_std
  ),
  keep_all = FALSE,
  action = "replace_table"
  # output_path = "./data/ochsenwasen_dssat.json"
)

# --- Generate initial layers ---
init_layers <- calculate_initial_layers(
  soil_profile = soil_dssat_std,
  percent_available_water = 100,
  total_n_kgha = 50
  # output_path = "./data/ochsenwasen_init_layers.json"
)

# Update initial conditions with simulated layers ('merge')
dataset_dssat <- assemble_dataset(
  components = list(dataset_dssat, init_layers),
  keep_all = FALSE,
  action = "merge_properties",
  join_type = "full"
  # output_path = "./data/ochsenwasen_dssat.json"
)


###----- Compile crop modeling data -------------------------------------------------

# Assemble full input dataset
dataset_dssat_input <- build_simulation_files(
  dataset = dataset_dssat,
  sol_append = FALSE,
  write = TRUE,
  # If set to TRUE, files are written in the DSSAT locations (needed for simulation)
  write_in_dssat_dir = TRUE,
  path = "./data",
  control_config = "./inst/examples/sciwin/dssat_simulation_controls.yaml"
)


###----- Run DSSAT simulation ------------------------------------------------------

simulations <- run_simulations(
  filex_path = "C:/DSSAT48/Wheat/HWOC2501.WHX",  # the crop management file in the DSSAT location
  treatments = c(1, 3, 7),  # treatment index
  framework = "dssat",
  dssat_dir = NULL,
  sim_dir = "./data"
)


###----- Plot output ---------------------------------------------------------------
plot_output <- function(sim_output){
  
  # Observed data
  obs_summary_growth <- sim_output$SUMMARY %>%
    filter(TRNO %in% c(1,3,7)) %>%  # FIX
    mutate(MDAT = as.POSIXct(as.Date(MDAT, format = "%y%j")),
           HDAT = as.POSIXct(as.Date(HDAT, format = "%y%j")))
  
  # Simulated data
  sim_growth <- sim_output$plant_growth
  
  # Plot
  plot_growth <- sim_growth %>%
    mutate(TRNO = as.factor(TRNO)) %>%
    ggplot(aes(x = DATE, y = GWAD)) +
    # Line plot for simulated data
    geom_line(aes(group = TRNO, colour = TRNO, linewidth = "Simulated")) +
    # Points for observed data
    geom_point(data = obs_summary_growth,
               aes(x = HDAT, y = GWAM, colour = as.factor(TRNO), size = "Observed"), shape = 20) +
    # Phenology (estimated)
    # geom_vline(data = obs_summary_growth, aes(xintercept = EDAT), colour = "darkblue") +
    # geom_vline(data = obs_summary_growth, aes(xintercept = ADAT), colour = "darkgreen") +
    # geom_vline(data = obs_summary_growth, aes(xintercept = MDAT), colour = "purple") +
    # General appearance
    scale_colour_manual(name = "Fertilization (kg[N]/ha)",
                        breaks = c("1","3","7"),
                        labels = c("0","147","180"),
                        values = c("#999999", "#E18727", "#BC3C29")) +
    scale_size_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
    scale_linewidth_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
    labs(size = NULL, linewidth = NULL, y = "Yield (kg/ha)") +
    guides(
      size = guide_legend(
        override.aes = list(linetype = c("solid", "blank"), shape = c(NA, 16))
      )
    ) +
    theme_bw() + 
    theme(legend.text = element_text(size = 8), legend.title = element_text(size = 8),
          axis.title.x = ggplot2::element_blank(), axis.title.y = element_text(size = 10),
          axis.text = element_text(size = 9, colour = "black"))
  
  return(plot_growth)
}

plot_output(sims)


# TODO: comparison soil moisture measured vs simulated
# TODO: same thing for anthesis

tmp <- sims$SUMMARY %>%
  select(TRNO, GWAM) %>%
  left_join(sims$plant_growth %>%
              filter(DATE == max(DATE)))
