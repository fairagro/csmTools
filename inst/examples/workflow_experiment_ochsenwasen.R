## -----------------------------------------------------------------------------------
## Script name: example_pipeline_experiment_sciwin.R
## Purpose of script: showcasing etl pipeline prototype of FAIRagro UC6 as an example
## of application of FAIR RDM
##
## Author: Benjamin Leroy
## Date Created: 2024-01-31
## Copyright (c) Benjamin Leroy, 2024
## Email: benjamin.leroy@tum.de
## -----------------------------------------------------------------------------------
## Notes: simulation results have yet to be refined
## 
## -----------------------------------------------------------------------------------

###----- Crop management/manually input data (template) ------------------------------
## -----------------------------------------------------------------------------------
## This section demonstrates the import of ICASA-compliant field (meta)data via the
## manual csmTemplate
##
## -----------------------------------------------------------------------------------

# Extract template data
mngt_obs_icasa <- get_field_data(
  path = "./inst/examples/sciwin/template_icasa_vba.xlsm",
  exp_id = "HWOC2501",
  headers = "long",
  keep_null_events = FALSE
)


###----- IoT weather stations data ---------------------------------------------------
## -----------------------------------------------------------------------------------
## This section showcase the acquisition of IoT sensor data (weather time series)
## from a weather station operating on the study field in Triesdorf and stored on
## FROST server (OGC SensorThings API). The steps include (1) fetching the data;
## (2) transform to DSSAT input format.The 'extract' routine requires API credentials.
##
## -----------------------------------------------------------------------------------

# --- Extract all sensor weather data from the field location for 2025 ---

# Set keycloack and FROST credentials
uc6_creds <- list(
  url = "https://keycloak.hef.tum.de/realms/master/protocol/openid-connect/token",
  client_id = Sys.getenv("FROST_CLIENT_ID"),
  client_secret = Sys.getenv("FROST_CLIENT_SECRET"),
  username = Sys.getenv("FROST_USERNAME"),
  password = Sys.getenv("FROST_PASSWORD")
)
## Note: here you can also use the path to a YAML or JSON credentials file:
# uc6_creds <- "./inst/examples/sciwin/frost_credentials.yaml"
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
  to = "2025-08-09",
)

# --- Map raw weather data to ICASA ---
wth_sensor_icasa <- convert_dataset(
  dataset = "./inst/examples/sciwin/ochsenwasen_weather_sensor.json",
  input_model = "user",
  output_model = "icasa",
  unmatched_code = "na",
  output_path = "./inst/examples/sciwin/ochsenwasen_weather_sensor_icasa.json"
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
)

# --- Map complementary weather data to ICASA ---
wth_model_icasa <- convert_dataset(
  dataset = wth_model_nasapower,
  input_model = "nasa-power",
  output_model = "icasa"
)

# --- Combine with field data ---
wth_icasa <- assemble_dataset(
  components = list(wth_sensor_icasa, wth_model_icasa),  # replace
  keep_all = FALSE,
  action = "merge_properties",
  join_type = "left"
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
  delete_raw_files = TRUE
)


###----- Phenology data - GDD model and drone orthophotos ---------------------------
## ----------------------------------------------------------------------------------
## This section links to the UAV image processing workflow (raster2sensor + phenology
## estimation). The function takes the process output and pick one date for the focal
## growth stages by scaling the estimated growth stage date sequences to the selected
## growth stage scale.
## ----------------------------------------------------------------------------------

gs_raw <- lookup_gs_dates(
  data = "./inst/examples/sciwin/wheat_phenology_results.csv",
  gs_scale = "zadoks",
  gs_codes = c(65, 87),  # Zadoks codes for anthesis and maturity stages
  date_select_rule = "median"
)

gs_icasa <- convert_dataset(
  dataset = gs_raw,
  input_model = "user",
  output_model = "icasa"
)


###----- Data integration and mapping to DSSAT --------------------------------------
## ----------------------------------------------------------------------------------
## Combine all ICASA datasets, either by merging or aggregating same-named data frames
## or appending new dataframes to a single dataset object. The object is then mapped
## to DSSAT
## ----------------------------------------------------------------------------------

# Integrate all data sources
dataset_icasa <- assemble_dataset(
  components = list(mngt_obs_icasa, gs_icasa, soil_icasa, wth_icasa),
  keep_all = TRUE,
  action = "merge_properties",
  join_type = "full"
)

# Map from ICASA to DSSAT (write-ready)
dataset_dssat <- convert_dataset(
  dataset = dataset_icasa,
  input_model = "icasa",
  output_model = "dssat"
)
#TODO: check why dssat mappping is 'unboxed' in json output


###----- Adjust data ----------------------------------------------------------------
## ----------------------------------------------------------------------------------
## At this stage, researchers may want to tweak their input files to refine their
## simulation results. This may involve normalizing soil profiles sequence depth,
## generating initial condition layers based on whole soil estimates of water/N
## content, or editing single attributes
##
## ----------------------------------------------------------------------------------

# TODO: add default + calibration

# --- Test new soil profiles ---
generic_loam_slp <- read_sol(file_name = "C:/DSSAT48/Soil/SOIL.SOL", id_soil = "IB00000007")
generic_loam_slp <- unnest(
  generic_loam_slp,
  cols = c(SLB, SLMH, SLLL, SDUL, SSAT, SRGF, SSKS, SBDM, SLOC, SLCL, SLSI, SLCF, SLNI, SLHW, SLHB, SCEC, SADC)
)
generic_loam_slp$PEDON <- "DE02114767"
generic_loam_slp$SDUL - generic_loam_slp$SLLL  # Not very high...
generic_loam_slp$SDUL <- generic_loam_slp$SDUL * 1.2
generic_loam_slp$SSAT <- generic_loam_slp$SSAT * 1.2

# --- Normalize soil profile ---

# Apply standard depth sequence for NWheat
soil_dssat_std <- normalize_soil_profile(
  data = generic_loam_slp,
  depth_seq = c(5,10,20,30,40,50,60,70,90,110,130,150,170,190,210),
  method = "linear"
)
# Update dataset with normalized soil profile ('replace')
dataset_dssat <- assemble_dataset(
  components = list(dataset_dssat, soil_dssat_std),
  keep_all = FALSE,
  action = "replace_table"
)


# --- Generate initial layers ---

# Generate layer water and N based on single values provided in the field book
init_layers <- calculate_initial_layers(
  soil_profile = soil_dssat_std,
  percent_available_water = 100,
  total_n_kgha = 50  # Note: value provided for 0-60 cm
)
# Update initial conditions with simulated layers ('merge')
dataset_dssat <- assemble_dataset(
  components = list(dataset_dssat, init_layers),
  keep_all = FALSE,
  action = "merge_properties",
  join_type = "full"
)


# --- Add cultivar using default values ---
cultivar <- edit_cultivar(
  cname = dataset_dssat$EXPERIMENT$CULTIVARS$CNAME,
  model = "WHAPS",
  ccode = dataset_dssat$EXPERIMENT$CULTIVARS$INGENO,
  write =  TRUE
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

###----- Compile crop modeling data -------------------------------------------------

# Assemble full input dataset
dataset_dssat_input <- build_simulation_files(
  dataset = dataset_dssat,
  sol_append = FALSE,
  write = TRUE,
  # If set to TRUE, files are written in the DSSAT locations (needed for simulation)
  write_in_dssat_dir = TRUE,
  path = "./inst/examples",
  control_config = list(
    # DSSAT Simulation Control Parameters
    RSEED = 7461,
    SMODEL = "WHAPS",
    # Simulation options
    WATER = "Y",
    NITRO = "Y",
    TILL = "Y",
    # Simulation methods
    INFIL = "R",
    PHOTO = "C",
    MESEV = "S",
    MESOL = 3,
    # Management options
    FERTI = "R",
    HARVS = "R",
    # Output options
    GROUT = "Y",
    VBOSE = "Y",
    NIOUT = "Y"
  )
)


###----- Run DSSAT simulation ------------------------------------------------------

simulations <- run_simulations(
  filex_path = "C:/DSSAT48/Wheat/HWOC2501.WHX",  # the crop management file in the DSSAT location
  treatments = c(1, 3, 7),  # treatment index
  framework = "dssat",
  dssat_dir = NULL,
  sim_dir = "./inst/examples"
)


###----- Plot output ---------------------------------------------------------------
#
# Note: this is a custom 'user' script, not a packaged function, so dependencies must
# be documented for CWL formatting
#

# Load dependencies
library(DSSAT)
library(dplyr)
library(ggplot2)

# Load data
sims <- DSSAT::read_output(file_name = file.path("./inst/examples/sciwin", "PlantGro.OUT"))  # simulated
obs <- DSSAT::read_filea(file_name = file.path("./inst/examples/sciwin", "HWOC2501.WHA"))  # observed

# Select focal treatments and format dates for plotting
obs <- obs %>%
  filter(TRNO %in% c(1,3,7)) %>%
  mutate(MDAT = as.POSIXct(as.Date(MDAT, format = "%y%j")),
         HDAT = as.POSIXct(as.Date(HDAT, format = "%y%j")))

# Plot biomass growth, simulation vs. observed
plot_growth <- sims %>%
  mutate(TRNO = as.factor(TRNO)) %>%
  ggplot(aes(x = DATE, y = GWAD)) +
  # Line plot for simulated data
  geom_line(aes(group = TRNO, colour = TRNO, linewidth = "Simulated")) +
  # Points for observed data
  geom_point(data = obs,
             aes(x = HDAT, y = GWAM, colour = as.factor(TRNO), size = "Observed"), shape = 20) +
  # Phenology (estimated)
  # geom_vline(data = obs, aes(xintercept = EDAT), colour = "darkblue") +
  # geom_vline(data = obs, aes(xintercept = ADAT), colour = "darkgreen") +
  # geom_vline(data = obs, aes(xintercept = MDAT), colour = "purple") +
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
plot_growth


ggsave(
  filename = "./inst/examples/sciwin/simulation_results.png",
  plot_growth,
  width = 15, height = 12, units = "cm",
  dpi = 600,
  bg = "white"
)

