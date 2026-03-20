### Model analysis
library(dplyr)
library(ggplot2)

# Load data
sims <- DSSAT::read_output(file_name = file.path("./inst/examples/sciwin", "PlantGro.OUT"))  # simulated
obs <- DSSAT::read_filea(file_name = file.path("./inst/examples/sciwin", "HWOC2501.WHA"))  # observed

soil_nitrogen <- DSSAT::read_output(file_name = file.path("./inst/examples/sciwin", "SoilNi.OUT"))

# Select focal treatments and format dates for plotting
obs <- obs %>%
  filter(TRNO %in% c(1,3,7)) %>%
  mutate(MDAT = as.POSIXct(as.Date(MDAT, format = "%y%j")),
         HDAT = as.POSIXct(as.Date(HDAT, format = "%y%j")))

# Plot biomass growth, simulation vs. observed
plot_yield <- sims %>%
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
plot_yield


# --- Weather ---

# Format data
planting <- dataset_dssat$MANAGEMENT$PLANTING_DETAILS
wth <- reduce(dataset_dssat$WEATHER, bind_rows) %>%
  bind_cols(planting %>% select(PDATE)) %>%
  mutate(across(c(DATE, PDATE), ~ format(as.Date(.x, format = "%y%j"), "%Y-%m-%d"))) %>%
  mutate(DAP = as.Date(DATE) - as.Date(PDATE)) %>%
  filter(DAP >= 0) %>%
  select(DAP, DATE, SRAD, TMAX, TMIN, RAIN) %>%
  arrange(DAP)

# Create scaling factors for multiple axes 
primary_max <- max(wth$TMAX, na.rm = TRUE)
srad_max <- max(wth$SRAD, na.rm = TRUE)
rain_max <- max(wth$RAIN, na.rm = TRUE)
srad_coeff <- primary_max / srad_max
rain_coeff <- primary_max / rain_max


# Plot weather
weather_plot <- wth %>%
  mutate(DATE = as.Date(DATE)) %>%
  ggplot(aes(x = DATE)) +
  # Precipitation bars
  geom_col(aes(y = RAIN * rain_coeff, fill = "RAIN"), alpha = 1) +
  # Temperature lines
  geom_line(aes(y = TMAX, color = "TMAX"), size = 0.5) +
  geom_line(aes(y = TMIN, color = "TMIN"), size = 0.5) +
  # Solar radiation lines
  geom_line(aes(y = SRAD * srad_coeff, color = "SRAD (scaled)"), , size = 0.5) +
  # --- 4. Define the Axes ---
  scale_y_continuous(
    name = "Temperature (°C)",
    # Secondary axis (unscale values for axis labels)
    sec.axis = sec_axis(~ . / rain_coeff, name = "Rain (mm)")
  ) +
  scale_color_manual(name = "", values = c("TMAX" = "red", "TMIN" = "blue", "SRAD (scaled)" = "orange")) +
  scale_fill_manual(name = "", values = c("RAIN" = "lightblue")) +
  # scale_x_continuous(breaks = scales::pretty_breaks(4)) +
  labs(size = NULL, linewidth = NULL, y = "Daily weather data") +
  guides(
    size = guide_legend(
      override.aes = list(linetype = c("solid", "blank"), shape = c(NA, 16))
    )
  ) +
  theme_bw() + 
  theme(legend.text = element_text(size = 8), legend.title = element_text(size = 8),
        axis.title.x = ggplot2::element_blank(), axis.title.y = element_text(size = 10),
        axis.text = element_text(size = 9, colour = "black"))
weather_plot

ggsave(
  filename = "./inst/examples/sciwin/weather_plot.png",
  weather_plot,
  width = 20, height = 12, units = "cm",
  dpi = 600,
  bg = "white"
)

# --- Water stress ---

water_stress_part_plot <- sims %>%
  mutate(TRNO = as.factor(TRNO)) %>%
  ggplot(aes(x = DATE, y = WSGD)) +
  # Line plot for simulated data
  geom_line(aes(group = TRNO, colour = TRNO), linewidth = 1) + 
  scale_colour_manual(name = "Fertilization (kg[N]/ha)",
                      breaks = c("1","3","7"),
                      labels = c("0","147","180"),
                      values = c("#999999", "#E18727", "#BC3C29")) +
  scale_size_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
  labs(size = NULL, linewidth = NULL, y = "Water stress factor (WSGD)") +
  guides(
    size = guide_legend(
      override.aes = list(linetype = c("solid", "blank"), shape = c(NA, 16))
    )
  ) +
  theme_bw() + 
  theme(legend.text = element_text(size = 8), legend.title = element_text(size = 8),
        axis.title.x = ggplot2::element_blank(), axis.title.y = element_text(size = 10),
        axis.text = element_text(size = 9, colour = "black"))
water_stress_part_plot

water_stress_photo_plot <- sims %>%
  mutate(TRNO = as.factor(TRNO)) %>%
  ggplot(aes(x = DATE, y = WSPD)) +
  # Line plot for simulated data
  geom_line(aes(group = TRNO, colour = TRNO), linewidth = 1) + 
  scale_colour_manual(name = "Fertilization (kg[N]/ha)",
                      breaks = c("1","3","7"),
                      labels = c("0","147","180"),
                      values = c("#999999", "#E18727", "#BC3C29")) +
  scale_size_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
  labs(size = NULL, linewidth = NULL, y = "Water stress factor (WSPD)") +
  guides(
    size = guide_legend(
      override.aes = list(linetype = c("solid", "blank"), shape = c(NA, 16))
    )
  ) +
  theme_bw() + 
  theme(legend.text = element_text(size = 8), legend.title = element_text(size = 8),
        axis.title.x = ggplot2::element_blank(), axis.title.y = element_text(size = 10),
        axis.text = element_text(size = 9, colour = "black"))
water_stress_photo_plot

# --- Nitrogen stress ---
n_stress_plot <- sims %>%
  mutate(TRNO = as.factor(TRNO)) %>%
  ggplot(aes(x = DATE, y = NSTD)) +
  # Line plot for simulated data
  geom_line(aes(group = TRNO, colour = TRNO), linewidth = 1) + 
  scale_colour_manual(name = "Fertilization (kg[N]/ha)",
                      breaks = c("1","3","7"),
                      labels = c("0","147","180"),
                      values = c("#999999", "#E18727", "#BC3C29")) +
  scale_size_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
  labs(size = NULL, linewidth = NULL, y = "Water stress factor (WSGD)") +
  guides(
    size = guide_legend(
      override.aes = list(linetype = c("solid", "blank"), shape = c(NA, 16))
    )
  ) +
  theme_bw() + 
  theme(legend.text = element_text(size = 8), legend.title = element_text(size = 8),
        axis.title.x = ggplot2::element_blank(), axis.title.y = element_text(size = 10),
        axis.text = element_text(size = 9, colour = "black"))
n_stress_plot

# --- Nitrogen leaching ---
n_leaching_plot <- soil_nitrogen %>%
  mutate(TRNO = as.factor(TRNO)) %>%
  ggplot(aes(x = DATE, y = NLCC)) +
  # Line plot for simulated data
  geom_line(aes(group = TRNO, colour = TRNO), linewidth = 1) + 
  scale_colour_manual(name = "Fertilization (kg[N]/ha)",
                      breaks = c("1","3","7"),
                      labels = c("0","147","180"),
                      values = c("#999999", "#E18727", "#BC3C29")) +
  scale_size_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
  labs(size = NULL, linewidth = NULL, y = "Cumulative N leachate (NLCCC; kg/ha)") +
  guides(
    size = guide_legend(
      override.aes = list(linetype = c("solid", "blank"), shape = c(NA, 16))
    )
  ) +
  theme_bw() + 
  theme(legend.text = element_text(size = 8), legend.title = element_text(size = 8),
        axis.title.x = ggplot2::element_blank(), axis.title.y = element_text(size = 10),
        axis.text = element_text(size = 9, colour = "black"))
n_leaching_plot

# --- Root depth ---
plot_root_depth <- sims %>%
  mutate(TRNO = as.factor(TRNO)) %>%
  ggplot(aes(x = DATE, y = RDPD)) +
  # Line plot for simulated data
  geom_line(aes(group = TRNO, colour = TRNO), linewidth = 1) + 
  scale_colour_manual(name = "Fertilization (kg[N]/ha)",
                      breaks = c("1","3","7"),
                      labels = c("0","147","180"),
                      values = c("#999999", "#E18727", "#BC3C29")) +
  scale_size_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
  labs(size = NULL, linewidth = NULL, y = "Root depth (m)") +
  guides(
    size = guide_legend(
      override.aes = list(linetype = c("solid", "blank"), shape = c(NA, 16))
    )
  ) +
  theme_bw() + 
  theme(legend.text = element_text(size = 8), legend.title = element_text(size = 8),
        axis.title.x = ggplot2::element_blank(), axis.title.y = element_text(size = 10),
        axis.text = element_text(size = 9, colour = "black"))
plot_root_depth

# --- Field capacity ---
plot_soil_water <- dataset_dssat$SOIL %>%
  select(SLB, SLLL, SDUL, SSAT) %>%
  gather("water_retention", "value", 2:4) %>%
  ggplot(aes(x = value, y = -SLB, colour = water_retention)) +
  geom_path(linewidth = 1) +
  labs(size = NULL, linewidth = NULL, x = "Water retention (cm3/cm3)", y = "Depth (mm)") +
  guides(
    size = guide_legend(
      override.aes = list(linetype = c("solid", "blank"), shape = c(NA, 16))
    )
  ) +
  theme_bw() + 
  theme(legend.text = element_text(size = 8), legend.title = element_text(size = 8),
        axis.title = element_text(size = 10),
        axis.text = element_text(size = 9, colour = "black"))
plot_soil_water
