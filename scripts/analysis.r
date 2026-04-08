library(tidytransit)
library(tidyverse)
library(sf)
library(lubridate)
library(hms)

# Load one GTFS package
gt <- read_gtfs("new_data/CTA-08-14.zip")

# --------------------------------------------------
# 1. Define the three analysis periods
# --------------------------------------------------
periods <- tibble(
  period_id    = c("P1", "P2", "P3"),
  period_label = c(
    "2025-08-04_to_2025-08-25",
    "2025-08-25_to_2025-09-08",
    "2025-09-08_to_2025-09-29"
  ),
  period_start = as.Date(c("2025-08-04", "2025-08-25", "2025-09-08")),
  period_end   = as.Date(c("2025-08-25", "2025-09-08", "2025-09-29")),
  rep_date     = as.Date(c("2025-08-20", "2025-08-27", "2025-09-17")),
  note         = c(
    "Feed starts on 2025-08-12, so P1 is represented by the available portion only",
    "Typical Wednesday",
    "Typical Wednesday"
  )
)

# Check whether representative dates are actually in the feed
available_dates <- gt$.$dates_services %>%
  distinct(date)

date_check <- periods %>%
  mutate(in_feed = rep_date %in% available_dates$date)

print(date_check)

# --------------------------------------------------
# 2. Helper functions
# --------------------------------------------------
get_active_ids <- function(gt, target_date) {
  gt$.$dates_services %>%
    filter(date == target_date) %>%
    distinct(service_id) %>%
    pull(service_id)
}

get_route_frequency_by_date <- function(gt, target_date, start_time, end_time, period_id) {
  active_ids <- get_active_ids(gt, target_date)

  gt %>%
    get_route_frequency(
      start_time  = start_time,
      end_time    = end_time,
      service_ids = active_ids
    ) %>%
    mutate(
      period_id = period_id,
      analysis_date = target_date
    )
}

get_trip_counts_by_date <- function(gt, target_date, start_time, end_time, period_id) {
  active_ids <- get_active_ids(gt, target_date)

  gt$stop_times %>%
    mutate(dep = hms::as_hms(departure_time)) %>%
    filter(dep >= hms::as_hms(start_time), dep < hms::as_hms(end_time)) %>%
    inner_join(gt$trips, by = "trip_id") %>%
    filter(service_id %in% active_ids) %>%
    inner_join(gt$routes, by = "route_id") %>%
    distinct(route_id, direction_id, trip_id) %>%
    count(route_id, direction_id, name = "n_trips") %>%
    mutate(
      period_id = period_id,
      analysis_date = target_date
    )
}

make_headway_compare <- function(freq_df) {
  freq_df %>%
    select(period_id, route_id, median_headways) %>%
    distinct() %>%
    pivot_wider(names_from = period_id, values_from = median_headways) %>%
    mutate(
      status_P2_vs_P1 = case_when(
        !is.na(P1) & !is.na(P2) ~ "both",
        is.na(P1) & !is.na(P2)  ~ "only_P2",
        !is.na(P1) & is.na(P2)  ~ "only_P1",
        TRUE ~ "neither"
      ),
      status_P3_vs_P2 = case_when(
        !is.na(P2) & !is.na(P3) ~ "both",
        is.na(P2) & !is.na(P3)  ~ "only_P3",
        !is.na(P2) & is.na(P3)  ~ "only_P2",
        TRUE ~ "neither"
      ),
      status_P3_vs_P1 = case_when(
        !is.na(P1) & !is.na(P3) ~ "both",
        is.na(P1) & !is.na(P3)  ~ "only_P3",
        !is.na(P1) & is.na(P3)  ~ "only_P1",
        TRUE ~ "neither"
      ),
      diff_P2_vs_P1_min = if_else(status_P2_vs_P1 == "both", (P2 - P1) / 60, NA_real_),
      diff_P3_vs_P2_min = if_else(status_P3_vs_P2 == "both", (P3 - P2) / 60, NA_real_),
      diff_P3_vs_P1_min = if_else(status_P3_vs_P1 == "both", (P3 - P1) / 60, NA_real_)
    ) %>%
    arrange(desc(abs(coalesce(diff_P3_vs_P1_min, 0))))
}

make_trip_compare <- function(trip_df) {
  trip_df %>%
    select(period_id, route_id, direction_id, n_trips) %>%
    distinct() %>%
    pivot_wider(
      names_from = period_id,
      values_from = n_trips,
      values_fill = 0
    ) %>%
    mutate(
      diff_P2_vs_P1 = P2 - P1,
      diff_P3_vs_P2 = P3 - P2,
      diff_P3_vs_P1 = P3 - P1
    ) %>%
    arrange(desc(abs(diff_P3_vs_P1)))
}

# --------------------------------------------------
# 3. Compute route frequency for peak and midday
# --------------------------------------------------
peak_freq_all <- map2_dfr(
  periods$rep_date,
  periods$period_id,
  ~ get_route_frequency_by_date(gt, .x, "07:30:00", "09:30:00", .y)
)

midday_freq_all <- map2_dfr(
  periods$rep_date,
  periods$period_id,
  ~ get_route_frequency_by_date(gt, .x, "11:00:00", "14:00:00", .y)
)

# --------------------------------------------------
# 4. Compute trip counts for peak and midday
# --------------------------------------------------
peak_trip_all <- map2_dfr(
  periods$rep_date,
  periods$period_id,
  ~ get_trip_counts_by_date(gt, .x, "07:30:00", "09:30:00", .y)
)

midday_trip_all <- map2_dfr(
  periods$rep_date,
  periods$period_id,
  ~ get_trip_counts_by_date(gt, .x, "11:00:00", "14:00:00", .y)
)

# --------------------------------------------------
# 5. Build comparison tables
# --------------------------------------------------
peak_headway_compare   <- make_headway_compare(peak_freq_all)
midday_headway_compare <- make_headway_compare(midday_freq_all)

peak_trip_compare   <- make_trip_compare(peak_trip_all)
midday_trip_compare <- make_trip_compare(midday_trip_all)

# --------------------------------------------------
# 6. Period-level summary table
# --------------------------------------------------
peak_freq_summary <- peak_freq_all %>%
  group_by(period_id, analysis_date) %>%
  summarise(
    n_routes_peak = n_distinct(route_id),
    median_headway_peak_min = median(median_headways, na.rm = TRUE) / 60,
    .groups = "drop"
  )

midday_freq_summary <- midday_freq_all %>%
  group_by(period_id, analysis_date) %>%
  summarise(
    n_routes_midday = n_distinct(route_id),
    median_headway_midday_min = median(median_headways, na.rm = TRUE) / 60,
    .groups = "drop"
  )

peak_trip_summary <- peak_trip_all %>%
  group_by(period_id, analysis_date) %>%
  summarise(
    total_peak_trips = sum(n_trips, na.rm = TRUE),
    n_route_direction_peak = n(),
    .groups = "drop"
  )

midday_trip_summary <- midday_trip_all %>%
  group_by(period_id, analysis_date) %>%
  summarise(
    total_midday_trips = sum(n_trips, na.rm = TRUE),
    n_route_direction_midday = n(),
    .groups = "drop"
  )

period_summary <- periods %>%
  left_join(
    peak_freq_summary,
    by = c("period_id", "rep_date" = "analysis_date")
  ) %>%
  left_join(
    midday_freq_summary,
    by = c("period_id", "rep_date" = "analysis_date")
  ) %>%
  left_join(
    peak_trip_summary,
    by = c("period_id", "rep_date" = "analysis_date")
  ) %>%
  left_join(
    midday_trip_summary,
    by = c("period_id", "rep_date" = "analysis_date")
  )

print(period_summary)

# --------------------------------------------------
# 7. Save outputs
# --------------------------------------------------
dir.create("outputs", showWarnings = FALSE)

write_csv(periods, "outputs/selected_representative_dates.csv")
write_csv(period_summary, "outputs/period_summary.csv")

write_csv(peak_headway_compare, "outputs/peak_headway_compare.csv")
write_csv(midday_headway_compare, "outputs/midday_headway_compare.csv")

write_csv(peak_trip_compare, "outputs/peak_trip_compare.csv")
write_csv(midday_trip_compare, "outputs/midday_trip_compare.csv")

# --------------------------------------------------
# 8. Optional plots
# --------------------------------------------------
ggplot(peak_headway_compare %>% filter(!is.na(diff_P3_vs_P1_min)),
       aes(x = diff_P3_vs_P1_min)) +
  geom_histogram(binwidth = 5, fill = "blue", color = "black", alpha = 0.7) +
  labs(
    title = "Peak Headway Difference: P3 - P1",
    x = "Headway Difference (minutes)",
    y = "Frequency"
  ) +
  theme_minimal()

ggplot(midday_headway_compare %>% filter(!is.na(diff_P3_vs_P1_min)),
       aes(x = diff_P3_vs_P1_min)) +
  geom_histogram(binwidth = 5, fill = "blue", color = "black", alpha = 0.7) +
  labs(
    title = "Midday Headway Difference: P3 - P1",
    x = "Headway Difference (minutes)",
    y = "Frequency"
  ) +
  theme_minimal()

ggplot(peak_trip_compare, aes(x = diff_P3_vs_P1)) +
  geom_histogram(binwidth = 1, fill = "blue", color = "black", alpha = 0.7) +
  labs(
    title = "Peak Trip Count Difference: P3 - P1",
    x = "Trip Count Difference",
    y = "Frequency"
  ) +
  theme_minimal()

ggplot(midday_trip_compare, aes(x = diff_P3_vs_P1)) +
  geom_histogram(binwidth = 1, fill = "blue", color = "black", alpha = 0.7) +
  labs(
    title = "Midday Trip Count Difference: P3 - P1",
    x = "Trip Count Difference",
    y = "Frequency"
  ) +
  theme_minimal()
