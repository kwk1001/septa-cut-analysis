library(tidytransit)
library(tidyverse)
library(sf)
library(lubridate)
library(progress)

# --------------------------------------------------
# 1. Load two GTFS feeds
# --------------------------------------------------
message("Step 1/14: Loading GTFS feeds...")

gt_0711 <- read_gtfs("new_data/CTA-07-11.zip")
gt_0814 <- read_gtfs("new_data/CTA-08-14.zip")

feed_registry <- tibble(
  feed_name  = c("CTA_0711", "CTA_0814"),
  valid_start = as.Date(c("2025-07-08", "2025-08-12")),
  valid_end   = as.Date(c("2025-09-30", "2025-10-31")),
  gt          = list(gt_0711, gt_0814)
)

# --------------------------------------------------
# 2. Define study periods
# --------------------------------------------------
message("Step 2/14: Defining study periods...")

periods <- tibble(
  period_id    = c("P1", "P2", "P3"),
  period_label = c(
    "2025-08-04_to_2025-08-25",
    "2025-08-25_to_2025-09-08",
    "2025-09-08_to_2025-09-29"
  ),
  period_start = as.Date(c("2025-08-04", "2025-08-25", "2025-09-08")),
  period_end   = as.Date(c("2025-08-25", "2025-09-08", "2025-09-29"))
)

# --------------------------------------------------
# 3. User settings
# --------------------------------------------------
message("Step 3/14: Applying user settings...")

weekday_only <- TRUE
excluded_dates <- as.Date(c("2025-09-01", "2025-09-02"))

# --------------------------------------------------
# 4. Route lookup
# --------------------------------------------------
message("Step 4/14: Building route lookup...")

route_lookup <- bind_rows(
  gt_0711$routes %>%
    select(route_id, route_short_name, route_long_name, route_type),
  gt_0814$routes %>%
    select(route_id, route_short_name, route_long_name, route_type)
) %>%
  distinct(route_id, .keep_all = TRUE)

# --------------------------------------------------
# 5. Helper functions
# --------------------------------------------------
message("Step 5/14: Defining helper functions...")

get_gt_by_name <- function(feed_name, registry) {
  registry$gt[[match(feed_name, registry$feed_name)]]
}

get_active_ids <- function(gt, target_date) {
  gt$.$dates_services %>%
    filter(date == target_date) %>%
    distinct(service_id) %>%
    pull(service_id)
}

choose_feed_name <- function(target_date, registry) {
  candidates <- registry %>%
    filter(valid_start <= target_date, valid_end >= target_date) %>%
    arrange(desc(valid_start))

  if (nrow(candidates) == 0) {
    return(NA_character_)
  }

  candidates$feed_name[[1]]
}

time_to_seconds <- function(x) {
  parts <- stringr::str_split_fixed(x, ":", 3)
  as.numeric(parts[, 1]) * 3600 +
    as.numeric(parts[, 2]) * 60 +
    as.numeric(parts[, 3])
}

build_analysis_dates <- function(periods, weekday_only, excluded_dates, registry) {
  out <- periods %>%
    rowwise() %>%
    mutate(analysis_date = list(seq(period_start, period_end, by = "day"))) %>%
    unnest(analysis_date) %>%
    ungroup()

  if (weekday_only) {
    out <- out %>%
      filter(wday(analysis_date, week_start = 1) <= 5)
  }

  if (length(excluded_dates) > 0) {
    out <- out %>%
      filter(!analysis_date %in% excluded_dates)
  }

  out %>%
    mutate(
      feed_name = map_chr(analysis_date, choose_feed_name, registry = registry)
    ) %>%
    filter(!is.na(feed_name))
}

get_route_frequency_one_day <- function(feed_name, target_date, period_id, period_label,
                                        start_time, end_time, time_window, registry) {
  gt <- get_gt_by_name(feed_name, registry)
  active_ids <- get_active_ids(gt, target_date)

  if (length(active_ids) == 0) {
    return(tibble())
  }

  gt %>%
    get_route_frequency(
      start_time  = start_time,
      end_time    = end_time,
      service_ids = active_ids
    ) %>%
    mutate(
      period_id     = period_id,
      period_label  = period_label,
      analysis_date = target_date,
      feed_name     = feed_name,
      time_window   = time_window
    )
}

get_trip_counts_one_day <- function(feed_name, target_date, period_id, period_label,
                                    start_time, end_time, time_window, registry) {
  gt <- get_gt_by_name(feed_name, registry)
  active_ids <- get_active_ids(gt, target_date)

  if (length(active_ids) == 0) {
    return(tibble())
  }

  start_sec <- time_to_seconds(start_time)
  end_sec   <- time_to_seconds(end_time)

  gt$stop_times %>%
    mutate(dep_sec = time_to_seconds(departure_time)) %>%
    filter(dep_sec >= start_sec, dep_sec < end_sec) %>%
    inner_join(gt$trips, by = "trip_id") %>%
    filter(service_id %in% active_ids) %>%
    inner_join(gt$routes %>% select(route_id), by = "route_id") %>%
    distinct(route_id, direction_id, trip_id) %>%
    count(route_id, direction_id, name = "n_trips") %>%
    mutate(
      period_id     = period_id,
      period_label  = period_label,
      analysis_date = target_date,
      feed_name     = feed_name,
      time_window   = time_window
    )
}

# --------------------------------------------------
# 6. Build date table
# --------------------------------------------------
message("Step 6/14: Building analysis date table...")

analysis_dates <- build_analysis_dates(
  periods = periods,
  weekday_only = weekday_only,
  excluded_dates = excluded_dates,
  registry = feed_registry
) %>%
  mutate(
    n_active_ids = map2_int(
      feed_name,
      analysis_date,
      ~ length(get_active_ids(get_gt_by_name(.x, feed_registry), .y))
    )
  ) %>%
  filter(n_active_ids > 0)

message(paste("Analysis dates selected:", nrow(analysis_dates)))
print(analysis_dates)

# --------------------------------------------------
# 7. Daily route frequency with progress bars
# --------------------------------------------------
message("Step 7/14: Computing peak route frequency...")

pb_peak_freq <- progress_bar$new(
  format = "  Peak frequency [:bar] :current/:total (:percent) eta: :eta",
  total = nrow(analysis_dates),
  clear = FALSE,
  width = 80
)

peak_freq_list <- vector("list", nrow(analysis_dates))

for (i in seq_len(nrow(analysis_dates))) {
  row_i <- analysis_dates[i, ]

  peak_freq_list[[i]] <- get_route_frequency_one_day(
    feed_name    = row_i$feed_name,
    target_date  = row_i$analysis_date,
    period_id    = row_i$period_id,
    period_label = row_i$period_label,
    start_time   = "07:30:00",
    end_time     = "09:30:00",
    time_window  = "peak",
    registry     = feed_registry
  )

  pb_peak_freq$tick()
}

peak_freq_daily <- bind_rows(peak_freq_list)

message("Step 8/14: Computing midday route frequency...")

pb_midday_freq <- progress_bar$new(
  format = "  Midday frequency [:bar] :current/:total (:percent) eta: :eta",
  total = nrow(analysis_dates),
  clear = FALSE,
  width = 80
)

midday_freq_list <- vector("list", nrow(analysis_dates))

for (i in seq_len(nrow(analysis_dates))) {
  row_i <- analysis_dates[i, ]

  midday_freq_list[[i]] <- get_route_frequency_one_day(
    feed_name    = row_i$feed_name,
    target_date  = row_i$analysis_date,
    period_id    = row_i$period_id,
    period_label = row_i$period_label,
    start_time   = "11:00:00",
    end_time     = "14:00:00",
    time_window  = "midday",
    registry     = feed_registry
  )

  pb_midday_freq$tick()
}

midday_freq_daily <- bind_rows(midday_freq_list)

# --------------------------------------------------
# 8. Daily trip counts with progress bars
# --------------------------------------------------
message("Step 9/14: Computing peak trip counts...")

pb_peak_trip <- progress_bar$new(
  format = "  Peak trips     [:bar] :current/:total (:percent) eta: :eta",
  total = nrow(analysis_dates),
  clear = FALSE,
  width = 80
)

peak_trip_list <- vector("list", nrow(analysis_dates))

for (i in seq_len(nrow(analysis_dates))) {
  row_i <- analysis_dates[i, ]

  peak_trip_list[[i]] <- get_trip_counts_one_day(
    feed_name    = row_i$feed_name,
    target_date  = row_i$analysis_date,
    period_id    = row_i$period_id,
    period_label = row_i$period_label,
    start_time   = "07:30:00",
    end_time     = "09:30:00",
    time_window  = "peak",
    registry     = feed_registry
  )

  pb_peak_trip$tick()
}

peak_trip_daily <- bind_rows(peak_trip_list)

message("Step 10/14: Computing midday trip counts...")

pb_midday_trip <- progress_bar$new(
  format = "  Midday trips   [:bar] :current/:total (:percent) eta: :eta",
  total = nrow(analysis_dates),
  clear = FALSE,
  width = 80
)

midday_trip_list <- vector("list", nrow(analysis_dates))

for (i in seq_len(nrow(analysis_dates))) {
  row_i <- analysis_dates[i, ]

  midday_trip_list[[i]] <- get_trip_counts_one_day(
    feed_name    = row_i$feed_name,
    target_date  = row_i$analysis_date,
    period_id    = row_i$period_id,
    period_label = row_i$period_label,
    start_time   = "11:00:00",
    end_time     = "14:00:00",
    time_window  = "midday",
    registry     = feed_registry
  )

  pb_midday_trip$tick()
}

midday_trip_daily <- bind_rows(midday_trip_list)

# --------------------------------------------------
# 9. Daily summary tables
# --------------------------------------------------
message("Step 11/14: Building daily summary tables...")

peak_freq_day_summary <- peak_freq_daily %>%
  group_by(period_id, period_label, analysis_date, feed_name) %>%
  summarise(
    n_routes_peak = n_distinct(route_id),
    median_route_headway_peak_min = median(median_headways, na.rm = TRUE) / 60,
    mean_route_headway_peak_min   = mean(median_headways, na.rm = TRUE) / 60,
    .groups = "drop"
  )

midday_freq_day_summary <- midday_freq_daily %>%
  group_by(period_id, period_label, analysis_date, feed_name) %>%
  summarise(
    n_routes_midday = n_distinct(route_id),
    median_route_headway_midday_min = median(median_headways, na.rm = TRUE) / 60,
    mean_route_headway_midday_min   = mean(median_headways, na.rm = TRUE) / 60,
    .groups = "drop"
  )

peak_trip_day_summary <- peak_trip_daily %>%
  group_by(period_id, period_label, analysis_date, feed_name) %>%
  summarise(
    total_peak_trips = sum(n_trips, na.rm = TRUE),
    n_route_direction_peak = n(),
    .groups = "drop"
  )

midday_trip_day_summary <- midday_trip_daily %>%
  group_by(period_id, period_label, analysis_date, feed_name) %>%
  summarise(
    total_midday_trips = sum(n_trips, na.rm = TRUE),
    n_route_direction_midday = n(),
    .groups = "drop"
  )

# --------------------------------------------------
# 10. Period level summary
# --------------------------------------------------
message("Step 12/14: Building period summaries...")

period_meta <- analysis_dates %>%
  group_by(period_id, period_label) %>%
  summarise(
    n_analysis_days = n(),
    feeds_used = paste(sort(unique(feed_name)), collapse = "; "),
    .groups = "drop"
  )

period_summary <- period_meta %>%
  left_join(
    peak_freq_day_summary %>%
      group_by(period_id, period_label) %>%
      summarise(
        avg_n_routes_peak = mean(n_routes_peak, na.rm = TRUE),
        avg_median_route_headway_peak_min = mean(median_route_headway_peak_min, na.rm = TRUE),
        avg_mean_route_headway_peak_min   = mean(mean_route_headway_peak_min, na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("period_id", "period_label")
  ) %>%
  left_join(
    midday_freq_day_summary %>%
      group_by(period_id, period_label) %>%
      summarise(
        avg_n_routes_midday = mean(n_routes_midday, na.rm = TRUE),
        avg_median_route_headway_midday_min = mean(median_route_headway_midday_min, na.rm = TRUE),
        avg_mean_route_headway_midday_min   = mean(mean_route_headway_midday_min, na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("period_id", "period_label")
  ) %>%
  left_join(
    peak_trip_day_summary %>%
      group_by(period_id, period_label) %>%
      summarise(
        avg_total_peak_trips = mean(total_peak_trips, na.rm = TRUE),
        avg_n_route_direction_peak = mean(n_route_direction_peak, na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("period_id", "period_label")
  ) %>%
  left_join(
    midday_trip_day_summary %>%
      group_by(period_id, period_label) %>%
      summarise(
        avg_total_midday_trips = mean(total_midday_trips, na.rm = TRUE),
        avg_n_route_direction_midday = mean(n_route_direction_midday, na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("period_id", "period_label")
  )

print(period_summary)

# --------------------------------------------------
# 11. Route level headway averages by period
# --------------------------------------------------
message("Step 13/14: Building route-level comparison tables...")

period_sizes <- analysis_dates %>%
  group_by(period_id) %>%
  summarise(n_days_in_period = n(), .groups = "drop")

peak_headway_period_avg <- peak_freq_daily %>%
  group_by(period_id, route_id) %>%
  summarise(
    days_present = n_distinct(analysis_date),
    avg_headway_min = mean(median_headways, na.rm = TRUE) / 60,
    median_headway_min = median(median_headways, na.rm = TRUE) / 60,
    .groups = "drop"
  ) %>%
  left_join(period_sizes, by = "period_id") %>%
  mutate(
    share_days_present = days_present / n_days_in_period
  ) %>%
  left_join(route_lookup, by = "route_id")

midday_headway_period_avg <- midday_freq_daily %>%
  group_by(period_id, route_id) %>%
  summarise(
    days_present = n_distinct(analysis_date),
    avg_headway_min = mean(median_headways, na.rm = TRUE) / 60,
    median_headway_min = median(median_headways, na.rm = TRUE) / 60,
    .groups = "drop"
  ) %>%
  left_join(period_sizes, by = "period_id") %>%
  mutate(
    share_days_present = days_present / n_days_in_period
  ) %>%
  left_join(route_lookup, by = "route_id")

route_dir_lookup <- bind_rows(
  peak_trip_daily %>% select(route_id, direction_id),
  midday_trip_daily %>% select(route_id, direction_id)
) %>%
  distinct()

date_route_dir_grid <- analysis_dates %>%
  select(period_id, period_label, analysis_date) %>%
  distinct() %>%
  tidyr::crossing(route_dir_lookup)

peak_trip_daily_complete <- date_route_dir_grid %>%
  left_join(
    peak_trip_daily %>%
      select(period_id, analysis_date, route_id, direction_id, n_trips),
    by = c("period_id", "analysis_date", "route_id", "direction_id")
  ) %>%
  mutate(n_trips = replace_na(n_trips, 0L))

midday_trip_daily_complete <- date_route_dir_grid %>%
  left_join(
    midday_trip_daily %>%
      select(period_id, analysis_date, route_id, direction_id, n_trips),
    by = c("period_id", "analysis_date", "route_id", "direction_id")
  ) %>%
  mutate(n_trips = replace_na(n_trips, 0L))

peak_trip_period_avg <- peak_trip_daily_complete %>%
  group_by(period_id, route_id, direction_id) %>%
  summarise(
    avg_n_trips = mean(n_trips, na.rm = TRUE),
    median_n_trips = median(n_trips, na.rm = TRUE),
    days_with_service = sum(n_trips > 0, na.rm = TRUE),
    share_days_with_service = mean(n_trips > 0, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(route_lookup, by = "route_id")

midday_trip_period_avg <- midday_trip_daily_complete %>%
  group_by(period_id, route_id, direction_id) %>%
  summarise(
    avg_n_trips = mean(n_trips, na.rm = TRUE),
    median_n_trips = median(n_trips, na.rm = TRUE),
    days_with_service = sum(n_trips > 0, na.rm = TRUE),
    share_days_with_service = mean(n_trips > 0, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(route_lookup, by = "route_id")

peak_headway_compare <- peak_headway_period_avg %>%
  select(period_id, route_id, route_short_name, route_long_name, avg_headway_min, share_days_present) %>%
  pivot_wider(
    names_from  = period_id,
    values_from = c(avg_headway_min, share_days_present),
    names_glue  = "{period_id}_{.value}"
  ) %>%
  mutate(
    diff_P2_vs_P1 = P2_avg_headway_min - P1_avg_headway_min,
    diff_P3_vs_P2 = P3_avg_headway_min - P2_avg_headway_min,
    diff_P3_vs_P1 = P3_avg_headway_min - P1_avg_headway_min
  ) %>%
  arrange(desc(abs(coalesce(diff_P3_vs_P1, 0))))

midday_headway_compare <- midday_headway_period_avg %>%
  select(period_id, route_id, route_short_name, route_long_name, avg_headway_min, share_days_present) %>%
  pivot_wider(
    names_from  = period_id,
    values_from = c(avg_headway_min, share_days_present),
    names_glue  = "{period_id}_{.value}"
  ) %>%
  mutate(
    diff_P2_vs_P1 = P2_avg_headway_min - P1_avg_headway_min,
    diff_P3_vs_P2 = P3_avg_headway_min - P2_avg_headway_min,
    diff_P3_vs_P1 = P3_avg_headway_min - P1_avg_headway_min
  ) %>%
  arrange(desc(abs(coalesce(diff_P3_vs_P1, 0))))

peak_trip_compare <- peak_trip_period_avg %>%
  select(period_id, route_id, direction_id, route_short_name, route_long_name,
         avg_n_trips, share_days_with_service) %>%
  pivot_wider(
    names_from  = period_id,
    values_from = c(avg_n_trips, share_days_with_service),
    names_glue  = "{period_id}_{.value}"
  ) %>%
  mutate(
    diff_P2_vs_P1 = P2_avg_n_trips - P1_avg_n_trips,
    diff_P3_vs_P2 = P3_avg_n_trips - P2_avg_n_trips,
    diff_P3_vs_P1 = P3_avg_n_trips - P1_avg_n_trips
  ) %>%
  arrange(desc(abs(coalesce(diff_P3_vs_P1, 0))))

midday_trip_compare <- midday_trip_period_avg %>%
  select(period_id, route_id, direction_id, route_short_name, route_long_name,
         avg_n_trips, share_days_with_service) %>%
  pivot_wider(
    names_from  = period_id,
    values_from = c(avg_n_trips, share_days_with_service),
    names_glue  = "{period_id}_{.value}"
  ) %>%
  mutate(
    diff_P2_vs_P1 = P2_avg_n_trips - P1_avg_n_trips,
    diff_P3_vs_P2 = P3_avg_n_trips - P2_avg_n_trips,
    diff_P3_vs_P1 = P3_avg_n_trips - P1_avg_n_trips
  ) %>%
  arrange(desc(abs(coalesce(diff_P3_vs_P1, 0))))

# --------------------------------------------------
# 12. Save outputs
# --------------------------------------------------
message("Step 14/14: Saving outputs...")

dir.create("gtfs_avg_outputs", showWarnings = FALSE)

write_csv(analysis_dates, "gtfs_avg_outputs/analysis_dates_used.csv")
write_csv(period_summary, "gtfs_avg_outputs/period_summary.csv")

write_csv(peak_freq_day_summary, "gtfs_avg_outputs/peak_freq_day_summary.csv")
write_csv(midday_freq_day_summary, "gtfs_avg_outputs/midday_freq_day_summary.csv")
write_csv(peak_trip_day_summary, "gtfs_avg_outputs/peak_trip_day_summary.csv")
write_csv(midday_trip_day_summary, "gtfs_avg_outputs/midday_trip_day_summary.csv")

write_csv(peak_headway_period_avg, "gtfs_avg_outputs/peak_headway_period_avg.csv")
write_csv(midday_headway_period_avg, "gtfs_avg_outputs/midday_headway_period_avg.csv")
write_csv(peak_trip_period_avg, "gtfs_avg_outputs/peak_trip_period_avg.csv")
write_csv(midday_trip_period_avg, "gtfs_avg_outputs/midday_trip_period_avg.csv")

write_csv(peak_headway_compare, "gtfs_avg_outputs/peak_headway_compare.csv")
write_csv(midday_headway_compare, "gtfs_avg_outputs/midday_headway_compare.csv")
write_csv(peak_trip_compare, "gtfs_avg_outputs/peak_trip_compare.csv")
write_csv(midday_trip_compare, "gtfs_avg_outputs/midday_trip_compare.csv")

message("Done. All outputs saved to the gtfs_avg_outputs folder.")