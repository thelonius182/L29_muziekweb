pacman::p_load(readr, dplyr)

cz_pgm_hours_raw <- read_delim(
  "~/Documents/cz_pgm_hours.csv",
  delim = "\t",
  escape_double = FALSE,
  trim_ws = TRUE,
  show_col_types = F
)

cz_pgm_hours.1 <- cz_pgm_hours_raw |> mutate(max_hrs = max(tot_hours),
                                             hrs_rank = tot_hours/max_hrs)

# Sort the data based on the attribute
cz_pgm_hours.2 <- cz_pgm_hours.1[order(cz_pgm_hours.1$hrs_rank),]

# Create a vector of quantiles (0 to 1) to split the data into 5 groups
quantiles <- seq(0, 1, length.out = 6)

# Use the quantiles to split the data into 5 groups
groups <-
  cut(
    cz_pgm_hours.2$hrs_rank,
    quantile(cz_pgm_hours.2$hrs_rank, probs = quantiles),
    include.lowest = TRUE,
    labels = FALSE
  )

# Check the distribution of the groups
table(groups)

cz_pgm_hours.3 <- cz_pgm_hours.2 |> bind_cols(groups)
cz_pgm_hours.4 <- cz_pgm_hours.3 |> rename(hour_group = `...5`) |> group_by(hour_group) |> mutate(grp_min = min(tot_hours),
                                                                                                  grp_max = max(tot_hours)) |> 
  arrange(hour_group, pgm_title) |> ungroup() |> select(hour_group, pgm_title)
