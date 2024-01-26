pacman::p_load(RcppAlgos, DBI, dplyr, fs, stringr, lubridate, yaml, futile.logger, tibble, readr, tidyr)

# import functions ----
source("src/basie_tools.R", encoding = "UTF-8")

# init logging ----
basie_config <- read_yaml("basie_config.yaml")
log_path <- paste(basie_config$log_home, "basie_beats.log", sep = "/")
lg_ini <- flog.appender(appender.file(log_path), "bsblog")

# connect to mAirList-DB ----
maldb <- get_mal_conn()
db_conn_result <- typeof(maldb)
stopifnot("mAirList-DB not available" = db_conn_result == "S4")

flog.info("mAirList-DB is connected", name = "bsblog")

sql_stm <- "SELECT idx, artist, title, duration,
	   a1.value as album
FROM items join item_attributes a1 on a1.item = idx
where a1.name = 'album'"
ml_albums_a <- dbGetQuery(maldb, sql_stm)

ml_albums_b <- ml_albums_a |> arrange(idx) |> group_by(artist, album) |> mutate(grp_id = row_number()) |> 
  ungroup()

ml_albums_c <- ml_albums_b |> filter(grp_id == 1)

sql_stm <- "SELECT * FROM item_attributes"
ml_attrs <- dbGetQuery(maldb, sql_stm)

ml_albums_d <- ml_albums_c |> left_join(ml_attrs, by = c("idx" = "item")) |> filter(name != "album") |> 
  select(-grp_id, -value)

ml_albums_e <- ml_albums_d |> mutate(attr_value = 1) |> 
  pivot_wider(names_from = name, values_from = attr_value)

n1 <- names(ml_albums_e) |> sort()

ml_albums_f1 <- ml_albums_e |> select(all_of(n1)) |> select(idx, artist, album, everything()) |> 
  mutate_all(~replace(., is.na(.), 0))

ml_albums_f2 <- ml_albums_e |> select(all_of(n1)) |> select(idx, artist, album, everything()) |> 
  mutate_all(~replace(., is.na(.), 0)) |> select(-duration, -title)
# 
# ml_stats_s1 <- ml_albums_f2 |> 
#   group_by(`1900\`s`, `1910\`s`, `1920\`s`, `1930\`s`, `1940\`s`, `1950\`s`, `1960\`s`, `1970\`s`, `1980\`s`, `1990\`s`, 
#            `2000\`s`, `2010+`, afrika, any_jazz, any_world, azië, bebop, `big band`, blues, `cool jazz, west coast`, 
#            `easy listening`, electronica, europa, `free jazz, avantgarde`, `funk, soul`, fusion, hiphop, instrumentaal, 
#            jazzrock, `mainstream, swing`, modern, nederland, `neo-bop`, `neo-soul`, `noord-amerika`, `nu jazz`, pop, 
#            reggae, `rhythm and blues`, rock, soundtrack, vocaal, wereldjazz, `zuid-amerika`) |> summarise(n = n())

# bebop or neo-bop ----
pl_bebop_neobop <- ml_albums_f2 |> filter(any_world == 0 & (bebop == 1 | `neo-bop` == 1))

# Artist
pl_bebop_neobop_R <- pl_bebop_neobop |> select(artist) |> distinct() |> 
  sample() |> mutate(bb_idx_R = row_number()) |> select(bb_idx_R, artist)

# Track
pl_bebop_neobop_T <- ml_albums_a |> inner_join(pl_bebop_neobop_R) |> 
  select(bb_idx_R, artist, album, title, duration, track_id = idx) |> arrange(artist, album, title) |> 
  group_by(artist, album, title) |> mutate(idx = row_number()) |> filter(idx == 1) |> 
  select(-idx) |> ungroup()

# Artist, Album
pl_bebop_neobop_RL <- pl_bebop_neobop_T |> select(bb_idx_R, artist, album) |> distinct() |>
  group_by(artist) |> mutate(bb_idx_L = row_number()) |> ungroup() |> 
  select(bb_idx_R, artist, bb_idx_L, album)

# Artist, Album, Track
pl_bebop_neobop_RLT <- pl_bebop_neobop_RL |> inner_join(pl_bebop_neobop_T) |> 
  group_by(artist, album) |> mutate(bb_idx_T = row_number()) |> 
  ungroup() |> select(bb_idx_R, bb_idx_L, bb_idx_T, everything())


# = = = = = = = = = = = = = = = = = = = = = = = = =

# Example data: a list of objects with weights and other properties
weights <- sample(x = ml_albums_f$duration, size = 30)

# Solve the subset sum problem using partitionsGeneral
pi_dur <- partitionsIter(v = weights, target = 3600, m = 12, tolerance = 10, nThreads = 4)
dur_seq <- pi_dur$nextIter()
sum(dur_seq)
weights <- setdiff(weights, dur_seq)
pi_dur <- partitionsIter(v = weights, target = 3600, m = 12, tolerance = 10, nThreads = 4)
dur_seq <- pi_dur$nextIter()
sum(dur_seq)

cur_album <- ml_albums_f |> filter(duration == 171.851)

# Get the keys of selected objects
selected_keys <- names(objects)[selected_indices]

# Print the keys of selected objects
print(selected_keys)

# Get the keys of selected objects
selected_keys <- names(objects)[selected_indices]

# Print the keys of selected objects
print(selected_keys)