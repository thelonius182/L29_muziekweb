pacman::p_load(DBI, dplyr, fs, stringr, lubridate, yaml, futile.logger, tibble, readr, tidyr)

# import functions ----
source("src/basie_tools.R", encoding = "UTF-8")

# init logging ----
basie_config <- read_yaml("basie_config.yaml")
log_path <- paste(basie_config$log_home, "basie_beats.log", sep = "/")
lg_ini <- flog.appender(appender.file(log_path), "bsblog")
flog.info("= = = = = START muziekwweb-2-mairlist (version 2024-03-05 23:54) = = = = =", name = "bsblog")

# connect to mAirList-DB ----
maldb <- get_mal_conn()
db_conn_result <- typeof(maldb)
stopifnot("mAirList-DB not available" = db_conn_result == "S4")

flog.info("mAirList-DB is connected", name = "bsblog")

# GET NEW ID's ----
# get MuW-id's from title; skip jingles and shows
sql_stm <- "select idx, title from public.items where type = 'Music';"
mal_items <- dbGetQuery(maldb, sql_stm)

# get new CD-id's
# . tracks
new_mal_album_ids <- mal_items |> filter(str_detect(title, "[A-Z][A-Z0-9]+-0\\d{3}$")) |> 
  # . albums
  mutate(muw_id = str_extract(title, pattern = "([A-Z][A-Z0-9]+)-0\\d{3}$", group = 1)) |> 
  filter(!is.na(muw_id)) |> select(muw_id) |> distinct() |> arrange(muw_id)

if (nrow(new_mal_album_ids) == 0) {
  flog.info("No new Muziekweb-albums found in mAirList-db.", name = "bsblog")
  dbDisconnect(maldb)
  flog.info("mAirList-DB is disconnected", name = "bsblog")
  flog.info("= = = = = STOP muziekwweb-2-mairlist = = = = =", name = "bsblog")
  stop("no new albums (not an error).")
}

# MuW WEBSERVICE ----
# result = new_album_info.RDS
source("src/get_muw_metadata.R", encoding = "UTF-8")

# get new muw tracks
new_mal_track_ids <- mal_items |> filter(str_detect(title, ".*[0-9]{3}-0\\d{3}$"))

# link to muziekweb metadata
new_muw_track_info <- read_rds("c:/Users/nipper/Documents/BasieBeats/new_album_info.RDS") |> 
  inner_join(new_mal_track_ids, by = c("track_id" = "title")) |> 
  select(-muw_catalogue_type, -deel_in_titel) |> 
  mutate(genre_A = if_else(str_detect(genre, "jazz"), "any_jazz", "any_world"),
         muw_album_id = str_extract(track_id, pattern = "([A-Z][A-Z0-9]+)-0\\d{3}$", group = 1)) |> 
  # put every genre label on a row of its own, to allow conversion to WoJ-label
  separate_longer_delim(cols = genre, delim = ",") |> rename(muw_genre = genre) |> 
  filter(!muw_genre %in% c("landen", "wereld", "populair", "overige talen"))

# split jazz / world
new_muw_track_info_jazz <- new_muw_track_info |> filter(genre_A == "any_jazz")
new_muw_track_info_world <- new_muw_track_info |> filter(genre_A == "any_world")

# get unique muw_genres
new_muw_genres_jazz <- new_muw_track_info_jazz |> select(muw_genre) |> distinct() |> arrange(muw_genre)
new_muw_genres_world <- new_muw_track_info_world |> select(muw_genre) |> distinct() |> arrange(muw_genre)

# woj_dir <- "c:/Users/nipper/Documents/BasieBeats"
# tsv_ymd <- now(tzone = "Europe/Amsterdam") |> as_date() |> as.character()
# qfn <- paste0("genres_jazz_muw_", tsv_ymd, ".tsv")
# write_delim(genres_jazz_muw, path_join(c(woj_dir, qfn)), delim = "\t")
# qfn <- paste0("genres_world_muw_", tsv_ymd, ".tsv")
# write_delim(genres_world_muw, path_join(c(woj_dir, qfn)), delim = "\t")
#
# --> now, outside of this script, manually map muw-genres to woj-genres

# CONVERT_2_WOJ ----
gd_muw2woj_jazz <- read_rds("C:/Users/nipper/cz_rds_store/branches/cz_gdrive/muw2woj_jazz.RDS")
gd_muw2woj_world <- read_rds("C:/Users/nipper/cz_rds_store/branches/cz_gdrive/muw2woj_world.RDS")
gd_muw2woj <- gd_muw2woj_jazz |> bind_rows(gd_muw2woj_world) |> arrange(muw_genre) |> distinct()

new_cvt_genres_jazz <- new_muw_genres_jazz |> left_join(gd_muw2woj)
new_cvt_genres_jazz_failed <- new_cvt_genres_jazz |> filter(is.na(woj_genre))
if (nrow(new_cvt_genres_jazz_failed) > 0) {
  flog.info(paste("Missing jazz genres:", paste(new_cvt_genres_jazz_failed$muw_genre, collapse = ", ")), 
            name = "bsblog")
  flog.info(paste("Requires changing the GD-spreadsheet.",
                  "New WoJ-genres also require changing the standard attributes in db-config"), 
            name = "bsblog")
}

new_cvt_genres_world <- new_muw_genres_world |> left_join(gd_muw2woj)
new_cvt_genres_world_failed <- new_cvt_genres_world |> filter(is.na(woj_genre))
if (nrow(new_cvt_genres_world_failed) > 0) {
  flog.info(paste("Missing world genres:", paste(new_cvt_genres_world_failed$muw_genre, collapse = ", ")), 
            name = "bsblog")
  flog.info(paste("Requires changing the GD-spreadsheet.",
                  "New WoJ-genres also require changing the standard attributes in db-config"), 
            name = "bsblog")
}

if (nrow(new_cvt_genres_jazz_failed) > 0 || nrow(new_cvt_genres_world_failed) > 0) {
  dbDisconnect(maldb)
  flog.info("mAirList-DB is disconnected", name = "bsblog")
  flog.info("= = = = = STOP muziekwweb-2-mairlist = = = = =", name = "bsblog")
  stop("Some muziekweb genre conversions are missing. See joblog")
}

# EXTENDED MUW TRACKS ----
# ext_muw_track_info = muziekweb trackinfo on new tracks, muw_genres replaced by woj-genres
ext_muw_track_info_jazz <- new_muw_track_info_jazz |> left_join(new_cvt_genres_jazz) |> 
  rename(genre_B = woj_genre) |> select(-muw_genre)
ext_muw_track_info_world <- new_muw_track_info_world |> left_join(new_cvt_genres_world) |> 
  rename(genre_B = woj_genre) |> select(-muw_genre)

# CLEAN ARTISTS ----
raw_artists_jazz <- ext_muw_track_info_jazz |> select(muw_album_id, performers) |> distinct()
clean_artists_jazz <- clean_artists(raw_artists_jazz)
ext_muw_track_info_jazz <- ext_muw_track_info_jazz |> left_join(clean_artists_jazz) |> select(-performers)

raw_artists_world <- ext_muw_track_info_world |> select(muw_album_id, performers) |> distinct()
clean_artists_world <- clean_artists(raw_artists_world)
ext_muw_track_info_world <- ext_muw_track_info_world |> left_join(clean_artists_world) |> select(-performers)

# UPD JAZZ TRACKS ----
upd_jazz_items <- ext_muw_track_info_jazz |> select(idx, track_id, titel, artist) |> distinct()
sql_drop <- "drop table if exists basiebeats_upd_jazz_items"
dbExecute(maldb, sql_drop)
dbWriteTable(maldb, "basiebeats_upd_jazz_items", upd_jazz_items)

sql_stmt <- "update items
set externalid = u1.track_id,
    title = u1.titel,
    artist = u1.artist
from basiebeats_upd_jazz_items u1
where items.idx = u1.idx;"
n_updates <- dbExecute(maldb, sql_stmt)
flog.info(paste0("Updated ", n_updates, " table = items, Jazz"), name = "bsblog")

upd_jazz_genre_A <- ext_muw_track_info_jazz |> select(item = idx) |> distinct() |> 
  mutate(name = "any_jazz", value = "yes") |> select(item, name, value)

upd_jazz_genre_B <- ext_muw_track_info_jazz |> select(item = idx, name = genre_B) |> distinct() |> 
  mutate(value = "yes") |> select(item, name, value)

upd_jazz_albums <- ext_muw_track_info_jazz |> select(item = idx, value = album) |> distinct() |> 
  mutate(name = "album") |> select(item, name, value)

upd_jazz_all_attrs <- bind_rows(upd_jazz_genre_A, upd_jazz_genre_B, upd_jazz_albums) |> 
  arrange(item, name)

sql_drop <- "drop table if exists basiebeats_upd_jazz_all_attrs"
dbExecute(maldb, sql_drop)
dbWriteTable(maldb, "basiebeats_upd_jazz_all_attrs", upd_jazz_all_attrs)

sql_stmt <- "insert into item_attributes
select * from basiebeats_upd_jazz_all_attrs;"
n_inserts <- dbExecute(maldb, sql_stmt)
flog.info(paste0("Inserted ", n_inserts, " table - item_attributes, Jazz"), name = "bsblog")

# UPD WORLD TRACKS ----
upd_world_items <- ext_muw_track_info_world |> select(idx, track_id, titel, artist) |> distinct()
sql_drop <- "drop table if exists basiebeats_upd_world_items"
dbExecute(maldb, sql_drop)
dbWriteTable(maldb, "basiebeats_upd_world_items", upd_world_items)

sql_stmt <- "update items
set externalid = u1.track_id,
    title = u1.titel,
    artist = u1.artist
from basiebeats_upd_world_items u1
where items.idx = u1.idx;"
n_updates <- dbExecute(maldb, sql_stmt)
flog.info(paste0("Updated ", n_updates, " table = items, World"), name = "bsblog")

upd_world_genre_A <- ext_muw_track_info_world |> select(item = idx) |> distinct() |> 
  mutate(name = "any_world", value = "yes") |> select(item, name, value)

upd_world_genre_B <- ext_muw_track_info_world |> select(item = idx, name = genre_B) |> distinct() |> 
  mutate(value = "yes") |> select(item, name, value)

upd_world_albums <- ext_muw_track_info_world |> select(item = idx, value = album) |> distinct() |> 
  mutate(name = "album") |> select(item, name, value)

upd_world_all_attrs <- bind_rows(upd_world_genre_A, upd_world_genre_B, upd_world_albums) |> 
  arrange(item, name)

sql_drop <- "drop table if exists basiebeats_upd_world_all_attrs"
dbExecute(maldb, sql_drop)
dbWriteTable(maldb, "basiebeats_upd_world_all_attrs", upd_world_all_attrs)

sql_stmt <- "insert into item_attributes
select * from basiebeats_upd_world_all_attrs;"
n_inserts <- dbExecute(maldb, sql_stmt)
flog.info(paste0("Inserted ", n_inserts, " table = item_attributes, World"), name = "bsblog")

# UPDATE_DECADES ----
upd_decades <- bind_rows(ext_muw_track_info_jazz, ext_muw_track_info_world) |> 
  select(item = idx, released, recorded) |> 
  mutate(tr_year = if_else(str_length(recorded) > 0, as.integer(parse_number(recorded)), 
                           as.integer(year(ymd(released)))),
         name = case_when(tr_year >= 2010 ~ "2010+",
                          tr_year >= 2000 ~ "2000`s",
                          tr_year >= 1990 ~ "1990`s",
                          tr_year >= 1980 ~ "1980`s",
                          tr_year >= 1970 ~ "1970`s",
                          tr_year >= 1960 ~ "1960`s",
                          tr_year >= 1950 ~ "1950`s",
                          tr_year >= 1940 ~ "1940`s",
                          tr_year >= 1930 ~ "1930`s",
                          tr_year >= 1920 ~ "1920`s",
                          tr_year >= 1910 ~ "1910`s",
                          T ~ "1900`s"),
         value = "yes") |> select(-released, -recorded, -tr_year) |> distinct() |> arrange(item, name)

sql_drop <- "drop table if exists basiebeats_ymd_attrs"
dbExecute(maldb, sql_drop)
dbWriteTable(maldb, "basiebeats_ymd_attrs", upd_decades)

sql_stmt <- "insert into item_attributes select * from basiebeats_ymd_attrs"
n_inserts <- dbExecute(maldb, sql_stmt)
flog.info(paste0("Inserted ", n_inserts, " table = item_attributes, Decades"), name = "bsblog")

dbDisconnect(maldb)
flog.info("mAirList-DB is disconnected", name = "bsblog")
flog.info("= = = = = STOP muziekwweb-2-mairlist = = = = =", name = "bsblog")
