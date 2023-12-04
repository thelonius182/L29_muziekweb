pacman::p_load(DBI, dplyr, fs, stringr, lubridate, yaml, futile.logger, tibble, readr, tidyr)

# import functions ----
source("src/basie_tools.R", encoding = "UTF-8")

# init logging ----
basie_config <- read_yaml("basie_config.yaml")
log_path <- paste(basie_config$log_home, "basie_beats.log", sep = "/")
lg_ini <- flog.appender(appender.file(log_path), "bsblog")
flog.info("
= = = = = START BasieBeats (version 2023-10-27 21:11) = = = = =", name = "bsblog")

# connect to mAirList-DB ----
maldb <- get_mal_conn()
db_conn_result <- typeof(maldb)
stopifnot("mAirList-DB not available" = db_conn_result == "S4")

flog.info("mAirList-DB is connected", name = "bsblog")

# skip jingles and shows
sql_stm <- "select title from public.items where type = 'Music';"
ml_items_db <- dbGetQuery(maldb, sql_stm)

# get new muw tracks
new_muw_track_ids <- ml_items_db |> filter(str_detect(title, "^[A-Z0-9]+-\\d+$"))

# link to muziekweb metadata
muw_track_info <- read_rds("c:/Users/nipper/Documents/BasieBeats/all_album_info.RDS") |> 
  inner_join(new_muw_track_ids, by = c("track_id" = "title")) |> 
  select(-muw_catalogue_type, -deel_in_titel) |> 
  mutate(genre_A = if_else(str_detect(genre, "jazz"), "jazz", "world")) |> 
  separate_longer_delim(cols = genre, delim = ",") |> rename(muw_genre = genre) |> 
  filter(!muw_genre %in% c("landen", "wereld", "populair", "overige talen"))

# split jazz / world
muw_track_info_jazz <- muw_track_info |> filter(genre_A == "jazz") |> select(-genre_A)
muw_track_info_world <- muw_track_info |> filter(genre_A == "world") |> select(-genre_A)

# get unique muw_genres
genres_jazz_muw <- muw_track_info_jazz |> select(muw_genre) |> distinct() |> arrange(muw_genre)
genres_world_muw <- muw_track_info_world |> select(muw_genre) |> distinct() |> arrange(muw_genre)

# compact to woj_genres
woj_dir <- "c:/Users/nipper/Documents/BasieBeats"
tsv_ymd <- now(tzone = "Europe/Amsterdam") |> as_date() |> as.character()
qfn <- paste0("genres_jazz_muw_", tsv_ymd, ".tsv")
write_delim(genres_jazz_muw, path_join(c(woj_dir, qfn)), delim = "\t")
qfn <- paste0("genres_world_muw_", tsv_ymd, ".tsv")
write_delim(genres_world_muw, path_join(c(woj_dir, qfn)), delim = "\t")

fs_muw2woj_world <- read_delim("C:/Users/nipper/Documents/BasieBeats/muw2woj-world.txt")
all_track_info_world <- muw_track_info_world |> left_join(fs_muw2woj_world)

fs_muw2woj_jazz <- read_delim("C:/Users/nipper/Documents/BasieBeats/muw2woj-jazz.txt")
all_track_info_jazz <- muw_track_info_jazz |> left_join(fs_muw2woj_jazz)

sql_stm <- "select idx, title from items where type = 'Music';"
ml_items_db <- dbGetQuery(maldb, sql_stm)
ml_items_db_new <- ml_items_db |> filter(str_detect(title, "^[A-Z0-9]+-\\d+$"))
all_track_info_jazz_ml <- all_track_info_jazz |> left_join(ml_items_db_new, by = c("track_id" = "title"))
all_track_info_world_ml <- all_track_info_world |> left_join(ml_items_db_new, by = c("track_id" = "title"))

upd_jazz_items <- all_track_info_jazz_ml |> select(idx, track_id, titel, performers) |> distinct()
dbWriteTable(maldb, "basiebeats_upd_jazz_items", upd_jazz_items)
sql_stmt <- "update items
set externalid = basiebeats_upd_jazz_items.track_id,
    title = basiebeats_upd_jazz_items.titel,
    artist = basiebeats_upd_jazz_items.performers
from basiebeats_upd_jazz_items
where items.idx = basiebeats_upd_jazz_items.idx;"
dbExecute(maldb, sql_stmt)

upd_jazz_item_albums <- all_track_info_jazz_ml |> select(idx, value = album) |> distinct() |> 
  mutate(name = "album") |> select(item = idx, name, value)

upd_jazz_item_attrs <- all_track_info_jazz_ml |> select(idx, name = woj_genre) |> distinct() |> 
  mutate(value = "yes") |> select(item = idx, name, value)

upd_jazz_genre_A <- all_track_info_jazz_ml |> select(item = idx) |> distinct() |> 
  mutate(name = "jazz", value = "yes") |> select(item, name, value) |> arrange(item)
dbWriteTable(maldb, "basiebeats_upd_jazz_genre_a", upd_jazz_genre_A)

upd_jazz_all_attrs <- bind_rows(upd_jazz_genre_A, upd_jazz_item_albums, upd_jazz_item_attrs) |> 
  arrange(item)
dbWriteTable(maldb, "basiebeats_upd_jazz_all_attrs", upd_jazz_all_attrs)

sql_stmt <- "insert into item_attributes
select * from basiebeats_upd_jazz_all_attrs;"
dbExecute(maldb, sql_stmt)

sql_stmt <- "insert into item_attributes
select * from basiebeats_upd_jazz_genre_a;"
dbExecute(maldb, sql_stmt)


# get CD-id's
muw_ids <- new_muw_track_ids |> mutate(muw_id = sub(".*?([A-Z0-9]+)-\\d+.*", "\\1", title, perl=TRUE)) |> 
  select(muw_id) |> distinct() |> arrange(muw_id)


# test update
tid <- "HEX10717-0016"
sql_stm <- "select * from public.items where title = 'HEX12644-0011'"
t1 <- dbGetQuery(maldb, sql_stm)

t2 <- all_album_info |> filter(track_id == "HEX12644-0011") |> select(track_id, titel, album, performers)
upd_stm <- sprintf("
update public.items set externalid = 'HEX12644-0011', title = '%s' 
where title = 'HEX12644-0011'", t2$titel)
dbExecute(maldb, upd_stm)

rm(ml_items_db)

dbDisconnect(maldb)
flog.info("mAirList-DB is disconnected", name = "bsblog")
flog.info("= = = = = STOP BasieBeats = = = = =", name = "bsblog")
