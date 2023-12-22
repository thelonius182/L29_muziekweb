pacman::p_load(DBI, dplyr, fs, stringr, lubridate, yaml, futile.logger, tibble, readr, tidyr)

# import functions ----
source("src/basie_tools.R", encoding = "UTF-8")

# init logging ----
basie_config <- read_yaml("basie_config.yaml")
log_path <- paste(basie_config$log_home, "basie_beats.log", sep = "/")
lg_ini <- flog.appender(appender.file(log_path), "bsblog")
flog.info("
= = = = = START BasieBeats (version 2023-12-22 15:40) = = = = =", name = "bsblog")

# connect to mAirList-DB ----
maldb <- get_mal_conn()
db_conn_result <- typeof(maldb)
stopifnot("mAirList-DB not available" = db_conn_result == "S4")

flog.info("mAirList-DB is connected", name = "bsblog")

# skip jingles and shows
sql_stm <- "select title from public.items where type = 'Music';"
ml_items_db <- dbGetQuery(maldb, sql_stm)

# glimpse items
sql_stm <- "select * from public.items where type = 'Music';"
ml_glimpse_db <- dbGetQuery(maldb, sql_stm)

# glimpse attr
sql_stm <- "select * from public.item_attributes;"
ml_glimpse_attr_db <- dbGetQuery(maldb, sql_stm)

# get new muw tracks
new_muw_track_ids <- ml_items_db |> filter(str_detect(title, "^[A-Z]{1,3}[0-9]+-\\d{4}$"))

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
#
# --> now, outside of this script, manually map muw-genres to woj-genres
#
fs_muw2woj_world <- read_delim("C:/Users/nipper/Documents/BasieBeats/muw2woj-world.txt")
all_track_info_world <- muw_track_info_world |> left_join(fs_muw2woj_world)

fs_muw2woj_jazz <- read_delim("C:/Users/nipper/Documents/BasieBeats/muw2woj-jazz.txt")
all_track_info_jazz <- muw_track_info_jazz |> left_join(fs_muw2woj_jazz)

sql_stm <- "select idx, title from items where type = 'Music';"
ml_items_db <- dbGetQuery(maldb, sql_stm)
ml_items_db_new <- ml_items_db |> filter(str_detect(title, "^[A-Z0-9]+-\\d+$"))
all_track_info_jazz_ml <- all_track_info_jazz |> left_join(ml_items_db_new, by = c("track_id" = "title"))
all_track_info_world_ml <- all_track_info_world |> left_join(ml_items_db_new, by = c("track_id" = "title"))

# UPD JAZZ TRACKS ----
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

# sql_stmt <- "insert into item_attributes
# select * from basiebeats_upd_jazz_genre_a;"
# dbExecute(maldb, sql_stmt)

# UPD WORLD TRACKS ----
upd_world_items <- all_track_info_world_ml |> select(idx, track_id, titel, performers) |> distinct()
dbWriteTable(maldb, "basiebeats_upd_world_items", upd_world_items)
sql_stmt <- "update items
set externalid = basiebeats_upd_world_items.track_id,
    title = basiebeats_upd_world_items.titel,
    artist = basiebeats_upd_world_items.performers
from basiebeats_upd_world_items
where items.idx = basiebeats_upd_world_items.idx;"
dbExecute(maldb, sql_stmt)

upd_world_item_albums <- all_track_info_world_ml |> select(idx, value = album) |> distinct() |> 
  mutate(name = "album") |> select(item = idx, name, value)

upd_world_item_attrs <- all_track_info_world_ml |> select(idx, name = woj_genre) |> distinct() |> 
  mutate(value = "yes") |> select(item = idx, name, value)

upd_world_genre_A <- all_track_info_world_ml |> select(item = idx) |> distinct() |> 
  mutate(name = "world", value = "yes") |> select(item, name, value) |> arrange(item)
dbWriteTable(maldb, "basiebeats_upd_world_genre_a", upd_world_genre_A)

upd_world_all_attrs <- bind_rows(upd_world_genre_A, upd_world_item_albums, upd_world_item_attrs) |> 
  arrange(item)
dbWriteTable(maldb, "basiebeats_upd_world_all_attrs", upd_world_all_attrs)

sql_stmt <- "insert into item_attributes
select * from basiebeats_upd_world_all_attrs;"
dbExecute(maldb, sql_stmt)

# sql_stmt <- "insert into item_attributes
# select * from basiebeats_upd_world_genre_a;"
# dbExecute(maldb, sql_stmt)

# clean artists
sql_stm <- "select idx, artist from public.items where type = 'Music';"
ml_artists_db <- dbGetQuery(maldb, sql_stm)

# ml_artists.1 <- ml_artists_db |> arrange(artist) |> distinct()
# ml_artists.2 <- ml_artists_db |> 
#   filter(str_detect(artist, "and|AND|And|[,&]")) |> mutate(rrn = row_number())

ml_artists.3 <- ml_artists_db |> 
  separate_longer_delim(artist, delim = ", ") |> 
  separate_longer_delim(artist, delim = "; ") |> 
  separate_longer_delim(artist, delim = ";") |> 
  separate_longer_delim(artist, delim = "_") |> 
  separate_longer_delim(artist, delim = ",") |> 
  separate_longer_delim(artist, delim = " & ") |> 
  separate_longer_delim(artist, delim = "&") |> 
  separate_longer_delim(artist, delim = " + ") |> 
  separate_longer_delim(artist, delim = " / ") |> 
  separate_longer_delim(artist, delim = " ／ ") |> 
  separate_longer_delim(artist, delim = "／") |> 
  separate_longer_delim(artist, delim = " and ") |> 
  separate_longer_delim(artist, delim = " AND ") |> 
  separate_longer_delim(artist, delim = " And ") |> 
  separate_longer_delim(artist, delim = "-") |> 
  separate_longer_delim(artist, delim = " - ") |> 
  separate_longer_delim(artist, delim = " w ") |> 
  separate_longer_delim(artist, delim = " W ") |> 
  separate_longer_delim(artist, delim = " w") |> 
  separate_longer_delim(artist, delim = " With ") |> 
  separate_longer_delim(artist, delim = " with ") |> 
  separate_longer_delim(artist, delim = " WITH ") |> 
  separate_longer_delim(artist, delim = " Feat. ") |> 
  separate_longer_delim(artist, delim = " feat. ") |> 
  separate_longer_delim(artist, delim = " featuring ") |> 
  filter(!is.na(artist) & str_length(str_trim(artist, "both")) > 0) |> 
  mutate(artist = str_trim(artist, "both")) |> group_by(idx) |> mutate(rrn = row_number()) |> ungroup() |> 
  filter(rrn == 1)

ml_artists.4 <- ml_artists.3 |> 
  mutate(artist = if_else(artist == "Anita O'Day (Giants Of Jazz Series)", "Anita O'Day", artist),
         artist = if_else(artist == "Stan Getz (tenor saxophone)", "Stan Getz", artist), 
         artist = if_else(artist == "The Free Spirits (Featuring John Mclaughlin)", "The Free Spirits", artist),
         artist = if_else(artist == "Verzamel (inleiding door Pim Jacobs)", "Pim Jacobs", artist),
         artist = if_else(artist == "Carolina Dandies (Sunny Clapp", "Carolina Dandies", artist),
         artist = if_else(artist == "The Street Singer (Arthur Tracy)", "Arthur Tracy", artist),
         artist = if_else(artist == "The Jungle Band (Duke Ellington", "Duke Ellington", artist),
         artist = if_else(artist == "Avishai Cohen [IL]", "Avishai Cohen", artist),
         artist = if_else(artist == "Bill Evans [US I]", "Bill Evans", artist),
         artist = if_else(artist == "Sidiki Diabaté [I]", "Sidiki Diabaté", artist),
         artist = if_else(artist == 'Alvin StollerBarney KesselBillie HolidayBen WebsterRed MitchellJimmy RowlesHarry "Sweets" Edison', "Alvin Stoller", artist),
         artist = if_else(artist == 'Jimmy RowlesJohn SimmonsBarney KesselLarry BunkerHarry "Sweets" EdisonBillie HolidayBenny Carter', "Jimmy Rowles", artist),
         artist = if_else(idx == 9614, "Oscar Peterson", artist),
         artist = if_else(idx == 17905, "Benny Carter", artist),
         artist = if_else(idx == 17909, "Benny Carter", artist),
         artist = if_else(idx == 17910, "Benny Carter", artist),
         artist = if_else(idx == 17913, "Larry Bunker", artist),
         artist = if_else(idx == 17916, "Barney Kessel", artist),
         artist = if_else(idx == 17929, "John Simmons", artist),
         artist = if_else(idx == 17999, "Ben Webster", artist),
         artist = if_else(idx == 18001, "Barney Kessel", artist),
         artist = if_else(idx == 18010, "Harry 'Sweets' Edison", artist),
         artist = if_else(idx == 18012, "Harry 'Sweets' Edison", artist),
         artist = if_else(idx == 18017, "Red Mitchell", artist),
         artist = if_else(idx == 18019, "Alvin Stoller", artist),
         artist = if_else(idx == 18011, "Ben Webster", artist),
         artist = if_else(idx == 22904, "Me'Shell Ndegé", artist),
         artist = if_else(idx == 14168, "Count Basie", artist),
         artist = if_else(idx == 14172, "Joe Williams", artist),
         artist = if_else(idx == 12731, "Dewey Redman", artist),
         artist = if_else(idx == 12732, "Cecil Taylor", artist),
         artist = if_else(idx == 12733, "Dewey Redman", artist),
         artist = if_else(idx == 12734, "Cecil Taylor", artist),
         artist = if_else(idx == 12735, "Dewey Redman", artist),
         artist = if_else(idx == 12736, "Dewey Redman", artist),
         artist = if_else(idx == 12737, "Elvin Jones", artist),
         artist = if_else(idx == 16963, "Les Brown", artist),
         artist = if_else(idx == 12157, "Bill Evans", artist),
         artist = if_else(idx == 12159, "Bill Evans", artist),
  ) |> select(-rrn) |> mutate(len = str_length(artist))

# UPD ARTISTS ----
upd_artists <- ml_artists.4 |> select(idx, artist) |> distinct()
dbWriteTable(maldb, "basiebeats_upd_artists", upd_artists)
sql_stmt <- "update items
set artist = basiebeats_upd_artists.artist
from basiebeats_upd_artists
where items.idx = basiebeats_upd_artists.idx;"
dbExecute(maldb, sql_stmt)

id3tag_2_muw_raw <- read_delim("C:/Users/nipper/Documents/BasieBeats/id3tag_genre-2-muw.txt",
                               delim = "\t", escape_double = FALSE,
                               col_names = FALSE, trim_ws = TRUE) |> rename(id3tag = X1, muwtag = X2)

dbWriteTable(maldb, "basiebeats_conv_id3tags", id3tag_2_muw_raw)

sql_stmt <- "select idx, externalid from items;"
muw_track_ids <- dbGetQuery(maldb, sql_stmt)
muw_album_ids <- muw_track_ids |> filter(str_detect(externalid, ".+-.+")) |> 
  mutate(album_id = str_extract(externalid, "(.+?)-.+", group = 1))
neo_bop <- muw_album_ids |> filter(album_id %in% c('DPE00114',
                                                   'JE15282',
                                                   'JE19086',
                                                   'JE24370',
                                                   'JE26100',
                                                   'JE26936',
                                                   'JE29104',
                                                   'JE31124',
                                                   'JE36272',
                                                   'JE40553',
                                                   'JE48544',
                                                   'JE49802',
                                                   'JFX4343')) |> 
  mutate(name = "neo-bop", value = "yes") |> 
  select(item = idx, name, value)
dbWriteTable(maldb, "basiebeats_neo_bop", neo_bop)

jazzrock <- muw_album_ids |> filter(album_id %in% c('JEX1148',
                                                   'JKX0173',
                                                   'JKX0476')) |> 
  mutate(name = "jazzrock", value = "yes") |> 
  select(item = idx, name, value)
dbWriteTable(maldb, "basiebeats_jazzrock", jazzrock)

neo_soul <- muw_album_ids |> filter(album_id %in% c('JK246886',
                                                   'JK250202',
                                                   'JK260863')) |> 
  mutate(name = "neo-soul", value = "yes") |> 
  select(item = idx, name, value)
dbWriteTable(maldb, "basiebeats_neo_soul", neo_soul)

# get CD-id's
muw_ids <- new_muw_track_ids |> mutate(muw_id = sub(".*?([A-Z0-9]+)-\\d+.*", "\\1", title, perl=TRUE)) |> 
  select(muw_id) |> distinct() |> arrange(muw_id)

sql_stmt <- "select distinct name from item_attributes where value = 'yes' order by 1;"
standard_attrs <- dbGetQuery(maldb, sql_stmt)


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
