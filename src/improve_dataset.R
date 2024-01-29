pacman::p_load(DBI, dplyr, fs, stringr, lubridate, yaml, futile.logger, tibble, readr, tidyr)

# import functions ----
source("src/basie_tools.R", encoding = "UTF-8")

# init logging ----
basie_config <- read_yaml("basie_config.yaml")
log_path <- paste(basie_config$log_home, "basie_beats.log", sep = "/")
lg_ini <- flog.appender(appender.file(log_path), "bsblog")
# flog.info("
# = = = = = START BasieBeats (version 2023-12-22 15:40) = = = = =", name = "bsblog")

# connect to mAirList-DB ----
maldb <- get_mal_conn()
db_conn_result <- typeof(maldb)
stopifnot("mAirList-DB not available" = db_conn_result == "S4")

# flog.info("mAirList-DB is connected", name = "bsblog")

sql_stm <- "select item, count(*) as n_tags from item_attributes
where name not in ('any_jazz', 'any_world', 'album')
group by item order by 2 desc;"
ml_tags_count <- dbGetQuery(maldb, sql_stm)

sql_stm <- "select idx, filename from items 
where idx not in (select item from item_attributes) and type = 'Music';"
a2 <- dbGetQuery(maldb, sql_stm)
a2_artist <- a2 |> 
  mutate(artist = sub("(.*)/(.*?)([-+ ] .*)?/.*", "\\2", filename, perl=TRUE, ignore.case=TRUE),
         artist = if_else(artist == "django 1937-38", "Django Reinhardt", artist),
         artist = if_else(artist == "Django Reinhardt 1937-1938 integrale fremeaux fa 305", "Django Reinhardt", artist),
         artist = if_else(artist == "Ray Brown Milt Jackson Oliver Nelson Verve V6-8615", "Ray Brown", artist),
         artist = if_else(artist == "JAZZ OUD/Benny Carter - Selected Favorites, Volume 1 11 All of Me.m4a", "Benny Carter", artist)
         ) |> select(-filename)

dbWriteTable(maldb, "basiebeats_tagged_artists", a2_artist)

sql_stmt <- "update items
set artist = basiebeats_tagged_artists.artist
from basiebeats_tagged_artists
where items.idx = basiebeats_tagged_artists.idx;"
dbExecute(maldb, sql_stmt)

sql_stm <- "insert into item_attributes
select distinct idx as item, 'any_jazz' as name, 'yes' as value
from items where type = 'Music'
and filename ~* 'selectie David' and filename ~* 'vocaal';"
dbExecute(maldb, sql_stm)

sql_stm <- "select * from item_attributes where item = '5265'"
dbGetQuery(maldb, sql_stm)

sql_stm <- "select * from item_attributes where name ~* 'vocaal'"
a1 <- dbGetQuery(maldb, sql_stm)

sql_stm <- "select * from items where idx = 5265;"
a1 <- dbGetQuery(maldb, sql_stm)
