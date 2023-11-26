pacman::p_load(DBI, dplyr, fs, stringr, lubridate, yaml, futile.logger, tibble, readr)

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

# get unmatched CD-id's only (drop track numbers)
muw_ids <- ml_items_db |> mutate(muw_id = sub(".*?([A-Z0-9]+)-\\d+.*", "\\1", title, perl=TRUE)) |> 
  select(muw_id) |> distinct() |> arrange(muw_id)
rm(ml_items_db)

dbDisconnect(maldb)
flog.info("mAirList-DB is disconnected", name = "bsblog")
flog.info("= = = = = STOP BasieBeats = = = = =", name = "bsblog")
