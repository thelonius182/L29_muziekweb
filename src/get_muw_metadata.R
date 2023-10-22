pacman::p_load(httr, xml2, tidyverse, keyring, googlesheets4, yaml, fs, magrittr, hms, readr)

config <- read_yaml("config_nip_nxt.yaml")

source("src/nip_tools.R", encoding = "UTF-8") # funcs_only

# load list of muw_id's to request
muw_ids_jazz_raw <- read_csv("~/woj_muw_album_ids/muw_ids_jazz.txt", col_names = FALSE, show_col_types = F)
names(muw_ids_jazz_raw) <- "album_id"

# Muziekweb cred's voor track-info webservice
muw_ws_cred_home <- config$muw_cred
muw_ws_cred <- read_rds(file = muw_ws_cred_home)

all_album_info <- tibble(track_id = character(), 
                         album = character(), 
                         secs = character(), 
                         deel_in_titel = character(), 
                         performers = character(),
                         muw_catalogue_type = character(),
                         titel = character(),
                         genre = character())

for (cur_album_id in muw_ids_jazz_raw$album_id) {
  
  cat("album-id =", cur_album_id)
  
  muw_result <-
    GET(
      url = paste0(
        "https://api.cdr.nl/ExtendedInfo/v3/albumInformation.xml?albumID=",
        cur_album_id
      ),
      authenticate(muw_ws_cred$muw_usr, muw_ws_cred$muw_pwd)
    )
  
  muw_txt <-
    content(
      x = muw_result,
      as = "text",
      type = "text/xml",
      encoding = "UTF-8"
    )
  
  muw_xml <- as_xml_document(muw_txt)
  
  muw_album <- muw_track_elm("a", muw_col_name = "album")
  # muw_gen_a <- muw_track_elm("g", "Name[@Language='nl']")
  # muw_gen_b <- muw_track_elm("g", "Category/Name[@Language='nl']")
  # muw_gen_c <- muw_track_elm("g", "Category/Style/Name[@Language='nl']")
  
  stopifnot("Bovenstaande CD-code niet gevonden in Muziekweb-catalogus" = nrow(muw_album) > 0)
  muw_tracks_id <-
    muw_track_elm("t", "AlbumTrackID", muw_col_name = "track_id")
  muw_tracks_len <-
    muw_track_elm("t", "PlayTimeInSec", muw_col_name = "secs")
  muw_tracks_tit <-
    muw_track_elm("t", "TrackTitle[@Language='nl']", muw_col_name = "titel_en_deel")
  muw_tracks_tit_b <-
    muw_track_elm("t", "UniformTitle[@Language='nl']", muw_col_name = "titel")
  muw_tracks_tit_add <-
    muw_track_elm("t", "AddonForUniformTitle[@Language='nl']", muw_col_name = "deel_in_titel")
  
  muw_n_performers <- xml_attr(xml_find_all(muw_xml,
                                            "//Result/Album/Tracks/Track/Performers"),
                               "Count") %>% as_tibble()
  names(muw_n_performers) <- "count"
  
  muw_catalogue_type <- 
    muw_track_elm("p", "PrimaryCatalogueCode", muw_col_name = "cat_type")[[1,1]]
  muw_performer_name <-
    muw_track_elm("p", "PresentationName", muw_col_name = "performer")
  muw_performer_role <-
    muw_track_elm("p", "Role[@Language='nl']", muw_col_name = "rol")
  
  # compile performers
  cur_performer_idx <- 0L
  muw_performers <- tibble(performers = character())
  
  for (n_performers_chr in muw_n_performers$count) {
    
    performer_tib_by_track <- tibble(performer_txt = character())
    n_performers <- as.integer(n_performers_chr)
    n1_start <- cur_performer_idx + 1
    n1_stop <- cur_performer_idx + n_performers
    
    for (n1 in n1_start:n1_stop) {
      
      if (muw_catalogue_type == "POPULAR") {
        cur_performer_by_track <-
          tibble(performer_txt = muw_performer_name$performer[[n1]])
      } else {
        cur_performer_by_track <-
          tibble(performer_txt = paste0(muw_performer_name$performer[[n1]],
                                  " (",
                                  muw_performer_role$rol[[n1]],
                                  ")"))
      }
      
      performer_tib_by_track %<>% add_row(cur_performer_by_track)
    }
    
    cur_performer_text <- performer_tib_by_track %>% unlist() %>% str_flatten(collapse = ", ")
    cur_performer_tib <- tibble(performers = cur_performer_text) %>% 
      mutate(performers = str_replace_all(performers, " \\(\\)", ""))
    
    muw_performers %<>% add_row(cur_performer_tib)
    cur_performer_idx <- cur_performer_idx + n_performers
  }
  
  # compile genre
  n_main_cats <- xml_attr(xml_find_all(muw_xml, "//Result/Album/MusicStyles"), "CountMainCategory") %>% as.integer()
  n_cats <- xml_attr(xml_find_all(muw_xml, "//Result/Album/MusicStyles"), "CountCategory") %>% as.integer()
  n_styles <- xml_attr(xml_find_all(muw_xml, "//Result/Album/MusicStyles"), "CountStyle") %>% as.integer()
  muw_genre <- tibble()
  
  for (c1 in 1:n_main_cats) {
    for (c2 in 1:n_cats) {
      for (s1 in 1:n_styles) {
        muw_cat1 <- muw_album_genre("mc", c1, c2, s1, muw_col_name = "main_category")
        muw_cat2 <- muw_album_genre("c", c1, c2, s1, muw_col_name = "category")
        muw_cat3 <- muw_album_genre("s", c1, c2, s1, muw_col_name = "style")
        muw_genre_row <- muw_cat1 %>% bind_cols(muw_cat2) %>% bind_cols(muw_cat3)
        muw_genre <- muw_genre %>% bind_rows(muw_genre_row)
      }
    }
  }
  
  genre_txt <- muw_genre %>% group_by(main_category, category) %>% unlist() %>% str_flatten(collapse = ",")
  elements <- trimws(str_split(genre_txt, ",")[[1]])
  unique_elements <- unique(elements)
  genre_txt_clean <- paste(unique_elements, collapse = ",") %>% as_tibble()
  names(genre_txt_clean) <- "genre"

  # album-info compileren
  album_info <- muw_tracks_id
  album_info %<>% add_column(muw_album)
  album_info %<>% add_column(muw_tracks_len)
  album_info %<>% add_column(muw_tracks_tit)
  album_info %<>% add_column(muw_tracks_tit_b)
  album_info %<>% add_column(muw_tracks_tit_add)
  album_info %<>% add_column(muw_performers)
  album_info %<>% add_column(muw_catalogue_type)
  album_info %<>% add_column(genre_txt_clean)
  
  # zorg dat er altijd een titel is, zonodig door 'titel_en_deel' te kiezen
  album_info.1 <- album_info %>%
    mutate(tmp_titel = if_else(str_length(str_trim(titel, side = "both")) == 0, titel_en_deel, titel)) %>%
    rename(titel_w_nulls = titel, titel = tmp_titel) %>%
    select(-titel_w_nulls, -titel_en_deel)
  
  # verzamelen
  all_album_info %<>% add_row(album_info.1)
}

write_delim(all_album_info, "c:/Users/gergiev/Documents/woj_album_info.tsv", delim = "\t")

# g3 <- g1$g2 %>% unlist() %>% as_tibble() %>% distinct() %>% 
#   mutate(value = str_to_sentence(value)) %>% distinct() %>% arrange(value)

# if (nrow(df_albums_and_tracks.1) > 0) {
#   # uitdunnen: alleen de tracks die in de spreadsheet staan
#   df_albums_and_tracks.2 <- df_albums_and_tracks.1 %>%
#     inner_join(all_album_info, by = c("muw_track_id" = "track_id"))
#   
#   # groepeer op titel
#   df_albums_and_tracks.3 <- df_albums_and_tracks.2 %>%
#     group_by(titel) %>%
#     mutate(werk = row_number(),
#            werk_lengte = sum(as.integer(secs))) %>%
#     ungroup() %>%
#     mutate(album_key = if_else(werk == 1, muw_track_id, NA_character_)) %>%
#     fill(album_key, .direction = "down")
#   
#   # bewaar koppeling album_key/tracks, voor de bestelling later (in muziekweb_audio.R)
#   # NB - df_albums_and_tracks_all bewaart album_keys van ALLE selecties tot nu toe, omdat een playlist
#   #      soms in delen ontstaat. Alleen het laatste deel bewaren is dan te weinig.
#   df_albums_and_tracks_file <-
#     "C:/Users/gergiev/cz_rds_store/df_albums_and_tracks_all.RDS"
#   # bestaande set inlezen
#   df_albums_and_tracks_all <- read_rds(df_albums_and_tracks_file)
#   # aanvulling voorbereiden
#   df_albums_and_tracks.3.1 <- df_albums_and_tracks.3 %>%
#     select(album_key, muw_album_id, muw_track)
#   # dubbele album_keys voorkomen (bv als zelfde df_3.1 per ongeluk nog een keer toegevoegd wordt)
#   df_albums_and_tracks_all %<>% add_row(df_albums_and_tracks.3.1) %>% distinct()
#   # nu pas opslaan
#   saveRDS(object = df_albums_and_tracks_all, file = df_albums_and_tracks_file)
#   
#   # 1 track per werk (voor gids, RL, draaiboek)
#   df_albums_and_tracks.3.2 <-
#     df_albums_and_tracks.3 %>% filter(werk == 1)
#   
#   # prep het blok voor GD-sheet "nipper-select"
#   df_albums_and_tracks.4 <- df_albums_and_tracks.3.2 %>%
#     mutate(
#       # catalogue_type = muw_catalogue_type,
#       componist = if_else(muw_catalogue_type == "POPULAR", 
#                           # POPULAR: het album
#                           # sub(",.*", "", paste0(performers, ","), perl=TRUE), 
#                           paste0("van het album ", album), 
#                           # CLASSICAL: de componist
#                           sub("^([^(]+) \\(componist\\), (.*)$",
#                               "\\1",
#                               performers,
#                               perl = TRUE,
#                               ignore.case = TRUE)
#       ),
#       performers = sub(
#         "^([^(]+) \\(componist\\), (.*)$",
#         "\\2",
#         performers,
#         perl = TRUE,
#         ignore.case = TRUE
#       ),
#       tot_time = NA_real_,
#       detect = NA,
#       keuze = T,
#       lengte = as.character(hms(seconds = werk_lengte)),
#       vt_blok_id = "Z99",
#       opnameNr = muw_track_id
#     ) %>%
#     arrange(playlist, componist, titel) %>%
#     # group_by(playlist, componist, titel) %>%
#     # mutate(vt_blok = row_number()) %>%
#     # ungroup() %>%
#     # group_by(vt_blok) %>%
#     # mutate(vt_blok_index = if_else(vt_blok == 1, row_number(), NA_integer_)) %>%
#     # ungroup() %>%
#     # select(componist, titel, tot_time, detect, keuze, lengte, playlist, vt_blok_index, vt_blok, performers, album, opnameNr) %>%
#     # arrange(componist, vt_blok_index, vt_blok) %>%
#     # fill(vt_blok_index) %>%
#     # mutate(vt_blok_id = paste0(LETTERS[vt_blok_index], str_pad(vt_blok, width = 2, side = "left", pad = "0"))) %>%
#     select(
#       componist,
#       titel,
#       tot_time,
#       detect,
#       keuze,
#       lengte,
#       playlist,
#       vt_blok_id,
#       performers,
#       album,
#       opnameNr
#     )
# }
