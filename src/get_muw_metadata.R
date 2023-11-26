pacman::p_load(httr, xml2, keyring, readr, magrittr, dplyr, tidyr)

flog.info("getting Muziekweb data", name = "bsblog")

# load list of muw_id's to request
# muw_ids_woj_raw <- read_csv("C:/Users/nipper/Documents/BasieBeats/muw_ids_woj_20231022.csv", show_col_types = F)
# names(muw_ids_jazz_raw) <- "album_id"

# Muziekweb cred's voor track-info webservice
muw_ws_cred_home <- basie_config$muw_cred
muw_ws_cred <- read_rds(file = muw_ws_cred_home)

all_album_info <- tibble(muw_catalogue_type = character(),
                         album = character(), 
                         track_id = character(), 
                         secs = character(), 
                         titel = character(),
                         deel_in_titel = character(), 
                         performers = character(),
                         genre = character(),
                         released = character(),
                         spotify_id = character(),
                         instruments = character())
muw_genre_all <- tibble()

for (cur_album_id in muw_ids$muw_id) {
  
  cat("album-id =", cur_album_id, "\n")
  
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
  
  if (nrow(muw_album) == 0) {
    cd_not_found <- sprintf("CD-code %s niet gevonden in Muziekweb-catalogus", cur_album_id)
    flog.info(cd_not_found, name = "bsblog")
    next
  }
  
  muw_released <- muw_track_elm("r", muw_col_name = "released")
  
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
  
  muw_spotify_id <- muw_external_link()
  
  muw_n_performers <- xml_attr(xml_find_all(muw_xml,
                                            "//Result/Album/Tracks/Track/Performers"),
                               "Count") %>% as_tibble()
  names(muw_n_performers) <- "count"
  
  muw_catalogue_type <- 
    muw_track_elm("p", "PrimaryCatalogueCode", muw_col_name = "cat_type")[[1,1]]
  
  muw_performer_name <-
    muw_track_elm("p", "PresentationName", muw_col_name = "performer")
  
  muw_performer_years <-
    muw_track_elm("p", "Years", muw_col_name = "performer_years")
  
  muw_instruments <-
    muw_track_elm("p", "Annotation", muw_col_name = "instruments")
  
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
  
  muw_genre <- muw_genre |> mutate(across(c("main_category", "category", "style"), ~ str_to_lower(.x)))
  genre_txt <- muw_genre %>% group_by(main_category, category) %>% unlist() %>% str_flatten(collapse = ",")
  elements <- trimws(str_split(genre_txt, ",")[[1]])
  unique_elements <- unique(elements)
  genre_txt_clean <- paste(unique_elements, collapse = ",") %>% as_tibble()
  names(genre_txt_clean) <- "genre"
  
  instruments <- muw_instruments |> filter(str_length(str_trim(instruments, side = "both")) > 0) |> distinct() |> 
    unlist() |> str_flatten(collapse = ",")
  
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
  album_info %<>% add_column(muw_released)
  album_info %<>% add_column(muw_spotify_id)
  album_info %<>% add_column(instruments)
  
  # zorg dat er altijd een titel is, zonodig door 'titel_en_deel' te kiezen
  album_info.1 <- album_info |> 
    mutate(tmp_titel = if_else(str_length(str_trim(titel, side = "both")) == 0, titel_en_deel, titel)) |> 
    rename(titel_w_nulls = titel, titel = tmp_titel) |> 
    select(-titel_w_nulls, -titel_en_deel) |> 
    select(track_id, album, titel, deel_in_titel, everything())
  
  # verzamelen
  all_album_info %<>% add_row(album_info.1)
  muw_genre <- muw_genre |> add_column(muw_id = cur_album_id) 
  muw_genre_all <- muw_genre_all |> bind_rows(muw_genre)
}

all_album_info <- all_album_info |> mutate(genre = str_to_lower(genre))

muw_genre_by_album <- muw_genre_all |> pivot_longer(cols = c(main_category, category, style), 
                                                   names_to = "genre_lbl", 
                                                   values_to = "genre") |> 
  select(-genre_lbl) |> arrange(muw_id, genre) |> distinct()

# Jazz-cd's
muw_ids_jazz <- muw_genre_by_album |> mutate(woj_cat = "jazz", is_jazz = str_detect(genre, "jazz")) |> 
  filter(is_jazz) |> select(muw_id, woj_cat) |> arrange(muw_id) |> distinct()

# world-cd's
suppressMessages(muw_ids_world <- muw_genre_by_album |> select(muw_id) |> arrange(muw_id) |> distinct() |> 
                   anti_join(muw_ids_jazz) |> mutate(woj_cat = "world"))

muw_genre_triplets_jazz <- muw_genre_all |> inner_join(muw_ids_jazz) |> 
  select(woj_cat, muw_cat = category, style) |> arrange(woj_cat, muw_cat, style) |> distinct()

muw_genre_triplets_world <- muw_genre_all |> inner_join(muw_ids_world) |> 
  select(woj_cat, muw_cat = category, style) |> arrange(woj_cat, muw_cat, style) |> distinct()

# unique_genres <- muw_genre_by_album |> select(genre) |> mutate(genre = str_to_lower(genre)) |> 
#   arrange(genre) |> distinct()

genre_stats_jazz <- tibble()

for (cur_style in muw_genre_triplets_jazz$style) {
  
  aai <- all_album_info |> 
    filter(str_extract(all_album_info$track_id, "[A-Z0-9]+") %in% muw_ids_jazz$muw_id & 
             str_detect(genre, paste0("\\b", cur_style, "\\b"))) |> 
    summarise(n_tracks = n(), tot_hrs = round(sum(as.integer(secs)) / 3600, 2))
  aai_tib <- tibble(genre = cur_style, n_tracks = aai$n_tracks, tot_hrs = aai$tot_hrs)
  genre_stats_jazz <- genre_stats_jazz |> bind_rows(aai_tib)
}

genre_stats_world <- tibble()

for (cur_style in muw_genre_triplets_world$style) {
  
  aai <- all_album_info |> 
    filter(str_extract(all_album_info$track_id, "[A-Z0-9]+") %in% muw_ids_world$muw_id & 
             str_detect(genre, paste0("\\b", cur_style, "\\b"))) |> 
    summarise(n_tracks = n(), tot_hrs = round(sum(as.integer(secs)) / 3600, 2))
  aai_tib <- tibble(genre = cur_style, n_tracks = aai$n_tracks, tot_hrs = aai$tot_hrs)
  genre_stats_world <- genre_stats_world |> bind_rows(aai_tib)
}

write_delim(all_album_info, "c:/Users/nipper/Documents/BasieBeats/woj_album_info_2023-11-26.tsv", delim = "\t")
write_delim(muw_genre_all, "c:/Users/nipper/Documents/BasieBeats/muw_genre_all_2023-11-26.tsv", delim = "\t")
write_delim(muw_genre_by_album, "c:/Users/nipper/Documents/BasieBeats/muw_genre_by_album_2023-11-26.tsv", delim = "\t")
write_delim(genre_stats_jazz, "c:/Users/nipper/Documents/BasieBeats/genre_stats_jazz_2023-11-26.tsv", delim = "\t")
write_delim(genre_stats_world, "c:/Users/nipper/Documents/BasieBeats/genre_stats_world_2023-11-26.tsv", delim = "\t")
