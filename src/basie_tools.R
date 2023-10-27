muw_track_elm <- function(muw_req, muw_col_name, muw_xpath_var = NULL) {
  
  muw_xpath_base <- "//Result/Album/"
  muw_xpath_pfx <- case_when(muw_req == "t" ~ "Tracks/Track/",
                             muw_req == "p" ~ "Tracks/Track/Performers/Performer/",
                             muw_req == "a" ~ "AlbumTitle[@Language='nl']",
                             muw_req == "g" ~ "MusicStyles/MainCategory[1]/"
  )
  result <- xml_text(xml_find_all(muw_xml, 
                                  xpath = paste0(muw_xpath_base,
                                                 muw_xpath_pfx,
                                                 muw_xpath_var))
  ) %>% as_tibble()
  names(result) <- muw_col_name
  return(result)
}

muw_album_genre <- function(muw_req, muw_col_name, c1, c2, s1) {
  
  muw_xpath_base <- "//Result/Album/"
  muw_xpath_pfx <- case_when(muw_req == "mc" ~ sprintf("MusicStyles/MainCategory[%s]/Name[@Language='nl']", c1),
                             muw_req == "c" ~ sprintf("MusicStyles/MainCategory[%s]/Category[%s]/Name[@Language='nl']", c1, c2),
                             muw_req == "s" ~ sprintf("MusicStyles/MainCategory[%s]/Category[%s]/Style[%s]/Name[@Language='nl']", c1, c2, s1)
  )
  result <- xml_text(xml_find_all(muw_xml, xpath = paste0(muw_xpath_base, muw_xpath_pfx))) %>% as_tibble()
  names(result) <- muw_col_name
  return(result)
}

gd_open_playlists <- function() {
  playlists_raw <-
    read_sheet(ss = config$url_nip_nxt, sheet = "playlists") %>% as_tibble()
  
  # pick the open playlists
  playlists.1 <-
    playlists_raw %>% filter(!is.na(playlist_id) &
                               afgeleverd_op == "NULL")
  return(playlists.1)
}

gd_albums_and_tracks <- function(open_playlists) {
  
  # TEST
  # open_playlists <- df_open_playlists.1
  # TEST
  
  # collect album-keys in nipperNxt-spreadsheet
  nipsel_raw <- read_sheet(ss = config$url_nip_nxt, sheet = "nipper-select")
  
  # limit to selected album-id's and tracks of open playlists
  nipsel.1 <- nipsel_raw %>% 
    filter(keuze & playlist %in% open_playlists$playlist) # playlists.1$playlist)

  return(nipsel.1)
}

gd_albums_and_tracks_muw <- function(open_playlists) {
  
  # TEST
  # open_playlists <- df_open_playlists.1
  # TEST
  
  # collect album-keys in nipperNxt-spreadsheet
  muziekweb_raw <-
    read_sheet(ss = config$url_nip_nxt, sheet = "muziekweb")
  
  # beperk tot gekozen album-id's en tracks van open playlists
  muziekweb.1 <- muziekweb_raw %>%
    filter(keuze &
             playlist %in% open_playlists$playlist) # playlists.1$playlist)
  
  # separate the track-id's
  muziekweb.2 <- muziekweb.1 %>%
    mutate(track_list = strsplit(tracks, "(?i),", perl = TRUE),
           rrn = row_number())
  # cur_muziekweb <- muziekweb.2 %>% filter(playlist == cur_playlist$playlist)
  
  track_items <- tibble(track_rrn = 0, muw_track = 0)
  
  for (cur_rrn in muziekweb.2$rrn) {
    #TEST
    # cur_rrn <- 2
    #TEST
    cur_row <- muziekweb.2 %>% filter(rrn == cur_rrn)
    
    for (track_item in cur_row$track_list[[1]]) {
      # TEST
      # track_item <- "5-11"
      # TEST
      
      if (str_detect(track_item, "-")) {
        track_seq <- strsplit(track_item, "(?i)-", perl = TRUE)
        track_start <- track_seq[[1]][1]
        track_stop <- track_seq[[1]][2]
        
        for (cur_track_id in track_start:track_stop) {
          cur_track_item <-
            tibble(track_rrn = cur_rrn, muw_track = cur_track_id)
          track_items <- add_row(track_items, cur_track_item)
        }
        
      } else {
        cur_track_item <-
          tibble(track_rrn = cur_rrn,
                 muw_track = as.integer(track_item))
        track_items <- add_row(track_items, cur_track_item)
      }
    }
  }
  
  muziekweb.3 <- muziekweb.2 %>%
    inner_join(track_items, by = c("rrn" = "track_rrn")) %>%
    rename(muw_album_id = `muziekweb-id`) %>%
    select(starts_with("muw"), playlist) %>%
    mutate(muw_track_id = paste0(
      muw_album_id,
      "-",
      str_pad(
        string = as.character(muw_track),
        width = 4,
        side = "left",
        pad = "0"
      )
    ))
  
  return(muziekweb.3)
}

get_mal_conn <- function() {

  result <- tryCatch( 
    dbConnect(odbc::odbc(), "mAirlistDB", timeout = 10),
    error = function(cond) {
      flog.error("mAirList database unavailable", name = "bsblog")
      return("connection-error")
    }
  )
  
  return(result)
}
