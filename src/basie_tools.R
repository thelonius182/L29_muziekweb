muw_track_elm <- function(muw_req, muw_col_name, muw_xpath_var = NULL) {
  
  muw_xpath_base <- "//Result/Album/"
  muw_xpath_pfx <- case_when(muw_req == "t" ~ "Tracks/Track/",
                             muw_req == "p" ~ "Tracks/Track/Performers/Performer/",
                             muw_req == "r" ~ "ReleaseDate",
                             muw_req == "a" ~ "AlbumTitle[@Language='nl']",
                             muw_req == "g" ~ "MusicStyles/MainCategory[1]/",
                             muw_req == "pr" ~ "PICA3/Recording4252"
  )
  
  muw_xpath <- paste0(muw_xpath_base, muw_xpath_pfx, muw_xpath_var)
  result <- xml_text(xml_find_all(muw_xml, xpath = muw_xpath)) %>% as_tibble()
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

muw_external_link <- function() {
  
  spotify_links <- xml_find_all(muw_xml, ".//ExternalLink[@Provider='SPOTIFY']")
  
  if (length(spotify_links) == 0) {
    return(tibble(spotify_id = NA))
  }
  
  spotify_links_tib <- xml_text(spotify_links) %>% as_tibble() %>% mutate(rrn = row_number()) %>% 
    select(rrn, spotify_id = value)
  
  # Extract the TrackNumber for each Spotify link
  track_numbers <- sapply(spotify_links, function(link) {
    closest_track <- xml_find_first(link, ".//ancestor::Track")
    xml_attr(closest_track, "TrackNumber")
  }) %>% as_tibble() %>% mutate(rrn = row_number(), spotify_key = as.integer(value)) %>% select(-value)
  
  suppressMessages(spotify_links_by_key <- spotify_links_tib %>% left_join(track_numbers) %>% select(-rrn))
  
  suppressMessages(result <- muw_tracks_id %>% mutate(spotify_key = row_number()) %>% 
                     left_join(spotify_links_by_key) %>% select(spotify_id))
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
  
  suppressMessages(muziekweb.3 <- muziekweb.2 %>%
                     inner_join(track_items, by = c("rrn" = "track_rrn")) %>%
                     rename(muw_album_id = `muziekweb-id`) %>%
                     select(starts_with("muw"), playlist) %>%
                     mutate(muw_track_id = paste0(muw_album_id,
                                                  "-",
                                                  str_pad(string = as.character(muw_track),
                                                          width = 4,
                                                          side = "left",
                                                          pad = "0"))))
  
  return(muziekweb.3)
}

# get_mal_conn <- function() {
# 
#   result <- tryCatch( 
#     dbConnect(odbc::odbc(), "mAirlistDB", timeout = 10),
#     error = function(cond) {
#       flog.error("mAirList database unavailable", name = "bsblog")
#       return("connection-error")
#     }
#   )
#   
#   return(result)
# }
# 
get_mal_conn <- function() {
  
  # just to prevent uploading credentials to github
  basie_creds <- read_rds("C:/Users/nipper/Documents/BasieBeats/basie.creds")

  result <- tryCatch( 
    dbConnect(RPostgres::Postgres(),
              dbname = 'mairlist7', 
              host = '192.168.178.91', 
              port = 5432,
              user = basie_creds$usr,
              password = basie_creds$pwd),
    error = function(cond) {
      flog.error("mAirList database unavailable", name = "bsblog")
      return("connection-error")
    }
  )
  
  return(result)
}

clean_artists <- function(arg_artists) {
  # Clean artist-info: only keep the first name
  result <- arg_artists |> 
    separate_longer_delim(performers, delim = ",") |> 
    separate_longer_delim(performers, delim = ";") |> 
    separate_longer_delim(performers, delim = "_") |> 
    separate_longer_delim(performers, delim = ",") |> 
    separate_longer_delim(performers, delim = "&") |> 
    separate_longer_delim(performers, delim = "+") |> 
    separate_longer_delim(performers, delim = "/") |> 
    separate_longer_delim(performers, delim = " and ") |> 
    separate_longer_delim(performers, delim = " AND ") |> 
    separate_longer_delim(performers, delim = " And ") |> 
    separate_longer_delim(performers, delim = "-") |> 
    separate_longer_delim(performers, delim = " w ") |> 
    separate_longer_delim(performers, delim = " W ") |> 
    separate_longer_delim(performers, delim = " With ") |> 
    separate_longer_delim(performers, delim = " with ") |> 
    separate_longer_delim(performers, delim = " WITH ") |> 
    separate_longer_delim(performers, delim = " Feat. ") |> 
    separate_longer_delim(performers, delim = " feat. ") |> 
    separate_longer_delim(performers, delim = " featuring ") |> 
    filter(!is.na(performers) & str_length(str_trim(performers, "both")) > 0) |> 
    mutate(performers = str_trim(performers, "both")) |> group_by(muw_album_id) |> 
    mutate(rrn = row_number()) |> ungroup() |> 
    filter(rrn == 1) |> select(muw_album_id, artist = performers) |> distinct()
}
