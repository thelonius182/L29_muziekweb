pacman::p_load(dplyr, tidyr)

artists <- sample(14) |> as_tibble()
names(artists) <- "R"

n_albums_by_artist <- tibble(n_L = integer())

for (i1 in 1:14) {
  t1 <- tibble(n_L = sample(1:7, 1))
  n_albums_by_artist <- add_row(n_albums_by_artist, t1)
}

mal_info <- artists |> bind_cols(n_albums_by_artist)

n_tracks_by_album <- tibble(n_T = character())

for (l1 in seq_along(mal_info$n_L)) {
  t1 <- tibble(n_T = paste(sample(1:22, mal_info$n_L[l1]), collapse = ","))
  n_tracks_by_album <- add_row(n_tracks_by_album, t1)
}

mal_info <- mal_info |> bind_cols(n_tracks_by_album)
mal_info.1 <- mal_info |> separate_wider_delim(n_T, delim = ",", names = paste0("n_T_L", 1:7), too_few = "align_start")

album_order_by_artist <- tibble(Lx = character())

for (l1 in seq_along(mal_info$n_L)) {
  t1 <- tibble(Lx = paste(sample(mal_info$n_L[l1]), collapse = ","))
  album_order_by_artist <- add_row(album_order_by_artist, t1)
}

mal_info.1 <- mal_info.1 |> bind_cols(album_order_by_artist)
mal_info.2 <- mal_info.1 |> separate_wider_delim(Lx, delim = ",", names = paste0("L", 1:7), too_few = "align_start") |> 
  select(R, n_L, L1:L7, n_T_L1:n_T_L7)

bb_R <- function() {
  idx_R <<- idx_R + 1
  if (idx_R > n_R) {
    idx_R <<- 1
  }
  mal_info.2$R[idx_R]
}

bb_L <- function() {
  n_L <- mal_info.2$n_L[idx_R]
  idx_L <<- idx_RLT[cur_R, ]$L + 1
  if (idx_L > n_L) {
    idx_L <<- 1
  }
  L_col <- paste0("^L", idx_L)
  cur_L <- mal_info.2|> filter(R == cur_R) |> select(matches(L_col)) |> unlist()
  cur_L[1]
}

bb_T <- function() {
  mal_info.2$n_T_L1[1]
}

idx_R <- 0
idx_L <- 0
idx_T <- 0
n_R <- nrow(mal_info.2)
idx_RLT <- tibble(R = 1:n_R, L = 0L, T = 0L)

for (r1 in 1:(3*14)) {
  cur_R <- bb_R()
  cur_L <- bb_L()
  idx_RLT[cur_R, ]$L <- idx_L
  cur_T <- bb_T()
  cat(cur_R, "\t", cur_L, "\t", cur_T, "\n")
}
