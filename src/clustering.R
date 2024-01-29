# Install and load the proxy package
library(proxy)

# Create a sample boolean dataset
set.seed(123)
data <- matrix(sample(c(TRUE, FALSE), 400, replace = TRUE), ncol = 10)

# Convert the matrix to a data frame
df <- as.data.frame(data)

# Compute Jaccard dissimilarity matrix
jaccard_dist <- 1 - proxy::simil(df, method = "Jaccard")

# Perform K-means clustering with base kmeans
k <- 7  # specify the number of clusters
kmeans_model <- kmeans(jaccard_dist, centers = k)

# Get cluster assignments
cluster_assignments <- kmeans_model$cluster

# Print the cluster assignments
print(cluster_assignments)

wss <- numeric(15)  # To store within-cluster sum of squares

for (i in 1:15) {
  kmeans_model <- kmeans(jaccard_dist, centers = i)
  wss[i] <- kmeans_model$tot.withinss
}

# Plot the elbow graph
plot(1:15, wss, type = "b", xlab = "Number of Clusters (k)", ylab = "Within-cluster Sum of Squares")

library(fpc)

silhouette_scores <- numeric(15)

for (i in 2:15) {
  kmeans_model <- kmeans(jaccard_dist, centers = i)
  cluster_assignments <- as.integer(kmeans_model$cluster)
  silhouette_scores[i] <- cluster.stats(as.dist(jaccard_dist), cluster_assignments)$avg.silwidth
}

# Plot silhouette scores
plot(1:15, silhouette_scores[1:15], type = "b", xlab = "Number of Clusters (k)", ylab = "Average Silhouette Width")

# Assume silhouette_scores is your vector of silhouette scores
optimal_k <- which.max(silhouette_scores)
cat("Optimal number of clusters (k):", optimal_k, "\n")

# Plot with a vertical line indicating the optimal k
plot(1:15, silhouette_scores[1:15], type = "b", xlab = "Number of Clusters (k)", ylab = "Average Silhouette Width")
abline(v = optimal_k, col = "red", lty = 2)

# ------------------------------------
pacman::p_load(fpc)

# Assuming your_data is your tibble
# Extract the attribute columns for clustering
attributes_data <- ml_albums_f[, -1]

# Convert the data to a matrix
data_matrix <- as.matrix(attributes_data)

# Compute the Jaccard distance matrix
jaccard_dist <- proxy::dist(data_matrix, method = "Jaccard")

# Perform K-means clustering with a range of k values
k_values <- 8:20
silhouette_scores <- numeric(length(k_values))

for (i in seq_along(k_values)) {
  kmeans_model <- kmeans(jaccard_dist, centers = k_values[i])
  cluster_assignments <- as.integer(kmeans_model$cluster)
  silhouette_scores[i] <- cluster.stats(jaccard_dist, cluster_assignments)$avg.silwidth
}

# Find the optimal k
optimal_k <- k_values[which.max(silhouette_scores)]
cat("Optimal number of clusters (k):", optimal_k, "\n")

# Plot silhouette scores
plot(k_values, silhouette_scores, type = "b", xlab = "Number of Clusters (k)", ylab = "Average Silhouette Width")
abline(v = optimal_k, col = "red", lty = 2)

# ------------------------------
kmeans_model <- kmeans(jaccard_dist, centers = 19)
cluster_assignments <- as.integer(kmeans_model$cluster)
cluster_assignments_tib <- cluster_assignments |> as_tibble()

ml_row_sums <- rowSums(ml_albums_f[, -1])
ml_row_sums_tib <- ml_row_sums |> as_tibble()
ml_albums_g <- ml_albums_f |> bind_cols(ml_row_sums_tib)
ml_albums_h <- ml_albums_g |> bind_cols(cluster_assignments_tib) |> rename(cluster_id = `value...47`)
ml_albums_i <- ml_albums_h |> group_by(cluster_id) |> summarise(across(everything(), sum))
ml_albums_j <- ml_albums_i |> pivot_longer(!cluster_id, names_to = "attr", values_to = "frequency") 
ml_albums_k <- ml_albums_j |> filter(!attr %in% c("idx", "value...46") & frequency > 0) |> arrange(cluster_id, desc(frequency))

woj_dir <- "c:/Users/nipper/Documents/BasieBeats"
tsv_ymd <- now(tzone = "Europe/Amsterdam") |> as_date() |> as.character()
qfn <- paste0("clusters_", tsv_ymd, ".tsv")
write_delim(ml_albums_k, path_join(c(woj_dir, qfn)), delim = "\t")

ml_clusters <- ml_albums_h |> select(idx, cluster_id) |> arrange(idx)
ml_albums_m <- ml_albums_a |> arrange(idx) |> left_join(ml_clusters) |> fill(cluster_id, .direction = "down") |> 
  group_by(cluster_id) |> mutate(n_tracks = n(), tot_hours = round(sum(duration) / 3600, 2)) |> ungroup() |> 
  select(cluster_id, artist, album, title, everything()) |> arrange(cluster_id, artist, album)

woj_dir <- "c:/Users/nipper/Documents/BasieBeats"
tsv_ymd <- now(tzone = "Europe/Amsterdam") |> as_date() |> as.character()
qfn <- paste0("clustered_albums_", tsv_ymd, ".tsv")
write_delim(ml_albums_m, path_join(c(woj_dir, qfn)), delim = "\t")

ml_albums_n <- ml_albums_m |> select(cluster_id, n_tracks, tot_hours) |> distinct()

woj_dir <- "c:/Users/nipper/Documents/BasieBeats"
tsv_ymd <- now(tzone = "Europe/Amsterdam") |> as_date() |> as.character()
qfn <- paste0("cluster_stats_", tsv_ymd, ".tsv")
write_delim(ml_albums_n, path_join(c(woj_dir, qfn)), delim = "\t")

ml_row_sum_counts <- as.data.frame(table(ml_row_sums))
colnames(ml_row_sum_counts) <- c("Row_Sum", "Frequency")
