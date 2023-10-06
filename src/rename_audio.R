pacman::p_load(readr, stringr, dplyr, fs)

# Input folder containing WAV files
shared_input_folder <- "/mnt/muw/orgue_mystique/"
local_input_folder <- "~/Documents/muw_trf/"
mapping_file <- "/mnt/hgfs/muw_w11/muw_id2title/muw_mapping.tsv"

# Output folder for AIFF files
shared_output_folder <- "/mnt/muw/orgue_mystique/aiff/"
local_output_folder <- "~/Documents/muw_trf/aiff/"

# load mapping of Muw-ID's to track titles
mapping <- read_delim(mapping_file, delim = "\t", col_names = F, lazy = F, show_col_types = F)

# List all WAV files in the input folder
wav_files <- dir_ls(shared_input_folder, regexp = "\\.wav$", recurse = F)

# Iterate through the WAV files and transcode to AIFF
for (wav_file in wav_files) {
  
  local_wav_file = str_replace(wav_file, shared_input_folder, local_input_folder)
  file_copy(path = wav_file, new_path = local_wav_file, overwrite = T)
  
  # Get the Muziekweb-ID + track title
  muw_id <- local_wav_file |> path_ext_remove() |> path_file()
  track_title <- mapping |> filter(X1 == muw_id) |> select(X2)
  
  # Construct the output AIFF file path
  local_output_aiff_file <- path(local_output_folder, track_title, ext = "aiff")
  
  # Construct the ffmpeg command 
  ffmpegCommand <- c(
    "ffmpeg",
    "-y", # Automatically overwrite output file if it exists
    "-i", # Input file flag
    shQuote(path.expand(local_wav_file)),  # Input file path (quoted for spaces in path)
    shQuote(path.expand(local_output_aiff_file))  # Output file path (quoted for spaces in path)
  )
  
  # transform wav to aiff
  tryCatch(
    expr = suppressMessages(system(paste(ffmpegCommand, collapse = " "), intern = TRUE)),
    error = function(e1) {
      message(e1)
    }
  )
  
  # Check if the transcoding was successful
  if (file_exists(local_output_aiff_file)) {
    cat("Transcoding successful. AIFF file created:", path_file(local_output_aiff_file), "\n")
  } else {
    cat("Transcoding failed for file:", path_file(wav_file), "\n")
    next
  }
  
  # copy it back to the shared folder
  file_copy(local_output_aiff_file, shared_output_folder, overwrite = T)
}
