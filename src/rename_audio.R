pacman::p_load(readr, stringr, dplyr, fs)

# Input folder containing WAV files
input_folder <- "/mnt/muw/orgue_mystique"

# Output folder for AIFF files
output_folder <- "/mnt/muw/orgue_mystique/aiff"

# Set the desired sample rate and sample size
sample_rate <- 44100
sample_size <- 16

# List all WAV files in the input folder
wav_files <- dir_ls(input_folder, regexp = "\\.wav$")

# Iterate through the WAV files and transcode to AIFF
for (wav_file in wav_files) {
  browser()
  # Get the file name without extension using fs::path_file
  file_name <- path_ext_remove(wav_file)
  
  # Construct the output AIFF file path
  output_aiff_file <- path(output_folder, paste0(file_name, ".aiff"))
  
  # Run ffmpeg command to transcode with specified parameters
  command <- sprintf("ffmpeg -i %s -ar %d -sample_fmt s%d -acodec pcm_s%dbe %s",
                     shQuote(wav_file), sample_rate, sample_size, sample_size, shQuote(output_aiff_file))
  tryCatch(
    expr = system2(command, stdout = TRUE, stderr = TRUE),
    error = function(e1) {
      message(e1)
    }
  )
  
  
  # Check if the transcoding was successful
  if (file.exists(output_aiff_file)) {
    cat("Transcoding successful. AIFF file created:", output_aiff_file, "\n")
  } else {
    cat("Transcoding failed for file:", wav_file, "\n")
  }
}
