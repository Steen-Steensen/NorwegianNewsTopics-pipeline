# ==============================================================================
# Script: 06_add_topic_values.R
# Purpose:
#   Load LDA topic distributions and merge them into the alltexts dataframe,
#   identify and export the top 50 articles per topic for manual validation,
#   and export the top 10 defining words per topic for topic labelling.
#
# Inputs:
#   - alltexts.rds: Full text corpus from 05_load_LDA_results.R (no Topic columns yet)
#   - topic_distribution.rds: LDA topic distribution matrix from 05_load_LDA_results.R
#   - topic-keys-28.txt: MALLET output with top words per topic (tab-separated:
#     topic_id, alpha_weight, space-separated words)
#
# Outputs:
#   - alltexts_with_TP_distribution.rds: alltexts with Topic1-Topic28 columns merged
#   - Top_stories_in_topics.xlsx: Top 50 articles per topic for manual inspection/validation
#     (text columns truncated to first 200 words for file size management)
#   - topic_top_words.xlsx: Top 10 defining words per topic for manual topic labelling
#     (one row per topic, columns: Topic, Alpha, Word1-Word10, Top_Words)
#
# Dependencies: dplyr, openxlsx, writexl
# ==============================================================================

library(dplyr)
library(openxlsx)
library(writexl)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

# ==============================================================================
# 1. LOAD DATA
# ==============================================================================

# Load alltexts corpus (no topics yet)
alltexts <- readRDS("alltexts.rds")

# Load topic distribution matrix (ID + Topic1-Topic28 columns)
topic_distribution <- readRDS("topic_distribution.rds")

# ==============================================================================
# 2. MERGE TOPIC DISTRIBUTION INTO ALLTEXTS
# ==============================================================================

# Left join to add Topic1-Topic28 columns while preserving all original rows
alltexts_TP <- alltexts %>%
  left_join(topic_distribution, by = "ID")

# Verify merge succeeded
cat("Rows in alltexts:", nrow(alltexts), "\n")
cat("Rows in alltexts_TP:", nrow(alltexts_TP), "\n")
cat("Topic columns added:", sum(grepl("^Topic[0-9]", names(alltexts_TP))), "\n")

# ==============================================================================
# 3. SAVE ALLTEXTS WITH TOPIC DISTRIBUTION
# ==============================================================================

saveRDS(alltexts_TP, "alltexts_with_TP_distribution.rds")
cat("Saved alltexts_TP to alltexts_with_TP_distribution.rds\n")

# ==============================================================================
# 4. IDENTIFY TOP 50 ARTICLES PER TOPIC
# ==============================================================================

# Initialize list to store top stories by topic
top_stories_list <- list()

# Loop through Topic1 to Topic28
for (i in 1:28) {
  topic_col <- paste0("Topic", i)

  # Sort by topic column descending, take top 50 articles
  top_50 <- alltexts_TP %>%
    arrange(desc(!!sym(topic_col))) %>%
    slice(1:50) %>%
    mutate(topic = i) %>%  # Add topic label column
    dplyr::select(ID, newsroom, topic, headline, lead, body_text, cleaned_text, url)

  # Append to list
  top_stories_list[[topic_col]] <- top_50
}

# Combine all topics into single dataframe
Top_stories_in_topics <- bind_rows(top_stories_list)

cat("Total top stories compiled:", nrow(Top_stories_in_topics), "\n")
cat("Topics represented:", n_distinct(Top_stories_in_topics$topic), "\n")

# ==============================================================================
# 5. PREPARE DATA FOR EXCEL EXPORT
# ==============================================================================

# Truncate body_text and cleaned_text to first 200 words for file size management
truncate_text <- function(text, n_words = 200) {
  if (is.na(text)) return(NA_character_)

  # Split text into words
  words <- strsplit(text, "\\s+")[[1]]

  # Take first n_words or all if fewer
  truncated <- paste(words[1:min(n_words, length(words))], collapse = " ")

  # Add ellipsis if text was truncated
  if (length(words) > n_words) {
    truncated <- paste0(truncated, "...")
  }

  return(truncated)
}

# Apply truncation to export version
Top_stories_export <- Top_stories_in_topics %>%
  mutate(
    body_text = sapply(body_text, truncate_text),
    cleaned_text = sapply(cleaned_text, truncate_text)
  )

# ==============================================================================
# 6. EXPORT TO EXCEL
# ==============================================================================

# Export for manual topic validation
write_xlsx(Top_stories_export, "Top_stories_in_topics.xlsx")
cat("Exported Top_stories_in_topics.xlsx\n")

# ==============================================================================
# 7. EXTRACT AND EXPORT TOP WORDS PER TOPIC
# ==============================================================================

# Read MALLET topic-keys file (tab-separated: topic_id, alpha, words...)
# Note: topic IDs in MALLET are 0-indexed; Topic1 in alltexts = MALLET topic 0
topic_keys_raw <- readLines("topic-keys-28.txt")

# Parse each line into topic number, alpha weight, and top words
topic_words_list <- lapply(topic_keys_raw, function(line) {
  parts <- strsplit(line, "\t")[[1]]
  topic_id   <- as.integer(parts[1])
  alpha      <- as.numeric(gsub(",", ".", parts[2]))  # handle locale decimal separator
  words      <- strsplit(trimws(parts[3]), "\\s+")[[1]]
  top10      <- words[1:min(10, length(words))]       # take top 10 (or fewer if available)

  # Build named list: Topic (1-indexed), Alpha, Word1-Word10, Top_Words
  row <- c(
    list(
      Topic     = topic_id + 1L,
      Alpha     = alpha
    ),
    setNames(as.list(top10), paste0("Word", seq_along(top10))),
    list(
      Top_Words = paste(top10, collapse = ", ")
    )
  )
  as.data.frame(row, stringsAsFactors = FALSE)
})

# Combine into a single dataframe and sort by topic number
topic_top_words <- bind_rows(topic_words_list) %>%
  arrange(Topic)

cat("Topic word table created:", nrow(topic_top_words), "topics\n")

# Export to Excel for manual topic labelling
write_xlsx(topic_top_words, "topic_top_words.xlsx")
cat("Exported topic_top_words.xlsx\n")

# ==============================================================================
# END OF SCRIPT
# ==============================================================================
