# ==============================================================================
# Script: 09_match_instagram_topics.R
# Purpose:
#   Match Instagram post URLs to article IDs and merge topic distributions from LDA
#
# Inputs:
#   - final_results_Insta: Instagram post data from 08_match_instagram.R (in memory or from RDS)
#   - alltexts_with_TP_distribution.rds: Article corpus with ID and Topic1-Topic28 columns from 06_add_topic_values.R
#
# Outputs:
#   - Insta_Data_Matched.rds: Instagram data with matched article IDs (no topics yet)
#   - Insta_Data_Matched_TP.rds: Final Instagram dataset with article IDs and topic distributions
#
# Dependencies: dplyr
# ==============================================================================

library(dplyr)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

# ==============================================================================
# 1. LOAD DATA
# ==============================================================================

# Load alltexts_TP with topic distributions (contains ID + Topic1-Topic28)
alltexts_TP <- readRDS("alltexts_with_TP_distribution.rds")

# Note: final_results_Insta should already be in memory from 08_match_instagram.R
# If not, load from RDS: final_results_Insta <- readRDS("final_results_Insta.rds")

cat("Loaded alltexts_TP with", nrow(alltexts_TP), "rows\n")
cat("Loaded final_results_Insta with", nrow(final_results_Insta), "rows\n")

# ==============================================================================
# 2. MATCH INSTAGRAM POSTS TO ARTICLE IDS
# ==============================================================================

# Extract article IDs from alltexts_TP (url + ID mapping)
articles_url_id <- alltexts_TP %>%
  select(url, ID)

# Left join Instagram data with article IDs
# Matches on similar_url (from Instagram) = url (from articles)
Insta_Data_Matched <- final_results_Insta %>%
  left_join(articles_url_id, by = c("similar_url" = "url"))

# Relocate ID column to first position for clarity
Insta_Data_Matched <- Insta_Data_Matched %>%
  relocate(ID, .before = everything())

# Remove rows where ID is NA (unmatched posts)
n_before_filter <- nrow(Insta_Data_Matched)
Insta_Data_Matched <- Insta_Data_Matched %>%
  filter(!is.na(ID))
n_after_filter <- nrow(Insta_Data_Matched)

cat("Matched Instagram posts: ", n_after_filter, " out of ", n_before_filter, "\n")
cat("Unmatched posts removed: ", n_before_filter - n_after_filter, "\n")

# ==============================================================================
# 3. SAVE MATCHED DATA (WITHOUT TOPICS)
# ==============================================================================

saveRDS(Insta_Data_Matched, "Insta_Data_Matched.rds")
cat("Saved Insta_Data_Matched.rds\n")

# ==============================================================================
# 4. ADD TOPIC DISTRIBUTIONS
# ==============================================================================

# Extract topic columns from alltexts_TP (ID + all Topic columns)
# Note: Select ID (not url) to ensure correct join key
topics_data <- alltexts_TP %>%
  select(ID, starts_with("Topic"))

# Left join topic distributions onto Instagram data
Insta_Data_Matched_TP <- Insta_Data_Matched %>%
  left_join(topics_data, by = "ID")

# Verify merge
cat("Topics added. Final dataset has", ncol(Insta_Data_Matched_TP), "columns\n")
cat("Topic columns:", sum(grepl("^Topic[0-9]", names(Insta_Data_Matched_TP))), "\n")

# ==============================================================================
# 5. SAVE FINAL MATCHED DATA WITH TOPICS
# ==============================================================================

saveRDS(Insta_Data_Matched_TP, "Insta_Data_Matched_TP.rds")
cat("Saved Insta_Data_Matched_TP.rds\n")

# ==============================================================================
# END OF SCRIPT
# ==============================================================================
