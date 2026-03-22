# ============================================================================
# Script: 13_match_tiktok_topics.R
# ============================================================================
#
# PURPOSE:
#   Enhances matched TikTok posts with article metadata (topic distributions,
#   engagement metrics, and performance analysis) by joining with article
#   reference data. Produces both a basic matched dataset and a fully enriched
#   version with topic distributions and calculated engagement metrics.
#
# INPUTS:
#   - final_results_Tiktok.rds (from 12_match_tiktok.R): TikTok posts matched
#     to articles via cosine similarity, containing similar_url column
#   - alltexts_with_TP_distribution.rds (from 06_add_topic_values.R): Article
#     metadata including topic distributions and URLs
#
# OUTPUTS:
#   - TikTok_Data_Matched.rds: TikTok posts matched to articles (with ID,
#                               without topic distributions)
#   - TikTok_Data_Matched_TP.rds: TikTok posts with complete topic
#                                  distributions and engagement metrics
#                                  (overperforming %, total interactions)
#
# DEPENDENCIES:
#   - dplyr: Data frame operations (join, select, filter, mutate, relocate)
#
# ============================================================================

library(dplyr)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

# Load article metadata with topic distributions
# (if not already in memory from previous script)
if (!exists("alltexts_TP")) {
  alltexts_TP <- readRDS("alltexts_with_TP_distribution.rds")
}

# Load TikTok data matched via cosine similarity (from script 12)
# Script 12 saves results as final_results_Tiktok.rds
TikTok_Data_Matched <- readRDS("final_results_Tiktok.rds")

# ============================================================================
# ADD ARTICLE ID VIA URL MATCHING
# ============================================================================
# Join TikTok data with article metadata by matching similar_url to article URL

TikTok_Data_Matched <- TikTok_Data_Matched %>%
  left_join(
    alltexts_TP %>% select(url, ID),
    by = c("similar_url" = "url"),
    relationship = "many-to-one"
  ) %>%
  # Move ID to first column for clarity
  relocate(ID, .before = 1)

# Remove rows where ID matching failed (no corresponding article found)
TikTok_Data_Matched <- TikTok_Data_Matched %>%
  filter(!is.na(ID))

# Save matched TikTok data without topic distributions
saveRDS(TikTok_Data_Matched, "TikTok_Data_Matched.rds")

cat("TikTok article matching complete.\n")
cat(sprintf("  Matched posts saved: %d rows\n", nrow(TikTok_Data_Matched)))

# ============================================================================
# ADD TOPIC DISTRIBUTIONS AND ENGAGEMENT METRICS
# ============================================================================
# Enrich matched posts with topic distributions from articles and calculate
# engagement-based performance metrics

# Select all Topic columns and ID from article reference data
# Topics are named Topic1-Topic28 (no underscore) as created in script 06
topic_columns <- alltexts_TP %>%
  select(ID, starts_with("Topic")) %>%
  distinct()

# Join topic distributions to matched TikTok posts
TikTok_Data_Matched_TP <- TikTok_Data_Matched %>%
  left_join(
    topic_columns,
    by = "ID",
    relationship = "many-to-one"
  )

# Calculate total interactions (sum of all engagement metrics)
TikTok_Data_Matched_TP <- TikTok_Data_Matched_TP %>%
  mutate(
    total_interactions = like_count + view_count + comment_count + share_count
  )

# Calculate overperforming metric: percentage difference from newsroom average
# This shows how much each post outperforms the average for its newsroom
newsroom_avg_interactions <- TikTok_Data_Matched_TP %>%
  group_by(newsroom) %>%
  summarise(
    avg_interactions = mean(total_interactions, na.rm = TRUE),
    .groups = "drop"
  )

TikTok_Data_Matched_TP <- TikTok_Data_Matched_TP %>%
  left_join(
    newsroom_avg_interactions,
    by = "newsroom",
    relationship = "many-to-one"
  ) %>%
  mutate(
    overperforming_pct = ((total_interactions - avg_interactions) / avg_interactions) * 100
  ) %>%
  select(-avg_interactions)

# Save final enriched dataset
saveRDS(TikTok_Data_Matched_TP, "TikTok_Data_Matched_TP.rds")

# ============================================================================
# SUMMARY STATISTICS
# ============================================================================

# Print row counts per newsroom
cat("\nRow counts by newsroom:\n")
newsroom_summary <- TikTok_Data_Matched_TP %>%
  group_by(newsroom) %>%
  summarise(n = n(), .groups = "drop") %>%
  arrange(desc(n))

print(newsroom_summary)

cat(sprintf("\nTotal matched TikTok posts with topics: %d rows\n",
            nrow(TikTok_Data_Matched_TP)))
cat("Output: TikTok_Data_Matched_TP.rds\n")
