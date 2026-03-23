# ==============================================================================
# Script: 14_combine_social_media_matches.R
# Purpose:
#   Combine all social media matching results and add published_on_fb,
#   published_on_insta, and published_on_tiktok boolean columns to alltexts.
#   An article is marked TRUE if at least one social media post was matched
#   to it above the platform-specific similarity threshold.
#
# Inputs:
#   - alltexts_with_TP_distribution.rds: article corpus (from script 06)
#   - FB_Data_Matched.rds: Facebook posts matched via article_id or direct URL
#     (from script 10 — exact matches, no similarity threshold applied)
#   - final_results_FB.rds: Facebook posts matched via cosine similarity
#     (from script 11)
#   - final_results_Insta.rds: Instagram posts matched via cosine similarity
#     (from script 08)
#   - final_results_Tiktok.rds: TikTok posts matched via cosine similarity
#     (from script 12)
#
# Output:
#   - alltexts_final.rds: full article corpus with boolean social media columns
#
# ==============================================================================
# THRESHOLD CONFIGURATION
# ==============================================================================
# Set these values manually based on inspection of the sampled result files
# produced by scripts 08, 11, and 12. The values below are from the original
# manual inspection (see Methods Appendix):
#
#   Facebook (cosine): posts with similarity > 0.20 and message length ≥ 10
#     words were found reliable in a 2% sample of 460 posts.
#     URL/article_id matched posts (FB_Data_Matched.rds) are always included
#     regardless of this threshold — they are exact matches.
#
#   Instagram: posts with similarity > 0.12 were found reliable in a 5% sample
#     of 244 posts (94% of posts above threshold matched the correct story).
#
#   TikTok: posts with similarity > 0.17 were found reliable based on manual
#     inspection of all VG videos (169) and all videos from three other
#     newsrooms (268 total).
#
# !! UPDATE THESE VALUES AFTER YOUR OWN MANUAL INSPECTION !!

threshold_fb       <- 0.20   # cosine similarity threshold for Facebook
threshold_fb_words <- 10     # minimum word count in Facebook post message
threshold_insta    <- 0.12   # cosine similarity threshold for Instagram
threshold_tiktok   <- 0.17   # cosine similarity threshold for TikTok

# ==============================================================================
# 1. LOAD DATA
# ==============================================================================

library(dplyr)
library(stringr)

select <- dplyr::select

alltexts_TP <- readRDS("alltexts_with_TP_distribution.rds")

# URL → ID lookup for joining cosine similarity results back to article IDs
url_to_id <- alltexts_TP %>%
  select(url, ID) %>%
  distinct()

# ==============================================================================
# 2. FACEBOOK
# ==============================================================================

# --- Exact matches from script 10 (article_id or direct URL) ---
# These are direct matches — no similarity threshold applied.
fb_exact <- readRDS("FB_Data_Matched.rds")
fb_ids_exact <- unique(fb_exact$ID[!is.na(fb_exact$ID)])
cat(sprintf("Facebook exact matches (article_id / direct URL): %d articles\n",
            length(fb_ids_exact)))

# --- Cosine similarity matches from script 11 ---
fb_cosine <- readRDS("final_results_FB.rds")

# Apply similarity threshold and minimum post word count
fb_cosine_filtered <- fb_cosine %>%
  filter(
    !is.na(similarity_score),
    similarity_score > threshold_fb,
    str_count(message, "\\w+") >= threshold_fb_words
  )

fb_ids_cosine <- fb_cosine_filtered %>%
  left_join(url_to_id, by = c("similar_url" = "url")) %>%
  pull(ID) %>%
  unique() %>%
  na.omit()

cat(sprintf("Facebook cosine matches (similarity > %.2f, words >= %d): %d articles\n",
            threshold_fb, threshold_fb_words, length(fb_ids_cosine)))

fb_article_ids <- unique(c(fb_ids_exact, as.integer(fb_ids_cosine)))

# --- Post-level counts ---
n_fb_exact_posts  <- nrow(fb_exact)
n_fb_cosine_posts <- nrow(fb_cosine_filtered)
n_fb_pipeline     <- n_fb_exact_posts + nrow(fb_cosine)  # all posts that entered pipeline
n_fb_matched      <- n_fb_exact_posts + n_fb_cosine_posts

cat(sprintf("\nFacebook matching summary:\n"))
cat(sprintf("  Posts in pipeline:           %d\n", n_fb_pipeline))
cat(sprintf("  Matched via article_id/URL:  %d posts  →  %d unique articles\n",
            n_fb_exact_posts, length(fb_ids_exact)))
cat(sprintf("  Matched via cosine:          %d posts  →  %d unique articles\n",
            n_fb_cosine_posts, length(as.integer(fb_ids_cosine))))
cat(sprintf("  Total matched posts:         %d  (%.1f%%)\n",
            n_fb_matched, 100 * n_fb_matched / max(n_fb_pipeline, 1)))
cat(sprintf("  Total unique articles hit:   %d\n", length(fb_article_ids)))
cat(sprintf("  Avg FB posts per article:    %.2f\n",
            n_fb_matched / max(length(fb_article_ids), 1)))

# ==============================================================================
# 3. INSTAGRAM
# ==============================================================================

insta <- readRDS("final_results_Insta.rds")

insta_filtered <- insta %>%
  filter(!is.na(similarity_score), similarity_score > threshold_insta)

insta_article_ids <- insta_filtered %>%
  left_join(url_to_id, by = c("similar_url" = "url")) %>%
  pull(ID) %>%
  unique() %>%
  na.omit()

cat(sprintf("Instagram matches (similarity > %.2f): %d articles\n",
            threshold_insta, length(insta_article_ids)))

# ==============================================================================
# 4. TIKTOK
# ==============================================================================

tiktok <- readRDS("final_results_Tiktok.rds")

tiktok_filtered <- tiktok %>%
  filter(!is.na(similarity_score), similarity_score > threshold_tiktok)

tiktok_article_ids <- tiktok_filtered %>%
  left_join(url_to_id, by = c("similar_url" = "url")) %>%
  pull(ID) %>%
  unique() %>%
  na.omit()

cat(sprintf("TikTok matches (similarity > %.2f): %d articles\n",
            threshold_tiktok, length(tiktok_article_ids)))

# ==============================================================================
# 5. ADD BOOLEAN COLUMNS TO ALLTEXTS
# ==============================================================================

alltexts_final <- alltexts_TP %>%
  mutate(
    published_on_fb     = ID %in% fb_article_ids,
    published_on_insta  = ID %in% insta_article_ids,
    published_on_tiktok = ID %in% tiktok_article_ids
  )

# Summary
cat(sprintf("\nArticles published on Facebook:  %d (%.1f%%)\n",
            sum(alltexts_final$published_on_fb),
            100 * mean(alltexts_final$published_on_fb)))
cat(sprintf("Articles published on Instagram: %d (%.1f%%)\n",
            sum(alltexts_final$published_on_insta),
            100 * mean(alltexts_final$published_on_insta)))
cat(sprintf("Articles published on TikTok:    %d (%.1f%%)\n",
            sum(alltexts_final$published_on_tiktok),
            100 * mean(alltexts_final$published_on_tiktok)))
cat(sprintf("Articles on any platform:        %d (%.1f%%)\n",
            sum(alltexts_final$published_on_fb |
                alltexts_final$published_on_insta |
                alltexts_final$published_on_tiktok),
            100 * mean(alltexts_final$published_on_fb |
                       alltexts_final$published_on_insta |
                       alltexts_final$published_on_tiktok)))

# ==============================================================================
# 6. SAVE
# ==============================================================================

saveRDS(alltexts_final, "alltexts_final.rds")
cat("\nSaved alltexts_final.rds\n")

# ==============================================================================
# END OF SCRIPT
# ==============================================================================
