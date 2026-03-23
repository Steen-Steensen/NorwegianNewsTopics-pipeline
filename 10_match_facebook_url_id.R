# ============================================================================
# Script: 10_match_facebook_url_id.R
# ============================================================================
#
# PURPOSE:
#   Implements a two-step matching pipeline to link Facebook posts to articles:
#   Step 1:  Article ID matching — split URL tokens from final_link, then link,
#            and look them up in the per-newsroom article_id index
#   Step 1b: Direct URL matching — normalise and compare final_link then link
#            directly against article URLs in alltexts (catches slug-based URLs
#            with no embedded article_id)
#   Unmatched posts proceed to cosine similarity in script 11.
#
# INPUTS:
#   - Facebook_Data.csv: Facebook post data. The page_name column must already
#                        match the newsroom keys used in alltexts (lowercase, no
#                        spaces or special characters). Recode before running.
#   - alltexts_with_TP_distribution.rds: Article metadata with article_ids and URLs
#     (output from 06_add_topic_values.R)
#
# OUTPUTS:
#   - FB_Data_Matched.rds: Facebook posts matched via article ID or direct URL
#   - FB_Data_filtered_remaining.rds: Unmatched posts passed to cosine
#                                      similarity matching in script 11
#
# DEPENDENCIES:
#   - data.table: Efficient data manipulation
#   - stringr: String processing (trimming, detection)
#   - dplyr: Data frame operations (joins, select, filter)
#
# ============================================================================

library(data.table)
library(stringr)
library(dplyr)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

# Load Facebook data and rename page_name to newsroom
FB_Data <- read.csv("Facebook_Data.csv", stringsAsFactors = FALSE)
FB_Data <- FB_Data %>% rename(newsroom = page_name)

# Load article metadata with topic distributions (output of 06_add_topic_values.R)
alltexts_TP <- readRDS("alltexts_with_TP_distribution.rds")

# ============================================================================
# SHARED HELPERS AND CONFIGURATION
# ============================================================================

# Check whether a URL string is usable (not NA, empty, or literal "NA")
valid_url <- function(u) !is.na(u) && trimws(u) != "" && toupper(u) != "NA"

# Normalise a URL for direct comparison: lowercase and strip trailing slash
normalize_url <- function(u) tolower(str_trim(sub("/$", "", u)))

# ============================================================================
# STEP 1: ARTICLE ID MATCHING (final_link, then link)
# ============================================================================
# For each post, split URL tokens from final_link first; if no article_id is
# found there, fall back to link. Article IDs are alphanumeric codes (e.g.
# "dwayQX", "0Q1WdG") so matching is case-sensitive and digit-only extraction
# would miss most IDs. Splitting on URL delimiters is O(posts) vs the original
# O(posts × articles) inner loop.

# Clean article_id column: remove empty strings and trim whitespace
alltexts_TP$article_id <- str_trim(alltexts_TP$article_id)
alltexts_TP$article_id <- ifelse(alltexts_TP$article_id == "", NA, alltexts_TP$article_id)

# Build a per-newsroom article_id → pipeline ID lookup
article_id_mapping <- alltexts_TP %>%
  filter(!is.na(article_id)) %>%
  select(article_id, ID, newsroom) %>%
  distinct()

newsroom_id_lookups <- split(article_id_mapping, article_id_mapping$newsroom)
newsroom_id_lookups <- lapply(newsroom_id_lookups, function(df) {
  setNames(df$ID, df$article_id)
})

# Helper: extract first matching article_id from a URL string
extract_article_id <- function(url, lookup) {
  if (!valid_url(url)) return(NA_character_)
  candidates <- unlist(str_split(url, "[/\\-_.?=&]"))
  candidates <- candidates[nchar(candidates) > 0]
  matched    <- intersect(candidates, names(lookup))
  if (length(matched) > 0) matched[1] else NA_character_
}

cat("Step 1: article_id matching (final_link, then link)...\n")
# newsroom_id_lookups is a named integer vector (name = article_id, value = pipeline ID).
# Look up the pipeline ID directly — no join required, no cross-newsroom collision.
FB_Data$ID <- vapply(seq_len(nrow(FB_Data)), function(i) {
  lookup <- newsroom_id_lookups[[FB_Data$newsroom[i]]]
  if (is.null(lookup)) return(NA_integer_)
  # Try final_link first, fall back to link
  article_id <- extract_article_id(FB_Data$final_link[i], lookup)
  if (is.na(article_id)) article_id <- extract_article_id(FB_Data$link[i], lookup)
  if (is.na(article_id)) return(NA_integer_)
  as.integer(lookup[[article_id]])   # pipeline ID straight from the per-newsroom vector
}, integer(1))

article_id_matched_count <- sum(!is.na(FB_Data$ID))
cat(sprintf("  Matched: %d posts\n", article_id_matched_count))

# ============================================================================
# STEP 1B: DIRECT URL MATCHING (final_link, then link)
# ============================================================================
# For posts still unmatched, compare normalised final_link and link directly
# against article URLs in alltexts. Catches newsrooms using slug-based URLs
# with no embedded article_id. Implemented as a hash join — instant.

# Build a per-newsroom normalised URL → pipeline ID lookup
url_lookup <- alltexts_TP %>%
  select(url, ID, newsroom) %>%
  mutate(url_norm = normalize_url(url)) %>%
  filter(!is.na(url_norm), url_norm != "")

newsroom_url_lookups <- split(url_lookup, url_lookup$newsroom)
newsroom_url_lookups <- lapply(newsroom_url_lookups, function(df) {
  setNames(df$ID, df$url_norm)
})

cat("Step 1b: direct URL matching (final_link, then link)...\n")
unmatched_mask <- is.na(FB_Data$ID)

direct_url_ids <- vapply(which(unmatched_mask), function(i) {
  lookup <- newsroom_url_lookups[[FB_Data$newsroom[i]]]
  if (is.null(lookup)) return(NA_character_)
  # Try final_link first, fall back to link
  for (col in c("final_link", "link")) {
    url <- FB_Data[[col]][i]
    if (!valid_url(url)) next
    key <- normalize_url(url)
    if (key %in% names(lookup)) return(as.character(lookup[[key]]))
  }
  NA_character_
}, character(1))

FB_Data$ID[unmatched_mask] <- direct_url_ids

direct_url_matched_count <- sum(!is.na(direct_url_ids))
cat(sprintf("  Matched: %d posts\n", direct_url_matched_count))

# ============================================================================
# CLEANUP AND OUTPUT
# ============================================================================

# Posts matched via Steps 1 and 1b
FB_Data_Matched <- FB_Data %>%
  filter(!is.na(ID)) %>%
  mutate(ID = as.integer(ID)) %>%
  select(-starts_with("...")) %>%
  distinct()

# Posts still unmatched — passed to cosine similarity matching in script 11
FB_Data_filtered_remaining <- FB_Data %>%
  filter(is.na(ID)) %>%
  select(-starts_with("..."))

# Save outputs
saveRDS(FB_Data_Matched, "FB_Data_Matched.rds")
saveRDS(FB_Data_filtered_remaining, "FB_Data_filtered_remaining.rds")

remaining_count <- nrow(FB_Data_filtered_remaining)

cat(sprintf("\nScript 10 matching summary:\n"))
cat(sprintf("  Step 1  — article_id:        %d posts\n", article_id_matched_count))
cat(sprintf("  Step 1b — direct URL:        %d posts\n", direct_url_matched_count))
cat(sprintf("  Passing to cosine (script 11): %d posts\n", remaining_count))
cat(sprintf("\n  Output: FB_Data_Matched.rds (%d rows)\n", nrow(FB_Data_Matched)))
cat(sprintf("  Output: FB_Data_filtered_remaining.rds (%d rows)\n", remaining_count))
