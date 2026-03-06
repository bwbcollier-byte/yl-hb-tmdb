-- Add TMDb columns to social_profiles
-- Run this in Supabase SQL Editor before running the TMDb Social Enrichment workflow

ALTER TABLE social_profiles
  ADD COLUMN IF NOT EXISTS tmdb_check          text,
  ADD COLUMN IF NOT EXISTS tmdb_birthday        text,
  ADD COLUMN IF NOT EXISTS tmdb_deathday        text,
  ADD COLUMN IF NOT EXISTS tmdb_gender          integer,
  ADD COLUMN IF NOT EXISTS tmdb_known_for       text,
  ADD COLUMN IF NOT EXISTS tmdb_place_of_birth  text,
  ADD COLUMN IF NOT EXISTS tmdb_popularity      numeric,
  ADD COLUMN IF NOT EXISTS tmdb_imdb_id         text,
  ADD COLUMN IF NOT EXISTS tmdb_instagram_id    text,
  ADD COLUMN IF NOT EXISTS tmdb_twitter_id      text,
  ADD COLUMN IF NOT EXISTS tmdb_facebook_id     text,
  ADD COLUMN IF NOT EXISTS tmdb_tiktok_id       text,
  ADD COLUMN IF NOT EXISTS tmdb_wikidata_id     text,
  ADD COLUMN IF NOT EXISTS tmdb_images          text;
