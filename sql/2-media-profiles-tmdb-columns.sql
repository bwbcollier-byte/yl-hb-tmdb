-- Add TMDb columns to media_profiles
-- Run this in Supabase SQL Editor before running the TMDb Media Enrichment workflow

ALTER TABLE media_profiles
  ADD COLUMN IF NOT EXISTS tmdb_check          text,
  ADD COLUMN IF NOT EXISTS tmdb_media_type     text,       -- 'movie' or 'tv' (defaults to movie if null)
  ADD COLUMN IF NOT EXISTS tmdb_title          text,
  ADD COLUMN IF NOT EXISTS tmdb_overview       text,
  ADD COLUMN IF NOT EXISTS tmdb_tagline        text,
  ADD COLUMN IF NOT EXISTS tmdb_release_date   text,
  ADD COLUMN IF NOT EXISTS tmdb_runtime        integer,
  ADD COLUMN IF NOT EXISTS tmdb_genres         text,
  ADD COLUMN IF NOT EXISTS tmdb_vote_average   numeric,
  ADD COLUMN IF NOT EXISTS tmdb_vote_count     integer,
  ADD COLUMN IF NOT EXISTS tmdb_poster_path    text,
  ADD COLUMN IF NOT EXISTS tmdb_backdrop_path  text,
  ADD COLUMN IF NOT EXISTS tmdb_imdb_id        text,
  ADD COLUMN IF NOT EXISTS tmdb_status         text,
  ADD COLUMN IF NOT EXISTS tmdb_popularity     numeric,
  ADD COLUMN IF NOT EXISTS tmdb_cast           text,
  ADD COLUMN IF NOT EXISTS tmdb_director       text,
  ADD COLUMN IF NOT EXISTS tmdb_images         text,
  ADD COLUMN IF NOT EXISTS tmdb_budget         bigint,
  ADD COLUMN IF NOT EXISTS tmdb_revenue        bigint;
