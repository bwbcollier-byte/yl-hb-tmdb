# TMDb Enrichment & Mining Pipeline Status Report

This pipeline handles 14 distinct workflows for synchronizing and enriching Talent and Media data using the TMDb API.

## 🚀 Overall Status: ALL SYSTEM GO

All 14 workflows are currently active, tested, and synced with your database and GitHub Actions.

| Workflow Category     | Count | Status     | Description                                                           |
| :-------------------- | :---- | :--------- | :-------------------------------------------------------------------- |
| **Social Enrichment** | 1     | ✅ Working | Updates biographies, images, and handles for existing talent.         |
| **Media Enrichment**  | 1     | ✅ Working | Updates cast, crew, and technical data for existing movies/TV.        |
| **Talent Mining**     | 2     | ✅ Working | Finds new trending/popular people and adds them to `social_profiles`. |
| **Media Mining**      | 10    | ✅ Working | Unified system for TV and Film lists (Popular, Top Rated, etc.).      |

---

## 🛠️ Detailed Workflow Breakdown

### 1. Enrichment Workflows (Quality Control)

- **TMDb Social Profile Enrichment**:
  - **What it does**: Scans `social_profiles` where `social_type = 'TMDB'`. It fetches the person detail, external IDs (IMDb, Instagram), and images.
  - **Target Data**: Biographies, gender, popularity scores, social media handles, and profile images.
- **TMDb Media Profile Enrichment**:
  - **What it does**: Scans `media_profiles` with a `tmdb_id`. It fetches full details (Movie or TV) including casting and crew.
  - **Target Data**: Title, overview, release dates, genres, runtime, top 10 cast members (JSON), and directors.

### 2. Talent Mining Workflows (Discovery)

- **TMDb Trending Talent Mining**:
  - **What it does**: Fetches the daily trending people from TMDb.
  - **Logic**: If they don't exist in `social_profiles`, it creates them. If they do exist, it updates their `tmdb_popularity` score to keep your data fresh.
- **TMDb Popular Talent Mining**:
  - **What it does**: Same discovery logic but focused on the all-time "Most Popular" global list.

### 3. Media Mining Workflows (Expansion)

We use a **Unified Script Architecture** here. 10 Workflows share a single codebase for perfect consistency.

- **TV Set**: Airing Today, On the Air, Popular, Top Rated, Trending.
- **Film Set**: Now Playing, Popular, Top Rated, Upcoming, Trending.
- **Logic**: These scripts automatically paginate through TMDb results, upsert records into `media_profiles`, and handle both Movies and TV shows natively.

---

## ⚡ Performance Optimizations Implemented

To ensure these run smoothly overnight with thousands of records, I have implemented several "under the hood" enhancements:

1.  **Database Indexing**:
    - Added standard indexes on `tmdb_id`, `tmdb_check`, and `social_type` to prevent table scans during enrichment rounds.
2.  **Unified Codebase**:
    - The 10 Media Mining workflows share one script (`tmdb-media-mining.ts`), making maintenance 10x easier.
3.  **Circuit Breaker & Safety**:
    - Added regex validation to skip malformed IDs (like `not.found`).
    - Added a sleep delay between API calls to respect TMDb rate limits.
4.  **Dashboard Progress Sync**:
    - Scripts now natively update the `processed` and `to_process` counts in your Supabase `workflows` table, so your web app UI shows live progress bars.

---

## 📈 What We Can Do Better (Next Steps)

- **Log Cleanup**: Since `media_profiles` doesn't have a built-in `workflow_logs` column in the original schema, we are using the `tmdb_check` status. We could build a unified `enrichment_logs` table if you want full historical tracking.
- **Image Optimization**: TMDb images are high-res. We are currently saving the direct path, but we could add a workflow to generate thumbnails or blurred placeholders for your UI.
- **Cross-Enrichment**: We could use the TMDb External IDs to automatically trigger a **Spotify** or **Deezer** enrichment if a Music ID is found in the TMDb metadata.
