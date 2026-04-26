# CLAUDE.md — `yl-hb-tmdb` (TMDb mining + enrichment)

Conventions shared across the `yl-hb-*` fleet live in
[`SCRAPER-CLAUDE-TEMPLATE.md`](../SCRAPER-CLAUDE-TEMPLATE.md) — read both.
This is the largest scraper repo in the fleet (15 workflows). Expect
to consult both files when editing.

## What this repo does

A bundle of TMDb (The Movie Database) ingest jobs. Two broad classes:

1. **Mining** — periodically sweep TMDb's "popular / trending /
   now-playing / upcoming / top-rated / on-the-air / airing-today"
   endpoints to discover new movie / TV titles and persist them into
   `public.hb_media`. Plus a "trending people" job into `public.hb_talent`.
2. **Enrichment** — for already-discovered media + people, fill in
   detail: cast/crew, posters, social-link blocks, runtime, etc.

A separate `update_airtable.ts` and `airtable-heartbeat.ts` mirror a
slice of the data into Airtable. A `heal-orphaned-socials.ts` does
periodic cleanup.

## Stack

**Standard enrichment** variant: Node 20, TypeScript via `ts-node`,
`@supabase/supabase-js`. No browser. Plus Airtable + a `sync-dashboard`
helper.

## Repo layout

```
src/
  tmdb-api.ts                        # shared TMDb client
  supabase.ts                        # service-role client
  tmdb-media-enrichment.ts           # detail fill for movies/TV
  tmdb-social-enrichment.ts          # social-link enrichment
  tmdb-media-mining.ts               # generic mining engine (many workflows reuse it)
  tmdb-media-linker.ts               # cast/crew linking
  tmdb-popular-mining.ts             # popular people
  tmdb-trending-mining.ts            # trending people
  tmdb-now-playing-mining.ts         # film-specific
  airtable-heartbeat.ts              # Airtable sync ping
  heal-orphaned-socials.ts           # cleanup
  test-airtable.ts                   # dev probe
  test-connection.ts                 # dev probe

scripts/                             # additional helpers
sql/                                 # ad-hoc SQL (audit / repair queries)
update_airtable.ts                   # one-off Airtable update
overnight-tmdb-*.sh                  # 14 local-runner wrappers, one per workflow
workflows_table.json                 # snapshot of public.workflows rows for this repo
updates.md                           # change log

.github/workflows/                   # 15 workflows
  sync-dashboard.yml
  tmdb-media-enrichment.yml
  tmdb-social-enrichment.yml
  tmdb-trending-mining.yml
  tmdb-popular-mining.yml
  tmdb-{movie,tv}-{popular,top-rated,trending}-mining.yml
  tmdb-movie-{now-playing,upcoming}-mining.yml
  tmdb-tv-{airing-today,on-the-air}-mining.yml

package.json
tsconfig.json
```

## Supabase auth

Standard fleet convention — `SUPABASE_URL` + `SUPABASE_SERVICE_KEY`
in `src/supabase.ts`.

## Workflow lifecycle convention

All 15 workflows call `log_workflow_run` start + result with hardcoded
GitHub workflow ids. The `sync-dashboard` workflow is meta — it
periodically updates the dashboard rows for all the others.

## Tables this repo touches

| Table | Operation | Notes |
|---|---|---|
| `public.hb_media` | UPSERT | Movie / TV title rows. Primary write target for mining workflows. |
| `public.hb_talent` | UPSERT | TMDb people (actors, directors, crew). Trending-people mining writes here. |
| `public.hb_socials` | UPSERT | External-link blocks (twitter, instagram, facebook, official-site) attached to talent. |
| `public.media` | (legacy?) | A `media` table also appears in some queries — verify whether it's an alias / view or a stale reference before adding new code that targets it. |
| `public.countries` | SELECT (lookup) | Country-code resolution for production-country fields. |
| `public.workflows` | RPC `log_workflow_run` | Lifecycle reporting. |

## Running locally

```bash
npm install
cp .env.example .env.local            # if present
# Set: SUPABASE_URL, SUPABASE_SERVICE_KEY, TMDB_BEARER_TOKEN
#      MEDIA_TYPE (movie/tv), MINING_ENDPOINT (popular/top_rated/...),
#      LIMIT, MAX_PAGES, STALE_DAYS, SLEEP_MS
#      AIRTABLE_PAT, AIRTABLE_BASE_ID, AIRTABLE_RECORD_ID  (heartbeat / mirror)
npx ts-node --transpile-only src/tmdb-media-mining.ts
```

## Per-repo gotchas

- **`MINING_ENDPOINT` and `MEDIA_TYPE` parameterise the mining engine.**
  The 14 mining workflows are 14 invocations of the same TS file with
  different env vars — don't fork the engine into 14 copies.
- **TMDb v3 vs v4 auth.** This repo uses `TMDB_BEARER_TOKEN` (v4) — do
  not regress to the old `TMDB_API_KEY` query-param style.
- **TMDb pagination caps at 500.** For sources that have more, use the
  date-window approach (filter by `release_date` ranges).
- **Heartbeat workflow expects an Airtable record id** (`AIRTABLE_RECORD_ID`)
  to ping. Don't delete that env var.
- **`heal-orphaned-socials.ts`** is a periodic repair — runs against
  rows where `linked_talent` points at a deleted `hb_talent.id`. Don't
  remove without checking what's calling it.
- **`workflows_table.json` is a snapshot, not a source of truth.** It's
  used to set up the rows in `public.workflows`. Update by hand when
  adding a new workflow.
- **`public.media` references in some queries** — investigate whether
  this is a view, a stale legacy table, or a typo before propagating
  new uses of it.

## Conventions Claude should follow when editing this repo

All the fleet-wide rules from [`SCRAPER-CLAUDE-TEMPLATE.md`](../SCRAPER-CLAUDE-TEMPLATE.md)
apply. Specifically here:

- **Don't fork mining engines.** Use env-var parameterisation. New
  endpoints get a new YAML, not new TS.
- **Hardcode the GitHub workflow id in YAML** matching this repo's
  local convention.
- **`update_airtable.ts` is a one-off** — don't import its helpers
  into the scheduled workflows.

## Related repos

- `yl-hb-imdbp` — has a `tmdb-media-induction.js` that does similar
  enrichment from the IMDbPro side. Coordinate field changes.
- `yl-hb-imdb` — sibling for the **public** IMDb site.
- `hb_app_build` — Next.js app reading `hb_media` / `hb_talent`.
