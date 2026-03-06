import fetch from 'node-fetch';

const TMDB_BASE_URL = 'https://api.themoviedb.org/3';
export const TMDB_IMAGE_BASE_URL = 'https://image.tmdb.org/t/p/original';

const BEARER_TOKEN = process.env.TMDB_BEARER_TOKEN || '';

if (!BEARER_TOKEN) {
    console.error('❌ TMDB_BEARER_TOKEN environment variable is not set');
    process.exit(1);
}

export const SLEEP_MS = parseInt(process.env.SLEEP_MS || '250');
export const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export const getImageUrl = (path: string | null | undefined): string | null =>
    path ? `${TMDB_IMAGE_BASE_URL}${path}` : null;

let totalApiCalls = 0;
let failedApiCalls = 0;

export function getApiStats() {
    return {
        totalApiCalls,
        failedApiCalls,
        successRate: totalApiCalls > 0
            ? Math.round(((totalApiCalls - failedApiCalls) / totalApiCalls) * 100)
            : 0
    };
}

async function tmdbFetch(url: string): Promise<any | null> {
    totalApiCalls++;
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${BEARER_TOKEN}`,
                'accept': 'application/json'
            }
        });

        if (response.status === 404) {
            console.warn(`   ⚠️  Not found (404): ${url}`);
            failedApiCalls++;
            return null;
        }

        if (!response.ok) {
            failedApiCalls++;
            console.error(`   ❌ API Error ${response.status} ${response.statusText}: ${url}`);
            return null;
        }

        return await response.json();
    } catch (err: any) {
        failedApiCalls++;
        console.error(`   ❌ Network error: ${err.message}`);
        return null;
    }
}

/**
 * Fetch person data from TMDb with external IDs and images appended.
 * @param personId TMDb person ID
 */
export async function fetchTmdbPerson(personId: string | number): Promise<any | null> {
    return tmdbFetch(
        `${TMDB_BASE_URL}/person/${personId}?append_to_response=external_ids%2Cimages&language=en-US`
    );
}

/**
 * Fetch movie data from TMDb with credits, images, and external IDs appended.
 * @param movieId TMDb movie ID
 */
export async function fetchTmdbMovie(movieId: string | number): Promise<any | null> {
    return tmdbFetch(
        `${TMDB_BASE_URL}/movie/${movieId}?append_to_response=credits%2Cimages%2Cexternal_ids&language=en-US`
    );
}

/**
 * Fetch TV show data from TMDb with credits, images, and external IDs appended.
 * @param tvId TMDb TV show ID
 */
export async function fetchTmdbTv(tvId: string | number): Promise<any | null> {
    return tmdbFetch(
        `${TMDB_BASE_URL}/tv/${tvId}?append_to_response=credits%2Cimages%2Cexternal_ids&language=en-US`
    );
}

/**
 * Fetch a page of trending people (day window).
 * @param page Page number (default 1)
 */
export async function fetchTmdbTrendingPeople(page: number = 1): Promise<any | null> {
    return tmdbFetch(
        `${TMDB_BASE_URL}/trending/person/day?language=en-US&page=${page}`
    );
}
