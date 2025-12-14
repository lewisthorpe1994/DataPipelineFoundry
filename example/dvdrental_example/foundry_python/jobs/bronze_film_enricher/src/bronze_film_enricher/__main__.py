from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from typing import Iterable

from dlt.destinations.impl.postgres.configuration import PostgresCredentials

from dpf_python import source, DataResourceType, destination

import dlt
import requests
from dlt.destinations import postgres

TMDB = source(name="tmdb_api", ep_type=DataResourceType.API, identifier=None)
FILMS_WAREHOUSE_TABLE = destination(
    name="dvdrental_analytics",
    ep_type=DataResourceType.WAREHOUSE,
    identifier="raw.trending_external_films",
)

DEFAULT_LANGUAGE = "en-US"
DEFAULT_MAX_PAGES = 5


def _resolve_tmdb_api_key() -> str:
    auth = getattr(TMDB.config, "auth", None)
    token_name_or_value = getattr(auth, "token", None) if auth else None
    if token_name_or_value:
        return os.getenv(token_name_or_value) or token_name_or_value
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        raise SystemExit("TMDB API key is required (set TMDB_API_KEY)")
    return api_key


def _fetch_tmdb_popular_page(
    session: requests.Session,
    api_key: str,
    page: int,
) -> dict:
    api_cfg = TMDB.config
    endpoint = TMDB.config.get_endpoint("popular_movies")
    url = f"{TMDB.config.base_url}{endpoint.path}"

    params = dict(endpoint.query_params or {})
    params.update(
        {
            "api_key": api_key,
            "page": str(page),
        }
    )

    headers = dict(getattr(api_cfg, "default_headers", {}) or {})
    headers.update(endpoint.headers or {})

    response = session.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def _fetch_genres(session: requests.Session, api_key: str) -> dict[int, str]:
    api_cfg = TMDB.config
    endpoint = TMDB.config.get_endpoint("genre")
    url = f"{TMDB.config.base_url}{endpoint.path}"

    params = dict(endpoint.query_params or {})
    params.update(
        {
            "api_key": api_key,
        }
    )

    headers = dict(getattr(api_cfg, "default_headers", {}) or {})
    headers.update(endpoint.headers or {})

    response = session.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    payload = response.json()

    genre_map: dict[int, str] = {}
    for item in payload.get("genres") or []:
        genre_id = item.get("id")
        name = item.get("name")
        if genre_id is None or not name:
            continue
        try:
            genre_map[int(genre_id)] = str(name)
        except (TypeError, ValueError):
            continue
    return genre_map



@dlt.resource(
    name="trending_external_films",
    max_table_nesting=0,
    write_disposition="merge",
    primary_key="tmdb_movie_id",
    file_format="typed-jsonl",
    columns={
        "tmdb_movie_id": {"data_type": "bigint", "nullable": False},
        "as_of_date": {"data_type": "date", "nullable": False},
        "ingested_at": {"data_type": "timestamp", "nullable": False},
        "rank": {"data_type": "bigint"},
        "page": {"data_type": "bigint"},
        "release_date": {"data_type": "date"},
        "vote_count": {"data_type": "bigint"},
        "adult": {"data_type": "bool"},
        "video": {"data_type": "bool"},
        # Keep as JSON in the main table (avoid dlt normalization into child tables).
        "genre_ids": {"data_type": "json"},
        "payload_json": {"data_type": "json"},
    },
)
def trending_external_films_resource(
    api_key: str,
    max_pages: int,
) -> Iterable[dict]:
    ingested_at = datetime.now(timezone.utc)
    as_of_date = ingested_at.date()
    session = requests.Session()
    yielded = 0
    genre_map = _fetch_genres(session, api_key)
    try:
        for page in range(1, max_pages + 1):
            try:
                payload = _fetch_tmdb_popular_page(
                    session,
                    api_key=api_key,
                    page=page,
                )
            except requests.HTTPError as exc:
                logging.warning("TMDB popular fetch failed for page %s: %s", page, exc)
                break
            results = payload.get("results") or []
            if not results:
                logging.info("TMDB popular returned no results for page %s", page)
                break
            logging.info("TMDB popular page %s: %s results", page, len(results))

            for idx, movie in enumerate(results):
                tmdb_movie_id = movie.get("id")
                if tmdb_movie_id is None:
                    continue

                genre_ids = movie.get("genre_ids") or []
                genre_names = [
                    genre_map.get(int(genre_id))
                    for genre_id in genre_ids
                    if isinstance(genre_id, int) and int(genre_id) in genre_map
                ]
                genre_names_str = ", ".join(genre_names)

                release_date_raw = movie.get("release_date")
                release_date: date | None = None
                if isinstance(release_date_raw, str) and release_date_raw:
                    try:
                        release_date = datetime.fromisoformat(release_date_raw).date()
                    except ValueError:
                        release_date = None

                yielded += 1
                yield {
                    "tmdb_movie_id": tmdb_movie_id,
                    "as_of_date": as_of_date,
                    "ingested_at": ingested_at,
                    "rank": (page - 1) * len(results) + (idx + 1),
                    "page": page,
                    "title": movie.get("title"),
                    "original_title": movie.get("original_title"),
                    "overview": movie.get("overview"),
                    "release_date": release_date,
                    "popularity": movie.get("popularity"),
                    "vote_average": movie.get("vote_average"),
                    "vote_count": movie.get("vote_count"),
                    "original_language": movie.get("original_language"),
                    "adult": movie.get("adult"),
                    "video": movie.get("video"),
                    "poster_path": movie.get("poster_path"),
                    "backdrop_path": movie.get("backdrop_path"),
                    "genre_ids": genre_ids,
                    "genre_names": genre_names_str,
                    "payload_json": movie,
                    "source": "tmdb:/movie/popular",
                }
    finally:
        logging.info("Emitted %s TMDB rows from API", yielded)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    resource = trending_external_films_resource(
        api_key=TMDB.config.auth.token,
        max_pages=10
    )

    pipeline = dlt.pipeline(
        pipeline_name="tmdb_trending_external_films",
        destination=postgres(
            credentials=PostgresCredentials(
                connection_string=FILMS_WAREHOUSE_TABLE.connection_string()
            )
        ),
        dataset_name="raw",
    )

    load_info = pipeline.run(resource)

    logging.info("Finished loading rows into table raw.trending_external_films")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
