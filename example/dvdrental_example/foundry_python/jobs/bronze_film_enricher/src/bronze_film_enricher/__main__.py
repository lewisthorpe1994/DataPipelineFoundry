from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Optional

from dpf_python import source, DataResourceType, FoundryConfig, destination

import dlt
import psycopg
import requests
from dlt.destinations import postgres

TMDB_DISCOVER_URL = "https://api.themoviedb.org/3/discover/movie"

FILMS_SOURCE_TABLE = source(name='dvd_rental', ep_type=DataResourceType.SOURCE_DB, identifier='public.film')
TMDB = source(name='tmdb_api', ep_type=DataResourceType.API)
FILMS_WAREHOUSE_TABLE = destination(name='dvdrentals_analytics', ep_type=DataResourceType.WAREHOUSE, identifier='raw.film_supplementary')

@dataclass
class FilmRow:
    film_id: int
    title: str
    release_year: Optional[int] = None


def load_films(conn_str: str) -> list[FilmRow]:
    films: list[FilmRow] = []
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT film_id, title, release_year FROM {}".format(FILMS_SOURCE_TABLE))
            for film_id, title, release_year in cur.fetchall():
                films.append(
                    FilmRow(
                        film_id=film_id,
                        title=title.strip() if title else title,
                        release_year=release_year,
                    )
                )
    return films


def _tmdb_request_params(title: str, language: str, api_key: str) -> dict[str, str]:
    return {
        "api_key": api_key,
        "include_adult": "false",
        "include_video": "false",
        "language": language,
        "page": "1",
        "sort_by": "popularity.desc",
        "with_keywords": title,
    }


def _fetch_tmdb_row(
    session: requests.Session,
    film: FilmRow,
    api_key: str,
    language: str,
) -> Optional[dict]:
    params = _tmdb_request_params(film.title, language, api_key)
    response = session.get(TMDB_DISCOVER_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    results = payload.get("results") or []
    if not results:
        logging.info("No TMDB match for '%s' (film_id=%s)", film.title, film.film_id)
        return None

    best_match = results[0]
    return {
        "film_id": film.film_id,
        "keyword_query": film.title,
        "film_release_year": film.release_year,
        "tmdb_movie_id": best_match.get("id"),
        "tmdb_title": best_match.get("title"),
        "overview": best_match.get("overview"),
        "popularity": best_match.get("popularity"),
        "release_date": best_match.get("release_date"),
        "vote_average": best_match.get("vote_average"),
        "vote_count": best_match.get("vote_count"),
        "language": language,
        "source": TMDB_DISCOVER_URL,
    }


@dlt.resource(name="film_supplementary", write_disposition="merge", primary_key="film_id")
def film_supplementary_resource(
    films: list[FilmRow],
    api_key: str,
    language: str,
) -> Iterable[dict]:
    session = requests.Session()
    for film in films:
        try:
            row = _fetch_tmdb_row(session, film, api_key=api_key, language=language)
        except requests.HTTPError as exc:
            logging.warning("TMDB lookup failed for '%s': %s", film.title, exc)
            continue
        if row:
            yield row


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    config = FoundryConfig()
    api_key = '9127fc7a1362d4b835d669216761589d'
    if not api_key:
        raise SystemExit("TMDB API key is required (set TMDB_API_KEY or pass --tmdb-api-key)")

    films = load_films(FILMS_SOURCE_TABLE.config)

    resource = film_supplementary_resource(films, api_key=api_key, language='en-us')

    dest_db_config = config.get_db_adapter_details("dvdrental_analytics")
    dest_credentials = {
        "database": dest_db_config.database,
        "host": dest_db_config.host,
        "port": dest_db_config.port,
        "user": dest_db_config.user,
        "password": dest_db_config.password,
    }

    pipeline = dlt.pipeline(
        pipeline_name="tmdb_film_supplementary",
        destination=postgres(),
        # credentials=dest_credentials,
    )

    load_info = pipeline.run(resource)
    metrics = getattr(load_info, "metrics", {}) or {}
    rows_loaded = metrics.get("rows") or metrics.get("rows_loaded") or 0
    logging.info(
        "Loaded %s TMDB supplement rows into table film_supplementary",
        rows_loaded,

    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
