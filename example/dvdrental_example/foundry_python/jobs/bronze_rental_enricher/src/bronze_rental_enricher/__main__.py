from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from typing import Iterable, Optional

import dlt
import psycopg
import requests

TMDB_DISCOVER_URL = "https://api.themoviedb.org/3/discover/movie"


@dataclass
class FilmRow:
    film_id: int
    title: str
    release_year: Optional[int] = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch TMDB metadata and load it into the film_supplimentatry table via dlt",
    )
    parser.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"), help="Source Postgres host")
    parser.add_argument(
        "--pg-port",
        type=int,
        default=int(os.getenv("PGPORT", "5432")),
        help="Source Postgres port",
    )
    parser.add_argument("--pg-user", default=os.getenv("PGUSER", "postgres"), help="Source Postgres user")
    parser.add_argument(
        "--pg-password",
        default=os.getenv("PGPASSWORD", "postgres"),
        help="Source Postgres password",
    )
    parser.add_argument(
        "--pg-database",
        default=os.getenv("PGDATABASE", "dvdrental"),
        help="Database that hosts the film table",
    )
    parser.add_argument(
        "--dest-host",
        default=os.getenv("DEST_HOST") or os.getenv("PGHOST", "localhost"),
        help="Destination Postgres host",
    )
    parser.add_argument(
        "--dest-port",
        type=int,
        default=int(os.getenv("DEST_PORT") or os.getenv("PGPORT", "5432")),
        help="Destination Postgres port",
    )
    parser.add_argument(
        "--dest-user",
        default=os.getenv("DEST_USER") or os.getenv("PGUSER", "postgres"),
        help="Destination Postgres user",
    )
    parser.add_argument(
        "--dest-password",
        default=os.getenv("DEST_PASSWORD") or os.getenv("PGPASSWORD", "postgres"),
        help="Destination Postgres password",
    )
    parser.add_argument(
        "--dest-database",
        default=os.getenv("DEST_DATABASE", "dvdrental_analytics"),
        help="Destination Postgres database",
    )
    parser.add_argument(
        "--dest-schema",
        default=os.getenv("DEST_SCHEMA", "film_enrichment"),
        help="Destination schema name",
    )
    parser.add_argument(
        "--language",
        default="en-US",
        help="Preferred TMDB language when selecting keyword matches",
    )
    parser.add_argument(
        "--tmdb-api-key",
        help="TMDB API key. Falls back to the TMDB_API_KEY environment variable if omitted",
    )
    return parser.parse_args()


def load_films(conn_str: str) -> list[FilmRow]:
    films: list[FilmRow] = []
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT film_id, title, release_year FROM film")
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


@dlt.resource(name="film_supplimentatry", write_disposition="merge", primary_key="film_id")
def film_supplimentatry_resource(
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
    args = parse_args()

    api_key = args.tmdb_api_key or os.getenv("TMDB_API_KEY")
    if not api_key:
        raise SystemExit("TMDB API key is required (set TMDB_API_KEY or pass --tmdb-api-key)")

    source_conn = (
        f"host={args.pg_host} port={args.pg_port} user={args.pg_user} "
        f"password={args.pg_password} dbname={args.pg_database}"
    )
    films = load_films(source_conn)
    if not films:
        logging.warning(
            "No films returned from dvdrental.film using connection %s:%s. Check credentials/permissions.",
            args.pg_host,
            args.pg_port,
        )
        return 0

    resource = film_supplimentatry_resource(films, api_key=api_key, language=args.language)

    dest_credentials = {
        "database": args.dest_database,
        "host": args.dest_host,
        "port": args.dest_port,
        "user": args.dest_user,
        "password": args.dest_password,
    }
    pipeline = dlt.pipeline(
        pipeline_name="tmdb_film_supplimentatry",
        destination="postgres",
        dataset_name=args.dest_schema,
        credentials=dest_credentials,
    )

    load_info = pipeline.run(resource)
    metrics = getattr(load_info, "metrics", {}) or {}
    rows_loaded = metrics.get("rows") or metrics.get("rows_loaded") or 0
    logging.info(
        "Loaded %s TMDB supplement rows into table film_supplimentatry (schema=%s, database=%s)",
        rows_loaded,
        args.dest_schema,
        args.dest_database,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
