# DVDRental Example — What it shows

This example is a medallion-style project using the public DVDRental dataset. It demonstrates the current end-to-end flow:

- **Scaffolded project**: `foundry-project.yml` / `connections.yml` / `foundry_models` laid out by medallion layers (bronze/silver/gold).
- **Model DAG**: SQL models using `ref` and `source` compile to `compiled/manifest.json` and `compiled/dag.dot`.
- **Execution**: `foundry run` can execute the compiled SQL against the configured connection profile (see `connections.yml`).
- **Python workspace stub**: `foundry_python/` reserved for ingestion or custom logic (placeholder).
- **UI**: Load `compiled/manifest.json` in the web viewer to see the DAG.

To try it:
```bash
cargo run -p foundry -- compile --project example/dvdrental_example
cargo run -p foundry -- run --project example/dvdrental_example
# Or view the DAG:
# cargo run -p foundry_backend -- --manifest example/dvdrental_example/compiled/manifest.json --static-dir foundry_web/ui/dist
```

What’s missing (future work):
- Hooks for ingestion (Kafka/API) wired into this example.
- Seeds/tests/freshness; incremental or snapshot patterns.
- A small Python job under `foundry_python/` that registers sources/destinations end-to-end.
