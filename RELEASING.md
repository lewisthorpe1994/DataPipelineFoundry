# Releasing Foundry

This repository publishes ready-to-run archives that bundle the `foundry` CLI with a precompiled copy of the React UI. Releases are automated through `.github/workflows/release.yml` and only require Node.js/Rust during the build phase.

## Prerequisites

- Access to push tags and create GitHub Releases.
- Rust toolchain updated (`rustup update`).
- Node.js 20+ installed locally if you want to smoke-test the frontend build.

## Release checklist

1. **Pick a version** – bump `package.version` in `Cargo.toml` (and any other crates) following SemVer.
2. **Update changelog/notes** – edit `README.md` or your changelog with highlights.
3. **Run validation locally**
   ```bash
   cargo fmt
   cargo clippy --all-targets -- -D warnings
   cargo test
   (cd dpf_web/ui && npm ci && npm run build)
   ```
4. **Commit and tag** – commit the version bump and create an annotated tag, e.g. `git tag -a v0.5.0 -m "v0.5.0"`.
5. **Push the tag** – `git push origin v0.5.0`.
6. **Wait for the Release workflow** – `.github/workflows/release.yml` triggers on the tag push (or manual dispatch) and builds archives for Linux, macOS (x86_64 + arm64), and Windows. Each archive contains:
   - `foundry`/`foundry.exe`
   - `INSTALL.md`, `README.md`
   - `web-ui-dist/` – compiled React frontend
7. **Publish release notes** – once the workflow finishes, edit the GitHub Release description if needed.

## Manual runs

To test packaging without tagging, trigger the workflow from the GitHub Actions UI via *Run workflow*. This produces downloadable artifacts but skips attaching them to a release.

## Local packaging (optional)

If you need to mirror CI locally, set `VERSION=<tag or commit>` and mimic the workflow steps:

```bash
(cd dpf_web/ui && npm ci && npm run build)
cargo build --locked --release -p dpf
mkdir -p dist/foundry-$VERSION-linux-x86_64
# copy target/release/dpf -> dist/.../foundry
# copy dpf_web/ui/dist -> dist/.../web-ui-dist
# tar czf dist/foundry-$VERSION-linux-x86_64.tar.gz -C dist foundry-$VERSION-linux-x86_64
```

Those archives should match what CI produces and are perfect for testing before publishing.
