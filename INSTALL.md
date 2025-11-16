# Installing Foundry binaries

Official releases publish platform-specific archives produced by `.github/workflows/release.yml`. Each archive contains a `foundry` (or `foundry.exe`) binary plus a `web-ui-dist/` directory that holds the prebuilt React frontend.

## 1. Download and extract

1. Grab the archive for your OS/architecture from the GitHub Releases page.
2. Extract it to a directory on disk, for example:
   - macOS/Linux: `tar -xzf foundry-v0.1.0-linux-x86_64.tar.gz`
   - Windows: unzip `foundry-v0.1.0-windows-x86_64.zip` with Explorer or PowerShell.
3. (macOS/Linux only) mark the binary as executable if needed: `chmod +x foundry`.

## 2. Run the CLI

Invoke the CLI directly from the extracted folder or add it to your `PATH`:

```bash
./foundry --help
./foundry init -n my_project -a medallion
./foundry run -m <model>
```

## 3. Launch the bundled web UI

The release archive ships a compiled frontend so you do not need Node.js:

```bash
./foundry web --static-dir ./web-ui-dist --manifest /path/to/manifest.json
```

`web-ui-dist/` may be moved elsewhere, mounted into a container, or served by any static file host. Pass its new path to `--static-dir` when running the backend.

## 4. Updating

Download a newer archive and replace the extracted directory. Each archive is self-contained and does not share state with previous installations.
