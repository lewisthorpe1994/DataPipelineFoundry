# Docs

This folder is a GitHub Pages–compatible Jekyll site.

## Enable GitHub Pages

In your repo settings:

- **Settings → Pages**
- **Build and deployment**
  - If using GitHub Actions: select **GitHub Actions**
  - If using legacy Pages: select **Deploy from a branch** and choose `main` + `/docs`

## Local preview (optional)

GitHub Pages uses Jekyll. If you have Ruby/Jekyll locally:

```bash
cd docs
bundle exec jekyll serve
```
