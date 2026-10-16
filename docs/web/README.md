# Ursula docs site

Vite, React and MDX. Docs pages live in `src/content/docs/pages`.

```sh
npm ci
npm run dev        # latest docs at http://localhost:5173/docs
npm run build      # latest docs only, into dist/
npm run build:all  # latest docs plus every archived version, what CI deploys
node scripts/docs-versions.mjs verify
```

## Versioned docs

`/docs/` always serves the latest release. Older minor versions are listed in
`docs-versions.json` and served at `/docs/<id>/` (for example `/docs/0.7/`).

`scripts/docs-versions.mjs fetch` reads that version's MDX pages from its
release tag with `git archive`. `build` renders them with the current site code
into `dist/docs/<id>/`, with its own search index, llms files and a noindex
rule. The archived pages are never checked in.

To work on an archived version locally, run `node scripts/docs-versions.mjs fetch`
and then `URSULA_DOCS_VERSION=0.7 npm run dev`. To look at a full build, serve
`dist/` with `python3 -m http.server -d dist`, because `vite preview` skips
paths that contain a dot.

Rule: don't remove or change an MDX component an archived version still uses.
If that becomes a burden, freeze that version's built dist as a release asset
instead.
