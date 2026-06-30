# AGENTS.md

## Working rules
- Follow the repository release process.
- Do not use Git commit SHAs as user-facing release versions.
- User-facing versions must follow Semantic Versioning: `vMAJOR.MINOR.PATCH`.
- User-facing release versions are derived from the merged pull request number: PR `#416` becomes `v4.1.6`, PR `#500` becomes `v5.0.0`.
- Do not manually invent or hardcode the next release number unless explicitly asked.
- Assume release tags are created automatically by CI after merge to `main`.

## Commit message rules
Use Conventional Commits exactly:
- `fix:` for bug fixes
- `feat:` for new features
- `feat!:` or `fix!:` for breaking changes
- Include `BREAKING CHANGE:` in the commit body when relevant

Examples:
- `fix: correct login redirect loop`
- `feat: add tenant dashboard filters`
- `feat!: replace legacy authentication flow`

## Pull request rules
- In PR summaries, briefly state that the release version will follow the merged pull request number.
- Keep release notes user-facing and concise.

## Version file rule
- If a `VERSION` file exists, update it only when the workflow or task explicitly requires it.
- Do not change version strings in documentation just to guess the next release.

## Branch and merge assumptions
- Primary release branch is `main`.
- Changes merged to `main` should be eligible for automated release.
