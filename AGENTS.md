# AGENTS.md

## Versioning and releases
- Do not use commit SHAs as user-facing version numbers.
- User-facing versions must follow Semantic Versioning: vMAJOR.MINOR.PATCH
- Current release line starts at v1.0.0 unless told otherwise.
- All release/version changes must be compatible with semantic-release.

## Commit message rules
Use Conventional Commits exactly:
- feat: for new features
- fix: for bug fixes
- feat!: for breaking changes
- Include BREAKING CHANGE: in the body when relevant

Examples:
- feat: add tenant dashboard filters
- fix: correct login redirect loop
- feat!: replace legacy authentication flow

## Release workflow
- Do not manually invent the next version number in code or docs unless asked.
- Assume GitHub Actions / semantic-release will calculate the next version tag.
- If a VERSION file exists, update it only if the workflow requires it.

## Pull requests
- In PR descriptions, summarize whether the change is expected to cause a patch, minor, or major release.
