# Publishing Guidebook (PyPI)

This guide explains **how**, **why**, and **what to do step-by-step** to publish `pandas-provenance`.

## 1) Is this release worth publishing?

Use this go/no-go checklist before publishing.

### Go (publish) only if all are true
- Core tests pass: `python -m pytest Pandas_Provenance_Project/ -v`
- Package builds: `python -m build`
- Package metadata validates: `python -m twine check dist/*`
- README and version are up to date
- CI status on `main` is green

### No-Go (delay release) if any are true
- CI is failing
- Core provenance behavior is untested
- Build or twine checks fail
- Known release blocker is unresolved

---

## 2) Which branch to publish from

Publish from `main` only.

Why:
- `main` is the integration branch for reviewed and merged changes.
- GitHub Actions workflows are configured to validate and publish from project state expected on `main`.

---

## 3) Pre-release preparation

1. Merge approved changes into `main`.
2. Bump version in `/home/runner/work/Pandas_Provenance/Pandas_Provenance/setup.py`.
3. Update release notes and README sections if feature scope changed.
4. Ensure repository secrets/environments for publishing are configured in GitHub.

---

## 4) Local validation (required)

From repository root `/home/runner/work/Pandas_Provenance/Pandas_Provenance`:

```bash
python -m pytest Pandas_Provenance_Project/ -v
python -m build
python -m twine check dist/*
```

Why this matters:
- Tests confirm functional stability.
- Build confirms wheel/sdist packaging integrity.
- Twine check confirms distribution metadata/rendering quality.

---

## 5) GitHub release workflow (publish path)

The workflow file is:
- `/home/runner/work/Pandas_Provenance/Pandas_Provenance/.github/workflows/publish.yml`

Current behavior:
1. Trigger on GitHub **Release published** event.
2. Build distribution artifacts.
3. Publish to **TestPyPI** environment.
4. Publish to **PyPI** environment.

Why staged publishing:
- TestPyPI gives a final safety gate before production PyPI.

---

## 6) Step-by-step publish runbook

1. Checkout latest `main`.
2. Run local validation commands (Section 4).
3. Push any final fixes to `main`.
4. Create and publish a GitHub Release tag (for example `v0.1.0`).
5. Watch GitHub Actions run for `Publish to PyPI`.
6. Confirm TestPyPI publish step succeeds.
7. Confirm PyPI publish step succeeds.
8. Install from PyPI in a clean environment and smoke-test import.

---

## 7) Post-release verification

1. Install package from PyPI:
   - `pip install pandas-provenance`
2. Run a basic import and tracker usage smoke test.
3. Confirm project page metadata is correct on PyPI.
4. Announce release and link changelog.

---

## 8) Rollback/incident response

If publish fails:
- Do not retry blindly.
- Inspect failed workflow job logs first.
- Fix root cause on branch, merge to `main`, and publish a new release/tag.

If bad package is already published:
- Publish a patched follow-up version quickly.
- Document issue and migration notes in release notes.
