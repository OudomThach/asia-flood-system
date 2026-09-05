# Plan: Scope the Flood Platform from Cambodia → Asia

## Context

The system was built as a **Cambodia / Lower Mekong** flash-flood early-warning platform, then
partly stretched to "Pan-Asian" by **adding data rows and UI labels** — but the **engine logic was
never generalized**. Today the database seeds 63 stations across 12 countries and the dashboard has
an `ASIA` scope filter, yet every station is still evaluated with **Cambodia/Mekong-specific
constants, Khmer-only messaging, and Cambodia local time**. This produces silent correctness
failures at Asia scale:

- **One global risk threshold for all rivers.** `RiskClassificationEngine.evaluate` hardcodes
  `DISCHARGE_DANGER = 22,000 m³/s` / `CAUTION = 16,000` and rain 50/80 mm, applied to *every*
  station ([risk_engine.py:31](cambodia_flood_core/risk_engine.py:31)). A small urban river
  (Jakarta Ciliwung, Manila Pasig) physically never reaches 22,000 m³/s → it stays **"Normal"
  forever**; a huge river (Yangtze at Wuhan) is a different regime entirely. The README even
  *claims* "Thresholds vary per station catchment category" — the code does not. This is the
  headline defect.
- **Khmer-only alerts for 12 countries.** Telegram/SMS/CAP/voice all emit `name_km` + Khmer body
  regardless of country ([notifications.py:53](cambodia_flood_core/notifications.py:53),
  [cap_protocol.py:44](cambodia_flood_core/cap_protocol.py:44)); CAP hardcodes `Cambodia` and
  `km-KH` ([cap_protocol.py:100](cambodia_flood_core/cap_protocol.py:100)).
- **Cambodia time hardcoded.** Audit + alert logs stamp ICT/UTC+7 for all regions
  ([storage.py:732](cambodia_flood_core/storage.py:732),
  [storage.py:870](cambodia_flood_core/storage.py:870)) — wrong for India (UTC+5:30), Pakistan,
  Nepal, Indonesia, China, Philippines.
- **Kratie-only history & stage data.** Historical benchmarks are the Kratie Mekong 2000/2011/2020
  peaks applied to all areas ([risk_engine.py:242](cambodia_flood_core/risk_engine.py:242)); MRC
  stage thresholds only exist for Mekong gauges, everyone else falls to a generic 15/17 m default
  ([data_intake.py:344](cambodia_flood_core/data_intake.py:344)); transboundary routing is
  hardcoded Laos→Cambodia corridors ([data_intake.py:394](cambodia_flood_core/data_intake.py:394)).
- **Cambodia-locked identity.** Package `cambodia_flood_core`, DB `cambodia_flood.db`, CAP sender
  `admin@ncdm.gov.kh`, User-Agent, class names, `pyproject`/Docker/entry-points all say Cambodia.
- **Inconsistent claims.** Docs say "43 stations" in some places and "63 / 12 countries" in others;
  "34 tests" vs "48 tests"; several `/api/hazards/*` endpoints are synthetic but described as
  "real-time."

**Intended outcome:** a platform that is *genuinely* Asia-scoped — each station judged by
thresholds appropriate to its river basin, timestamps in its own local time, English as the
universal alert language (with local language optional and pluggable), and a consistent Asia-wide
identity in code and docs.

**Decisions locked with the user:** (1) real engine refactor, not a cosmetic rebrand;
(2) full rename to an Asia-wide identity; (3) English-first localization, Khmer retained for KH,
structured so other languages drop in later.

**Status update (partial rename already applied externally):** the package is now
`asia_flood_core/` and the DB is `asia_flood.db` (old `cambodia_flood.db` + egg-info still present).
So Phase 4's mechanical rename is largely done — remaining work is the *content* inside those files.
**All file paths below now live under `asia_flood_core/`, not `cambodia_flood_core/`.**

---

## Phase 0 — Dashboard UI Asia reframe (highest-visibility; do first)

The header banner was swapped to "Pan-Asian & Mekong... 63 Stations across 12 Asian Nations", but
the rest of `/admin` still opens and reads as a Cambodia console. Concrete fixes in
`asia_flood_core/admin_server.py`:

1. **Titles & identity.** Browser `<title>` (`admin_server.py:1792`) and the FastAPI `title=`
   (`admin_server.py:64`) still say "Cambodia Flash-Flood... National Command" → make Asia-wide.
2. **Default scope = Asia, not Cambodia.** `scopeKhBtn` is `class="country-pill active"` by default
   (`admin_server.py:2567`) and `setCountryScope` defaults KPI label to "Cambodia Stations"
   (`admin_server.py:3513`). Default the landing scope to `ALL` (all 63) or `ASIA` so the platform
   presents as regional on load; keep KH/LA/ASIA/ALL as filters.
3. **Clock & time labels.** Live clock is hardcoded `ICT` (`admin_server.py:2560`); audit/alert
   tables are labeled "Cambodia Time (ICT)" (`admin_server.py:3077`, `:3082`, `:3101`). Make the
   clock UTC (or user-selectable) and rename columns to "Local Time" / show UTC — ties into the
   Phase 2 per-area local-time work.
4. **Cascade tab.** Tab 3 "Mekong Downstream Cascade" (`admin_server.py:2726`+) is hardcoded to the
   four Cambodian mainstem stations with 🇰🇭 flags. Either (a) relabel it clearly as the *Mekong*
   corridor view (one basin among many) or (b) make the corridor data-driven so it can show other
   basins. Option (a) is the honest, low-risk choice for the proposal.
5. **Search & chrome.** Search placeholder "Search province (e.g. Kratie…)" (`admin_server.py:2708`)
   → "Search station / country…"; the flag box + Khmer-only sub-title (`admin_server.py:1951`) →
   neutral Asia branding.
6. **Voice/EWS card.** "Cambodia EWS 1294 Voice Gateway" with Khmer-only IVR (`admin_server.py:3178`)
   → present as one country's channel, or generalize the label; ties into Phase 3 localization.
7. **Hazard-tab copy.** Multiple strings frame impacts as "Cambodian river crests" / "Mekong
   cascade" (e.g. `admin_server.py:3306`, `:245`, `:1192`) — reword to regional Asia framing.

Verification: load `/admin`, confirm it opens on the full Asia grid, the tab title/clock/labels are
Asia-neutral, and the map/KPIs show all 12 countries by default.

---

## Recommended approach (phased — each phase is independently shippable)

### Phase 1 — Per-basin risk thresholds (the core correctness fix)

This is the change that actually makes the system "work for Asia." Everything else is supporting.

1. **Introduce a basin-profile table.** Add a `BASIN_PROFILES` dict in `risk_engine.py` keyed by
   the existing `Area.basin_category` values already seeded in
   [storage.py:189](cambodia_flood_core/storage.py:189) (`mekong`, `tonle-sap`, `coastal`,
   `highland`, `laos-*`, plus the pan-Asian ones: `brahmaputra`, `ganges`, `meghna`, `indus`,
   `himalayas`, `yangtze`, `lancang`, `chao-phraya`, `red-river`, `mekong-delta`, `irrawaddy`,
   `philippines`, `indonesia`). Each profile holds `discharge_caution`, `discharge_danger`,
   `rain_heavy`, `rain_severe`, and a compound-combo discharge floor. Model this on the existing
   per-station `MRC_STATION_METADATA` pattern ([data_intake.py:344](cambodia_flood_core/data_intake.py:344))
   — the codebase already knows how to key hydrology config by area, so reuse that shape.
2. **Make the engine profile-aware.** Change `RiskClassificationEngine.evaluate(reading)` →
   `evaluate(reading, area)` (or accept a resolved profile), looking up the basin profile and
   falling back to the current Mekong constants when a basin is unknown (backward compatible).
   Update `get_threshold_summary` to report the resolved profile instead of the fixed Kratie block.
   Apply the same basin lookup to `CompoundRiskEngine.evaluate_compound_risk`
   ([risk_engine.py:156](cambodia_flood_core/risk_engine.py:156)), whose `DISCHARGE_BASELINE`/
   `DISCHARGE_MAX_CAP`/`RAIN_MAX_CAP` are likewise Mekong-scaled.
3. **Thread `area` through call sites.** The global singleton
   `engine = RiskClassificationEngine()` ([admin_server.py:38](cambodia_flood_core/admin_server.py:38))
   and the pipeline ([integration.py:58](cambodia_flood_core/integration.py:58)) both call
   `evaluate(reading)` with no area context — pass the `Area` (already fetched as `target_area`).
   Re-seed logic in [storage.py:286](cambodia_flood_core/storage.py:286) must pass the area too.
4. **Re-tune the demo seed.** [storage.py:289](cambodia_flood_core/storage.py:289) gives every
   non-Champasak/Stung-Treng station a flat `1500 m³/s` — set seed discharges that are plausible
   per basin so the dashboard shows a realistic spread of risk levels across Asia.

### Phase 2 — Region-aware time, benchmarks, stage & routing

1. **Local time by area, not hardcoded ICT.** Add a `country → UTC offset` (or IANA tz) map and
   derive `timestamp_local` from the event's area in `log_audit_event` and
   `log_user_alert_dispatch` ([storage.py:726](cambodia_flood_core/storage.py:726),
   [storage.py:856](cambodia_flood_core/storage.py:856)); keep UTC as the stored canonical. Rename
   the `*_ict` columns/labels to `*_local` (additive migration like the existing `country`/
   `basin_category` migration at [storage.py:87](cambodia_flood_core/storage.py:87)).
2. **Per-basin historical benchmarks.** Generalize `evaluate_historical_benchmark`
   ([risk_engine.py:242](cambodia_flood_core/risk_engine.py:242)) to look up known peaks per basin
   (Kratie Mekong 2000/2011/2020 stays as the `mekong` entry; add e.g. Chao Phraya 2011, Indus
   2010/2022, Brahmaputra); return a "no baseline available" result rather than Kratie numbers when
   a basin has none.
3. **Extend MRC/stage metadata beyond Mekong.** Broaden `MRC_STATION_METADATA` (or rename to a
   generic `GAUGE_METADATA`) so the pan-Asian gauges have real alarm/flood stages instead of the
   generic 15/17 m fallback ([data_intake.py:365](cambodia_flood_core/data_intake.py:365)).
4. **Generalize transboundary routing.** `calculate_transboundary_surge_routing`
   ([data_intake.py:394](cambodia_flood_core/data_intake.py:394)) is Pakse→Kratie→Phnom Penh only.
   Drive it from an upstream→downstream corridor table so it can express other basins (or scope it
   explicitly to Mekong and label it as such in the API/docs — acceptable if time-boxed).

### Phase 3 — English-first localization

1. **Language resolution by country.** Add a `language` field to `Area` (default `en`, `km` for KH)
   and a small `country → language` map. Keep the existing bilingual EN/KM structure but make the
   **second** language driven by the area instead of always Khmer.
2. **Message templates keyed by language.** Refactor `notifications.py`, `cap_protocol.py`, the FR10
   test-alert text in [integration.py:106](cambodia_flood_core/integration.py:106), and voice/IVR
   payloads so `message_en` is always populated and `message_local` is resolved from a template
   registry (`{en: ..., km: ...}`), falling back to English when no local template exists. This is
   the "structured so other languages drop in later" requirement — no need to actually author
   Hindi/Bangla/Thai now, just stop forcing Khmer on non-KH areas.
3. **CAP correctness.** Replace hardcoded `Cambodia` / `km-KH` / sender `admin@ncdm.gov.kh`
   ([cap_protocol.py:100](cambodia_flood_core/cap_protocol.py:100),
   [models.py:153](cambodia_flood_core/models.py:153)) with area-derived country name, language
   code, and a neutral sender identity.

### Phase 4 — Full rename to Asia-wide identity

Do this **last** (it's mechanical and touches everything; doing it first would churn every diff).

- Rename package `cambodia_flood_core/` → `asia_flood_core/` (proposed name) and update all imports
  across `cambodia_flood_core/*.py` and `tests/*.py`.
- DB default `cambodia_flood.db` → `asia_flood.db` ([storage.py:18](cambodia_flood_core/storage.py:18)),
  plus `.env.example`, `docker-compose.yml`, `Dockerfile`.
- `pyproject.toml` name/description/scripts + `egg-info` entry points (`cambodia-flood` →
  `asia-flood`), User-Agent strings in `data_intake.py`, class docstrings, and the CAP sender.
- Provide a one-time rename script / or keep the old DB filename working by env override so existing
  `cambodia_flood.db` data isn't orphaned.

### Phase 5 — Documentation & honesty pass

- Reconcile counts everywhere to the true seed: **63 stations / 12 countries** (README currently
  says 43 in the overview and 48 vs 34 tests). Verify the real test count with `pytest --collect-only`.
- Mark which `/api/hazards/*` endpoints are live vs. modeled/synthetic so the proposal doesn't
  overclaim "real-time" for mock feeds.
- Update `OUDOM_RESPONSIBILITIES.txt` narrative to describe the *new* per-basin engine (it's the
  strongest "why" story for the defense — "one threshold cannot serve both the Ciliwung and the
  Yangtze").

---

## Critical files

| File | Role in this change |
|---|---|
| [risk_engine.py](cambodia_flood_core/risk_engine.py) | Add `BASIN_PROFILES`; make both engines profile-aware; generalize benchmarks. **Core of Phase 1–2.** |
| [storage.py](cambodia_flood_core/storage.py) | Basin seed data + realistic seed discharges; local-time logging; additive column migration. |
| [data_intake.py](cambodia_flood_core/data_intake.py) | Extend gauge metadata beyond Mekong; generalize transboundary routing; User-Agent. |
| [integration.py](cambodia_flood_core/integration.py) | Pass `Area` into `evaluate`; localize FR10 test-alert text. |
| [admin_server.py](cambodia_flood_core/admin_server.py) | Thread area into risk eval; region-aware time labels; ~134 Cambodia refs to reconcile. |
| [notifications.py](cambodia_flood_core/notifications.py), [cap_protocol.py](cambodia_flood_core/cap_protocol.py) | Language-by-area templates; CAP country/language/sender. |
| [models.py](cambodia_flood_core/models.py) | Add `language` (and optional basin-profile ref) to `Area`; neutral CAP defaults. |
| `pyproject.toml`, `Dockerfile`, `docker-compose.yml`, `.env.example`, `tests/*` | Phase 4 rename. |

## Reuse (don't build new)

- `Area.basin_category` and `Area.country` already exist and are seeded for all 63 stations — the
  profile lookup keys off data that's already there.
- `MRC_STATION_METADATA` ([data_intake.py:344](cambodia_flood_core/data_intake.py:344)) is the exact
  pattern to copy for basin profiles and extended gauges.
- The additive `PRAGMA table_info` migration at [storage.py:87](cambodia_flood_core/storage.py:87)
  is the template for adding `language` / renaming `*_ict` columns without dropping the DB.
- `get_all_areas(country=...)` filtering already supports scoping queries by country.

---

## Verification

1. **Unit tests per basin.** Extend `tests/test_risk_engine.py` with cases proving a mid-size flow
   is `Danger` on a small-river profile but `Normal`/`Caution` on the Mekong profile — i.e. the same
   discharge yields *different* levels by basin. This is the regression guard for the headline bug.
2. **Full suite green:** `python -m pytest tests/ -v` (update any tests that call
   `evaluate(reading)` without an area; confirm the true test count and fix the docs to match).
3. **End-to-end dashboard check:** run
   `uv run uvicorn <new_pkg>.admin_server:app --port 8000`, open `/admin`, switch the scope filter
   to `ASIA`, and confirm non-Mekong stations now show a realistic spread of risk levels (not all
   "Normal"). Spot-check `/api/gis/stations.geojson` for varied `risk_level` values.
4. **Localization spot-check:** trigger a test alert for an India/Indonesia station and confirm the
   message is English (not Khmer) and CAP `language`/country reflect the area; trigger one for a KH
   station and confirm Khmer is still present.
5. **Time spot-check:** fire alerts for KH and IN stations and confirm the local timestamps differ
   (UTC+7 vs UTC+5:30) while stored UTC matches.
6. **Rename smoke test:** `pip install -e .` (or `uv sync`), then the new `asia-flood` /
   `asia-demo` console scripts and `docker compose up` build cleanly.
