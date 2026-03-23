# Test Coverage Analysis — hive-core

**Date:** 2026-03-23
**Codebase:** 29 Python files, ~22,300 lines of code

---

## Current State

The codebase has **near-zero test coverage**. Only one file (`agents/interactive_call.py`) contains inline self-tests (10 tests via raw `assert` statements in `__main__`). There is:

- No test framework (no pytest, unittest, or nose)
- No test directory or test files
- No CI/CD pipeline
- No coverage tooling
- No test configuration

---

## Coverage by Module

| Module | Files | Lines | Tests | Coverage |
|--------|-------|-------|-------|----------|
| `core/` | 9 files | ~8,200 | 0 | **0%** |
| `agents/` | 3 files | ~3,400 | 10 inline | **~2%** |
| `services/` | 4 files | ~3,960 | 0 | **0%** |
| `content/` | 6 files | ~3,750 | 0 | **0%** |
| `mcp/` | 7 files | ~2,850 | 0 | **0%** |

---

## Priority Areas for Test Improvement

### P0 — Critical (Security & Financial Risk)

#### 1. `services/webhook_server.py` (2,001 lines — 0 tests)

This is the most critical untested file. It handles inbound phone calls, routes voice commands, creates dispatch jobs (locksmith leads), and sends SMS/Telegram notifications. Key risks:

- **Missing Twilio webhook signature verification** — any attacker can POST fake webhooks
- **Voice command authentication relies solely on Caller ID** — trivially spoofable
- **No rate limiting** — attacker could trigger thousands of SMS to Chris
- **Dispatch job creation from unvalidated input** — potential for fake jobs

**Recommended tests:**
- Twilio signature validation (add it, then test it)
- Caller routing logic (`_is_external_caller`, family vs. external vs. client)
- Phone number normalization ("+1" prefix handling, formatting edge cases)
- Voice command parsing (`_try_hive_command`)
- Client lookup by phone number (`_get_client_for_number`)
- Caller info extraction regexes (`_extract_caller_info`)
- After-hours detection (timezone boundaries, DST transitions)
- AI model failover chain (`_phi4_response` with 5 endpoints)
- TwiML XML validity for all response paths
- Test number filtering (all test numbers excluded from notifications)

#### 2. `core/hive_mind.py` (983 lines — 0 tests)

Autonomous decision-making engine with 27 AI "Queens" that auto-execute business decisions when confidence > 60%. A bug here could cause cascading bad decisions.

**Recommended tests:**
- Queen selection by keyword/domain matching
- Confidence score extraction from AI responses
- Auto-execute threshold enforcement (only approved domains, only > 60%)
- Human-approval domains never auto-execute (trading, revenue, security)
- Consensus synthesis parsing
- Cycle state persistence (decisions stored correctly)
- Service health check aggregation (`alnitak_scan`)
- Inference endpoint failover ordering

#### 3. `services/locksmith_webhook.py` (64 lines — 0 tests)

Small but security-critical — no Twilio signature verification, hardcoded credentials.

**Recommended tests:**
- TwiML response is valid XML
- SMS notification triggered on POST
- Credential handling (should come from env vars, not hardcoded)

---

### P1 — High Priority (Business Logic Correctness)

#### 4. `core/quality_grader.py` (555 lines — 0 tests)

The scoring algorithm (0–100) is the quality gate before YouTube upload. Incorrect scoring means bad content goes live or good content gets rejected.

**Recommended tests:**
- Each scoring rule independently:
  - Episode < 30s → -20 points
  - File < 1MB → -30 points
  - No audio → -50 points
  - Resolution < 1080p → -10 points
  - SDXL art (>2MB) → +20 points
  - Duration 60–300s → +10 points
  - Short > 60s → -30 points
- Score clamping to 0–100 boundaries
- Verdict assignment at boundaries (49→REJECT, 50→NEEDS_IMPROVEMENT, 69→NEEDS_IMPROVEMENT, 70→UPLOAD_READY)
- `ffprobe_inspect` failure handling (missing binary, invalid file, timeout)
- `build_filename` path traversal prevention
- Grade persistence and retrieval from SQLite

#### 5. `core/hive_failover.py` (358 lines — 0 tests)

Manages service redistribution when machines go down. Bugs here mean services silently die.

**Recommended tests:**
- 3-strike failure threshold (fail at 3, not 2 or 4)
- Failover priority ordering (higher-priority machine takes over first)
- Service hand-back when primary recovers
- TCP fallback when HTTP health check fails
- Multiple simultaneous machine failures
- Port conflict detection before service start
- Process cleanup on service stop
- State consistency across start/stop cycles

#### 6. `core/hive_swarm.py` (682 lines — 0 tests)

Multi-agent debate engine for predictions (forex, leads, SEO, revenue).

**Recommended tests:**
- Agent round execution (correct context passed)
- Confidence percentage extraction from responses
- Mind-change detection between rounds
- Consensus score calculation (average of final-round confidences)
- Debate round sequencing (round 0 = independent, round 1+ = sees others' views)
- Simulation state persistence
- Outcome tracking and accuracy calculation

#### 7. `core/hive_operations.py` (410 lines — 0 tests)

24/7 monitoring with auto-cleanup and reporting. Uses `shell=True` subprocess calls.

**Recommended tests:**
- Report scheduling (7 AM and 9 PM ET, no duplicates in same hour)
- Disk usage parsing from `df` output
- Auto-cleanup triggers at correct thresholds (85% warning, 95% critical)
- Service restart logic for dead services
- Email and Telegram report delivery fallbacks
- `run_cmd` input sanitization (shell=True injection risk)

---

### P2 — Medium Priority (Functional Correctness)

#### 8. `agents/interactive_call.py` (1,843 lines — 10 tests)

Has inline tests but several gaps remain:

**Additional tests needed:**
- Concurrent outbound calls (session isolation)
- Speech confidence edge cases
- Director context building failure handling
- Jarvis mode (untested entirely)
- TwiML XML validity with special characters in business names
- Session memory cleanup (currently in-memory only)

#### 9. `agents/ai_cold_caller.py` (1,487 lines — 0 tests)

**Recommended tests:**
- Call scheduling logic
- Prospect list management
- Call outcome recording
- Rate limiting / do-not-call compliance

#### 10. `core/hive_content_empire.py` (1,836 lines — 0 tests)

**Recommended tests:**
- Content pipeline orchestration
- Template rendering
- Asset management

---

### P3 — Lower Priority

- `content/` modules (shorts_factory, anime_producer, etc.) — content generation pipelines
- `mcp/` servers — MCP tool definitions (largely declarative)
- `agents/director_gated.py` — small gating logic

---

## Recommended Testing Infrastructure

### 1. Adopt pytest

```bash
pip install pytest pytest-asyncio pytest-cov aioresponses
```

### 2. Create test directory structure

```
tests/
  conftest.py          # Shared fixtures (mock Twilio, mock Ollama, test DB)
  core/
    test_quality_grader.py
    test_hive_mind.py
    test_hive_failover.py
    test_hive_swarm.py
    test_hive_operations.py
  services/
    test_webhook_server.py
    test_locksmith_webhook.py
  agents/
    test_interactive_call.py
    test_ai_cold_caller.py
```

### 3. Add pytest configuration

```ini
# pytest.ini
[pytest]
asyncio_mode = auto
testpaths = tests
```

### 4. Add coverage configuration

```ini
# .coveragerc
[run]
source = core,services,agents,content,mcp
omit = */tests/*

[report]
fail_under = 50
show_missing = true
```

### 5. Add CI pipeline (GitHub Actions)

```yaml
# .github/workflows/test.yml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install pytest pytest-asyncio pytest-cov aioresponses
      - run: pytest --cov --cov-report=term-missing
```

---

## Quick Wins (Highest ROI tests to write first)

1. **`test_quality_grader.py`** — Pure scoring logic, no external deps, easy to unit test
2. **`test_webhook_server.py` caller routing** — `_is_external_caller`, `_get_client_for_number` are pure functions
3. **`test_hive_failover.py` threshold logic** — Fail-count and priority logic are testable in isolation
4. **`test_hive_mind.py` queen selection** — Keyword-to-queen routing is a pure mapping
5. **`test_hive_swarm.py` confidence extraction** — Regex-based parsing, easy to test with fixtures

---

## Security-Critical Findings (Fix Before Testing)

These issues should be addressed alongside adding tests:

1. **Add Twilio webhook signature verification** to `webhook_server.py` and `locksmith_webhook.py`
2. **Remove hardcoded credentials** — move Twilio SID/token, Telegram bot token to env vars
3. **Replace `shell=True`** subprocess calls in `hive_operations.py` with parameterized commands
4. **Add input validation** for `build_filename` in `quality_grader.py` (path traversal)
5. **Add rate limiting** to webhook endpoints
