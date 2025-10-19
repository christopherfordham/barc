
BARC v3.3.1 (Backend Patch)

Adds:
- Enhanced Rest Compliance:
  * Auto home/away policy by airport (defaults: LHR, LGW, LCY)
  * WOCL adjustment (EASA +2h; OMA Home +1h; OMA Away +30m)
  * Travel-time adjustment for OMA Away (+1h)
  * Detailed breakdown in `rest_checks`

Apply:
1) Replace `backend/main.py` with this version.
2) Add `backend/evaluator/rest_engine.py`.
3) Commit & push, then in Render: clear build cache, manual deploy.
4) /health -> version 3.3.1

Environment (optional):
- BARC_BASE_TZ=Europe/London
- BARC_BASE_AIRPORTS=LHR,LGW,LCY
