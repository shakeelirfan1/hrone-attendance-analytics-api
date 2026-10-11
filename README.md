# Employee Attendance & Analytics API

FastAPI + PyMongo implementation of the HROne API contract. All application code is in `app/main.py`.

## Run

Requires Python 3.11+ and MongoDB 6.0+. Install the dependencies and configure `MONGO_URI` and `MONGO_DB` through environment variables (see `.env.example` for the variable names):

```bash
python -m venv .venv
# Activate .venv for your shell
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```

Indexes are created idempotently at startup. `GET /health` checks MongoDB readiness. `openapi.yaml` is the API contract; `DATA_MODEL.md` describes the stored collections. `REVIEW.md` and `DECISIONS.md` document findings and design decisions.

Do not commit `.env` files or credentials. Verify explain plans, concurrency behavior and performance against the target database before release.
