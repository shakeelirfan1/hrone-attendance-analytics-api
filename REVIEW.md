# REVIEW.md

Defects found in the starter `app/main.py`, with a failing input for each, plus defects found in my own first pass.

| # | Where | What is wrong | How you'd notice it | Fix |
|---|---|---|---|---|
| 1 | `compute_late_minutes` | Shift start built on the punch-in's own date (breaks overnight shifts); `int(x/60)` on a float; no truncation to seconds; naive datetime used. | Shift 22:00-06:00, punch-in 02:00 IST is treated as 4 h late. Shift 09:30, punch-in 09:40:01 gives 10 only by float luck. | Convert to IST, truncate to seconds, pick the previous day's shift start for overnight shifts, floor on integer seconds, late only if > 600 s (R2). |
| 2 | `compute_work_hours` | Python `round()` is banker's rounding on a float; wrong for R4 (half-up). | 16182 s = 4.495 h should give 4.50; float rounding can give 4.49. | Exact `Decimal(seconds)/3600`, `ROUND_HALF_UP`, 2 decimals. |
| 3 | `compute_overtime` | No 30-minute threshold (R3); `max(0, ...)` returns 5 min as overtime; ignores overnight shifts; ignores shift_start. | Shift end 18:30, punch-out 18:40 returns 10, expected 0. | Overtime only if >= 30 min, shift end moved +1 day for overnight shifts, floored whole minutes. |
| 4 | `/health` | Always returns 200, even when MongoDB is down. | Stop MongoDB, `GET /health` still returns 200. | `ping` the DB; return 503 on failure. |
| 5 | `create_employee` | Check-then-insert race with no unique index; `created_at` is naive local time with microseconds, returned as ISO string, not epoch ms. No validation of codes, times, dates. | Two parallel POSTs with the same emp_code both get 201. Shift `25:99` accepted. | Unique index on `emp_code`; `DuplicateKeyError` -> 409; Pydantic patterns; UTC/IST-aware seconds-truncated `created_at`; epoch-ms serializer. |
| 6 | `list_employees` | `skip = page * page_size` skips the first page; `total` ignores the department filter; no sort; no page validation. | `page=1` returns page 2's rows; `department=X` returns total of all employees. | `(page-1)*page_size`; `count_documents(q)`; sort by `emp_code`; bounds -> 422. |
| 7 | `punch_in` | Unknown employee -> `None["shift_start"]` -> 500 (should be 404). | POST with unknown emp_code. | 404 check. |
| 8 | `punch_in` | `ts = fromtimestamp(...)` uses server local time, not IST; date taken from the wrong zone; `if body.punched_at` treats 0 as missing; floats/strings/seconds accepted; no overnight date rule (R1); microseconds kept. | Punch-in at 23:30 UTC gets the wrong IST date; `punched_at` in seconds accepted. | `parse_punch_timestamp` (strict int ms range, IST, truncated); `get_attendance_date` for overnight (R1). |
| 9 | `punch_in` | Check-then-insert race, no unique index. | Parallel identical punch-ins both return 201. | Unique `(emp_code, date)` index; `DuplicateKeyError` -> 409. |
| 10 | `punch_in` | Status not validated (any string stored, including ABSENT/LEAVE); response leaks `id` (stringified `_id`); `punch_in` returned as datetime, not epoch ms. | `status: "FOO"` returns 201. | `Literal` status; `_id` hidden; datetimes converted at the boundary. |
| 11 | `list_attendance` | Loads the whole collection into Python, sorts and slices in memory; `total` computed after load; no sort tie-breaker; no validation; `id` leaked. | 100k records: slow, memory heavy. `page=0` returns odd slices. | Mongo `find` + `sort(date desc, emp_code asc)` + `skip/limit`, `count_documents`; indexes `(date,-1,emp_code)` and `(emp_code,date)`; validation -> 422. |
| 12 | Models | `EmployeeIn`/`PunchInIn` accept anything (blank names, bad email, `status` free text, `punched_at` float). | `{"punched_at": 1.5}` accepted. | Field constraints, `StrictInt`, `Literal`. |
| 13 | `department_summary` (my first pass) | Used Python `true` spelled as lowercase `true` in the pipeline: `NameError` -> 500 on every call. | `GET /analytics/departments/summary?month=2026-01`. | Replaced with `True`. |
| 14 | `employee_monthly` (my first pass) | `$dateFromString` with `timezone: Asia/Kolkata` then `$dayOfWeek` evaluated in UTC: every date shifted back one weekday, so `present_days` counted the wrong days (R7). | A Monday-only present record counted as 0 present days. | Parse the date string as UTC and read the weekday in UTC (no timezone), in the shared builder. |
| 15 | Explain (my first pass) | Pipelines were copy-pasted from the endpoints; trend ran as a collection-less `$documents` pipeline so no `IXSCAN` can appear; raw explain output can contain BSON `Timestamp` (500 on replica sets). | `GET /admin/explain/department_trend` has no IXSCAN. | One builder per endpoint shared by live code and explain; trend now seeds from `employees` via the `department` index; explain output passed through `bson.json_util`. |

## Looked at and decided OK
- Unique index + `DuplicateKeyError` is the real authority for punch-in; the earlier `find_one` is only a fast path.
- Punch-out conditional update (`punch_out: None` in the filter) gives exactly one 200 under concurrency.
- PATCH uses an optimistic filter on every field it read and `$push`es the history entry in the same update; the loser gets 409.
- Leaderboard: `$setWindowFields` with `$rank` (1,2,2,4) then `rank <= limit` after ranking, ties ordered by `emp_code`.
- Department summary uses `$avg` over records, not per-employee averages; headcount starts from `employees`, so employees with no logs count.

## Verification and limits

Verified locally against MongoDB 8.3.4 using the attendance_assignment_test database:

- Python syntax compilation passed, and EmployeeIn rejected equal shift start/end times.
- Monthly analytics counted a PRESENT record dated Monday 2026-10-12 as 1.0 present day. For an employee joining on that date, October working days were 15 and attendance percentage was 6.6667%.
- The department trend pipeline starts with a department match, not a `$documents` stage. Its initial query used `EXPRESS_IXSCAN` with `department_1_emp_code_1`. The employee lookup used `department_1_joined_on_1_emp_code_1`, and the attendance lookup used `date_-1_emp_code_1`; both lookups reported zero collection scans.
- All five explain handlers (attendance_list, employee_monthly, department_summary, late_leaderboard, and department_trend) returned JSON-serializable results in local tests. Indexes were reported, with no `COLLSCAN` stages or positive collectionScans values in the inspected plans.

These tests used a small local dataset. They do not establish performance on 100,000 records, cover every hidden evaluator edge case, or verify an Atlas/replica-set deployment. Large-dataset performance and broader deployment testing remain outstanding.