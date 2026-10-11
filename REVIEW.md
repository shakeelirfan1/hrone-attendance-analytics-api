# Code Review

## Defects identified and fixes

1. **Employee input model (`app/main.py`, `EmployeeIn`).** The original model accepted arbitrary employee codes and did not enforce the contract's length/pattern constraints. Added the `EMP` + 4–6 digits rule, name/department/email bounds and shift-time patterns; added strict date validation.
2. **Employee response serialization (`create_employee`, `list_employees`).** MongoDB datetimes were returned as ISO strings rather than epoch milliseconds. Responses now use the shared serializer, and server-generated `created_at` is truncated to whole seconds.
3. **Timestamp validation (`parse_punch_timestamp`, punch-out and correction).** Epoch values must be integers in the supported millisecond range; strings/floats/seconds-looking values are rejected with 422, and accepted instants are truncated to seconds.
4. **Punch-in race (`punch_in`, unique `(emp_code, date)` index).** A pre-insert read alone could race. The unique index remains the final authority; duplicate-key failure is mapped to 409.
5. **Punch-out selection and race (`punch_out`).** Select the most recent punch-in at or before the requested instant, return 404 when no punch-in exists, 409 for an already-closed record, and use a conditional update so concurrent punch-outs cannot both succeed. Shift calculations account for overnight boundaries.
6. **Attendance listing (`list_attendance`).** Added ISO-date and date-range validation, status validation, total-before-pagination behavior, and required ordering by date descending then employee code ascending.
7. **Regularization request (`AttendanceCorrectionIn`, `correct_attendance`).** The contract field is `regularized_by` (the stored history key remains `by`). Added reason/user bounds, rejected extra/derived fields and punch times supplied alongside ABSENT/LEAVE, validated date/status/timestamp combinations, recomputed derived values and preserved correction history. Optimistic matching covers the state used for calculations.
8. **Overnight lateness (`compute_late_minutes`).** The shift-start date is adjusted for overnight shifts when the local punch time falls before shift end; the ten-minute grace period and full elapsed late minutes follow R2.
9. **Leaderboard response (`late_leaderboard`).** Results use the contract's `items` key. `$rank` preserves ties, the limit is applied after ranking, and equal totals are ordered by employee code.
10. **Department trend (`build_department_trend_pipeline`, `department_trend`).** Replaced Python-side row calculation with a shared database-level aggregation that generates every date, looks up eligible employees/attendance, computes weekday/headcount/presence/rates in MongoDB and uses `$setWindowFields` for the rolling average. The response is `{department, items}` and works when a date has no attendance logs.
11. **Explain endpoint (`explain_endpoint`).** Returns the specified `{endpoint, collection, explain}` envelope. The trend explain uses the same pipeline builder as the live trend endpoint; attendance-list explain filters and sort match the live list query.
12. **Indexes (`create_indexes`).** Startup creates idempotent indexes for unique employee codes, department and employee listing, join dates, unique employee/date attendance, attendance date ordering, employee history and leaderboard lookup.

## Behaviors reviewed

- Monthly metrics use an aggregation for stored attendance metrics; weekday working-day count accounts for mid-month joiners.
- Department summary starts from eligible employees so employees with no attendance logs still contribute to headcount.
- Attendance response serialization recursively converts stored instants in records and history to epoch milliseconds, hides MongoDB `_id` values, and defaults missing `history`/`half_day` fields to `[]`/`false`.

## Verification and limits

- This revision passed Python syntax compilation, OpenAPI YAML parsing, path-set comparison (all 11 paths), request-model checks, R2 lateness examples, overnight date/overtime checks, and a focused FastAPI integration test using an in-memory fake database for create/punch/correction behavior, response serialization, status codes and the explain envelope.
- These local tests do not execute MongoDB aggregation operators. Run this revision against MongoDB before submission to validate the collectionless `$documents` trend pipeline, query plans (IXSCAN/no COLLSCAN), concurrent requests, and results on the evaluator's 100,000-record dataset. Do not treat fake-database or small-sample tests as proof of large-scale performance.
