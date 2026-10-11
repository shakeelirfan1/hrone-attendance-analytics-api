# Technical Decisions

## 1. Indexes

Startup creates unique indexes on `employees.emp_code` and `attendance_logs(emp_code, date)` for identity and one row per employee/day. Department/code and join-date indexes support employee lists and headcount. Date/code and employee/date indexes support attendance pages and history; date/late-minutes/code supports leaderboard filtering.

## 2. Simultaneous punch-ins

Concurrent requests may both pass the initial read, but the unique employee/date index allows only one insert. The losing insert raises `DuplicateKeyError`, mapped to HTTP 409; one request should return 201.

## 3. Leaderboard ties

`$rank` ranks by total late minutes, so ties share rank and the next rank is skipped. Filtering `rank <= limit` after ranking includes every employee tied at the cutoff. Equal-ranked rows sort by employee code.

## 4. Employees without logs

Department summary starts from employees who joined by month-end and preserves empty attendance lookups. Those employees count toward headcount even without logs; their attendance metrics contribute zero.

## 5. Scaling

Benchmark explain plans and latency on representative data, keep filter/sort keys indexed, and monitor lookup selectivity and aggregation memory. At much larger scale, evaluate pre-aggregated metrics or workload isolation before sharding; preserve correction and audit-history consistency.
