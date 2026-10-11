"""Employee Attendance & Analytics API.

Run from the repository root with ``uvicorn app.main:app --port 8000``.
Configuration is read from ``MONGO_URI`` and ``MONGO_DB``; real environment
variables take precedence over a local ``.env`` file.
"""
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Literal
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from bson import json_util
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    field_validator,
    model_validator,
)
load_dotenv()
client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
db = client[os.getenv("MONGO_DB", "attendance_db")]
IST = timezone(timedelta(hours=5, minutes=30))
app = FastAPI(
    title="Employee Attendance & Analytics API",
    version="2.0.0",
)
def create_indexes():
    # Employee business identifier must be unique.
    db.employees.create_index(
        [("emp_code", 1)],
        unique=True,
    )
    # Supports department filtering and employee listing.
    db.employees.create_index(
        [("department", 1), ("emp_code", 1)],
    )
    # Supports department summary and trend headcount date filters.
    db.employees.create_index(
        [("joined_on", 1)],
    )
    # Supports trend lookups by department and join date.
    db.employees.create_index(
        [("department", 1), ("joined_on", 1), ("emp_code", 1)],
    )
    # Prevents duplicate attendance records for an employee/date.
    db.attendance_logs.create_index(
        [("emp_code", 1), ("date", 1)],
        unique=True,
    )
    # Supports date-filtered attendance listing and sorting.
    db.attendance_logs.create_index(
        [("date", -1), ("emp_code", 1)],
    )
    # Supports employee attendance history queries.
    db.attendance_logs.create_index(
        [("emp_code", 1), ("date", -1)],
    )
    # Supports leaderboard aggregation by late minutes and date.
    db.attendance_logs.create_index(
        [("date", 1), ("late_minutes", -1), ("emp_code", 1)],
    )
def build_department_trend_pipeline(department: str, start, end):
    """Trend pipeline; run on `employees` (seed doc via department index). All metrics computed in MongoDB."""
    start_day = datetime.combine(start, datetime.min.time(), tzinfo=IST)
    end_exclusive = datetime.combine(
        end + timedelta(days=1), datetime.min.time(), tzinfo=IST
    )
    present_statuses = ["PRESENT", "WFH", "ON_DUTY"]
    return [
        {"$match": {"department": department}},
        {"$limit": 1},
        {"$project": {
            "_id": 0,
            "department": {"$literal": department},
            "start_day": {"$literal": start_day},
            "end_exclusive": {"$literal": end_exclusive},
        }},
        {"$set": {
            "day_offsets": {"$range": [0, {"$dateDiff": {
                "startDate": "$start_day", "endDate": "$end_exclusive",
                "unit": "day", "timezone": "Asia/Kolkata",
            }}]},
        }},
        {"$unwind": "$day_offsets"},
        {"$set": {
            "day": {"$dateAdd": {
                "startDate": "$start_day", "unit": "day",
                "amount": "$day_offsets", "timezone": "Asia/Kolkata",
            }},
        }},
        {"$set": {
            "date": {"$dateToString": {
                "format": "%Y-%m-%d", "date": "$day", "timezone": "Asia/Kolkata",
            }},
            "weekday": {
                "$dayOfWeek": {"date": "$day", "timezone": "Asia/Kolkata"}
            },
        }},
        {"$lookup": {
            "from": "employees",
            "let": {"dept": "$department", "attendance_date": "$date"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$department", "$$dept"]},
                    {"$lte": ["$joined_on", "$$attendance_date"]},
                ]}}},
                {"$project": {"_id": 0, "emp_code": 1}},
            ],
            "as": "employees",
        }},
        {"$set": {
            "headcount": {"$size": "$employees"},
            "employee_codes": {
            "$map": {
                "input": "$employees",
                "as": "employee",
                "in": "$$employee.emp_code",
            }
        },
        }},
        {"$lookup": {
            "from": "attendance_logs",
            "let": {"attendance_date": "$date", "employee_codes": "$employee_codes"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$date", "$$attendance_date"]}}},
                {"$match": {"$expr": {"$in": ["$emp_code", "$$employee_codes"]}}},
                {"$project": {"_id": 0, "status": 1, "half_day": 1, "late_minutes": 1}},
            ],
            "as": "attendance",
        }},
        {"$set": {
            "is_working_day": {"$and": [{"$gte": ["$weekday", 2]}, {"$lte": ["$weekday", 6]}]},
            "present_count": {"$sum": {"$map": {
                "input": "$attendance", "as": "log",
                "in": {"$cond": [
                    {"$in": ["$$log.status", present_statuses]},
                    {"$cond": [{"$eq": ["$$log.half_day", True]}, 0.5, 1]},
                    0,
                ]},
            }}},
            "late_count": {"$size": {"$filter": {
                "input": "$attendance", "as": "log",
                "cond": {"$gt": [{"$ifNull": ["$$log.late_minutes", 0]}, 0]},
            }}},
        }},
        {"$set": {
            "attendance_rate": {"$cond": [
                {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                {"$divide": [{"$floor": {"$add": [
                    {"$multiply": [{"$divide": ["$present_count", "$headcount"]}, 10000]},
                    0.5,
                ]}}, 10000]},
                None,
            ]},
        }},
        {"$setWindowFields": {
            "sortBy": {"day": 1},
            "output": {"_moving_avg_raw": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}},
        }},
        {"$set": {
            "moving_avg_7d": {"$cond": [
                {"$ne": ["$_moving_avg_raw", None]},
                {"$divide": [{"$floor": {"$add": [
                    {"$multiply": ["$_moving_avg_raw", 10000]}, 0.5,
                ]}}, 10000]},
                None,
            ]},
        }},
        {"$project": {
            "_id": 0, "date": 1, "is_working_day": 1, "headcount": 1,
            "present_count": 1, "late_count": 1, "attendance_rate": 1,
            "moving_avg_7d": 1,
        }},
        {"$sort": {"date": 1}},
    ]

def build_monthly_pipeline(emp_code, start_date, end_date):
    pipeline = [
        {
            "$match": {
                "emp_code": emp_code,
                "date": {
                    "$gte": start_date,
                    "$lte": end_date,
                },
            }
        },
        {
            "$addFields": {
                "_parsed_date": {
                    "$dateFromString": {
                        "dateString": "$date",
                        "format": "%Y-%m-%d",
                    }
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {
                                        "$in": [
                                            "$status",
                                            ["PRESENT", "WFH", "ON_DUTY"],
                                        ]
                                    },
                                    {
                                        "$gte": [
                                            {"$dayOfWeek": "$_parsed_date"},
                                            2,
                                        ]
                                    },
                                    {
                                        "$lte": [
                                            {"$dayOfWeek": "$_parsed_date"},
                                            6,
                                        ]
                                    },
                                ]
                            },
                            {
                                "$cond": [
                                    {"$eq": ["$half_day", True]},
                                    0.5,
                                    1,
                                ]
                            },
                            0,
                        ]
                    }
                },
                "leave_days": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$status", "LEAVE"]},
                            1,
                            0,
                        ]
                    }
                },
                "late_count": {
                    "$sum": {
                        "$cond": [
                            {"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]},
                            1,
                            0,
                        ]
                    }
                },
                "total_late_minutes": {
                    "$sum": {"$ifNull": ["$late_minutes", 0]}
                },
                "total_overtime_minutes": {
                    "$sum": {"$ifNull": ["$overtime_minutes", 0]}
                },
            }
        },
    ]
    return pipeline


def build_summary_pipeline(start_date, end_date, department):
    employee_match = {"joined_on": {"$lte": end_date}}
    if department is not None:
        employee_match["department"] = department

    pipeline = [
        {"$match": employee_match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "let": {"employee_code": "$emp_code"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$emp_code", "$$employee_code"]
                            },
                            "date": {
                                "$gte": start_date,
                                "$lte": end_date,
                            },
                        }
                    }
                ],
                "as": "attendance",
            }
        },
        {
            "$unwind": {
                "path": "$attendance",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$group": {
                "_id": "$department",
                "employee_codes": {"$addToSet": "$emp_code"},
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$attendance.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                            {
                                "$cond": [
                                    {
                                        "$and": [
                                            {"$gte": [{"$dayOfWeek": {"$dateFromString": {"dateString": "$attendance.date", "format": "%Y-%m-%d"}}}, 2]},
                                            {"$lte": [{"$dayOfWeek": {"$dateFromString": {"dateString": "$attendance.date", "format": "%Y-%m-%d"}}}, 6]},
                                        ]
                                    },
                                    {"$cond": [{"$eq": ["$attendance.half_day", True]}, 0.5, 1]},
                                    0
                                ]
                            },
                            0
                        ]
                    }
                },
                "work_hours_values": {
                    "$push": {
                        "$cond": [
                            {
                                "$and": [
                                    {
                                        "$in": [
                                            "$attendance.status",
                                            ["PRESENT", "WFH", "ON_DUTY"],
                                        ]
                                    },
                                    {
                                        "$ne": [
                                            "$attendance.work_hours", None
                                        ]
                                    },
                                ]
                            },
                            "$attendance.work_hours",
                            None,
                        ]
                    }
                },
                "late_count": {
                    "$sum": {
                        "$cond": [
                            {"$gt": ["$attendance.late_minutes", 0]},
                            1,
                            0,
                        ]
                    }
                },
                "total_late_minutes": {
                    "$sum": {"$ifNull": ["$attendance.late_minutes", 0]}
                },
                "leave_count": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$attendance.status", "LEAVE"]},
                            1,
                            0,
                        ]
                    }
                },
                "on_duty_count": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$attendance.status", "ON_DUTY"]},
                            1,
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": {"$size": "$employee_codes"},
                "present_days": 1,
                "avg_work_hours": {"$avg": "$work_hours_values"},
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]
    return pipeline


def build_leaderboard_pipeline(start_date, end_date, department, limit):
    pipeline = [
        {
            "$match": {
                "date": {"$gte": start_date, "$lte": end_date},
                "late_minutes": {"$gt": 0},
            }
        },
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1},
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "emp_code",
                "as": "employee",
            }
        },
        {"$unwind": "$employee"},
    ]
    if department:
        pipeline.append({
            "$match": {"employee.department": department}
        })
    pipeline.extend([
        {
            "$setWindowFields": {
                "sortBy": {"total_late_minutes": -1},
                "output": {
                    "rank": {"$rank": {}}
                },
            }
        },
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$match": {"rank": {"$lte": limit}}},
        {
            "$project": {
                "_id": 0,
                "emp_code": "$_id",
                "name": "$employee.name",
                "department": "$employee.department",
                "total_late_minutes": 1,
                "late_count": 1,
                "rank": 1,
            }
        },
    ])
    return pipeline



@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: str,
    month: Optional[str] = None,
    emp_code: Optional[str] = None,
    status: Optional[str] = None,
    department: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    page: int = 1,
    page_size: int = 20,
    limit: int = 10,
):
    allowed = {
        "attendance_list",
        "employee_monthly",
        "department_summary",
        "late_leaderboard",
        "department_trend",
    }
    if endpoint not in allowed:
        raise HTTPException(
            status_code=422,
            detail=f"endpoint must be one of {sorted(allowed)}",
        )
    if page < 1 or page_size < 1 or page_size > 100:
        raise HTTPException(
            status_code=422,
            detail="Invalid pagination values",
        )
    if endpoint == "late_leaderboard" and not 1 <= limit <= 50:
        raise HTTPException(
            status_code=422,
            detail="limit must be between 1 and 50",
        )
    if endpoint == "attendance_list":
        query = {}
        if emp_code:
            query["emp_code"] = emp_code
        if date_from or date_to:
            date_filter = {}
            if date_from:
                date_filter["$gte"] = date_from
            if date_to:
                date_filter["$lte"] = date_to
            query["date"] = date_filter
        if status:
            if status not in {"PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"}:
                raise HTTPException(status_code=422, detail="Invalid status")
            query["status"] = status
        from datetime import date as date_type
        for field_name, value in (("date_from", date_from), ("date_to", date_to)):
            if value is not None:
                try:
                    parsed = date_type.fromisoformat(value)
                    if parsed.isoformat() != value:
                        raise ValueError
                except ValueError:
                    raise HTTPException(status_code=422, detail=f"{field_name} must be YYYY-MM-DD")
        if date_from and date_to and date_from > date_to:
            raise HTTPException(status_code=422, detail="date_from must be on or before date_to")
        explanation = db.command(
            "explain",
            {
                "find": "attendance_logs",
                "filter": query,
                "sort": {"date": -1, "emp_code": 1},
                "skip": (page - 1) * page_size,
                "limit": page_size,
            },
            verbosity="executionStats",
        )
    elif endpoint == "employee_monthly":
        if not month or not emp_code:
            raise HTTPException(
                status_code=422,
                detail="emp_code and month are required",
            )
        try:
            year, month_num = map(int, month.split("-"))
            if len(month) != 7 or not 1 <= month_num <= 12:
                raise ValueError
            import calendar
            from datetime import date as date_type
            start = date_type(year, month_num, 1).isoformat()
            end = date_type(
                year, month_num, calendar.monthrange(year, month_num)[1]
            ).isoformat()
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="month must be YYYY-MM",
            )
        pipeline = build_monthly_pipeline(emp_code, start, end)
        explanation = db.command(
            "explain",
            {
                "aggregate": "attendance_logs",
                "pipeline": pipeline,
                "cursor": {},
            },
            verbosity="executionStats",
        )
    else:
        if not month and endpoint != "department_trend":
            raise HTTPException(
                status_code=422,
                detail="month is required",
            )
        pipeline = []
        if endpoint == "department_trend":
            if not department or not from_date or not to_date:
                raise HTTPException(
                    status_code=422,
                    detail="department, from and to are required",
                )
            from datetime import date as date_type
            try:
                start = date_type.fromisoformat(from_date)
                end = date_type.fromisoformat(to_date)
                if start.isoformat() != from_date or end.isoformat() != to_date or start > end:
                    raise ValueError
            except ValueError:
                raise HTTPException(
                    status_code=422,
                    detail="from and to must be valid YYYY-MM-DD dates with from <= to",
                )
            if (end - start).days > 91:
                raise HTTPException(status_code=422, detail="Date range cannot exceed 92 calendar days")
            if db.employees.find_one({"department": department}, {"_id": 1}) is None:
                raise HTTPException(status_code=404, detail=f"Department '{department}' not found")
            pipeline = build_department_trend_pipeline(department, start, end)
        else:
            import calendar
            from datetime import date as date_type
            try:
                year, month_num = map(int, month.split("-"))
                if len(month) != 7 or not 1 <= month_num <= 12:
                    raise ValueError
                start = date_type(year, month_num, 1).isoformat()
                end = date_type(
                    year, month_num, calendar.monthrange(year, month_num)[1]
                ).isoformat()
            except ValueError:
                raise HTTPException(
                    status_code=422,
                    detail="month must be YYYY-MM",
                )
            if endpoint == "department_summary":
                pipeline = build_summary_pipeline(start, end, department)
            elif endpoint == "late_leaderboard":
                pipeline = build_leaderboard_pipeline(start, end, department, limit)
        explanation = db.command(
            "explain",
            {
                "aggregate": "attendance_logs" if endpoint in {"employee_monthly", "late_leaderboard"} else "employees",
                "pipeline": pipeline,
                "cursor": {},
            },
            verbosity="executionStats",
        )
    collection_name = {
        "attendance_list": "attendance_logs",
        "employee_monthly": "attendance_logs",
        "department_summary": "employees",
        "late_leaderboard": "attendance_logs",
        "department_trend": "employees",
    }[endpoint]
    explanation = json.loads(json_util.dumps(explanation, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"endpoint": endpoint, "collection": collection_name, "explain": explanation}
@app.on_event("startup")
def startup_event():
    create_indexes()
# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def compute_late_minutes(
    punch_in: datetime,
    shift_start: str,
    shift_end: str,
) -> int:
    """Apply R2 using the correct shift-start date, including overnight shifts."""
    if punch_in.tzinfo is None:
        punch_in = punch_in.replace(tzinfo=timezone.utc)
    local_in = punch_in.astimezone(IST).replace(microsecond=0)
    sh, sm = map(int, shift_start.split(":"))
    eh, em = map(int, shift_end.split(":"))
    shift_day = local_in.date()
    is_overnight = (eh, em) <= (sh, sm)
    if is_overnight and (local_in.hour, local_in.minute) < (eh, em):
        shift_day -= timedelta(days=1)
    shift_start_dt = datetime.combine(
        shift_day, datetime.min.time(), tzinfo=IST
    ).replace(hour=sh, minute=sm, second=0, microsecond=0)
    late_seconds = (local_in - shift_start_dt).total_seconds()
    return int(late_seconds // 60) if late_seconds > 600 else 0
def attendance_response(doc: dict) -> dict:
    """Apply documented attendance defaults, convert instants, and hide _id."""
    if isinstance(doc, dict) and "status" in doc and "date" in doc:
        doc = dict(doc)
        if doc.get("history") is None:
            doc["history"] = []
        if doc.get("half_day") is None:
            doc["half_day"] = False

    def convert_values(value):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return int(value.timestamp() * 1000)
        if isinstance(value, dict):
            return {
                key: convert_values(item)
                for key, item in value.items()
                if key != "_id"
            }
        if isinstance(value, list):
            return [convert_values(item) for item in value]
        return value
    return convert_values(doc)
def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    from decimal import Decimal, ROUND_HALF_UP
    elapsed_seconds = int((punch_out - punch_in).total_seconds())
    hours = (Decimal(elapsed_seconds) / Decimal(3600)).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )
    return float(hours)
def compute_overtime(
    punch_out: datetime,
    shift_start: str,
    shift_end: str,
    attendance_date: str,
) -> int:
    """Apply R3 relative to the shift end, including overnight shifts."""
    sh, sm = map(int, shift_start.split(":"))
    eh, em = map(int, shift_end.split(":"))
    shift_start_dt = datetime.fromisoformat(attendance_date).replace(
        hour=sh, minute=sm, second=0, microsecond=0, tzinfo=IST
    )
    shift_end_dt = datetime.fromisoformat(attendance_date).replace(
        hour=eh, minute=em, second=0, microsecond=0, tzinfo=IST
    )
    if shift_end_dt <= shift_start_dt:
        shift_end_dt += timedelta(days=1)
    if punch_out.tzinfo is None:
        punch_out = punch_out.replace(tzinfo=timezone.utc)
    local_out = punch_out.astimezone(IST).replace(microsecond=0)
    overtime_seconds = (local_out - shift_end_dt).total_seconds()
    return int(overtime_seconds // 60) if overtime_seconds >= 1800 else 0
# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP\d{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    shift_end: str = Field(default="18:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def validate_joined_on(cls, value: str) -> str:
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            raise ValueError("joined_on must be a valid YYYY-MM-DD date")
        if parsed.isoformat() != value:
            raise ValueError("joined_on must use YYYY-MM-DD format")
        return value

    @field_validator("name", "department")
    @classmethod
    def validate_nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value must not be blank")
        return value.strip()

    @model_validator(mode="after")
    def validate_shift(self):
        for field_name in ("shift_start", "shift_end"):
            value = getattr(self, field_name)
            try:
                parsed = datetime.strptime(value, "%H:%M")
                if parsed.strftime("%H:%M") != value:
                    raise ValueError
            except ValueError:
                raise ValueError(f"{field_name} must use HH:MM format")
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start and shift_end must differ")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[StrictInt] = Field(default=None, ge=100000000000, le=4102444800000)
    status: Literal["PRESENT", "WFH", "ON_DUTY"] = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[StrictInt] = Field(default=None, ge=100000000000, le=4102444800000)


class AttendanceCorrectionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"] = None
    punch_in: StrictInt = Field(default=None, ge=100000000000, le=4102444800000)
    punch_out: StrictInt = Field(default=None, ge=100000000000, le=4102444800000)
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

# --------------------------------------------------------------------------- #
# Endpoints provided
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        db.command("ping")
        return {"status": "ok"}
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable"
        )
@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = datetime.now(IST).replace(microsecond=0)
    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return attendance_response(doc)
@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
):
    if page < 1:
        raise HTTPException(422, "page must be at least 1")
    if page_size < 1 or page_size > 100:
        raise HTTPException(422, "page_size must be between 1 and 100")
    q = {}
    if department:
        q["department"] = department
    skip = (page - 1) * page_size
    total = db.employees.count_documents(q)
    items = [attendance_response(item) for item in db.employees.find(q, {"_id": 0})
        .sort("emp_code", 1)
        .skip(skip)
        .limit(page_size)]
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    if body.status not in {"PRESENT", "WFH", "ON_DUTY"}:
        raise HTTPException(
            status_code=422,
            detail="Punch-in status must be PRESENT, WFH, or ON_DUTY",
        )
    ts = parse_punch_timestamp(body.punched_at)
    d = get_attendance_date(ts, emp["shift_start"], emp["shift_end"])
    if db.attendance_logs.find_one({"emp_code": body.emp_code, "date": d}):
        raise HTTPException(
            status_code=409,
            detail="Already punched in for this date",
        )
    doc = {
        "emp_code": body.emp_code,
        "date": d,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(
            ts, emp["shift_start"], emp["shift_end"]
        ),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(
        status_code=409,
        detail="Already punched in for this date",
    )
    return attendance_response(doc)
def parse_punch_timestamp(punched_at: Optional[int]) -> datetime:
    if punched_at is None:
        ts = datetime.now(IST)
    else:
        if (
            isinstance(punched_at, bool)
            or not isinstance(punched_at, int)
            or punched_at < 100000000000 or punched_at > 4102444800000
        ):
            raise HTTPException(
                status_code=422,
                detail="punched_at must be an epoch timestamp in milliseconds",
            )
        try:
            ts = datetime.fromtimestamp(
                punched_at / 1000,
                tz=timezone.utc,
            ).astimezone(IST)
        except (OverflowError, OSError, ValueError):
            raise HTTPException(
                status_code=422,
                detail="Invalid punched_at timestamp",
            )
    return ts.replace(microsecond=0)
def get_attendance_date(
    ts: datetime,
    shift_start: str,
    shift_end: str,
) -> str:
    start_h, start_m = map(int, shift_start.split(":"))
    end_h, end_m = map(int, shift_end.split(":"))
    is_overnight = (end_h, end_m) <= (start_h, start_m)
    punch_time = (ts.hour, ts.minute)
    if is_overnight and punch_time < (end_h, end_m):
        ts = ts - timedelta(days=1)
    return ts.date().isoformat()
@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    from datetime import date as date_type
    query = {}
    if emp_code:
        query["emp_code"] = emp_code
    for field_name, value in (("date_from", date_from), ("date_to", date_to)):
        if value is not None:
            try:
                parsed = date_type.fromisoformat(value)
                if parsed.isoformat() != value:
                    raise ValueError
            except ValueError:
                raise HTTPException(status_code=422, detail=f"{field_name} must be YYYY-MM-DD")
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="date_from must be on or before date_to")
    if date_from or date_to:
        date_filter = {}
        if date_from:
            date_filter["$gte"] = date_from
        if date_to:
            date_filter["$lte"] = date_to
        query["date"] = date_filter
    if status:
        if status not in {"PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"}:
            raise HTTPException(status_code=422, detail="Invalid status")
        query["status"] = status
    total = db.attendance_logs.count_documents(query)
    skip = (page - 1) * page_size
    docs = list(
        db.attendance_logs.find(query, {"_id": 0})
        .sort([("date", -1), ("emp_code", 1)])
        .skip(skip)
        .limit(page_size)
    )
    return {
        "items": [attendance_response(doc) for doc in docs],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    if body.punched_at is None:
        punch_time = datetime.now(timezone.utc)
    else:
        if (
            isinstance(body.punched_at, bool)
            or not isinstance(body.punched_at, int)
            or body.punched_at < 100000000000 or body.punched_at > 4102444800000
        ):
            raise HTTPException(
                status_code=422,
                detail="punched_at must be epoch milliseconds",
            )
        try:
            punch_time = datetime.fromtimestamp(
                body.punched_at / 1000, tz=timezone.utc
            )
        except (ValueError, OverflowError, OSError):
            raise HTTPException(
                status_code=422,
                detail="Invalid punched_at timestamp",
            )
    punch_time = punch_time.replace(microsecond=0)
    # Select the most recent punch-in not later than this punch-out, even if
    # it has already been closed; repeated/concurrent punch-outs then return 409.
    selected = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$ne": None, "$lte": punch_time}},
        sort=[("punch_in", -1)],
    )
    if selected is None:
        raise HTTPException(status_code=404, detail="No punch-in found")
    if selected.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="Attendance record was already punched out")
    punch_in_time = selected["punch_in"]
    if punch_in_time.tzinfo is None:
        punch_in_time = punch_in_time.replace(tzinfo=timezone.utc)
    punch_in_time = punch_in_time.replace(microsecond=0)
    elapsed_seconds = (punch_time - punch_in_time).total_seconds()
    if elapsed_seconds <= 0 or elapsed_seconds > 24 * 60 * 60:
        raise HTTPException(
            status_code=422,
            detail="Punch-out must be after punch-in and within 24 hours",
        )
    shift_start = emp["shift_start"]
    shift_end = emp["shift_end"]
    work_hours = compute_work_hours(punch_in_time, punch_time)
    late_minutes = compute_late_minutes(punch_in_time, shift_start, shift_end)
    overtime_minutes = compute_overtime(
        punch_time, shift_start, shift_end, selected["date"]
    )
    half_day = work_hours < 4.50
    # Conditional update prevents two simultaneous punch-outs
    # from successfully closing the same record.
    result = db.attendance_logs.update_one(
        {
            "_id": selected["_id"],
            "punch_out": None,
        },
        {
            "$set": {
                "punch_in": punch_in_time,
                "punch_out": punch_time,
                "work_hours": work_hours,
                "late_minutes": late_minutes,
                "overtime_minutes": overtime_minutes,
                "half_day": half_day,
            }
        },
    )
    if result.modified_count != 1:
        raise HTTPException(
            status_code=409,
            detail="Attendance record was already updated",
        )
    updated = db.attendance_logs.find_one(
        {"_id": selected["_id"]},
        {"_id": 0},
    )
    return attendance_response(updated)
@app.patch("/attendance/{emp_code}/{date}")
def correct_attendance(
    emp_code: str,
    date: str,
    body: AttendanceCorrectionIn,
):
    from datetime import date as date_type
    try:
        date_type.fromisoformat(date)
        if len(date) != 10:
            raise ValueError
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail="date must be YYYY-MM-DD",
        )
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    current = db.attendance_logs.find_one({
        "emp_code": emp_code,
        "date": date,
    })
    if not current:
        raise HTTPException(
            status_code=404,
            detail="Attendance record not found",
        )
    allowed_statuses = {"PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"}
    updates = {}
    if body.status is not None:
        status = body.status.upper()
        if status not in allowed_statuses:
            raise HTTPException(status_code=422, detail="Invalid status")
        updates["status"] = status
    def parse_timestamp(value, field):
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise HTTPException(
                status_code=422,
                detail=f"{field} must be epoch milliseconds",
            )
        if value < 100000000000 or value > 4102444800000:
            raise HTTPException(
                status_code=422,
                detail=f"{field} must be epoch milliseconds",
            )
        try:
            return datetime.fromtimestamp(
                value / 1000, tz=timezone.utc
            ).replace(microsecond=0)
        except (ValueError, OverflowError, OSError):
            raise HTTPException(
                status_code=422,
                detail=f"Invalid {field} timestamp",
            )
    if "punch_in" in body.model_fields_set:
        updates["punch_in"] = parse_timestamp(body.punch_in, "punch_in")
    if "punch_out" in body.model_fields_set:
        updates["punch_out"] = parse_timestamp(body.punch_out, "punch_out")
    new_status = updates.get("status", current.get("status"))
    new_in = updates.get("punch_in", current.get("punch_in"))
    new_out = updates.get("punch_out", current.get("punch_out"))
    if new_status in {"ABSENT", "LEAVE"}:
        if "punch_in" in body.model_fields_set or "punch_out" in body.model_fields_set:
            raise HTTPException(
                status_code=422,
                detail="punch_in and punch_out must be omitted when status is ABSENT or LEAVE",
            )
        new_in = None
        new_out = None
        updates["punch_in"] = None
        updates["punch_out"] = None
    elif new_status in {"PRESENT", "WFH", "ON_DUTY"} and new_in is None:
        raise HTTPException(
            status_code=422,
            detail="Present attendance requires punch_in",
        )

    # Normalize stored Mongo datetimes (PyMongo normally returns naive UTC)
    # as well as client-supplied values. R1 truncates instants before every
    # calculation; fractional-second values already in stored data are fixed.
    for field_name, current_value in (("punch_in", new_in), ("punch_out", new_out)):
        if current_value is None:
            continue
        if current_value.tzinfo is None:
            normalized_value = current_value.replace(tzinfo=timezone.utc)
        else:
            normalized_value = current_value.astimezone(timezone.utc)
        normalized_value = normalized_value.replace(microsecond=0)
        if field_name in updates or getattr(current.get(field_name), "microsecond", 0) != 0:
            updates[field_name] = normalized_value
        if field_name == "punch_in":
            new_in = normalized_value
        else:
            new_out = normalized_value

    if new_in is not None:
        local_in = new_in.astimezone(IST)
        if get_attendance_date(
            local_in, emp["shift_start"], emp["shift_end"]
        ) != date:
            raise HTTPException(
                status_code=422,
                detail="punch_in must belong to the attendance date",
            )
    if new_out is not None:
        if new_in is None:
            raise HTTPException(
                status_code=422,
                detail="punch_out requires punch_in",
            )
        if new_out.tzinfo is None:
            new_out = new_out.replace(tzinfo=timezone.utc)
        if new_out <= new_in or (new_out - new_in).total_seconds() > 86400:
            raise HTTPException(
                status_code=422,
                detail="punch_out must be after punch_in and within 24 hours",
            )
    shift_start = emp["shift_start"]
    shift_end = emp["shift_end"]
    derived = {
        "work_hours": None,
        "late_minutes": 0,
        "overtime_minutes": 0,
        "half_day": False,
    }
    if new_in is not None:
        derived["late_minutes"] = compute_late_minutes(new_in, shift_start, shift_end)
    if new_in is not None and new_out is not None:
        derived["work_hours"] = compute_work_hours(new_in, new_out)
        derived["half_day"] = derived["work_hours"] < 4.50
        derived["overtime_minutes"] = compute_overtime(
            new_out, shift_start, shift_end, date
        )
    updates.update(derived)
    changes = {}
    def values_equal(key, old_value, new_value):
        if key in {"punch_in", "punch_out"}:
            if old_value is None or new_value is None:
                return old_value is None and new_value is None
            if isinstance(old_value, datetime) and isinstance(new_value, datetime):
                if old_value.tzinfo is None:
                    old_value = old_value.replace(tzinfo=timezone.utc)
                else:
                    old_value = old_value.astimezone(timezone.utc)
                if new_value.tzinfo is None:
                    new_value = new_value.replace(tzinfo=timezone.utc)
                else:
                    new_value = new_value.astimezone(timezone.utc)
                return old_value == new_value
        return old_value == new_value

    for key, value in updates.items():
        old_value = current.get(key)
        if not values_equal(key, old_value, value):
            changes[key] = {
                "from": old_value,
                "to": value,
            }
    if not changes:
        raise HTTPException(
            status_code=422,
            detail="Correction does not change any values",
        )
    history_entry = {
        "at": datetime.now(timezone.utc).replace(microsecond=0),
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": changes,
    }
    # Optimistic concurrency check: only update if the record
    # still matches the version we read.
    # Compare every input/derived field used to calculate this correction so
    # overlapping updates cannot overwrite a concurrent calculation/history.
    query = {"_id": current["_id"]}
    for key in (
        "status", "punch_in", "punch_out", "work_hours", "late_minutes",
        "overtime_minutes", "half_day",
    ):
        query[key] = current.get(key)
    result = db.attendance_logs.update_one(
        query,
        {
            "$set": updates,
            "$push": {"history": history_entry},
        },
    )
    if result.modified_count != 1:
        raise HTTPException(
            status_code=409,
            detail="Attendance changed concurrently; retry correction",
        )
    updated = db.attendance_logs.find_one(
        {"_id": current["_id"]},
        {"_id": 0},
    )
    return attendance_response(updated)
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(
    emp_code: str,
    month: str,
):
    import calendar
    from datetime import date as date_type
    from decimal import Decimal, ROUND_HALF_UP
    try:
        if len(month) != 7:
            raise ValueError
        year, month_num = map(int, month.split("-"))
        if not 1 <= month_num <= 12:
            raise ValueError
        start_date = date_type(year, month_num, 1)
        end_date = date_type(
            year,
            month_num,
            calendar.monthrange(year, month_num)[1],
        )
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=422,
            detail="month must be in YYYY-MM format",
        )
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    joined_on = date_type.fromisoformat(emp["joined_on"])
    effective_start = max(start_date, joined_on)
    if effective_start > end_date:
        working_days = 0
    else:
        working_days = sum(
            1
            for day in range(
                (end_date - effective_start).days + 1
            )
            if (effective_start + timedelta(days=day)).weekday() < 5
        )
    pipeline = build_monthly_pipeline(emp_code, start_date.isoformat(), end_date.isoformat())
    results = list(db.attendance_logs.aggregate(pipeline))
    metrics = results[0] if results else {
        "present_days": 0,
        "leave_days": 0,
        "late_count": 0,
        "total_late_minutes": 0,
        "total_overtime_minutes": 0,
    }
    present_days = metrics["present_days"]
    leave_days = metrics["leave_days"]
    late_count = metrics["late_count"]
    total_late_minutes = metrics["total_late_minutes"]
    total_overtime_minutes = metrics["total_overtime_minutes"]
    from decimal import Decimal, ROUND_HALF_UP
    def round2(value):
        return float(
            Decimal(str(value)).quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP,
            )
        )
    attendance_pct = (
        float(
            (Decimal(str(present_days)) * Decimal("100") / Decimal(working_days)).quantize(
                Decimal("0.0001"),
                rounding=ROUND_HALF_UP,
            )
        )
        if working_days > 0
        else None
    )
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": round2(present_days),
        "leave_days": leave_days,
        "late_count": late_count,
        "total_late_minutes": total_late_minutes,
        "total_overtime_minutes": total_overtime_minutes,
        "attendance_pct": attendance_pct,
    }
@app.get("/analytics/departments/summary")
def department_summary(month: str, department: Optional[str] = None):
    import calendar
    from datetime import date as date_type
    try:
        if len(month) != 7:
            raise ValueError
        year, month_num = map(int, month.split("-"))
        if not 1 <= month_num <= 12:
            raise ValueError
        start_date = date_type(year, month_num, 1).isoformat()
        end_date = date_type(
            year,
            month_num,
            calendar.monthrange(year, month_num)[1],
        ).isoformat()
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=422,
            detail="month must be in YYYY-MM format",
        )
    pipeline = build_summary_pipeline(start_date, end_date, department)
    results = list(db.employees.aggregate(pipeline))
    for item in results:
        if item["avg_work_hours"] is not None:
            from decimal import Decimal, ROUND_HALF_UP
            item["avg_work_hours"] = float(
                Decimal(str(item["avg_work_hours"])).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            )
    return {"month": month, "items": results}
@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str,
    department: Optional[str] = None,
    limit: int = 10,
):
    import calendar
    from datetime import date as date_type
    if limit < 1 or limit > 50:
        raise HTTPException(
            status_code=422,
            detail="limit must be between 1 and 50",
        )
    try:
        if len(month) != 7:
            raise ValueError
        year, month_num = map(int, month.split("-"))
        if not 1 <= month_num <= 12:
            raise ValueError
        start_date = date_type(year, month_num, 1).isoformat()
        end_date = date_type(
            year,
            month_num,
            calendar.monthrange(year, month_num)[1],
        ).isoformat()
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=422,
            detail="month must be in YYYY-MM format",
        )
    pipeline = build_leaderboard_pipeline(start_date, end_date, department, limit)
    results = list(db.attendance_logs.aggregate(pipeline))
    return {"month": month, "items": results}
@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_date: str = Query(alias="from"),
    to_date: str = Query(alias="to"),
):
    from datetime import date as date_type
    try:
        start = date_type.fromisoformat(from_date)
        end = date_type.fromisoformat(to_date)
        if start.isoformat() != from_date or end.isoformat() != to_date or start > end:
            raise ValueError
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail="from and to must be valid YYYY-MM-DD dates with from <= to",
        )
    if (end - start).days > 91:
        raise HTTPException(status_code=422, detail="Date range cannot exceed 92 calendar days")
    if db.employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(status_code=404, detail=f"Department '{department}' not found")
    pipeline = build_department_trend_pipeline(department, start, end)
    # Seed from the indexed department match so daily trend rows are generated
    # even when there are no attendance logs in the requested period.
    items = list(db.employees.aggregate(pipeline))
    return {"department": department, "items": items}
