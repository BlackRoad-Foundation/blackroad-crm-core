"""
BlackRoad CRM Core — production implementation.
Contact management, deal pipeline, interaction logging, revenue forecasting.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
import uuid


# ─────────────────────────── data models ────────────────────────────────────

@dataclass
class Contact:
    name: str
    email: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    phone: str = ""
    company: str = ""
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_contact: Optional[datetime] = None
    notes: str = ""

    def days_since_contact(self) -> Optional[int]:
        if not self.last_contact:
            return None
        return (datetime.utcnow() - self.last_contact).days


DEAL_STAGES = ["lead", "qualified", "proposal", "negotiation", "closed_won", "closed_lost"]


@dataclass
class Deal:
    contact_id: str
    title: str
    value: float
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    stage: str = "lead"
    probability: float = 0.1        # 0.0 – 1.0
    expected_close: Optional[date] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    notes: str = ""

    def weighted_value(self) -> float:
        return self.value * self.probability

    def is_open(self) -> bool:
        return self.stage not in ("closed_won", "closed_lost")


@dataclass
class Interaction:
    contact_id: str
    type: str                        # call | email | meeting | demo | note
    notes: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: datetime = field(default_factory=datetime.utcnow)
    deal_id: Optional[str] = None
    outcome: str = ""


# ──────────────────────────── database layer ────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS contacts (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    email        TEXT UNIQUE NOT NULL,
    phone        TEXT DEFAULT '',
    company      TEXT DEFAULT '',
    tags         TEXT DEFAULT '',
    notes        TEXT DEFAULT '',
    created_at   TEXT NOT NULL,
    last_contact TEXT
);

CREATE TABLE IF NOT EXISTS deals (
    id             TEXT PRIMARY KEY,
    contact_id     TEXT NOT NULL REFERENCES contacts(id),
    title          TEXT NOT NULL,
    value          REAL NOT NULL DEFAULT 0,
    stage          TEXT NOT NULL DEFAULT 'lead',
    probability    REAL NOT NULL DEFAULT 0.1,
    expected_close TEXT,
    notes          TEXT DEFAULT '',
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS interactions (
    id          TEXT PRIMARY KEY,
    contact_id  TEXT NOT NULL REFERENCES contacts(id),
    deal_id     TEXT REFERENCES deals(id),
    type        TEXT NOT NULL,
    notes       TEXT NOT NULL,
    outcome     TEXT DEFAULT '',
    occurred_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_deals_contact   ON deals(contact_id);
CREATE INDEX IF NOT EXISTS idx_deals_stage     ON deals(stage);
CREATE INDEX IF NOT EXISTS idx_inter_contact   ON interactions(contact_id);
CREATE INDEX IF NOT EXISTS idx_inter_occurred  ON interactions(occurred_at);
"""

# Default stage→probability mapping
STAGE_PROBS = {
    "lead": 0.05,
    "qualified": 0.20,
    "proposal": 0.40,
    "negotiation": 0.70,
    "closed_won": 1.00,
    "closed_lost": 0.00,
}


class CRMCore:
    """
    Production CRM engine backed by SQLite.
    Supports contact lifecycle, deal pipeline, interaction history,
    revenue forecasting, and automated follow-up queue.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(DDL)
        self.conn.commit()

    # ── contact CRUD ─────────────────────────────────────────────────────────

    def add_contact(self, contact: Contact) -> Contact:
        self.conn.execute(
            "INSERT INTO contacts VALUES (?,?,?,?,?,?,?,?,?)",
            (contact.id, contact.name, contact.email, contact.phone,
             contact.company, ",".join(contact.tags), contact.notes,
             contact.created_at.isoformat(),
             contact.last_contact.isoformat() if contact.last_contact else None),
        )
        self.conn.commit()
        return contact

    def get_contact(self, contact_id: str) -> Optional[Contact]:
        row = self.conn.execute(
            "SELECT * FROM contacts WHERE id=?", (contact_id,)
        ).fetchone()
        return self._row_to_contact(row) if row else None

    def find_contact_by_email(self, email: str) -> Optional[Contact]:
        row = self.conn.execute(
            "SELECT * FROM contacts WHERE email=?", (email,)
        ).fetchone()
        return self._row_to_contact(row) if row else None

    def _row_to_contact(self, row: sqlite3.Row) -> Contact:
        return Contact(
            id=row["id"], name=row["name"], email=row["email"],
            phone=row["phone"] or "", company=row["company"] or "",
            tags=[t for t in (row["tags"] or "").split(",") if t],
            notes=row["notes"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
            last_contact=datetime.fromisoformat(row["last_contact"])
                         if row["last_contact"] else None,
        )

    def update_contact(self, contact_id: str, **kwargs: Any) -> bool:
        allowed = {"name", "email", "phone", "company", "tags", "notes"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if "tags" in updates and isinstance(updates["tags"], list):
            updates["tags"] = ",".join(updates["tags"])
        if not updates:
            return False
        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [contact_id]
        cur = self.conn.execute(
            f"UPDATE contacts SET {set_clause} WHERE id=?", values
        )
        self.conn.commit()
        return cur.rowcount > 0

    def list_contacts(self, company: Optional[str] = None,
                      tag: Optional[str] = None) -> List[Contact]:
        if company:
            rows = self.conn.execute(
                "SELECT * FROM contacts WHERE company=? ORDER BY name", (company,)
            ).fetchall()
        elif tag:
            rows = self.conn.execute(
                "SELECT * FROM contacts WHERE tags LIKE ? ORDER BY name",
                (f"%{tag}%",),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM contacts ORDER BY name"
            ).fetchall()
        return [self._row_to_contact(r) for r in rows]

    # ── interaction logging ──────────────────────────────────────────────────

    def log_interaction(self, contact_id: str, type: str, notes: str,
                        deal_id: Optional[str] = None,
                        outcome: str = "") -> Interaction:
        ix = Interaction(contact_id=contact_id, type=type, notes=notes,
                         deal_id=deal_id, outcome=outcome)
        self.conn.execute(
            "INSERT INTO interactions VALUES (?,?,?,?,?,?,?)",
            (ix.id, ix.contact_id, ix.deal_id, ix.type, ix.notes,
             ix.outcome, ix.occurred_at.isoformat()),
        )
        # Update last_contact timestamp
        self.conn.execute(
            "UPDATE contacts SET last_contact=? WHERE id=?",
            (ix.occurred_at.isoformat(), contact_id),
        )
        self.conn.commit()
        return ix

    def get_contact_history(self, contact_id: str,
                            limit: int = 50) -> List[Interaction]:
        rows = self.conn.execute(
            "SELECT * FROM interactions WHERE contact_id=? "
            "ORDER BY occurred_at DESC LIMIT ?",
            (contact_id, limit),
        ).fetchall()
        return [
            Interaction(
                id=r["id"], contact_id=r["contact_id"], deal_id=r["deal_id"],
                type=r["type"], notes=r["notes"], outcome=r["outcome"] or "",
                occurred_at=datetime.fromisoformat(r["occurred_at"]),
            )
            for r in rows
        ]

    # ── deal management ──────────────────────────────────────────────────────

    def add_deal(self, deal: Deal) -> Deal:
        self.conn.execute(
            "INSERT INTO deals VALUES (?,?,?,?,?,?,?,?,?)",
            (deal.id, deal.contact_id, deal.title, deal.value, deal.stage,
             deal.probability,
             deal.expected_close.isoformat() if deal.expected_close else None,
             deal.notes, deal.created_at.isoformat()),
        )
        self.conn.commit()
        return deal

    def advance_deal_stage(self, deal_id: str, stage: str) -> bool:
        if stage not in DEAL_STAGES:
            raise ValueError(f"Unknown stage {stage!r}; valid: {DEAL_STAGES}")
        probability = STAGE_PROBS[stage]
        cur = self.conn.execute(
            "UPDATE deals SET stage=?, probability=? WHERE id=?",
            (stage, probability, deal_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_deal(self, deal_id: str) -> Optional[Deal]:
        row = self.conn.execute(
            "SELECT * FROM deals WHERE id=?", (deal_id,)
        ).fetchone()
        return self._row_to_deal(row) if row else None

    def _row_to_deal(self, row: sqlite3.Row) -> Deal:
        return Deal(
            id=row["id"], contact_id=row["contact_id"], title=row["title"],
            value=row["value"], stage=row["stage"],
            probability=row["probability"],
            expected_close=date.fromisoformat(row["expected_close"])
                           if row["expected_close"] else None,
            notes=row["notes"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    # ── pipeline & forecasting ───────────────────────────────────────────────

    def get_deal_pipeline(self) -> Dict[str, Any]:
        """
        Return per-stage totals and conversion metrics for the deal pipeline.
        """
        rows = self.conn.execute(
            "SELECT stage, COUNT(*) as cnt, SUM(value) as total, "
            "SUM(value*probability) as weighted FROM deals GROUP BY stage"
        ).fetchall()
        stages: Dict[str, Dict[str, Any]] = {}
        for stage in DEAL_STAGES:
            stages[stage] = {"count": 0, "total_value": 0.0, "weighted_value": 0.0}
        for r in rows:
            stages[r["stage"]] = {
                "count": r["cnt"],
                "total_value": round(r["total"] or 0, 2),
                "weighted_value": round(r["weighted"] or 0, 2),
            }
        open_stages = [s for s in DEAL_STAGES if s not in ("closed_won", "closed_lost")]
        pipeline_value = sum(stages[s]["total_value"] for s in open_stages)
        pipeline_weighted = sum(stages[s]["weighted_value"] for s in open_stages)
        return {
            "stages": stages,
            "pipeline_value": round(pipeline_value, 2),
            "pipeline_weighted": round(pipeline_weighted, 2),
        }

    def forecast_revenue(self, days: int = 30) -> Dict[str, Any]:
        """
        Forecast revenue for deals expected to close within *days* using
        probability × value.  Breaks down by week for granular projection.
        """
        horizon = date.today() + timedelta(days=days)
        rows = self.conn.execute(
            "SELECT * FROM deals WHERE stage NOT IN ('closed_won','closed_lost') "
            "AND expected_close IS NOT NULL AND expected_close <= ?",
            (horizon.isoformat(),),
        ).fetchall()

        weekly: Dict[int, float] = defaultdict(float)
        total_expected = 0.0
        deals_snapshot = []

        today = date.today()
        for row in rows:
            d = self._row_to_deal(row)
            weighted = d.weighted_value()
            total_expected += weighted
            close_date = d.expected_close
            week_num = ((close_date - today).days // 7) + 1  # type: ignore[operator]
            weekly[week_num] += weighted
            deals_snapshot.append({
                "title": d.title,
                "value": d.value,
                "probability": d.probability,
                "weighted": round(weighted, 2),
                "expected_close": close_date.isoformat(),  # type: ignore[union-attr]
            })

        return {
            "forecast_days": days,
            "total_expected": round(total_expected, 2),
            "deal_count": len(deals_snapshot),
            "weekly_breakdown": {f"week_{k}": round(v, 2) for k, v in sorted(weekly.items())},
            "deals": sorted(deals_snapshot, key=lambda x: x["expected_close"]),
        }

    # ── follow-up queue ──────────────────────────────────────────────────────

    def get_follow_up_queue(self, days_overdue: int = 3) -> List[Dict[str, Any]]:
        """
        Return contacts who haven't been contacted in more than *days_overdue*
        days AND have open deals — sorted by deal weighted value descending.
        """
        cutoff = datetime.utcnow() - timedelta(days=days_overdue)
        rows = self.conn.execute(
            "SELECT c.id, c.name, c.email, c.company, c.last_contact, "
            "       SUM(d.value * d.probability) as weighted "
            "FROM contacts c "
            "JOIN deals d ON d.contact_id = c.id "
            "WHERE d.stage NOT IN ('closed_won','closed_lost') "
            "  AND (c.last_contact IS NULL OR c.last_contact < ?) "
            "GROUP BY c.id ORDER BY weighted DESC",
            (cutoff.isoformat(),),
        ).fetchall()
        result = []
        for r in rows:
            last_c = r["last_contact"]
            days_ago = (
                (datetime.utcnow() - datetime.fromisoformat(last_c)).days
                if last_c else None
            )
            result.append({
                "contact_id": r["id"],
                "name": r["name"],
                "email": r["email"],
                "company": r["company"] or "",
                "days_since_contact": days_ago,
                "open_deal_weighted_value": round(r["weighted"] or 0, 2),
            })
        return result

    def close(self) -> None:
        self.conn.close()
