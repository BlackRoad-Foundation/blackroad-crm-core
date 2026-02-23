# blackroad-crm-core

> Production Python CRM engine — part of [BlackRoad Foundation](https://github.com/BlackRoad-Foundation).

## Features

- **Contact Management** — Full CRUD with tags, company, interaction history
- **Deal Pipeline** — Stage-based deals with automatic probability mapping
- **Interaction Logging** — Calls, emails, meetings with outcome tracking
- **Revenue Forecasting** — Probability × value, weekly breakdown
- **Follow-Up Queue** — Contacts with open deals not contacted recently
- **Pipeline View** — Per-stage totals, weighted values, conversion metrics

## Quick Start

```python
from src.crm_core import CRMCore, Contact, Deal

crm = CRMCore("crm.db")

contact = crm.add_contact(Contact(
    name="Jane Doe", email="jane@acme.com",
    company="Acme Corp", tags=["enterprise", "vip"]
))

deal = crm.add_deal(Deal(
    contact_id=contact.id, title="Annual Contract",
    value=50000, stage="proposal", probability=0.4
))

crm.log_interaction(contact.id, "call", "Discussed pricing", outcome="positive")
crm.advance_deal_stage(deal.id, "negotiation")

pipeline = crm.get_deal_pipeline()
forecast = crm.forecast_revenue(days=30)
queue = crm.get_follow_up_queue(days_overdue=3)
```

## Database Schema

```
contacts      — id, name, email, phone, company, tags, notes, created_at, last_contact
deals         — id, contact_id, title, value, stage, probability, expected_close
interactions  — id, contact_id, deal_id, type, notes, outcome, occurred_at
```

## Running Tests

```bash
pip install pytest
pytest tests/ -v
```

## License

© BlackRoad OS, Inc. All rights reserved.
