<!-- BlackRoad SEO Enhanced -->

# ulackroad crm core

> Part of **[BlackRoad OS](https://blackroad.io)** — Sovereign Computing for Everyone

[![BlackRoad OS](https://img.shields.io/badge/BlackRoad-OS-ff1d6c?style=for-the-badge)](https://blackroad.io)
[![BlackRoad Foundation](https://img.shields.io/badge/Org-BlackRoad-Foundation-2979ff?style=for-the-badge)](https://github.com/BlackRoad-Foundation)
[![License](https://img.shields.io/badge/License-Proprietary-f5a623?style=for-the-badge)](LICENSE)

**ulackroad crm core** is part of the **BlackRoad OS** ecosystem — a sovereign, distributed operating system built on edge computing, local AI, and mesh networking by **BlackRoad OS, Inc.**

## About BlackRoad OS

BlackRoad OS is a sovereign computing platform that runs AI locally on your own hardware. No cloud dependencies. No API keys. No surveillance. Built by [BlackRoad OS, Inc.](https://github.com/BlackRoad-OS-Inc), a Delaware C-Corp founded in 2025.

### Key Features
- **Local AI** — Run LLMs on Raspberry Pi, Hailo-8, and commodity hardware
- **Mesh Networking** — WireGuard VPN, NATS pub/sub, peer-to-peer communication
- **Edge Computing** — 52 TOPS of AI acceleration across a Pi fleet
- **Self-Hosted Everything** — Git, DNS, storage, CI/CD, chat — all sovereign
- **Zero Cloud Dependencies** — Your data stays on your hardware

### The BlackRoad Ecosystem
| Organization | Focus |
|---|---|
| [BlackRoad OS](https://github.com/BlackRoad-OS) | Core platform and applications |
| [BlackRoad OS, Inc.](https://github.com/BlackRoad-OS-Inc) | Corporate and enterprise |
| [BlackRoad AI](https://github.com/BlackRoad-AI) | Artificial intelligence and ML |
| [BlackRoad Hardware](https://github.com/BlackRoad-Hardware) | Edge hardware and IoT |
| [BlackRoad Security](https://github.com/BlackRoad-Security) | Cybersecurity and auditing |
| [BlackRoad Quantum](https://github.com/BlackRoad-Quantum) | Quantum computing research |
| [BlackRoad Agents](https://github.com/BlackRoad-Agents) | Autonomous AI agents |
| [BlackRoad Network](https://github.com/BlackRoad-Network) | Mesh and distributed networking |
| [BlackRoad Education](https://github.com/BlackRoad-Education) | Learning and tutoring platforms |
| [BlackRoad Labs](https://github.com/BlackRoad-Labs) | Research and experiments |
| [BlackRoad Cloud](https://github.com/BlackRoad-Cloud) | Self-hosted cloud infrastructure |
| [BlackRoad Forge](https://github.com/BlackRoad-Forge) | Developer tools and utilities |

### Links
- **Website**: [blackroad.io](https://blackroad.io)
- **Documentation**: [docs.blackroad.io](https://docs.blackroad.io)
- **Chat**: [chat.blackroad.io](https://chat.blackroad.io)
- **Search**: [search.blackroad.io](https://search.blackroad.io)

---


> BlackRoad Foundation - blackroad crm core

Part of the [BlackRoad OS](https://blackroad.io) ecosystem — [BlackRoad-Foundation](https://github.com/BlackRoad-Foundation)

---

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
