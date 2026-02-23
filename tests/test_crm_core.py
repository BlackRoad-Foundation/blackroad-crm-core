"""Tests for BlackRoad CRM Core."""
import pytest
from datetime import date, datetime, timedelta
from crm_core import CRMCore, Contact, Deal, Interaction


@pytest.fixture
def crm():
    c = CRMCore(":memory:")
    yield c
    c.close()


@pytest.fixture
def alice(crm):
    return crm.add_contact(
        Contact(name="Alice Ng", email="alice@example.com",
                phone="555-0100", company="Acme Corp",
                tags=["vip", "enterprise"])
    )


@pytest.fixture
def bob(crm):
    return crm.add_contact(
        Contact(name="Bob Smith", email="bob@example.com",
                company="Beta LLC")
    )


# ── test 1: add and retrieve contact ────────────────────────────────────────
def test_add_and_get_contact(crm, alice):
    fetched = crm.get_contact(alice.id)
    assert fetched is not None
    assert fetched.email == "alice@example.com"
    assert "vip" in fetched.tags
    assert fetched.company == "Acme Corp"


# ── test 2: update contact fields ───────────────────────────────────────────
def test_update_contact(crm, alice):
    ok = crm.update_contact(alice.id, phone="555-9999", company="NewCorp")
    assert ok
    fetched = crm.get_contact(alice.id)
    assert fetched.phone == "555-9999"
    assert fetched.company == "NewCorp"


# ── test 3: log interaction updates last_contact ────────────────────────────
def test_log_interaction(crm, alice):
    ix = crm.log_interaction(alice.id, "call", "Discussed pricing", outcome="positive")
    assert ix.id is not None
    fetched = crm.get_contact(alice.id)
    assert fetched.last_contact is not None
    history = crm.get_contact_history(alice.id)
    assert len(history) == 1
    assert history[0].type == "call"


# ── test 4: deal pipeline stage totals ──────────────────────────────────────
def test_deal_pipeline(crm, alice, bob):
    crm.add_deal(Deal(contact_id=alice.id, title="Big Deal",
                      value=10000, stage="proposal", probability=0.4))
    crm.add_deal(Deal(contact_id=bob.id, title="Small Deal",
                      value=2000, stage="lead", probability=0.05))
    pipeline = crm.get_deal_pipeline()
    assert pipeline["stages"]["proposal"]["count"] == 1
    assert pipeline["stages"]["lead"]["count"] == 1
    assert pipeline["pipeline_value"] == pytest.approx(12000, 0.01)


# ── test 5: advance deal stage ───────────────────────────────────────────────
def test_advance_deal_stage(crm, alice):
    deal = crm.add_deal(Deal(contact_id=alice.id, title="Adv",
                             value=5000, stage="lead", probability=0.05))
    crm.advance_deal_stage(deal.id, "qualified")
    fetched = crm.get_deal(deal.id)
    assert fetched.stage == "qualified"
    assert fetched.probability == pytest.approx(0.20, 0.001)


# ── test 6: revenue forecast ─────────────────────────────────────────────────
def test_forecast_revenue(crm, alice):
    close_date = date.today() + timedelta(days=15)
    crm.add_deal(Deal(contact_id=alice.id, title="Near",
                      value=8000, stage="negotiation", probability=0.7,
                      expected_close=close_date))
    forecast = crm.forecast_revenue(days=30)
    assert forecast["total_expected"] == pytest.approx(5600, 0.1)
    assert forecast["deal_count"] == 1


# ── test 7: follow-up queue excludes recent contacts ────────────────────────
def test_follow_up_queue(crm, alice, bob):
    crm.add_deal(Deal(contact_id=alice.id, title="Active",
                      value=3000, stage="proposal", probability=0.4))
    crm.add_deal(Deal(contact_id=bob.id, title="Active B",
                      value=1000, stage="qualified", probability=0.2))
    # alice gets a recent interaction — should not appear
    crm.log_interaction(alice.id, "email", "Just followed up")
    queue = crm.get_follow_up_queue(days_overdue=1)
    contact_ids = [q["contact_id"] for q in queue]
    assert bob.id in contact_ids
    assert alice.id not in contact_ids


# ── test 8: find contact by email ────────────────────────────────────────────
def test_find_by_email(crm, alice):
    found = crm.find_contact_by_email("alice@example.com")
    assert found is not None
    assert found.id == alice.id
    assert crm.find_contact_by_email("unknown@x.com") is None
