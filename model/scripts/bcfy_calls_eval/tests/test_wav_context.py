"""Unit tests for wav_context — parsing framing from scanner WAV metadata."""

from __future__ import annotations

import pytest

from bcfy_calls_eval import wav_context as wc


# --- parse_title ----------------------------------------------------------


@pytest.mark.parametrize(
    "title,expected",
    [
        ('7106"MdTAP BayBr"', ("MdTAP BayBr", "")),
        ('751"Troop I"', ("Troop I", "")),
        ('P:9603 [9601]"SCHP Florence","SCHP Darln"', ("SCHP Florence", "SCHP Darln")),
        ('P:9603"SCHP Florence"', ("SCHP Florence", "")),
        ('37904-Joint Base Cape Cod Fire And Crash', ("Joint Base Cape Cod Fire And Crash", "")),
        ("FDLFD1", ("FDLFD1", "FDLFD1")),
        ("ISP EVA DISP", ("ISP EVA DISP", "ISP EVA DISP")),
        ("17014", ("", "")),
        ("", ("", "")),
        ('P:9603 [9601]"SCHP Florence","New Alias"', ("SCHP Florence", "")),  # drop placeholder
    ],
)
def test_parse_title(title, expected):
    assert wc.parse_title(title) == expected


# --- parse_proscan_comment ------------------------------------------------


def test_parse_proscan_comment_full():
    c = ("Date:2026-04-02 00:26:20.840;System:PALMETTO 800;Site:FC PUBLIC SAFETY ;"
         "Name:T-CONTROL ;Decoder:P25 Phase 1;Channel:0-1233;Frequency:858712500;")
    out = wc.parse_proscan_comment(c)
    assert out["system"] == "PALMETTO 800"
    assert out["site"] == "FC PUBLIC SAFETY"
    assert out["name"] == "T-CONTROL"
    assert out["frequency"] == "858712500"


def test_parse_proscan_comment_non_proscan():
    assert wc.parse_proscan_comment("") == {}
    assert wc.parse_proscan_comment("just some text") == {}


# --- infer_service --------------------------------------------------------


@pytest.mark.parametrize(
    "texts,expected",
    [
        (("BrCo FIRE/EMS Communications",), "fire and EMS dispatch"),
        (("Chicopee FD Disp",), "fire dispatch"),
        (("FIRE - Surry County - Disp",), "fire dispatch"),
        (("SCHP Florence",), "law enforcement dispatch"),
        (("KHP Troop A-1",), "law enforcement dispatch"),
        (("HAWAII STATE SHERIFF",), "law enforcement dispatch"),
        (("01-STA ALERT",), "paging and alerting"),
        (("MO PAGE 154.235",), "paging and alerting"),
        (("StM Alpha 1 Main",), ""),  # no service keyword
        (("",), ""),
    ],
)
def test_infer_service(texts, expected):
    assert wc.infer_service(*texts) == expected


def test_infer_service_fire_beats_law_order():
    # A combined fire/ems tag must resolve to the combined label, not law.
    assert wc.infer_service("Engine Co Fire Dispatch") == "fire dispatch"


# --- normalize_system -----------------------------------------------------


def test_normalize_system_proscan_passthrough():
    assert wc.normalize_system("Palmetto 800", "") == "Palmetto 800"


def test_normalize_system_strips_oh_marcs_site():
    assert wc.normalize_system("OH MARCS- BRCO - Mt Orab", "") == "OH MARCS"


def test_normalize_system_sdrtrunk_code():
    assert wc.normalize_system("", "p800a") == "Palmetto 800"
    assert wc.normalize_system("", "cfdl").startswith("Wisconsin Interoperable")


def test_normalize_system_unknown_code_blank():
    assert wc.normalize_system("", "sccsc") == ""
    assert wc.normalize_system("", "totally-unknown") == ""


def test_normalize_system_conventional_marker_blank():
    assert wc.normalize_system("Conv. Frequency:460.16250", "") == ""


# --- lookup_state ---------------------------------------------------------


@pytest.mark.parametrize(
    "system,state",
    [
        ("Palmetto 800", "South Carolina"),
        ("Maine State Communications Network (MSCommNet)", "Maine"),
        ("WyoLink", "Wyoming"),
        ("Mississippi Wireless Information Network (MSWIN)", "Mississippi"),
        ("STARCOM21", "Illinois"),
        ("Indiana Project Hoosier SAFE-T", "Indiana"),
        ("Unknown System", ""),
        ("", ""),
    ],
)
def test_lookup_state(system, state):
    assert wc.lookup_state(system) == state


def test_lookup_state_no_wisconsin_california_falsematch():
    # Regression: "Wisconsin Interoperable System" must NOT map to California.
    assert wc.lookup_state("Wisconsin Interoperable System for Communications (WISCOM)") == "Wisconsin"


# --- build_record (integration of the parsers) ----------------------------


def test_build_record_proscan_full():
    meta = {
        "title": 'P:9603 [9601]"SCHP Florence","SCHP Darln"',
        "comment": "Date:..;System:Palmetto 800;Site:Chesterfield;Name:T-Chesterfield Site;",
        "artist": "2503356",
    }
    r = wc.build_record(meta, "1775123736-9603")
    assert r["groupId"] == "1775123736-9603"
    assert r["tgCname"] == "SCHP Florence"
    assert r["tagDescr"] == "law enforcement dispatch"
    assert r["sName"] == "Palmetto 800"
    assert r["county"] == "Chesterfield"
    assert r["state"] == "South Carolina"
    assert r["tgDisplay"] == "SCHP Darln"


def test_build_record_sdrtrunk_minimal():
    meta = {"title": "FDLFD1", "artist": "cfdl", "comment": ""}
    r = wc.build_record(meta, "1775173303-20107")
    assert r["tgCname"] == "FDLFD1"
    assert r["tagDescr"] == "fire dispatch"
    assert r["sName"].startswith("Wisconsin Interoperable")
    assert r["state"] == "Wisconsin"
    # alpha tag equals the alias for SDRTrunk plain titles -> not duplicated
    assert r["tgDisplay"] == ""


def test_build_record_county_strips_site_suffix():
    meta = {
        "title": '8501"BrCo FIRE/EMS Communications"',
        "comment": "System:OH MARCS- BRCO - Mt Orab;Site:Brown - Mt Orab;Name:T-Control;",
        "artist": "",
    }
    r = wc.build_record(meta, "1775151465-8501")
    assert r["county"] == "Brown"  # not "Brown - Mt Orab"
    assert r["sName"] == "OH MARCS"
    assert r["tagDescr"] == "fire and EMS dispatch"


def test_build_record_empty_meta_degrades():
    r = wc.build_record({"title": "", "artist": "", "comment": ""}, "123-456")
    assert r["tgCname"] == ""
    assert r["tagDescr"] == ""
    # renderer will treat this as baseline (no agency+service)
