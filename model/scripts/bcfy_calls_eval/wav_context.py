"""Parse framing context from Broadcastify-Calls eval WAV metadata.

Provenance (confirmed 2026-05-20): the eval clips were captured by scanner
software (ProScan / SDRTrunk) that embedded the talkgroup's identity directly
in the audio file's ID3-style tags. Jake's collection script downloaded the
Broadcastify-Calls MP3s (named ``{groupId}_{call_ts}``) and a later rename
dropped the system ``sid``, keeping only ``{call_ts}-{tgDec}`` -- which is why
the integer in our filenames is a *talkgroup decimal*, not a system id, and why
recovering the groupId from the Broadcastify API is impossible (the sid is
gone and tgDec collides across systems). The tags, however, carry the system
name, site, and talkgroup alias per call -- a more faithful per-call source
than any API round-trip. This module turns those tags into the record shape
the framing renderer consumes.

Two tag dialects appear:
- **ProScan**: ``comment`` = "Date:..;System:X;Site:Y;Name:Z;Decoder:..;..",
  ``title`` = '{tg}"{alias}"' or 'P:{tg} [{x}]"{alias1}","{alias2}"'.
- **SDRTrunk**: ``artist`` = a system short-code (p800a, cfdl, ICAWIN),
  ``title`` = a bare alpha tag or alias (FDLFD1, "SCHP Oburg Disp").

All functions here are pure (no I/O). ``build_context_records.py`` does the
download + ffprobe and calls ``build_record`` per clip.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Curated tables
#
# These are authoritative hand-mappings I verified against the in-band agency
# abbreviations in each system's talkgroup aliases (e.g. SCHP -> South Carolina
# Highway Patrol confirms Palmetto 800 = SC). Unlike a fuzzy index lookup, a
# curated table can't false-match Wisconsin to California. Systems not listed
# simply contribute no state clause (the renderer drops it).
# ---------------------------------------------------------------------------

# SDRTrunk artist short-code -> clean system name. Codes whose system is not
# confidently identifiable map to None (kept out of the prose).
SYSTEM_CODE_MAP: dict[str, str | None] = {
    "p800a": "Palmetto 800",
    "cfdl": "Wisconsin Interoperable System for Communications (WISCOM)",
    "icawin": "Idaho Cooperative Agencies Wireless Interoperable Network (ICAWIN)",
    "sc21102": "STARCOM21",
    "d14-massachusetts state police": "Massachusetts State Police",
    "monroebcfy": "Monroe County",
    "sccsc": None,
    "ptrsbrg": None,
    "jasper": None,
    "p25": None,
}

# Distinctive token (lower-cased) found in the system name -> US state. Matched
# longest-key-first against the normalized system string. Acronyms and full
# names both included so either tag dialect resolves.
SYSTEM_STATE_MAP: list[tuple[str, str]] = [
    ("palmetto 800", "South Carolina"),
    ("schp", "South Carolina"),
    ("starcom21", "Illinois"),
    ("maine state communications", "Maine"),
    ("mscommnet", "Maine"),
    ("kansas statewide interoperable", "Kansas"),
    ("ksics", "Kansas"),
    ("mississippi wireless information", "Mississippi"),
    ("mswin", "Mississippi"),
    ("nebraska state radio", "Nebraska"),
    ("arkansas wireless information", "Arkansas"),
    ("awin", "Arkansas"),
    ("oh marcs", "Ohio"),
    ("wyolink", "Wyoming"),
    ("connecticut land mobile", "Connecticut"),
    ("clmrn", "Connecticut"),
    ("wisconsin interoperable system", "Wisconsin"),
    ("wiscom", "Wisconsin"),
    ("indiana project hoosier", "Indiana"),
    ("hoosier safe-t", "Indiana"),
    ("missouri statewide wireless", "Missouri"),
    ("moswin", "Missouri"),
    ("hawaii wireless", "Hawaii"),
    ("hiwin", "Hawaii"),
    ("alabama interoperable radio", "Alabama"),
    ("airs", "Alabama"),
    ("michigan's public safety", "Michigan"),
    ("mpscs", "Michigan"),
    ("mpssc", "Montana"),  # Montana Public Safety; MHP/Smelter site confirm MT
    ("idaho cooperative agencies", "Idaho"),
    ("icawin", "Idaho"),
    ("iowa statewide interoperable", "Iowa"),
    ("isics", "Iowa"),
    ("viper", "North Carolina"),
    ("topaz regional wireless", "Arizona"),
    ("arizona wireless integrated", "Arizona"),
    ("az wins", "Arizona"),
    ("st. mary's county", "Maryland"),
    ("first responders interoperable", "Maryland"),
    ("westcomm regional", "Massachusetts"),
    ("erie county", "New York"),
]

# Service inference: ordered (label, keyword-regex). First match wins, so the
# combined Fire/EMS pattern precedes the single-service ones. Labels are plain
# words (no acronyms) because the renderer lower-cases tagDescr then
# capitalizes only the first letter.
_SERVICE_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("fire and EMS dispatch", re.compile(r"\bfire\s*/\s*ems\b|\bfire\s*&\s*ems\b", re.I)),
    # `fd\d` catches the common alpha-tag convention (FDLFD1, STAFD2) where the
    # service code is embedded without a word boundary.
    ("fire dispatch", re.compile(r"\bfire\b|\bfd\b|fd\d|\barff\b|crash|rescue|\bf/?r\b", re.I)),
    ("emergency medical dispatch", re.compile(r"\bems\b|\bmedic\b|medical|ambulance|\bald\b", re.I)),
    (
        "law enforcement dispatch",
        re.compile(
            r"\bpd\b|pd\d|police|sheriff|\bso\b|\bhp\b|\bsp\b|troop|\btrp\b|patrol|"
            r"\bdps\b|\bschp\b|\bkhp\b|\bmhp\b|\bahp\b|\bcsp\b|\bisp\b|\balea\b|"
            r"\bchp\b|\bnsp\b|law\b|trooper",
            re.I,
        ),
    ),
    ("paging and alerting", re.compile(r"\balert\b|\bpage\b|paging|\btone\b|alerting", re.I)),
    ("radio dispatch", re.compile(r"\bdisp\b|dispatch|\bdsp\b|\bdp\b", re.I)),
]


# ---------------------------------------------------------------------------
# Pure parsers
# ---------------------------------------------------------------------------


def parse_title(title: str) -> tuple[str, str]:
    """Return ``(alias, alpha)`` from a scanner title tag.

    - ``alias``: the human-readable talkgroup name used as ``tgCname``.
    - ``alpha``: a short alpha tag used as ``tgDisplay`` (aggressive variant),
      when one is distinguishable from the alias; otherwise "".

    Handles the four observed dialects:
      '7106"MdTAP BayBr"'                       -> ("MdTAP BayBr", "")
      'P:9603 [9601]"SCHP Florence","SCHP Darl"'-> ("SCHP Florence", "SCHP Darl")
      '37904-Joint Base Cape Cod Fire And Crash'-> ("Joint Base Cape Cod Fire And Crash", "")
      'FDLFD1'                                  -> ("FDLFD1", "FDLFD1")
      '17014' (bare tgDec)                      -> ("", "")
      ''                                        -> ("", "")
    """
    t = (title or "").strip()
    if not t:
        return "", ""

    quoted = re.findall(r'"([^"]*)"', t)
    if quoted:
        quoted = [q.strip() for q in quoted if q.strip() and q.strip().lower() != "new alias"]
        if quoted:
            alias = quoted[0]
            alpha = quoted[1] if len(quoted) > 1 else ""
            return alias, alpha

    m = re.match(r"^\d+\s*-\s*(.+)$", t)  # {tgDec}-{alias}
    if m:
        return m.group(1).strip(), ""

    if t.isdigit():  # bare tgDec, no alias
        return "", ""

    # SDRTrunk plain: the title *is* the alpha tag / alias.
    return t, t


def parse_proscan_comment(comment: str) -> dict[str, str]:
    """Parse the ProScan ``comment`` 'Key:Value;Key:Value;..' string.

    Returns a dict with any of system/site/name/decoder/frequency present.
    Empty dict for a non-ProScan comment.
    """
    c = comment or ""
    if "System:" not in c and "Site:" not in c:
        return {}
    out: dict[str, str] = {}
    for key, dst in (("System", "system"), ("Site", "site"), ("Name", "name"),
                     ("Decoder", "decoder"), ("Frequency", "frequency")):
        m = re.search(rf"{key}:([^;]*)", c)
        if m and m.group(1).strip():
            out[dst] = m.group(1).strip()
    return out


def infer_service(*texts: str) -> str:
    """Infer a service descriptor (``tagDescr``) from talkgroup text.

    Scans the provided strings (alias, site, name) against ordered service
    rules; returns the first matching label, or "" if none match.
    """
    blob = " ".join(t for t in texts if t)
    for label, pat in _SERVICE_RULES:
        if pat.search(blob):
            return label
    return ""


def normalize_system(system_raw: str, artist: str) -> str:
    """Resolve a clean system name from the ProScan System field or the
    SDRTrunk artist short-code. Returns "" when neither is usable.
    """
    s = (system_raw or "").strip()
    if s:
        # "Conv. Frequency:.." is a conventional-frequency marker, not a system.
        if s.lower().startswith("conv"):
            return ""
        # Strip ProScan site suffixes: "OH MARCS- BRCO - Mt Orab" -> "OH MARCS".
        s = re.split(r"\s*-\s*", s)[0].strip() if s.upper().startswith("OH MARCS") else s
        return s
    code = (artist or "").strip().lower()
    if code in SYSTEM_CODE_MAP:
        return SYSTEM_CODE_MAP[code] or ""
    return ""


def lookup_state(system_name: str) -> str:
    """Map a system name to its US state via the curated table ("" if absent)."""
    s = (system_name or "").lower()
    if not s:
        return ""
    for token, state in sorted(SYSTEM_STATE_MAP, key=lambda kv: -len(kv[0])):
        if token in s:
            return state
    return ""


def _clean_county(site: str) -> str:
    """A ProScan Site is usually a county or facility. Keep county-ish sites,
    drop control/site-id noise ('037 (25)', 'Troop C__774_05625', 'County').
    """
    s = (site or "").strip()
    # ProScan often appends a site qualifier after " - " ("Brown - Mt Orab").
    s = s.split(" - ", 1)[0].strip()
    if not s or s.lower() == "county":
        return ""
    if re.search(r"\d", s):  # site IDs like "037 (25)" or "Casper 800"
        # keep a leading word if it's alphabetic (e.g. "Casper 800" -> "Casper")
        lead = re.match(r"^([A-Za-z][A-Za-z.' ]+?)\s*\d", s)
        return lead.group(1).strip() if lead else ""
    # Strip a trailing "Region"/"VHF"/"Site" qualifier.
    s = re.sub(r"\s+(Region|VHF|Site|Control)$", "", s, flags=re.I).strip()
    return s


def build_record(meta: dict, example_id: str) -> dict:
    """Assemble the framing record (renderer field shape) from one clip's tags.

    Fields: groupId(=example_id), tgCname, tagDescr, sName, county, state,
    tgDisplay, tgDescr. Missing/uninferable fields are "" (the renderer drops
    their clause, or falls back to baseline if agency+service are absent).
    """
    title = meta.get("title", "")
    artist = meta.get("artist", "")
    pros = parse_proscan_comment(meta.get("comment", ""))

    alias, alpha = parse_title(title)
    if alpha and alpha.strip().lower() == alias.strip().lower():
        alpha = ""  # SDRTrunk title is both alias and alpha — don't duplicate it
    system = normalize_system(pros.get("system", ""), artist)
    state = lookup_state(system or artist)
    county = _clean_county(pros.get("site", ""))
    service = infer_service(alias, pros.get("name", ""), title)

    return {
        "groupId": example_id,
        "tgCname": alias,
        "tagDescr": service,
        "sName": system,
        "county": county,
        "state": state,
        "tgDisplay": alpha,
        "tgDescr": "",  # ProScan 'Name' is control-channel noise; no clean source
    }
