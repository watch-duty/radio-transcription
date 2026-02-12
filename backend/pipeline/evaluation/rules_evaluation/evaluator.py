# This module provides a simple text evaluation mechanism based on predefined rules.
import re

RULES = [
    {
        "id": "basic_fire_terms",
        "pattern": r"\b(fire|burn|evacuation|spreading)\b",
    },
]


def evaluate_text(text: str) -> dict:
    """
    Analyzes text against the rules and returns a summary.
    """
    matches = []
    if text:
        for rule in RULES:
            if re.search(rule["pattern"], text, re.IGNORECASE):
                matches.append(
                    {
                        "rule_id": rule["id"],
                    }
                )

    return {"is_flagged": len(matches) > 0, "triggered_rules": matches}
