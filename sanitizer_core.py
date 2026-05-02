# sanitizer_core.py

from __future__ import annotations

import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class SegmentRecord:
    record_id: int
    file_name: str
    file_type: str
    unit_id: str

    source_lang: str
    target_lang: str

    source_text: str
    target_text: str

    source_path: str = ""
    target_path: str = ""
    notes: str = ""

    issue_count: int = 0
    severity: str = "OK"
    issue_categories: str = ""
    issue_details: str = ""
    repair_actions: str = ""

    meta: Dict[str, str] = field(default_factory=dict)


@dataclass
class SanitizerSettings:
    # Safe Auto Repairs
    normalize_unicode: bool = True
    trim_spaces: bool = True
    remove_zero_width: bool = True
    replace_nbsp: bool = True
    collapse_spaces: bool = True
    normalize_language_codes: bool = True

    # QA Checks
    flag_tag_issues: bool = True
    flag_source_equals_target: bool = True
    flag_german_micro_qa: bool = True
    flag_brand_protection: bool = True
    flag_placeholder_issues: bool = True
    flag_number_issues: bool = True
    flag_punctuation_issues: bool = True
    flag_length_ratio: bool = True

    # NEW TYPOGRAPHY
    flag_double_ellipsis: bool = True
    flag_double_spaces: bool = True
    flag_double_dot: bool = True
    flag_space_before_period: bool = True

    # NEW GLOSSARY
    flag_glossary_violations: bool = True


# ============================================================
# LANGUAGE NORMALIZATION
# ============================================================

LANGUAGE_CODE_MAP = {
    "de": "de-DE",
    "de-de": "de-DE",
    "de_de": "de-DE",
    "deu": "de-DE",
    "ger": "de-DE",

    "en": "en-US",
    "en-us": "en-US",
    "en_us": "en-US",
    "eng": "en-US",

    "en-gb": "en-GB",
    "en_gb": "en-GB",
    "en-uk": "en-GB",

    "fr": "fr-FR",
    "fr-ca": "fr-CA",

    "es": "es-ES",
    "es-mx": "es-MX",

    "pt": "pt-PT",
    "pt-br": "pt-BR",

    "it": "it-IT",
    "nl": "nl-NL",
    "ja": "ja-JP",
    "ko": "ko-KR",
    "zh": "zh-CN",
    "zh-tw": "zh-TW",
}


def normalize_language_code(code: str) -> str:
    raw = (code or "").strip()

    if not raw:
        return ""

    key = raw.lower().replace("_", "-")

    return LANGUAGE_CODE_MAP.get(key, raw.replace("_", "-"))


def is_german(code: str) -> bool:
    return (code or "").lower().startswith("de")


# ============================================================
# BRAND RULES
# ============================================================

class BrandRules:
    def __init__(self):
        self.rules = []

    def load_from_dataframe(self, df):
        if df.shape[1] < 2:
            raise ValueError("Brand file needs Column A + B")

        self.rules = []

        for _, row in df.iterrows():
            source = str(row.iloc[0]).strip()
            target = str(row.iloc[1]).strip()

            if source and target:
                self.rules.append({
                    "source": source,
                    "required": target
                })

        return len(self.rules)


# ============================================================
# GLOSSARY RULES
# ============================================================

class GlossaryRules:
    def __init__(self):
        self.rules = []

    def load_from_dataframe(self, df):
        if df.shape[1] < 2:
            raise ValueError("Glossary needs Column A = source / Column B = target")

        self.rules = []

        for _, row in df.iterrows():
            source = str(row.iloc[0]).strip()
            target = str(row.iloc[1]).strip()

            if source and target:
                self.rules.append({
                    "source": source,
                    "required": target
                })

        return len(self.rules)


# ============================================================
# REPAIR ENGINE
# ============================================================

class RepairEngine:
    ZERO_WIDTH = re.compile(r"[\u200b\u200c\u200d\ufeff]")

    @staticmethod
    def repair_text(text, settings):
        value = text or ""
        actions = []

        before = value

        if settings.normalize_unicode:
            value = unicodedata.normalize("NFC", value)
            if value != before:
                actions.append("Unicode normalized")

        before = value

        if settings.replace_nbsp:
            value = value.replace("\xa0", " ")
            if value != before:
                actions.append("NBSP replaced")

        before = value

        if settings.remove_zero_width:
            value = RepairEngine.ZERO_WIDTH.sub("", value)
            if value != before:
                actions.append("Zero-width removed")

        before = value

        if settings.collapse_spaces:
            value = re.sub(r"[ \t]{2,}", " ", value)
            if value != before:
                actions.append("Repeated spaces collapsed")

        before = value

        if settings.trim_spaces:
            value = value.strip()
            if value != before:
                actions.append("Trimmed")

        return value, actions

    @staticmethod
    def apply(records, settings):
        changed = 0

        for r in records:
            before = (
                r.source_text,
                r.target_text,
                r.source_lang,
                r.target_lang
            )

            src, a1 = RepairEngine.repair_text(r.source_text, settings)
            tgt, a2 = RepairEngine.repair_text(r.target_text, settings)

            r.source_text = src
            r.target_text = tgt

            actions = (
                [f"Source: {x}" for x in a1] +
                [f"Target: {x}" for x in a2]
            )

            if settings.normalize_language_codes:
                old1 = r.source_lang
                old2 = r.target_lang

                r.source_lang = normalize_language_code(r.source_lang)
                r.target_lang = normalize_language_code(r.target_lang)

                if old1 != r.source_lang:
                    actions.append(f"Source lang {old1}->{r.source_lang}")

                if old2 != r.target_lang:
                    actions.append(f"Target lang {old2}->{r.target_lang}")

            r.repair_actions = "; ".join(actions)

            after = (
                r.source_text,
                r.target_text,
                r.source_lang,
                r.target_lang
            )

            if before != after:
                changed += 1

        return changed


# ============================================================
# QA ENGINE
# ============================================================

class QAEngine:

    PLACEHOLDER_PATTERNS = [
        r"\{\d+\}",
        r"\{[A-Za-z0-9_]+\}",
        r"%s",
        r"%d",
        r"<[^>]+>",
    ]

    TAG_PATTERN = re.compile(r"</?([A-Za-z][A-Za-z0-9:_-]*)(?:\s[^>]*)?>")

    # --------------------------------------------------------
    # HELPERS
    # --------------------------------------------------------

    @staticmethod
    def placeholders(text):
        found = []

        for patt in QAEngine.PLACEHOLDER_PATTERNS:
            found.extend(re.findall(patt, text or ""))

        return sorted(found)

    @staticmethod
    def numbers(text):
        return re.findall(r"\d+(?:[\.,]\d+)?", text or "")

    @staticmethod
    def end_punct(text):
        text = (text or "").strip()

        return text[-1] if text and text[-1] in ".,:;!?" else ""

    @staticmethod
    def has_tag_issue(text):
        text = text or ""

        if "<" not in text:
            return False

        if text.count("<") != text.count(">"):
            return True

        stack = []

        for m in QAEngine.TAG_PATTERN.finditer(text):
            full = m.group(0)
            tag = m.group(1).lower()

            if full.endswith("/>"):
                continue

            if full.startswith("</"):
                if not stack or stack[-1] != tag:
                    return True
                stack.pop()
            else:
                stack.append(tag)

        return bool(stack)

    # --------------------------------------------------------
    # BRAND
    # --------------------------------------------------------

    @staticmethod
    def brand_violations(source, target, brand_rules):
        issues = []

        for rule in brand_rules.rules:
            patt = r"\b" + re.escape(rule["source"]) + r"\b"

            if re.search(patt, source, flags=re.IGNORECASE):
                required = r"\b" + re.escape(rule["required"]) + r"\b"

                if not re.search(required, target, flags=re.IGNORECASE):
                    issues.append(
                        f"Protected term '{rule['source']}' should be '{rule['required']}'"
                    )

        return issues

    # --------------------------------------------------------
    # GLOSSARY
    # --------------------------------------------------------

    @staticmethod
    def glossary_violations(source, target, glossary_rules):
        issues = []

        for rule in glossary_rules.rules:

            source_patt = r"\b" + re.escape(rule["source"]) + r"\b"

            if re.search(source_patt, source, flags=re.IGNORECASE):

                target_patt = r"\b" + re.escape(rule["required"]) + r"\b"

                if not re.search(target_patt, target, flags=re.IGNORECASE):
                    issues.append(
                        f"Glossary violation: {rule['source']} -> {rule['required']}"
                    )

        return issues

    # --------------------------------------------------------
    # GERMAN MICRO QA
    # --------------------------------------------------------

    @staticmethod
    def german_micro_issues(record):
        if not is_german(record.target_lang):
            return []

        target = record.target_text or ""
        issues = []

        if re.search(r"\s+[.,:;!?]", target):
            issues.append("German QA: space before punctuation")

        if re.search(r"\b(\w+)\s+\1\b", target, flags=re.IGNORECASE):
            issues.append("German QA: repeated word")

        if '"' in target:
            issues.append("German QA: straight quotes used")

        return issues

    # --------------------------------------------------------
    # TYPOGRAPHY
    # --------------------------------------------------------

    @staticmethod
    def typography_issues(target, settings):
        issues = []

        if settings.flag_double_ellipsis:
            if re.search(r"\.{4,}", target):
                issues.append("Repeated ellipsis / too many dots")

        if settings.flag_double_dot:
            if re.search(r"(?<!\.)\.\.(?!\.)", target):
                issues.append("Double period detected")

        if settings.flag_double_spaces:
            if re.search(r" {2,}", target):
                issues.append("Double spaces detected")

        if settings.flag_space_before_period:
            if re.search(r"\s+\.", target):
                issues.append("Space before period detected")

        return issues

    # --------------------------------------------------------
    # MAIN APPLY
    # --------------------------------------------------------

    @staticmethod
    def apply(records, settings, brand_rules, glossary_rules):
        for r in records:

            issues = []
            cats = []

            s = r.source_text or ""
            t = r.target_text or ""

            # Missing target
            if not t.strip():
                issues.append("Missing target")
                cats.append("Missing Target")

            # Source = target
            if settings.flag_source_equals_target:
                if s.strip() and t.strip() and s.strip() == t.strip():
                    issues.append("Target equals source")
                    cats.append("Source=Target")

            # Placeholders
            if settings.flag_placeholder_issues:
                if QAEngine.placeholders(s) != QAEngine.placeholders(t):
                    issues.append("Placeholder mismatch")
                    cats.append("Placeholders")

            # Numbers
            if settings.flag_number_issues:
                if QAEngine.numbers(s) != QAEngine.numbers(t):
                    issues.append("Number mismatch")
                    cats.append("Numbers")

            # Punctuation
            if settings.flag_punctuation_issues:
                if QAEngine.end_punct(s) != QAEngine.end_punct(t):
                    if QAEngine.end_punct(s) or QAEngine.end_punct(t):
                        issues.append("Ending punctuation mismatch")
                        cats.append("Punctuation")

            # Length ratio
            if settings.flag_length_ratio:
                if s:
                    ratio = len(t) / max(1, len(s))
                    if ratio < 0.35 or ratio > 2.8:
                        issues.append("Suspicious length ratio")
                        cats.append("Length")

            # Tags
            if settings.flag_tag_issues:
                if QAEngine.has_tag_issue(s):
                    issues.append("Source malformed tags")
                    cats.append("Tags")

                if QAEngine.has_tag_issue(t):
                    issues.append("Target malformed tags")
                    cats.append("Tags")

            # Brand protection
            if settings.flag_brand_protection:
                brand = QAEngine.brand_violations(s, t, brand_rules)

                if brand:
                    issues.extend(brand)
                    cats.append("Brand Protection")

            # Glossary
            if settings.flag_glossary_violations:
                glossary = QAEngine.glossary_violations(
                    s,
                    t,
                    glossary_rules
                )

                if glossary:
                    issues.extend(glossary)
                    cats.append("Glossary")

            # German QA
            if settings.flag_german_micro_qa:
                german = QAEngine.german_micro_issues(r)

                if german:
                    issues.extend(german)
                    cats.append("German QA")

            # Typography
            typo = QAEngine.typography_issues(t, settings)

            if typo:
                issues.extend(typo)
                cats.append("Typography")

            # Finalize
            r.issue_count = len(issues)
            r.severity = "Issues" if issues else "OK"
            r.issue_categories = "; ".join(sorted(set(cats)))
            r.issue_details = "; ".join(issues)


# ============================================================
# STATS
# ============================================================

def build_stats(records):
    return {
        "total_segments": len(records),
        "segments_with_issues": sum(1 for r in records if r.issue_count),
        "clean_segments": sum(1 for r in records if not r.issue_count),

        "issue_categories": Counter(
            cat.strip()
            for r in records
            for cat in r.issue_categories.split(";")
            if cat.strip()
        ),

        "file_types": Counter(r.file_type for r in records),

        "language_pairs": Counter(
            f"{r.source_lang}>{r.target_lang}"
            for r in records
        ),
    }


# ============================================================
# DATAFRAME
# ============================================================

def records_to_dataframe(records):
    import pandas as pd

    return pd.DataFrame([
        {
            "Record ID": r.record_id,
            "File": r.file_name,
            "Type": r.file_type,
            "Unit ID": r.unit_id,
            "Source Lang": r.source_lang,
            "Target Lang": r.target_lang,
            "Source": r.source_text,
            "Target": r.target_text,
            "Severity": r.severity,
            "Issue Count": r.issue_count,
            "Issue Categories": r.issue_categories,
            "Issue Details": r.issue_details,
            "Repair Actions": r.repair_actions,
            "Notes": r.notes,
        }
        for r in records
    ])
