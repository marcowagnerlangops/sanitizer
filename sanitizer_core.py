from __future__ import annotations

import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Tuple


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
    normalize_unicode: bool = True
    trim_spaces: bool = True
    remove_zero_width: bool = True
    replace_nbsp: bool = True
    collapse_spaces: bool = True
    normalize_language_codes: bool = True

    flag_tag_issues: bool = True
    flag_source_equals_target: bool = True
    flag_german_micro_qa: bool = True
    flag_brand_protection: bool = True
    flag_placeholder_issues: bool = True
    flag_number_issues: bool = True
    flag_punctuation_issues: bool = True
    flag_length_ratio: bool = True


LANGUAGE_CODE_MAP = {
    "de": "de-DE", "de-de": "de-DE", "de_de": "de-DE", "deu": "de-DE", "ger": "de-DE", "german": "de-DE",
    "de-at": "de-AT", "de_at": "de-AT", "de-ch": "de-CH", "de_ch": "de-CH",
    "en": "en-US", "en-us": "en-US", "en_us": "en-US", "eng": "en-US", "english": "en-US",
    "en-gb": "en-GB", "en_gb": "en-GB", "en-uk": "en-GB", "en_uk": "en-GB",
    "fr": "fr-FR", "fr-fr": "fr-FR", "fr_fr": "fr-FR", "fra": "fr-FR", "fre": "fr-FR",
    "fr-ca": "fr-CA", "fr_ca": "fr-CA",
    "es": "es-ES", "es-es": "es-ES", "es_es": "es-ES", "spa": "es-ES",
    "es-mx": "es-MX", "es_mx": "es-MX", "es-ww": "es-WW", "es_ww": "es-WW",
    "pt": "pt-PT", "pt-pt": "pt-PT", "pt_pt": "pt-PT", "pt-br": "pt-BR", "pt_br": "pt-BR",
    "it": "it-IT", "it-it": "it-IT", "it_it": "it-IT",
    "nl": "nl-NL", "nl-nl": "nl-NL", "nl_nl": "nl-NL",
    "ja": "ja-JP", "ja-jp": "ja-JP", "ja_jp": "ja-JP",
    "ko": "ko-KR", "ko-kr": "ko-KR", "ko_kr": "ko-KR",
    "zh": "zh-CN", "zh-cn": "zh-CN", "zh_cn": "zh-CN", "zh-hans": "zh-CN",
    "zh-tw": "zh-TW", "zh_tw": "zh-TW", "zh-hant": "zh-TW",
    "ar": "ar-SA", "ar-sa": "ar-SA", "ar_sa": "ar-SA",
    "pl": "pl-PL", "pl-pl": "pl-PL", "pl_pl": "pl-PL",
    "cs": "cs-CZ", "cs-cz": "cs-CZ", "cs_cz": "cs-CZ",
    "sv": "sv-SE", "sv-se": "sv-SE", "sv_se": "sv-SE",
}


def normalize_language_code(code: str) -> str:
    raw = (code or "").strip()
    if not raw:
        return ""
    key = raw.lower().replace(" ", "").replace("_", "-")
    alt_key = raw.lower().replace(" ", "")
    return LANGUAGE_CODE_MAP.get(key) or LANGUAGE_CODE_MAP.get(alt_key) or raw.replace("_", "-")


def is_german(code: str) -> bool:
    return (code or "").lower().startswith("de")


class BrandRules:
    def __init__(self) -> None:
        self.rules: List[Dict[str, str]] = []

    def load_from_dataframe(self, df) -> int:
        if df.shape[1] < 2:
            raise ValueError("Brand XLSX/CSV must use Column A = source term and Column B = required target representation.")
        rules = []
        for _, row in df.iterrows():
            src = "" if row.iloc[0] is None else str(row.iloc[0]).strip()
            req = "" if row.iloc[1] is None else str(row.iloc[1]).strip()
            note = "" if df.shape[1] < 3 or row.iloc[2] is None else str(row.iloc[2]).strip()
            if src and req and src.lower() != "source":
                rules.append({"source": src, "required": req, "note": note})
        self.rules = rules
        return len(rules)


class RepairEngine:
    ZERO_WIDTH = re.compile(r"[\u200b\u200c\u200d\ufeff]")

    @staticmethod
    def repair_text(text: str, settings: SanitizerSettings) -> Tuple[str, List[str]]:
        value = text or ""
        actions: List[str] = []

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
                actions.append("Zero-width character removed")

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
    def apply(records: List[SegmentRecord], settings: SanitizerSettings) -> int:
        changed = 0
        for r in records:
            before = (r.source_text, r.target_text, r.source_lang, r.target_lang)
            source, s_actions = RepairEngine.repair_text(r.source_text, settings)
            target, t_actions = RepairEngine.repair_text(r.target_text, settings)
            r.source_text = source
            r.target_text = target

            actions = [f"Source: {a}" for a in s_actions] + [f"Target: {a}" for a in t_actions]

            if settings.normalize_language_codes:
                old_src = r.source_lang
                old_tgt = r.target_lang
                r.source_lang = normalize_language_code(r.source_lang)
                r.target_lang = normalize_language_code(r.target_lang)
                if old_src != r.source_lang:
                    actions.append(f"Source language normalized: {old_src} -> {r.source_lang}")
                if old_tgt != r.target_lang:
                    actions.append(f"Target language normalized: {old_tgt} -> {r.target_lang}")

            r.repair_actions = "; ".join(actions)
            after = (r.source_text, r.target_text, r.source_lang, r.target_lang)
            if before != after:
                changed += 1
        return changed


class QAEngine:
    PLACEHOLDER_PATTERNS = [
        r"\{\d+\}",
        r"\{[A-Za-z0-9_]+\}",
        r"%s",
        r"%d",
        r"\$\{[A-Za-z0-9_]+\}",
        r"<[^>]+>",
    ]
    TAG_PATTERN = re.compile(r"</?([A-Za-z][A-Za-z0-9:_-]*)(?:\s[^>]*)?>")

    @staticmethod
    def placeholders(text: str) -> List[str]:
        found = []
        for patt in QAEngine.PLACEHOLDER_PATTERNS:
            found.extend(re.findall(patt, text or ""))
        return sorted(found)

    @staticmethod
    def numbers(text: str) -> List[str]:
        return re.findall(r"\d+(?:[\.,]\d+)?", text or "")

    @staticmethod
    def end_punct(text: str) -> str:
        text = (text or "").strip()
        return text[-1] if text and text[-1] in ".,:;!?" else ""

    @staticmethod
    def has_tag_issue(text: str) -> bool:
        text = text or ""
        if "<" not in text and ">" not in text:
            return False
        if text.count("<") != text.count(">"):
            return True

        stack = []
        for m in QAEngine.TAG_PATTERN.finditer(text):
            full = m.group(0)
            tag = m.group(1).lower()
            if full.endswith("/>") or tag in {"br", "hr", "img", "input", "meta", "link"}:
                continue
            if full.startswith("</"):
                if not stack or stack[-1] != tag:
                    return True
                stack.pop()
            else:
                stack.append(tag)
        return bool(stack)

    @staticmethod
    def brand_violations(source: str, target: str, brand_rules: BrandRules) -> List[str]:
        issues = []
        for rule in brand_rules.rules:
            src = rule["source"]
            required = rule["required"]
            src_pattern = r"(?<!\w)" + re.escape(src) + r"(?!\w)"
            if re.search(src_pattern, source or "", flags=re.IGNORECASE):
                req_pattern = r"(?<!\w)" + re.escape(required) + r"(?!\w)"
                if not re.search(req_pattern, target or "", flags=re.IGNORECASE):
                    issues.append(f"Protected term '{src}' should appear as '{required}'")
        return issues

    @staticmethod
    def german_micro_issues(r: SegmentRecord) -> List[str]:
        if not is_german(r.target_lang):
            return []
        target = r.target_text or ""
        issues = []
        if re.search(r"\s+[.,:;!?]", target):
            issues.append("German QA: space before punctuation")
        if re.search(r"\b(\w+)\s+\1\b", target, flags=re.IGNORECASE):
            issues.append("German QA: repeated word")
        if '"' in target:
            issues.append("German QA: straight quotes used; check German quotation style")
        if re.search(r"\b(the|and|with|for|from|your|our|you)\b", target, flags=re.IGNORECASE):
            issues.append("German QA: possible English word in German target")
        if re.search(r"\d+\s?%", target):
            issues.append("German QA: check percent spacing style")
        return issues

    @staticmethod
    def apply(records: List[SegmentRecord], settings: SanitizerSettings, brand_rules: BrandRules) -> None:
        for r in records:
            issues = []
            cats = []
            s = r.source_text or ""
            t = r.target_text or ""

            if not t.strip():
                issues.append("Missing target")
                cats.append("Missing Target")

            if settings.flag_source_equals_target and s.strip() and t.strip() and s.strip() == t.strip():
                issues.append("Target equals source; possible untranslated segment")
                cats.append("Source=Target")

            if settings.flag_placeholder_issues and QAEngine.placeholders(s) != QAEngine.placeholders(t):
                issues.append("Placeholder/tag mismatch")
                cats.append("Placeholders")

            if settings.flag_number_issues and QAEngine.numbers(s) != QAEngine.numbers(t):
                issues.append("Number mismatch")
                cats.append("Numbers")

            if settings.flag_punctuation_issues:
                sp = QAEngine.end_punct(s)
                tp = QAEngine.end_punct(t)
                if sp != tp and (sp or tp):
                    issues.append("Ending punctuation mismatch")
                    cats.append("Punctuation")

            if settings.flag_length_ratio and s:
                ratio = len(t) / max(1, len(s))
                if ratio < 0.35 or ratio > 2.8:
                    issues.append("Suspicious source/target length ratio")
                    cats.append("Length")

            if settings.flag_tag_issues:
                if QAEngine.has_tag_issue(s):
                    issues.append("Source contains malformed or unbalanced HTML/XML-like tags")
                    cats.append("Tags")
                if QAEngine.has_tag_issue(t):
                    issues.append("Target contains malformed or unbalanced HTML/XML-like tags")
                    cats.append("Tags")

            if settings.flag_brand_protection and brand_rules.rules:
                b = QAEngine.brand_violations(s, t, brand_rules)
                if b:
                    issues.extend(b)
                    cats.append("Brand Protection")

            if settings.flag_german_micro_qa:
                g = QAEngine.german_micro_issues(r)
                if g:
                    issues.extend(g)
                    cats.append("German Micro QA")

            r.issue_count = len(issues)
            r.severity = "Issues" if issues else "OK"
            r.issue_categories = "; ".join(sorted(set(cats)))
            r.issue_details = "; ".join(issues)


class DuplicateEngine:
    @staticmethod
    def norm(text: str) -> str:
        text = unicodedata.normalize("NFC", text or "")
        text = re.sub(r"\s+", " ", text.strip().lower())
        text = re.sub(r"[\W_]+", "", text, flags=re.UNICODE)
        return text

    @staticmethod
    def summary(records: List[SegmentRecord]) -> Dict[str, int]:
        exact = Counter((r.source_lang, r.target_lang, r.source_text, r.target_text) for r in records)
        same_source = defaultdict(set)
        norm_source = Counter((r.source_lang, DuplicateEngine.norm(r.source_text)) for r in records)

        for r in records:
            same_source[(r.source_lang, r.source_text)].add(r.target_text)

        return {
            "exact_source_target_duplicates": sum(1 for r in records if exact[(r.source_lang, r.target_lang, r.source_text, r.target_text)] > 1),
            "same_source_different_target": sum(1 for r in records if len(same_source[(r.source_lang, r.source_text)]) > 1),
            "normalized_source_duplicates": sum(1 for r in records if norm_source[(r.source_lang, DuplicateEngine.norm(r.source_text))] > 1),
        }


def build_stats(records: List[SegmentRecord]) -> Dict[str, object]:
    return {
        "total_segments": len(records),
        "segments_with_issues": sum(1 for r in records if r.issue_count),
        "clean_segments": sum(1 for r in records if not r.issue_count),
        "language_pairs": Counter(f"{r.source_lang} > {r.target_lang}" for r in records),
        "issue_categories": Counter(cat.strip() for r in records for cat in r.issue_categories.split(";") if cat.strip()),
        "file_types": Counter(r.file_type for r in records),
        "per_file_counts": Counter(r.file_name for r in records),
        "duplicate_summary": DuplicateEngine.summary(records),
    }


def records_to_dataframe(records: List[SegmentRecord]):
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
