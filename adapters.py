from __future__ import annotations

import os
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Tuple
import xml.etree.ElementTree as ET

import pandas as pd
from openpyxl import load_workbook

from sanitizer_core import SegmentRecord


def _strip_namespace(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def _xml_lang(elem: ET.Element) -> str:
    return elem.attrib.get("{http://www.w3.org/XML/1998/namespace}lang") or elem.attrib.get("lang") or ""


def _clean_text(text: str) -> str:
    import re
    return re.sub(r"\s+", " ", text or "").strip()


def _find_source_target_columns(df: pd.DataFrame) -> Tuple[int, int]:
    cols = [str(c).strip().lower() for c in df.columns]
    source_candidates = {"source", "src", "english", "en", "source text", "source_text"}
    target_candidates = {"target", "trg", "translation", "german", "de", "target text", "target_text"}

    src_idx = None
    tgt_idx = None

    for idx, col in enumerate(cols):
        if src_idx is None and col in source_candidates:
            src_idx = idx
        if tgt_idx is None and col in target_candidates:
            tgt_idx = idx

    if src_idx is None or tgt_idx is None:
        if len(df.columns) < 2:
            raise ValueError("File must contain at least two columns.")
        return 0, 1

    return src_idx, tgt_idx


def read_xlsx(uploaded_file, start_id: int, source_lang: str, target_lang: str) -> Tuple[List[SegmentRecord], Dict]:
    df = pd.read_excel(uploaded_file)
    src_idx, tgt_idx = _find_source_target_columns(df)
    records = []

    for row_idx, row in df.iterrows():
        source = "" if pd.isna(row.iloc[src_idx]) else str(row.iloc[src_idx])
        target = "" if pd.isna(row.iloc[tgt_idx]) else str(row.iloc[tgt_idx])
        records.append(SegmentRecord(
            record_id=start_id + len(records),
            file_name=uploaded_file.name,
            file_type="xlsx",
            unit_id=str(row_idx + 1),
            source_lang=source_lang,
            target_lang=target_lang,
            source_text=source,
            target_text=target,
            source_path=f"row:{row_idx + 2};col:{src_idx + 1}",
            target_path=f"row:{row_idx + 2};col:{tgt_idx + 1}",
        ))

    meta = {"kind": "xlsx", "source_col": src_idx, "target_col": tgt_idx, "original_name": uploaded_file.name}
    return records, meta


def write_xlsx(records: List[SegmentRecord]) -> bytes:
    df = pd.DataFrame({
        "source": [r.source_text for r in records],
        "target": [r.target_text for r in records],
        "issue_status": [r.severity for r in records],
        "issue_categories": [r.issue_categories for r in records],
        "issue_details": [r.issue_details for r in records],
        "repair_actions": [r.repair_actions for r in records],
    })
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sanitized")
        ws = writer.sheets["Sanitized"]
        for col in ["A", "B", "C", "D", "E", "F"]:
            ws.column_dimensions[col].width = 45
    output.seek(0)
    return output.getvalue()


def read_csv(uploaded_file, start_id: int, source_lang: str, target_lang: str) -> Tuple[List[SegmentRecord], Dict]:
    content = uploaded_file.getvalue()
    last_error = None
    df = None

    for enc in ["utf-8-sig", "utf-8", "cp1252", "latin1"]:
        for sep in [None, ",", ";", "\t", "|"]:
            try:
                buffer = BytesIO(content)
                if sep is None:
                    df = pd.read_csv(buffer, encoding=enc, sep=None, engine="python")
                else:
                    df = pd.read_csv(buffer, encoding=enc, sep=sep)
                break
            except Exception as exc:
                last_error = exc
        if df is not None:
            break

    if df is None:
        raise ValueError(f"Could not read CSV. Last error: {last_error}")

    src_idx, tgt_idx = _find_source_target_columns(df)
    records = []

    for row_idx, row in df.iterrows():
        source = "" if pd.isna(row.iloc[src_idx]) else str(row.iloc[src_idx])
        target = "" if pd.isna(row.iloc[tgt_idx]) else str(row.iloc[tgt_idx])
        records.append(SegmentRecord(
            record_id=start_id + len(records),
            file_name=uploaded_file.name,
            file_type="csv",
            unit_id=str(row_idx + 1),
            source_lang=source_lang,
            target_lang=target_lang,
            source_text=source,
            target_text=target,
            source_path=f"row:{row_idx + 2};col:{src_idx + 1}",
            target_path=f"row:{row_idx + 2};col:{tgt_idx + 1}",
        ))

    meta = {"kind": "csv", "source_col": src_idx, "target_col": tgt_idx, "original_name": uploaded_file.name}
    return records, meta


def write_csv(records: List[SegmentRecord]) -> bytes:
    df = pd.DataFrame({
        "source": [r.source_text for r in records],
        "target": [r.target_text for r in records],
        "issue_status": [r.severity for r in records],
        "issue_categories": [r.issue_categories for r in records],
        "issue_details": [r.issue_details for r in records],
        "repair_actions": [r.repair_actions for r in records],
    })
    return df.to_csv(index=False).encode("utf-8-sig")


def read_xliff_bytes(file_bytes: bytes, file_name: str, start_id: int, preferred_source: str, preferred_target: str, file_type: str = "xliff") -> Tuple[List[SegmentRecord], Dict]:
    root = ET.fromstring(file_bytes)
    records: List[SegmentRecord] = []

    source_lang = preferred_source or root.attrib.get("srcLang") or root.attrib.get("source-language") or ""
    target_lang = preferred_target or root.attrib.get("trgLang") or root.attrib.get("target-language") or ""

    trans_units = [e for e in root.iter() if _strip_namespace(e.tag) in {"trans-unit", "unit"}]

    for idx, unit in enumerate(trans_units, start=1):
        unit_id = unit.attrib.get("id") or unit.attrib.get("resname") or str(idx)

        source_text = ""
        target_text = ""

        # XLIFF 1.2
        for child in unit.iter():
            tag = _strip_namespace(child.tag)
            if tag == "source" and not source_text:
                source_text = _clean_text("".join(child.itertext()))
                if not source_lang:
                    source_lang = _xml_lang(child)
            if tag == "target" and not target_text:
                target_text = _clean_text("".join(child.itertext()))
                if not target_lang:
                    target_lang = _xml_lang(child)

        if source_text or target_text:
            records.append(SegmentRecord(
                record_id=start_id + len(records),
                file_name=file_name,
                file_type=file_type,
                unit_id=unit_id,
                source_lang=source_lang,
                target_lang=target_lang,
                source_text=source_text,
                target_text=target_text,
                source_path=f"unit:{unit_id};source",
                target_path=f"unit:{unit_id};target",
            ))

    meta = {"kind": file_type, "original_name": file_name, "original_bytes": file_bytes}
    return records, meta


def read_xliff(uploaded_file, start_id: int, preferred_source: str, preferred_target: str) -> Tuple[List[SegmentRecord], Dict]:
    ext = Path(uploaded_file.name).suffix.lower().lstrip(".") or "xliff"
    return read_xliff_bytes(uploaded_file.getvalue(), uploaded_file.name, start_id, preferred_source, preferred_target, ext)


def write_xliff(records: List[SegmentRecord]) -> bytes:
    root = ET.Element("xliff", version="1.2")
    file_elem = ET.SubElement(root, "file", {
        "source-language": records[0].source_lang if records else "",
        "target-language": records[0].target_lang if records else "",
        "datatype": "plaintext",
        "original": "sanitized_export",
    })
    body = ET.SubElement(file_elem, "body")

    for idx, r in enumerate(records, start=1):
        tu = ET.SubElement(body, "trans-unit", {"id": r.unit_id or str(idx)})
        src = ET.SubElement(tu, "source")
        src.text = r.source_text
        tgt = ET.SubElement(tu, "target")
        tgt.text = r.target_text

    output = BytesIO()
    ET.ElementTree(root).write(output, encoding="utf-8", xml_declaration=True)
    return output.getvalue()


def read_xlz(uploaded_file, start_id: int, preferred_source: str, preferred_target: str) -> Tuple[List[SegmentRecord], Dict]:
    records: List[SegmentRecord] = []
    archive_bytes = uploaded_file.getvalue()
    internal_files = []

    with zipfile.ZipFile(BytesIO(archive_bytes), "r") as zf:
        for name in zf.namelist():
            if name.lower().endswith((".xlf", ".xliff", ".txlf")):
                content = zf.read(name)
                inner_records, _ = read_xliff_bytes(content, f"{uploaded_file.name}/{name}", start_id + len(records), preferred_source, preferred_target, "xlz")
                for r in inner_records:
                    r.meta["xlz_inner_path"] = name
                records.extend(inner_records)
                internal_files.append(name)

    meta = {"kind": "xlz", "original_name": uploaded_file.name, "internal_files": internal_files}
    return records, meta


def write_xlz(records: List[SegmentRecord]) -> bytes:
    output = BytesIO()
    grouped = {}
    for r in records:
        inner = r.meta.get("xlz_inner_path") or "sanitized_export.xliff"
        grouped.setdefault(inner, []).append(r)

    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for inner, group in grouped.items():
            zf.writestr(inner, write_xliff(group))

    return output.getvalue()


def read_any(uploaded_file, start_id: int, source_lang: str, target_lang: str) -> Tuple[List[SegmentRecord], Dict]:
    ext = Path(uploaded_file.name).suffix.lower()

    if ext == ".xlsx":
        return read_xlsx(uploaded_file, start_id, source_lang, target_lang)
    if ext == ".csv":
        return read_csv(uploaded_file, start_id, source_lang, target_lang)
    if ext in {".xlf", ".xliff", ".txlf"}:
        return read_xliff(uploaded_file, start_id, source_lang, target_lang)
    if ext == ".xlz":
        return read_xlz(uploaded_file, start_id, source_lang, target_lang)

    raise ValueError(f"Unsupported file type: {uploaded_file.name}")


def write_by_type(records: List[SegmentRecord], file_type: str) -> Tuple[bytes, str, str]:
    if file_type == "xlsx":
        return write_xlsx(records), "sanitized_xlsx_export.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if file_type == "csv":
        return write_csv(records), "sanitized_csv_export.csv", "text/csv"
    if file_type in {"xlf", "xliff", "txlf"}:
        return write_xliff(records), "sanitized_xliff_export.xliff", "application/xml"
    if file_type == "xlz":
        return write_xlz(records), "sanitized_xlz_export.xlz", "application/zip"

    return write_xlsx(records), "sanitized_export.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
