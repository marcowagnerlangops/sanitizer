from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pandas as pd
import streamlit as st

from adapters import read_any, write_by_type
from exporters import build_xlsx_report
from sanitizer_core import (
    BrandRules,
    QAEngine,
    RepairEngine,
    SanitizerSettings,
    SegmentRecord,
    build_stats,
    records_to_dataframe,
)


APP_TITLE = "LangOps Sanitizer Pro"
APP_VERSION = "0.1.0"
MAKER_LINE = "Made by LangOps Solutions"


def init_state():
    if "records" not in st.session_state:
        st.session_state.records = []
    if "stats" not in st.session_state:
        st.session_state.stats = {}
    if "brand_rules" not in st.session_state:
        st.session_state.brand_rules = BrandRules()
    if "logs" not in st.session_state:
        st.session_state.logs = []


def log(msg: str):
    from datetime import datetime
    st.session_state.logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def settings_sidebar() -> SanitizerSettings:
    st.sidebar.header("Sanitizer Settings")

    st.sidebar.subheader("Safe Auto-Repairs")
    normalize_unicode = st.sidebar.checkbox("Normalize Unicode NFC", value=True)
    trim_spaces = st.sidebar.checkbox("Trim leading/trailing spaces", value=True)
    remove_zero_width = st.sidebar.checkbox("Remove zero-width characters", value=True)
    replace_nbsp = st.sidebar.checkbox("Replace non-breaking spaces", value=True)
    collapse_spaces = st.sidebar.checkbox("Collapse repeated spaces", value=True)
    normalize_language_codes = st.sidebar.checkbox("Normalize language codes", value=True)

    st.sidebar.subheader("Flag-Only QA Checks")
    flag_tag_issues = st.sidebar.checkbox("Flag malformed/unbalanced tags", value=True)
    flag_source_equals_target = st.sidebar.checkbox("Flag target equals source", value=True)
    flag_german_micro_qa = st.sidebar.checkbox("Flag German micro-QA issues", value=False)
    flag_brand_protection = st.sidebar.checkbox("Flag brand / do-not-translate issues", value=True)
    flag_placeholder_issues = st.sidebar.checkbox("Flag placeholder mismatch", value=True)
    flag_number_issues = st.sidebar.checkbox("Flag number mismatch", value=True)
    flag_punctuation_issues = st.sidebar.checkbox("Flag punctuation mismatch", value=True)
    flag_length_ratio = st.sidebar.checkbox("Flag suspicious length ratio", value=True)

    return SanitizerSettings(
        normalize_unicode=normalize_unicode,
        trim_spaces=trim_spaces,
        remove_zero_width=remove_zero_width,
        replace_nbsp=replace_nbsp,
        collapse_spaces=collapse_spaces,
        normalize_language_codes=normalize_language_codes,
        flag_tag_issues=flag_tag_issues,
        flag_source_equals_target=flag_source_equals_target,
        flag_german_micro_qa=flag_german_micro_qa,
        flag_brand_protection=flag_brand_protection,
        flag_placeholder_issues=flag_placeholder_issues,
        flag_number_issues=flag_number_issues,
        flag_punctuation_issues=flag_punctuation_issues,
        flag_length_ratio=flag_length_ratio,
    )


def rerun_qa(settings: SanitizerSettings):
    QAEngine.apply(st.session_state.records, settings, st.session_state.brand_rules)
    st.session_state.stats = build_stats(st.session_state.records)


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    with st.expander("Filters", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            severity = st.selectbox("Severity", ["All", "Issues", "OK"])
        with c2:
            file_type = st.selectbox("File Type", ["All"] + sorted(df["Type"].dropna().unique().tolist()))
        with c3:
            search = st.text_input("Search")

        categories = sorted({cat.strip() for val in df["Issue Categories"].fillna("") for cat in str(val).split(";") if cat.strip()})
        category = st.selectbox("Issue Category", ["All"] + categories)

    out = df.copy()

    if severity != "All":
        out = out[out["Severity"] == severity]

    if file_type != "All":
        out = out[out["Type"] == file_type]

    if category != "All":
        out = out[out["Issue Categories"].fillna("").str.contains(category, case=False, regex=False)]

    if search.strip():
        needle = search.strip().lower()
        out = out[
            out["File"].fillna("").str.lower().str.contains(needle, regex=False)
            | out["Source"].fillna("").str.lower().str.contains(needle, regex=False)
            | out["Target"].fillna("").str.lower().str.contains(needle, regex=False)
            | out["Issue Details"].fillna("").str.lower().str.contains(needle, regex=False)
        ]

    return out


def parse_uploaded_files(uploaded_files, source_lang: str, target_lang: str) -> list[SegmentRecord]:
    all_records = []
    next_id = 1

    for uploaded in uploaded_files:
        records, meta = read_any(uploaded, next_id, source_lang, target_lang)
        all_records.extend(records)
        if records:
            next_id = max(r.record_id for r in all_records) + 1
        log(f"Loaded {uploaded.name}: {len(records)} segments")

    return all_records


def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="🧼", layout="wide")
    init_state()

    st.title("🧼 LangOps Sanitizer Pro")
    st.caption("One tool to sanitize XLSX, CSV, XLIFF, XLF, TXLF, and XLZ localization files.")

    settings = settings_sidebar()

    st.sidebar.divider()
    st.sidebar.subheader("Brand Protection Rules")
    brand_file = st.sidebar.file_uploader("Upload Do Not Translate XLSX/CSV", type=["xlsx", "csv"])
    if brand_file is not None:
        try:
            if brand_file.name.lower().endswith(".csv"):
                df = pd.read_csv(brand_file)
            else:
                df = pd.read_excel(brand_file, header=None)
            count = st.session_state.brand_rules.load_from_dataframe(df)
            st.sidebar.success(f"Loaded {count} protected term rules.")
        except Exception as exc:
            st.sidebar.error(str(exc))

    st.sidebar.divider()
    st.sidebar.caption(f"{MAKER_LINE} · v{APP_VERSION}")

    tabs = st.tabs(["Upload & Analyze", "Dashboard", "Segments", "Duplicates", "Edit", "Export", "Logs"])

    with tabs[0]:
        st.subheader("Upload Files")
        st.write("Supported formats: XLSX, CSV, TMX, XLIFF, XLF, TXLF, XLZ")

        uploaded = st.file_uploader(
            "Upload one or multiple files",
            type=["xlsx","csv","tmx","xlf","xliff","txlf","xlz"],
            accept_multiple_files=True,
        )

        c1, c2 = st.columns(2)
        with c1:
            source_lang = st.text_input("Default source language for XLSX/CSV", value="en-US")
        with c2:
            target_lang = st.text_input("Default target language for XLSX/CSV", value="de-DE")

        c1, c2, c3 = st.columns(3)

        with c1:
            if st.button("Analyze Files", type="primary", use_container_width=True):
                if not uploaded:
                    st.warning("Please upload at least one file.")
                else:
                    try:
                        with st.spinner("Reading files and running sanitizer..."):
                            st.session_state.records = parse_uploaded_files(uploaded, source_lang, target_lang)
                            changed = RepairEngine.apply(st.session_state.records, settings)
                            rerun_qa(settings)
                            log(f"Analysis complete. Records: {len(st.session_state.records)}. Auto-repaired: {changed}.")
                        st.success(f"Loaded {len(st.session_state.records)} segments.")
                    except Exception as exc:
                        st.error(str(exc))
                        log(f"Error: {exc}")

        with c2:
            if st.button("Run Selected Auto-Repairs", use_container_width=True):
                if not st.session_state.records:
                    st.warning("Analyze files first.")
                else:
                    changed = RepairEngine.apply(st.session_state.records, settings)
                    rerun_qa(settings)
                    log(f"Auto-repairs run. Changed records: {changed}.")
                    st.success(f"Auto-repairs complete. Changed {changed} records.")

        with c3:
            if st.button("Clear Project", use_container_width=True):
                st.session_state.records = []
                st.session_state.stats = {}
                st.session_state.logs = []
                st.session_state.brand_rules = BrandRules()
                st.success("Project cleared.")
                st.rerun()

        st.info(
            "This tool intentionally flags risky problems instead of automatically rewriting them. "
            "Only selected safe repairs are applied automatically."
        )

    with tabs[1]:
        records = st.session_state.records
        stats = st.session_state.stats

        if not records:
            st.info("Upload and analyze files first.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Segments", stats.get("total_segments", 0))
            c2.metric("Segments with Issues", stats.get("segments_with_issues", 0))
            c3.metric("Clean Segments", stats.get("clean_segments", 0))
            c4.metric("Brand Rules", len(st.session_state.brand_rules.rules))

            st.divider()
            c1, c2 = st.columns(2)

            with c1:
                issue_categories = stats.get("issue_categories", {})
                if issue_categories:
                    st.subheader("Issue Categories")
                    chart_df = pd.DataFrame(issue_categories.items(), columns=["Category", "Count"])
                    st.bar_chart(chart_df, x="Category", y="Count")

            with c2:
                file_types = stats.get("file_types", {})
                if file_types:
                    st.subheader("File Types")
                    st.dataframe(pd.DataFrame(file_types.items(), columns=["Type", "Count"]), use_container_width=True, hide_index=True)

            st.subheader("Duplicate Summary")
            st.json(stats.get("duplicate_summary", {}))

    with tabs[2]:
        if not st.session_state.records:
            st.info("No records loaded.")
        else:
            df = records_to_dataframe(st.session_state.records)
            filtered = filter_dataframe(df)
            st.caption(f"Showing {len(filtered)} of {len(df)} records.")
            st.dataframe(filtered, use_container_width=True, hide_index=True, height=620)

    with tabs[3]:
        records = st.session_state.records
        if not records:
            st.info("No records loaded.")
        else:
            groups = defaultdict(list)
            for r in records:
                groups[(r.file_type, r.source_lang, r.target_lang, r.source_text)].append(r)
            dup_groups = {k: v for k, v in groups.items() if len(v) > 1}

            st.subheader("Same-Source Duplicate Groups")
            st.caption(f"Found {len(dup_groups)} duplicate group(s).")

            if dup_groups:
                labels = [
                    f"{i + 1}: {k[0]} | {k[1]}>{k[2]} | {k[3][:90]} ({len(v)} records)"
                    for i, (k, v) in enumerate(dup_groups.items())
                ]
                selected = st.selectbox("Select duplicate group", labels)
                idx = labels.index(selected)
                group_records = list(dup_groups.values())[idx]
                st.dataframe(records_to_dataframe(group_records), use_container_width=True, hide_index=True)

                keep_id = st.selectbox("Record ID to keep", [r.record_id for r in group_records])
                if st.button("Keep Selected / Remove Others"):
                    delete_ids = {r.record_id for r in group_records if r.record_id != keep_id}
                    st.session_state.records = [r for r in st.session_state.records if r.record_id not in delete_ids]
                    rerun_qa(settings)
                    log(f"Duplicate group resolved. Kept {keep_id}, removed {len(delete_ids)}.")
                    st.success(f"Removed {len(delete_ids)} records.")
                    st.rerun()

    with tabs[4]:
        records = st.session_state.records
        if not records:
            st.info("No records loaded.")
        else:
            st.subheader("Edit Segment")
            ids = [r.record_id for r in records]
            selected_id = st.selectbox("Record ID", ids)
            record = next(r for r in records if r.record_id == selected_id)

            with st.form("edit_form"):
                c1, c2, c3 = st.columns(3)
                source_lang = c1.text_input("Source language", value=record.source_lang)
                target_lang = c2.text_input("Target language", value=record.target_lang)
                unit_id = c3.text_input("Unit ID", value=record.unit_id)
                source = st.text_area("Source", value=record.source_text, height=160)
                target = st.text_area("Target", value=record.target_text, height=160)
                notes = st.text_area("Notes", value=record.notes, height=80)
                submitted = st.form_submit_button("Save Changes")

            if submitted:
                record.source_lang = source_lang.strip()
                record.target_lang = target_lang.strip()
                record.unit_id = unit_id.strip()
                record.source_text = source.strip()
                record.target_text = target.strip()
                record.notes = notes.strip()
                rerun_qa(settings)
                log(f"Edited record {selected_id}.")
                st.success("Record updated.")

            if st.button("Delete Selected Record"):
                st.session_state.records = [r for r in st.session_state.records if r.record_id != selected_id]
                rerun_qa(settings)
                log(f"Deleted record {selected_id}.")
                st.success("Record deleted.")
                st.rerun()

    with tabs[5]:
        records = st.session_state.records
        if not records:
            st.info("No records loaded.")
        else:
            st.subheader("Export")

            xlsx_report = build_xlsx_report(records, st.session_state.stats)
            st.download_button(
                "Download Full XLSX QA Report",
                data=xlsx_report,
                file_name="langops_sanitizer_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

            st.divider()
            st.write("Export sanitized files by type.")

            by_type = defaultdict(list)
            for r in records:
                by_type[r.file_type].append(r)

            for file_type, group in sorted(by_type.items()):
                data, name, mime = write_by_type(group, file_type)
                st.download_button(
                    f"Download sanitized {file_type.upper()} export ({len(group)} segments)",
                    data=data,
                    file_name=name,
                    mime=mime,
                    use_container_width=True,
                )

            st.warning("Always test-import sanitized exports into your CAT/TMS environment before replacing production files.")

    with tabs[6]:
        if not st.session_state.logs:
            st.info("No logs yet.")
        else:
            st.text_area("Logs", "\n".join(st.session_state.logs), height=650)


if __name__ == "__main__":
    main()
