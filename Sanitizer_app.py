from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import pandas as pd
import streamlit as st

from adapters import read_any, write_by_type
from exporters import build_xlsx_report
from sanitizer_core import (
    BrandRules,
    GlossaryRules,
    QAEngine,
    RepairEngine,
    SanitizerSettings,
    build_stats,
    records_to_dataframe,
)

# ============================================================
# CONFIG
# ============================================================

APP_TITLE = "LangOps Sanitizer Pro"
APP_VERSION = "3.0"
MAKER_LINE = "Made by LangOps Solutions"


# ============================================================
# SESSION STATE
# ============================================================

def init_state():
    if "records" not in st.session_state:
        st.session_state.records = []

    if "stats" not in st.session_state:
        st.session_state.stats = {}

    if "brand_rules" not in st.session_state:
        st.session_state.brand_rules = BrandRules()

    if "glossary_rules" not in st.session_state:
        st.session_state.glossary_rules = GlossaryRules()

    if "logs" not in st.session_state:
        st.session_state.logs = []


def log(msg):
    stamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.logs.append(f"[{stamp}] {msg}")


# ============================================================
# SETTINGS
# ============================================================

def sidebar_settings():

    st.sidebar.header("Sanitizer Settings")

    st.sidebar.subheader("Safe Auto Repairs")

    normalize_unicode = st.sidebar.checkbox("Normalize Unicode", True)
    trim_spaces = st.sidebar.checkbox("Trim Spaces", True)
    remove_zero_width = st.sidebar.checkbox("Remove Zero Width", True)
    replace_nbsp = st.sidebar.checkbox("Replace NBSP", True)
    collapse_spaces = st.sidebar.checkbox("Collapse Repeated Spaces", True)
    normalize_language_codes = st.sidebar.checkbox("Normalize Language Codes", True)

    st.sidebar.subheader("QA Checks")

    flag_tag_issues = st.sidebar.checkbox("Malformed Tags", True)
    flag_source_equals_target = st.sidebar.checkbox("Source = Target", True)
    flag_german_micro_qa = st.sidebar.checkbox("German Micro QA", False)
    flag_brand_protection = st.sidebar.checkbox("Brand Protection", True)
    flag_placeholder_issues = st.sidebar.checkbox("Placeholder Mismatch", True)
    flag_number_issues = st.sidebar.checkbox("Number Mismatch", True)
    flag_punctuation_issues = st.sidebar.checkbox("Punctuation Mismatch", True)
    flag_length_ratio = st.sidebar.checkbox("Suspicious Length Ratio", True)

    flag_double_ellipsis = st.sidebar.checkbox("Repeated Ellipsis / ....", True)
    flag_double_spaces = st.sidebar.checkbox("Double Spaces", True)
    flag_double_dot = st.sidebar.checkbox("Double Dot ..", True)
    flag_space_before_period = st.sidebar.checkbox("Space Before Period", True)
    flag_glossary_violations = st.sidebar.checkbox("Glossary Violations", True)

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

        flag_double_ellipsis=flag_double_ellipsis,
        flag_double_spaces=flag_double_spaces,
        flag_double_dot=flag_double_dot,
        flag_space_before_period=flag_space_before_period,
        flag_glossary_violations=flag_glossary_violations,
    )


# ============================================================
# QA
# ============================================================

def rerun_qa(settings):
    QAEngine.apply(
        st.session_state.records,
        settings,
        st.session_state.brand_rules,
        st.session_state.glossary_rules
    )

    st.session_state.stats = build_stats(
        st.session_state.records
    )


# ============================================================
# FILTER TABLE
# ============================================================

def filter_dataframe(df):

    if df.empty:
        return df

    with st.expander("Filters", expanded=True):

        c1, c2, c3 = st.columns(3)

        with c1:
            severity = st.selectbox(
                "Severity",
                ["All", "Issues", "OK"]
            )

        with c2:
            file_type = st.selectbox(
                "File Type",
                ["All"] + sorted(df["Type"].dropna().unique().tolist())
            )

        with c3:
            search = st.text_input("Search")

        categories = sorted({
            cat.strip()
            for val in df["Issue Categories"].fillna("")
            for cat in str(val).split(";")
            if cat.strip()
        })

        category = st.selectbox(
            "Issue Category",
            ["All"] + categories
        )

    out = df.copy()

    if severity != "All":
        out = out[out["Severity"] == severity]

    if file_type != "All":
        out = out[out["Type"] == file_type]

    if category != "All":
        out = out[
            out["Issue Categories"]
            .fillna("")
            .str.contains(category, case=False, regex=False)
        ]

    if search.strip():
        needle = search.lower()

        out = out[
            out["Source"].astype(str).str.lower().str.contains(needle)
            |
            out["Target"].astype(str).str.lower().str.contains(needle)
            |
            out["Issue Details"].astype(str).str.lower().str.contains(needle)
        ]

    return out


# ============================================================
# LOAD FILES
# ============================================================

def parse_uploaded_files(uploaded_files, src_lang, tgt_lang):

    all_records = []
    next_id = 1

    for file in uploaded_files:

        records, meta = read_any(
            file,
            next_id,
            src_lang,
            tgt_lang
        )

        all_records.extend(records)

        if records:
            next_id = max(
                x.record_id for x in all_records
            ) + 1

        log(f"Loaded {file.name}: {len(records)} segments")

    return all_records


# ============================================================
# MERGE HELPERS
# ============================================================

def dedupe_records(records, mode):

    if mode == "No Deduplication":
        return records

    result = []
    seen = set()

    for r in records:

        if mode == "Source + Target":
            key = (
                r.source_lang,
                r.target_lang,
                r.source_text.strip(),
                r.target_text.strip()
            )

        elif mode == "Source Only":
            key = (
                r.source_lang,
                r.source_text.strip()
            )

        else:
            key = (
                r.record_id
            )

        if key not in seen:
            seen.add(key)
            result.append(r)

    return result


# ============================================================
# MAIN
# ============================================================

def main():

    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="🧼",
        layout="wide"
    )

    init_state()

    st.title("🧼 LangOps Sanitizer Pro")

    st.caption(
        "Clean, QA, merge and export TMX, XLSX, CSV, XLIFF, XLF, TXLF and XLZ localization assets."
    )

    settings = sidebar_settings()

    # ========================================================
    # BRAND RULES
    # ========================================================

    st.sidebar.divider()
    st.sidebar.subheader("Brand Protection")

    brand_file = st.sidebar.file_uploader(
        "Upload Brand Rules XLSX / CSV",
        type=["xlsx", "csv"],
        key="brand"
    )

    if brand_file:
        try:
            if brand_file.name.lower().endswith(".csv"):
                df = pd.read_csv(brand_file, header=None)
            else:
                df = pd.read_excel(brand_file, header=None)

            count = st.session_state.brand_rules.load_from_dataframe(df)
            st.sidebar.success(f"{count} brand rules loaded")

        except Exception as exc:
            st.sidebar.error(str(exc))

    # ========================================================
    # GLOSSARY
    # ========================================================

    st.sidebar.divider()
    st.sidebar.subheader("Glossary")

    glossary_file = st.sidebar.file_uploader(
        "Upload Glossary XLSX / CSV",
        type=["xlsx", "csv"],
        key="glossary"
    )

    if glossary_file:
        try:
            if glossary_file.name.lower().endswith(".csv"):
                df = pd.read_csv(glossary_file, header=None)
            else:
                df = pd.read_excel(glossary_file, header=None)

            count = st.session_state.glossary_rules.load_from_dataframe(df)
            st.sidebar.success(f"{count} glossary terms loaded")

        except Exception as exc:
            st.sidebar.error(str(exc))

    st.sidebar.divider()
    st.sidebar.caption(f"{MAKER_LINE} · v{APP_VERSION}")

    # ========================================================
    # TABS
    # ========================================================

    tabs = st.tabs([
        "Upload & Analyze",
        "Dashboard",
        "Segments",
        "Merge Center",
        "Export",
        "Logs"
    ])

    # ========================================================
    # TAB 1
    # ========================================================

    with tabs[0]:

        st.subheader("Upload Files")

        uploaded = st.file_uploader(
            "Upload one or multiple files",
            type=[
                "tmx",
                "xlsx",
                "csv",
                "xlf",
                "xliff",
                "txlf",
                "xlz"
            ],
            accept_multiple_files=True
        )

        c1, c2 = st.columns(2)

        with c1:
            src_lang = st.text_input(
                "Default Source Language",
                "en-US"
            )

        with c2:
            tgt_lang = st.text_input(
                "Default Target Language",
                "de-DE"
            )

        c1, c2, c3 = st.columns(3)

        with c1:
            if st.button("Analyze Files", use_container_width=True):

                if not uploaded:
                    st.warning("Upload files first.")
                else:
                    try:
                        with st.spinner("Analyzing..."):

                            st.session_state.records = parse_uploaded_files(
                                uploaded,
                                src_lang,
                                tgt_lang
                            )

                            changed = RepairEngine.apply(
                                st.session_state.records,
                                settings
                            )

                            rerun_qa(settings)

                            log(
                                f"Analysis complete | "
                                f"{len(st.session_state.records)} segments | "
                                f"{changed} repaired"
                            )

                        st.success("Analysis complete.")

                    except Exception as exc:
                        st.error(str(exc))

        with c2:
            if st.button("Run Auto Repairs", use_container_width=True):

                changed = RepairEngine.apply(
                    st.session_state.records,
                    settings
                )

                rerun_qa(settings)

                st.success(f"{changed} segments updated")

        with c3:
            if st.button("Clear Project", use_container_width=True):

                st.session_state.records = []
                st.session_state.stats = {}
                st.session_state.logs = []
                st.session_state.brand_rules = BrandRules()
                st.session_state.glossary_rules = GlossaryRules()

                st.success("Project cleared")
                st.rerun()

    # ========================================================
    # DASHBOARD
    # ========================================================

    with tabs[1]:

        if not st.session_state.records:
            st.info("No project loaded.")

        else:

            stats = st.session_state.stats

            c1, c2, c3, c4 = st.columns(4)

            c1.metric("Segments", stats["total_segments"])
            c2.metric("Issues", stats["segments_with_issues"])
            c3.metric("Clean", stats["clean_segments"])
            c4.metric(
                "Glossary Terms",
                len(st.session_state.glossary_rules.rules)
            )

            st.divider()

            if stats["issue_categories"]:

                st.subheader("Issue Categories")

                chart = pd.DataFrame(
                    stats["issue_categories"].items(),
                    columns=["Category", "Count"]
                )

                st.bar_chart(
                    chart,
                    x="Category",
                    y="Count"
                )

    # ========================================================
    # SEGMENTS
    # ========================================================

    with tabs[2]:

        if not st.session_state.records:
            st.info("No records loaded.")
        else:

            df = records_to_dataframe(
                st.session_state.records
            )

            filtered = filter_dataframe(df)

            st.dataframe(
                filtered,
                use_container_width=True,
                hide_index=True,
                height=720
            )

    # ========================================================
    # MERGE CENTER
    # ========================================================

    with tabs[3]:

        if not st.session_state.records:
            st.info("Load files first.")
        else:

            st.subheader("Merge Center")

            st.write(
                "Merge all loaded files into one clean export."
            )

            dedupe_mode = st.selectbox(
                "Deduplication",
                [
                    "No Deduplication",
                    "Source + Target",
                    "Source Only"
                ]
            )

            export_type = st.selectbox(
                "Merged Export Format",
                [
                    "tmx",
                    "xlsx",
                    "csv",
                    "xliff"
                ]
            )

            merged = dedupe_records(
                st.session_state.records,
                dedupe_mode
            )

            st.info(
                f"Loaded records: {len(st.session_state.records)} | "
                f"After merge rules: {len(merged)}"
            )

            data, name, mime = write_by_type(
                merged,
                export_type
            )

            st.download_button(
                f"Download Merged {export_type.upper()}",
                data=data,
                file_name=f"merged_{name}",
                mime=mime,
                use_container_width=True
            )

    # ========================================================
    # EXPORT
    # ========================================================

    with tabs[4]:

        if not st.session_state.records:
            st.info("Nothing to export.")

        else:

            report = build_xlsx_report(
                st.session_state.records,
                st.session_state.stats
            )

            st.download_button(
                "Download XLSX QA Report",
                data=report,
                file_name="langops_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

            st.divider()

            grouped = defaultdict(list)

            for r in st.session_state.records:
                grouped[r.file_type].append(r)

            for file_type, group in grouped.items():

                data, name, mime = write_by_type(
                    group,
                    file_type
                )

                st.download_button(
                    f"Download {file_type.upper()} Export",
                    data=data,
                    file_name=name,
                    mime=mime,
                    use_container_width=True
                )

    # ========================================================
    # LOGS
    # ========================================================

    with tabs[5]:

        st.text_area(
            "Logs",
            "\n".join(st.session_state.logs),
            height=720
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()
