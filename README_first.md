# LangOps Sanitizer Pro

LangOps Sanitizer Pro is a Streamlit-based localization file sanitizer for bilingual and exchange formats.

It supports:

- XLSX
- CSV
- XLIFF
- XLF
- TXLF
- XLZ

The tool is designed for human-in-the-loop cleanup, QA, and controlled repair of localization assets.

## Features

- Upload one or multiple files
- Auto-detect supported formats
- Extract source and target segments into one common review model
- Apply selected safe auto-repairs
- Normalize language codes
- Normalize Unicode
- Remove zero-width characters
- Replace non-breaking spaces
- Collapse repeated spaces
- Flag source equals target
- Flag number mismatches
- Flag placeholder mismatches
- Flag punctuation mismatches
- Flag malformed or unbalanced tags
- Flag German micro-QA issues
- Upload a Do Not Translate / Brand Protection file
- Edit individual segments
- Resolve duplicate groups
- Export XLSX QA report
- Export sanitized files by format

## Brand Protection File

Upload an XLSX or CSV file with this structure:

| Column A | Column B | Column C optional |
|---|---|---|
| Source protected term | Required target representation | Note |

Example:

| Oracle | Oracle | Brand name |
| Azure | Azure | Product name |

## Installation

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Streamlit Cloud Deployment

Use this repository structure:

```text
app.py
sanitizer_core.py
adapters.py
exporters.py
requirements.txt
README.md
```

Push to GitHub and deploy via Streamlit Cloud.

## Important Notice

Use at your own risk. This is an ongoing project and should be tested with sample files before production use.

## Current Limitations

- XLIFF, XLF, TXLF, and XLZ exports are rebuilt into clean simplified bilingual files.
- Original formatting and full package structure may not be preserved.
- This version is best suited for QA review, cleanup, and controlled bilingual export.
- Always test-import sanitized files into your CAT/TMS environment before replacing production files.

## Author

LangOps Solutions
