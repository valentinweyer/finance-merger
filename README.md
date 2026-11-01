# finance-merge-summarizer

Python tools to **merge**, **clean**, and **summarize** personal finance CSV exports (from bank, credit card, PayPal, etc.).

Built to replace spreadsheet workflows (e.g., Google Sheets) with fast, reliable, and customizable Python scripts.  
Supports **category filtering**, **shared cost handling**, and **monthly summaries**.

---

## 🚀 Features

-  Merge CSVs from different financial sources (bank, credit card, PayPal)
-  Clean inconsistent formats (e.g., date formats, decimal commas)
-  Preserve and prioritize manually assigned categories
-  Handle shared expenses (e.g., split rent or utilities with roommate)
-  Filter and summarize by category and month
-  Export ready-to-use cleaned data or summaries

---

## 📦 Requirements

- [uv](https://docs.astral.sh/uv/) (installs Python 3.12+, `pandas` and `chardet` from `pyproject.toml`)

```bash
uv sync
uv run "Finance Tracker combiner.py"
```
