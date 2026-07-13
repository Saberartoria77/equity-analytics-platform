# equity-analytics-platform

## Project

Portfolio equity-data pipeline using Python 3.11, PostgreSQL, SQLAlchemy, Pandas, Streamlit, and Plotly.

## Source of truth

- `schema.sql` owns every table, constraint, index, and analytical view.
- Price and indicator persistence must use upserts because providers and calculations can change history.
- Backtests use end-of-day signals only for subsequent returns.
- Production database access must be lazy; pure calculations must import without `DB_URL`.

## Verification

Run `ruff check .`, `pytest -v`, and `python -m compileall -q .` before claiming completion. Tests must not use live providers or the production database.

## Remaining roadmap

- Predictive model
- Text-to-SQL interface
- Containerization
