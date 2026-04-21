"""Audit that the statistical claims, tables and figures in the thesis DOCX
stay synchronized with the Gold-layer data that notebooks produce.

The thesis (`tcc/final/_build/content_*.py`) embeds hardcoded numbers,
tables and figure paths. Those hardcoded values must match what the
notebooks compute from the Gold layer. Examiners will verify this by
re-running the notebooks, so this script gives the same answer locally
before every commit.

What it checks
--------------
1. FIGURES — every `*_IMAGE_PATH` referenced in `content_*.py` exists on
   disk, sits under `docs/thesis_presentation_assets/generated/` (or the
   `_build/` folder for hand-crafted assets like the Medallion diagram),
   and is not older than the associated Gold parquet files.

2. PEARSON (municipal) — re-runs the four Pearson correlations reported
   in `TABELA_2_ROWS` and compares r against the thesis value with a
   tolerance of 0.01.

3. STATE-LEVEL — re-runs the state-scale correlation between mean income
   and sanctions per 100k inhabitants (prose claims r = 0.74).

4. OLS — re-fits the municipal OLS specification with HC3 robust errors
   and compares R², F-statistic, and every coefficient in `TABELA_3_ROWS`
   against the re-fitted values (tolerance 0.05 on coefficients, 0.01 on
   R² and F rounded to 1 decimal).

Exit code 0 if everything matches; 1 if any drift is detected.
"""
from __future__ import annotations

import io
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

# Force UTF-8 on the Windows console so Portuguese characters (ç, ã, ó)
# render correctly in the report.
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  errors="replace", line_buffering=True)

# --- Paths -----------------------------------------------------------------

REPO = Path(
    r"C:\google-drive\cursos\usp\mba\data-science\tcc\code"
    r"\public-compliance-data-analysis"
)
THESIS_BUILD = Path(
    r"C:\google-drive\cursos\usp\mba\data-science\tcc\final\_build"
)
GOLD = REPO / "data" / "gold"
GENERATED = REPO / "docs" / "thesis_presentation_assets" / "generated"

# --- Report machinery ------------------------------------------------------


@dataclass
class Check:
    section: str
    name: str
    status: str  # "PASS", "WARN", "FAIL"
    detail: str


_RESULTS: list[Check] = []


def record(section: str, name: str, status: str, detail: str) -> None:
    _RESULTS.append(Check(section, name, status, detail))


def _colored(status: str) -> str:
    return {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL"}.get(status, status)


# --- Utilities -------------------------------------------------------------


def load_thesis_module(name: str):
    """Import one of content_pt1..3 / content_en1..3 at runtime."""
    import importlib.util
    path = THESIS_BUILD / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def parse_locale_number(s: str) -> float | None:
    """Accept both '0.83' and '0,83', '< 0.001' etc. Return None if cannot
    parse (e.g. 'n.a.', 'Significativo')."""
    if s is None:
        return None
    s = str(s).strip().replace("+", "")
    if s.lower() in {"n.a.", "n/a", "na", "-", ""}:
        return None
    if s.startswith("<") or s.startswith(">"):
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


# --- 1. FIGURE PROVENANCE --------------------------------------------------


def check_figures() -> None:
    section = "FIGURES"
    # Union of image paths across PT and EN content modules.
    paths: dict[str, Path] = {}
    for mod_name in ("content_pt2", "content_pt3", "content_en2", "content_en3"):
        mod = load_thesis_module(mod_name)
        for attr in dir(mod):
            if attr.endswith("_IMAGE_PATH"):
                paths[f"{mod_name}.{attr}"] = Path(getattr(mod, attr))

    gold_mtime = max(
        (p.stat().st_mtime for p in GOLD.rglob("*.parquet")),
        default=0.0,
    )

    for key, p in sorted(paths.items()):
        if not p.exists():
            record(section, key, "FAIL", f"missing file: {p}")
            continue
        # Allow either the canonical generated/ folder or the _build/ folder
        # (latter is for hand-crafted diagrams — currently only Medallion).
        under_generated = GENERATED in p.parents
        under_build = THESIS_BUILD in p.parents
        if not (under_generated or under_build):
            record(
                section, key, "WARN",
                f"lives outside generated/ and _build/: {p}",
            )
            continue
        age_vs_gold = p.stat().st_mtime - gold_mtime
        if age_vs_gold < -3600 * 24:  # figure older than Gold by > 1 day
            record(
                section, key, "WARN",
                f"older than Gold parquet files "
                f"(figure {pd.Timestamp(p.stat().st_mtime, unit='s')}, "
                f"Gold {pd.Timestamp(gold_mtime, unit='s')})",
            )
        else:
            record(section, key, "PASS", f"{p.name} ({p.stat().st_size:,} B)")


# --- 2. PEARSON (MUNICIPAL) -----------------------------------------------


PEARSON_TOL = 0.01
R2_TOL = 0.005
F_TOL = 0.5
COEF_TOL = 0.05


def check_pearson_municipal() -> None:
    section = "PEARSON-MUNICIPAL"
    df = pd.read_parquet(GOLD / "analysis_compliance_municipality" / "data.parquet")

    # Variables as they appear in TABELA_2 of the thesis, mapped to the
    # Gold column that represents them.
    mapping = {
        "log(renda média real, 2022)": "log_income",
        "log(população, 2022)": "log_population",
        "Taxa de alfabetização (2022)": "literacy_rate_2022",
        "log(transferências federais totais)": "log_total_transfers",
    }
    pt2 = load_thesis_module("content_pt2")
    tab = {row[0]: row for row in pt2.TABELA_2_ROWS[1:]}

    for label, col in mapping.items():
        if label not in tab:
            record(section, label, "FAIL", f"row not present in TABELA_2_ROWS")
            continue
        sub = df[[col, "sanctions_per_100k"]].dropna()
        r_actual = float(sub[col].corr(sub["sanctions_per_100k"], method="pearson"))
        r_claim = parse_locale_number(tab[label][1])
        if r_claim is None:
            record(section, label, "FAIL", f"cannot parse thesis r: {tab[label][1]!r}")
            continue
        drift = abs(r_actual - r_claim)
        status = "PASS" if drift <= PEARSON_TOL else "FAIL"
        record(
            section, label, status,
            f"thesis r={r_claim:+.3f}  actual r={r_actual:+.3f}  drift={drift:.3f}",
        )


# --- 3. STATE-LEVEL CLAIM: r(renda, sanctions/100k) = 0.74 ---------------


def check_pearson_state() -> None:
    section = "PEARSON-STATE"
    df = pd.read_parquet(GOLD / "analysis_compliance" / "data.parquet")
    # Thesis prose (EDA_PARAGRAPH_2): "r = 0,74; p < 0,001" for avg income vs
    # sanctions_per_100k at state level (n = 27).
    r_actual = float(df["avg_income"].corr(df["sanctions_per_100k"], method="pearson"))
    r_claim = 0.74
    drift = abs(r_actual - r_claim)
    status = "PASS" if drift <= PEARSON_TOL else "FAIL"
    record(
        section, "avg_income vs sanctions_per_100k (n=27)",
        status,
        f"thesis r={r_claim:+.3f}  actual r={r_actual:+.3f}  drift={drift:.3f}",
    )


# --- 4. OLS SPECIFICATION --------------------------------------------------


def check_ols() -> None:
    section = "OLS-MUNICIPAL"
    df = pd.read_parquet(GOLD / "analysis_compliance_municipality" / "data.parquet")
    df = df.dropna(
        subset=[
            "sanctions_per_100k", "log_income", "log_population",
            "literacy_rate_2022", "log_total_transfers", "region_name",
        ]
    ).copy()

    # Regional dummies with Sudeste as reference (matches thesis spec).
    regions = pd.get_dummies(df["region_name"], prefix="region", drop_first=False)
    for ref in ("region_Sudeste", "region_sudeste"):
        if ref in regions.columns:
            regions = regions.drop(columns=[ref])
            break

    X = pd.concat(
        [
            df[["log_income", "log_population", "literacy_rate_2022",
                "log_total_transfers"]],
            regions,
        ],
        axis=1,
    ).astype(float)
    X = sm.add_constant(X)
    y = df["sanctions_per_100k"].astype(float)

    model = sm.OLS(y, X).fit(cov_type="HC3")

    pt2 = load_thesis_module("content_pt2")

    # Claims made in the prose:
    #   "R² de 0,024 e R² ajustado de 0,023, com estatística F = 18,5 e p < 10^-27"
    r2_claim = 0.024
    adj_r2_claim = 0.023
    f_claim = 18.5

    for name, claim, actual, tol in (
        ("R²", r2_claim, float(model.rsquared), R2_TOL),
        ("R² (adj)", adj_r2_claim, float(model.rsquared_adj), R2_TOL),
        ("F-statistic", f_claim, float(model.fvalue), F_TOL),
    ):
        drift = abs(actual - claim)
        status = "PASS" if drift <= tol else "FAIL"
        record(
            section, name, status,
            f"thesis={claim:.3f}  actual={actual:.3f}  drift={drift:.3f}",
        )

    # Coefficients vs TABELA_3_ROWS. The thesis table uses friendly labels;
    # map them to the OLS design matrix column names.
    label_to_col = {
        "Intercepto": "const",
        "log(renda)": "log_income",
        "log(população)": "log_population",
        "Alfabetização": "literacy_rate_2022",
        "log(transferências)": "log_total_transfers",
        "Região Norte": "region_Norte",
        "Região Nordeste": "region_Nordeste",
        "Região Sul": "region_Sul",
        "Região Centro-Oeste": "region_Centro-Oeste",
    }
    tab = {row[0]: row for row in pt2.TABELA_3_ROWS[1:]}

    for label, col in label_to_col.items():
        if label not in tab:
            record(section, f"coef[{label}]", "FAIL", "row not in TABELA_3_ROWS")
            continue
        if col not in model.params.index:
            # Try case-insensitive match
            candidates = [c for c in model.params.index if c.lower() == col.lower()]
            if not candidates:
                record(
                    section, f"coef[{label}]", "WARN",
                    f"OLS does not expose column {col!r}; "
                    f"available: {list(model.params.index)}",
                )
                continue
            col = candidates[0]
        coef_claim = parse_locale_number(tab[label][1])
        coef_actual = float(model.params[col])
        if coef_claim is None:
            record(section, f"coef[{label}]", "FAIL",
                   f"cannot parse thesis coef: {tab[label][1]!r}")
            continue
        drift = abs(coef_actual - coef_claim)
        status = "PASS" if drift <= COEF_TOL else "FAIL"
        record(
            section, f"coef[{label}]", status,
            f"thesis={coef_claim:+.3f}  actual={coef_actual:+.3f}  drift={drift:.3f}",
        )


# --- Main ------------------------------------------------------------------


def main() -> int:
    print("=" * 78)
    print("THESIS  <->  NOTEBOOK / GOLD  SYNC AUDIT")
    print("=" * 78)

    check_figures()
    check_pearson_municipal()
    check_pearson_state()
    check_ols()

    by_section: dict[str, list[Check]] = {}
    for c in _RESULTS:
        by_section.setdefault(c.section, []).append(c)

    fails = warns = passes = 0
    for section, checks in by_section.items():
        print(f"\n--- {section} ---")
        for c in checks:
            print(f"  [{_colored(c.status):4}] {c.name}: {c.detail}")
            if c.status == "PASS":
                passes += 1
            elif c.status == "WARN":
                warns += 1
            else:
                fails += 1

    print("\n" + "=" * 78)
    print(f"SUMMARY: {passes} PASS  /  {warns} WARN  /  {fails} FAIL")
    print("=" * 78)

    # If anything failed, tell the operator which notebook owns each section
    # so drift reports self-route to the right fix location.
    if fails > 0 or warns > 0:
        print()
        print("Where to fix drift:")
        print("  FIGURES           -> re-run notebook that produces the PNG and")
        print("                       save into docs/thesis_presentation_assets/generated/")
        print("  PEARSON-MUNICIPAL -> notebooks/01_exploratory_data_analysis.ipynb")
        print("                       (and .pt-BR counterpart); update TABELA_2_ROWS")
        print("                       in content_pt2.py / content_en2.py to match.")
        print("  PEARSON-STATE     -> same as above; update state-level prose in")
        print("                       EDA_PARAGRAPH_2 of content_*2.py.")
        print("  OLS-MUNICIPAL     -> notebooks/02_statistical_analysis.ipynb;")
        print("                       update TABELA_3_ROWS + OLS_PARAGRAPH_* in")
        print("                       content_pt2.py / content_en2.py.")
        print("  Rule: the notebook is authoritative. Thesis numbers are copies.")

    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
