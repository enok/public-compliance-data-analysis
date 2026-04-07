#!/usr/bin/env python3
"""Build a compact inventory of USP MBA Data Science course materials."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree


SUPPORTED_EXTENSIONS = {".pdf", ".ipynb", ".md", ".txt", ".py", ".csv", ".xlsx"}
TEXT_EXTENSIONS = {".md", ".txt", ".py"}
NOISE_DIRECTORIES = {
    ".git",
    ".hg",
    ".idea",
    ".ipynb_checkpoints",
    ".spyproject",
    ".svn",
    ".venv",
    "__pycache__",
    "backups",
    "build",
    "config",
    "defaults",
    "dist",
    "env",
    "include",
    "lib",
    "node_modules",
    "scripts",
    "share",
    "site-packages",
    "venv",
}
IMPORT_PATTERN = re.compile(
    r"^\s*(?:from\s+([a-zA-Z_][\w.]*)\s+import|import\s+([a-zA-Z_][\w.]*))",
    re.MULTILINE,
)
TOKEN_PATTERN = re.compile(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9_-]{3,}")
WHITESPACE_PATTERN = re.compile(r"\s+")
STOPWORDS = {
    "about",
    "after",
    "against",
    "alguns",
    "analysis",
    "analise",
    "analytics",
    "aqui",
    "assim",
    "aula",
    "aulas",
    "aulasusp",
    "based",
    "between",
    "cada",
    "como",
    "com",
    "course",
    "cursos",
    "dados",
    "data",
    "das",
    "dataset",
    "depois",
    "dessa",
    "desse",
    "desta",
    "deste",
    "equipe",
    "essa",
    "esse",
    "esta",
    "este",
    "estudo",
    "from",
    "have",
    "into",
    "isso",
    "isto",
    "mais",
    "machine",
    "mba",
    "mesmo",
    "muito",
    "neste",
    "para",
    "pela",
    "pelo",
    "por",
    "porque",
    "python",
    "quando",
    "que",
    "science",
    "seja",
    "ser",
    "sera",
    "sobre",
    "sua",
    "tambem",
    "tcc",
    "their",
    "these",
    "this",
    "uma",
    "usando",
    "usp",
    "using",
    "with",
    "your",
}
PREFERRED_EXTENSIONS = [".pdf", ".ipynb", ".md", ".txt", ".py", ".xlsx", ".csv"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize the aulas corpus into Markdown and JSON."
    )
    parser.add_argument("--source", required=True, help="Root aulas directory.")
    parser.add_argument("--output", required=True, help="Directory for generated files.")
    return parser.parse_args()


def iter_course_directories(source_root: Path) -> Iterable[Path]:
    for child in sorted(source_root.iterdir()):
        if child.is_dir():
            yield child


def should_skip_directory(name: str) -> bool:
    lowered = name.lower()
    return lowered in NOISE_DIRECTORIES or lowered.startswith(".")


def iter_supported_files(course_dir: Path) -> Iterable[Path]:
    for root, dirs, files in os.walk(course_dir):
        dirs[:] = [d for d in dirs if not should_skip_directory(d)]
        for file_name in sorted(files):
            path = Path(root) / file_name
            if path.suffix.lower() in SUPPORTED_EXTENSIONS:
                yield path


def normalize_text(text: str) -> str:
    return WHITESPACE_PATTERN.sub(" ", text).strip()


def read_pdf(path: Path) -> str:
    process = subprocess.run(
        ["pdftotext", "-q", "-enc", "UTF-8", str(path), "-"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
        timeout=120,
        check=False,
    )
    return process.stdout if process.returncode == 0 else ""


def read_notebook(path: Path) -> str:
    payload = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    parts: list[str] = []
    for cell in payload.get("cells", []):
        source = cell.get("source", [])
        if isinstance(source, list):
            parts.extend(source)
        elif isinstance(source, str):
            parts.append(source)
    return "\n".join(parts)


def read_csv_preview(path: Path) -> str:
    rows: list[str] = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.reader(handle)
        for index, row in enumerate(reader):
            if index >= 5:
                break
            rows.append(" | ".join(cell.strip() for cell in row[:20]))
    return "\n".join(rows)


def read_xlsx_preview(path: Path) -> str:
    try:
        with zipfile.ZipFile(path) as archive:
            workbook_xml = archive.read("xl/workbook.xml")
        tree = ElementTree.fromstring(workbook_xml)
        namespace = {"ns": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        sheets = [
            sheet.attrib.get("name", "")
            for sheet in tree.findall(".//ns:sheets/ns:sheet", namespace)
        ]
        return "Workbook sheets: " + ", ".join(sheet for sheet in sheets if sheet)
    except (KeyError, ElementTree.ParseError, zipfile.BadZipFile):
        return ""


def read_text_payload(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in TEXT_EXTENSIONS:
        return path.read_text(encoding="utf-8", errors="ignore")
    if suffix == ".pdf":
        return read_pdf(path)
    if suffix == ".ipynb":
        return read_notebook(path)
    if suffix == ".csv":
        return read_csv_preview(path)
    if suffix == ".xlsx":
        return read_xlsx_preview(path)
    return ""


def extract_imports(text: str) -> Counter:
    imports: Counter[str] = Counter()
    for left, right in IMPORT_PATTERN.findall(text):
        module = (left or right).split(".")[0].strip()
        if module:
            imports[module] += 1
    return imports


def extract_keywords(text: str) -> Counter:
    counter: Counter[str] = Counter()
    for token in TOKEN_PATTERN.findall(text.lower()):
        if token in STOPWORDS or token.isdigit():
            continue
        counter[token] += 1
    return counter


def pick_representative_files(file_rows: list[dict[str, object]]) -> list[str]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in file_rows:
        grouped[str(row["extension"])].append(row)
    picked: list[str] = []
    for extension in PREFERRED_EXTENSIONS:
        candidates = sorted(
            grouped.get(extension, []),
            key=lambda row: (-int(row["size"]), str(row["relative_path"])),
        )
        if candidates:
            picked.append(str(candidates[0]["relative_path"]))
    return picked[:8]


def summarize_course(course_dir: Path, source_root: Path) -> dict[str, object]:
    file_rows: list[dict[str, object]] = []
    keyword_counter: Counter[str] = Counter()
    import_counter: Counter[str] = Counter()
    extension_counter: Counter[str] = Counter()
    text_bytes = 0

    for path in iter_supported_files(course_dir):
        relative_path = path.relative_to(source_root)
        extension = path.suffix.lower()
        size = path.stat().st_size
        payload = read_text_payload(path)
        normalized = normalize_text(payload)
        file_rows.append(
            {
                "relative_path": relative_path.as_posix(),
                "extension": extension,
                "size": size,
                "text_preview": normalized[:240],
            }
        )
        extension_counter[extension] += 1
        if normalized:
            keyword_counter.update(extract_keywords(normalized))
            import_counter.update(extract_imports(normalized))
            text_bytes += len(normalized.encode("utf-8", errors="ignore"))

    return {
        "course": course_dir.name,
        "path": course_dir.as_posix(),
        "file_count": len(file_rows),
        "text_bytes": text_bytes,
        "extensions": dict(sorted(extension_counter.items())),
        "top_keywords": [word for word, _ in keyword_counter.most_common(15)],
        "top_imports": [word for word, _ in import_counter.most_common(10)],
        "representative_files": pick_representative_files(file_rows),
        "files": file_rows,
    }


def build_markdown(summary: dict[str, object]) -> str:
    lines: list[str] = []
    lines.append("# USP MBA Data Science Course Inventory")
    lines.append("")
    lines.append(
        "Generated from the `aulas/` corpus. This inventory summarizes course folders, "
        "supported file types, representative teaching assets, and recurring keywords."
    )
    lines.append("")
    lines.append("## Global Summary")
    lines.append("")
    lines.append(
        f"- Source root: `{summary['source_root']}`"
    )
    lines.append(
        f"- Courses scanned: `{summary['course_count']}`"
    )
    lines.append(
        f"- Supported files scanned: `{summary['file_count']}`"
    )
    lines.append(
        f"- Approximate extracted text volume: `{summary['text_bytes']}` bytes"
    )
    lines.append("")
    lines.append("## Course Coverage")
    lines.append("")

    for course in summary["courses"]:
        lines.append(f"### {course['course']}")
        lines.append("")
        lines.append(
            f"- Files scanned: `{course['file_count']}`"
        )
        if course["extensions"]:
            ext_summary = ", ".join(
                f"`{extension}` x {count}"
                for extension, count in course["extensions"].items()
            )
            lines.append(f"- File types: {ext_summary}")
        if course["top_keywords"]:
            lines.append(
                "- Keywords: " + ", ".join(f"`{word}`" for word in course["top_keywords"])
            )
        if course["top_imports"]:
            lines.append(
                "- Frequent libraries: "
                + ", ".join(f"`{module}`" for module in course["top_imports"])
            )
        if course["representative_files"]:
            lines.append("- Representative files:")
            for relative_path in course["representative_files"]:
                lines.append(f"  - `{relative_path}`")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    source_root = Path(args.source).resolve()
    output_root = Path(args.output).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    courses = [summarize_course(course_dir, source_root) for course_dir in iter_course_directories(source_root)]
    summary = {
        "source_root": source_root.as_posix(),
        "course_count": len(courses),
        "file_count": sum(course["file_count"] for course in courses),
        "text_bytes": sum(course["text_bytes"] for course in courses),
        "courses": courses,
    }

    (output_root / "usp-mba-course-inventory.generated.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_root / "usp-mba-course-inventory.generated.md").write_text(
        build_markdown(summary),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
