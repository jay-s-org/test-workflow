#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def copy_readme(root_readme: Path, docs_index: Path, *, dry_run: bool) -> None:
    if root_readme.exists():
        if dry_run:
            logging.info("Dry run: Would copy %s to %s", root_readme, docs_index)
        else:
            shutil.copy2(root_readme, docs_index)
            logging.info("Copied %s to %s", root_readme, docs_index)
    else:
        logging.warning(
            "Root README.md not found at %s. Skipping copy to index.md.", root_readme
        )


def move_markdown_files(
    root_dir: Path,
    docs_dir: Path,
    excluded_dirs: list,
    root_readme: Path,
    *,
    dry_run: bool,
) -> None:
    for md_file in root_dir.rglob("*.md"):
        if md_file == root_readme:
            continue  # Skip the root README.md
        if any(excluded_dir in md_file.parents for excluded_dir in excluded_dirs):
            continue  # Skip files in excluded directories
        if md_file.name == "PULL_REQUEST_TEMPLATE.md" and ".github" in md_file.parts:
            continue  # Skip the pull request template
        dest_path = docs_dir / md_file.relative_to(root_dir)
        if dry_run:
            logging.info("Dry run: Would move %s to %s", md_file, dest_path)
        else:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(md_file), dest_path)
                logging.info("Moved %s to %s", md_file, dest_path)
            except Exception:
                logging.exception("Failed to move %s to %s", md_file, dest_path)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Move all Markdown (.md) files to the docs/ directory \
         while maintaining directory structure."
    )
    parser.add_argument(
        "--exclude-dirs",
        nargs="*",
        default=[],
        help="List of directories to exclude from moving",
    )
    parser.add_argument(
        "--root-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Root directory of the repository \
            (default: two levels up from script location)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without making any changes.",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    root_dir = args.root_dir
    excluded_dirs = [root_dir / dir_name for dir_name in args.exclude_dirs]
    excluded_dirs.append(root_dir / "docs")

    setup_logging()

    logging.info("Root directory: %s", root_dir)
    logging.info("Excluded directories: %s", [str(dir) for dir in excluded_dirs])
    logging.info("Dry run mode: %s", args.dry_run)

    docs_dir = root_dir / "docs"
    docs_dir.mkdir(exist_ok=True)

    root_readme = root_dir / "README.md"
    docs_index = docs_dir / "index.md"

    copy_readme(root_readme, docs_index, dry_run=args.dry_run)
    move_markdown_files(
        root_dir, docs_dir, excluded_dirs, root_readme, dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()
