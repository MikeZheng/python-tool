# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A duplicate file finder tool that scans directories, calculates SHA256 hashes, and identifies duplicate files. Supports both CLI and web API interfaces with pluggable storage backends (CSV or SQLite).

## Commands

### CLI Scanning
```bash
# Scan directories (default CSV storage)
python photo.py --directories "F:\\photo" "G:\\video"

# Use SQLite storage
python photo.py --storage sqlite --directories "F:\\photo"

# Refresh database (remove entries for deleted files)
python photo.py --storage sqlite --refresh

# Generate HTML viewer
python photo.py --generate-html
```

### Web API Server
```bash
python app.py  # Starts Flask server on port 5000
```

## Architecture

**Core Processing Pipeline** (`photo.py`):
- `collect_files_from_directories()` - Walks directory trees to collect file paths
- `process_single_file_with_cache()` - Calculates SHA256, uses cache to skip already-processed files
- `find_duplicates()` - Groups files by SHA256 hash
- `process_multiple_directories()` - Orchestrates parallel processing with ProcessPoolExecutor

**Storage Layer** (Strategy Pattern):
- `StorageInterface` (`storage_base.py`) - Abstract base class
- `SQLiteStorage` (`sqlite_storage.py`) - SQLite backend, stores in `file_database.db`
- `CSVStorage` (`csv_storage.py`) - CSV backend, stores in `file_list.csv` and `duplicate_files.csv`

**Web API** (`app.py`):
- `GET /duplicates?page=1` - Paginated duplicate groups (20 per page)
- `POST /scan-directory` - Scan a single directory
- `POST /delete-file` - Delete a file and update storage

**Data Models** (`models.py`):
- `FileInfo` - File metadata with SHA256 hash
- `DuplicateGroup` - Group of files with same hash

## Configuration

Storage type is persisted in `config.json`:
```json
{"storage_type": "sqlite", "last_updated": "..."}
```

## Output Files

- `file_list.csv` / `file_database.db` - All processed files
- `duplicate_files.csv` - Duplicate file groups (CSV storage only)
- `duplicate_viewer.html` - Generated HTML viewer for duplicates
- `file_processing.log` - Processing log
