import os
from datetime import datetime


def ensure_unique_path(target_path: str) -> str:
    """Return a unique file path by appending _counter if the target exists"""
    if not os.path.exists(target_path):
        return target_path

    directory = os.path.dirname(target_path)
    filename = os.path.basename(target_path)
    base, ext = os.path.splitext(filename)
    counter = 1
    while os.path.exists(target_path):
        target_path = os.path.join(directory, f"{base}_{counter}{ext}")
        counter += 1
    return target_path


def parse_iso_datetime(value: str) -> datetime:
    """Parse ISO format datetime string with fallback to Y-m-d H:M:S"""
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
