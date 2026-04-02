from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple


def discover_ignored_folders(library_root: str) -> List[str]:
    root = Path(str(library_root or "").strip()).resolve()
    if not root.exists() or not root.is_dir():
        return []
    matches: List[str] = []
    try:
        for marker in root.rglob(".chonkignore"):
            folder = marker.parent.resolve()
            try:
                rel_path = folder.relative_to(root)
            except ValueError:
                continue
            matches.append("." if str(rel_path) == "." else rel_path.as_posix())
    except (OSError, RuntimeError):
        return []
    return sorted(set(matches), key=lambda item: item.lower())


def resolve_library_relative_folder(library_root: str, relative_path: str, operation: str = "update") -> Tuple[Optional[Path], str]:
    root = Path(str(library_root or "").strip()).resolve()
    if not root.exists() or not root.is_dir():
        return None, "Ignored folder %s failed: library path is missing or not a directory." % operation
    cleaned = str(relative_path or "").strip().replace("\\", "/")
    if not cleaned:
        return None, "Ignored folder %s failed: relative path is required." % operation
    if cleaned in {".", "./"}:
        target = root
    else:
        target = (root / cleaned).resolve()
    try:
        target.relative_to(root)
    except ValueError:
        return None, "Ignored folder %s failed: path must stay inside the library root." % operation
    return target, ""
