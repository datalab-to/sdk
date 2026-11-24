"""Where the CLI saves each input's results, and the folders it makes for them.

Each input saves to its own folder under the output directory, mirroring the
input tree: `a/report.pdf` saves to `<output_dir>/a/report/`. Each folder
holds OUTPUT_MARKER, a file with the input's file name, so a directory scan
skips the folder and a later run only reuses it for the same input.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

from datalab_sdk.exceptions import DatalabError

OUTPUT_MARKER = ".datalab-output"


def _nearest_existing(path: Path) -> Path:
    path = path.absolute()
    while not path.exists() and path != path.parent:
        path = path.parent
    return path


def ignores_case(directory: Path) -> bool:
    """Whether the file system that holds directory (or its nearest existing
    parent) ignores case in names. Assumes it does when it can't tell."""
    probe_dir = _nearest_existing(directory)
    try:
        with tempfile.NamedTemporaryFile(prefix=".datalab-Case-", dir=probe_dir) as f:
            name = os.path.basename(f.name)
            return os.path.exists(os.path.join(probe_dir, name.swapcase()))
    except OSError:
        return True


def _extension(file_path: Path) -> str:
    return file_path.suffix.lstrip(".").lower()


def _marker_owner(marker: Path) -> str:
    """The name of the input whose results a marked folder holds, if recorded."""
    try:
        return marker.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return ""


class OutputLayout:
    """The output folder of each input in one CLI run.

    plan() checks every folder before anything is submitted. Entering the
    layout creates the folders and their markers; leaving it, also after an
    error or Ctrl-C, removes markers from folders that got no results and the
    folders this run created that are left empty.
    """

    def __init__(self, folders: Dict[Path, Path]):
        self._folders = folders
        self._created: List[Path] = []

    @classmethod
    def plan(
        cls, files: List[Path], input_root: Optional[Path], output_dir: Path
    ) -> "OutputLayout":
        """Plan each input's folder: its path under input_root, without the
        extension. Without input_root, a folder is named after the file alone.

        Raises DatalabError when two inputs would share a folder, one folder
        would be inside another, a folder exists and is not an output folder,
        or a folder holds another input's results from an earlier run.
        """
        case_blind = ignores_case(output_dir)
        folders: Dict[Path, Path] = {}
        seen: Dict[str, Path] = {}
        for file_path in files:
            try:
                relative = file_path.relative_to(input_root) if input_root else None
            except ValueError:
                relative = None
            relative = relative or Path(file_path.name)
            folder = output_dir / relative.parent / file_path.stem
            key = str(folder.resolve())
            if case_blind:
                key = key.casefold()
            if key in seen:
                # The folders are only the same because case is ignored when
                # their names differ, as for Doc.png and doc.pdf.
                only_case = folders[seen[key]] != folder
                raise DatalabError(
                    f"{seen[key]} and {file_path} would save to the same output "
                    f"folder {folder}"
                    + (", because this file system ignores case" if only_case else "")
                    + f". Process one type with --extensions {_extension(file_path)}, "
                    "rename one of them, or use another --output_dir."
                )
            marker = folder / OUTPUT_MARKER
            if folder.exists() and not marker.is_file():
                raise DatalabError(
                    f"The output folder for {file_path}, {folder}, already exists "
                    "and is not a datalab output folder. Move it, or choose another "
                    "--output_dir."
                )
            owner = _marker_owner(marker)
            if owner and (
                owner.casefold() != file_path.name.casefold()
                if case_blind
                else owner != file_path.name
            ):
                raise DatalabError(
                    f"The output folder for {file_path}, {folder}, holds the results "
                    f"of {owner} from an earlier run. Move them, process one type "
                    f"with --extensions {_extension(file_path)}, or use another "
                    "--output_dir."
                )
            seen[key] = file_path
            folders[file_path] = folder

        # One input's folder inside another's could put a folder where the other
        # input's result file goes, for example report/report.md/.
        for inner, inner_file in seen.items():
            for parent in Path(inner).parents:
                outer_file = seen.get(str(parent))
                if outer_file is not None:
                    raise DatalabError(
                        f"The output folder for {inner_file} would be inside the "
                        f"output folder for {outer_file}. Rename one of them, or "
                        "process one with another --output_dir."
                    )
        return cls(folders)

    @property
    def folders(self) -> List[Path]:
        return list(self._folders.values())

    def output_path(self, file_path: Path) -> Path:
        """The save_output path of an input: its folder plus its stem."""
        return self._folders[file_path] / file_path.stem

    def __enter__(self) -> "OutputLayout":
        # A with statement doesn't call __exit__ when __enter__ raises, so any
        # error here, Ctrl-C included, cleans up what was made so far.
        try:
            for file_path, folder in self._folders.items():
                missing = []
                path = folder
                while not path.exists() and path != path.parent:
                    missing.append(path)
                    path = path.parent
                for path in reversed(missing):
                    path.mkdir()
                    self._created.append(path)
                marker = folder / OUTPUT_MARKER
                marker.write_text(file_path.name, encoding="utf-8")
        except OSError as e:
            self.__exit__(None, None, None)
            raise DatalabError(f"Can't create the output folder {folder}: {e}")
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *exc) -> None:
        for folder in self._folders.values():
            try:
                if [p.name for p in folder.iterdir()] == [OUTPUT_MARKER]:
                    (folder / OUTPUT_MARKER).unlink()
                    folder.rmdir()
            except OSError:
                pass
        for path in reversed(self._created):
            try:
                path.rmdir()
            except OSError:
                pass

    def snapshot(self, file_path: Path) -> Dict[str, int]:
        """The names and modification times of an input's results so far."""
        try:
            return {
                p.name: p.stat().st_mtime_ns
                for p in self._folders[file_path].iterdir()
                if p.name != OUTPUT_MARKER
            }
        except OSError:
            return {}

    def wrote_output(self, file_path: Path, before: Dict[str, int]) -> bool:
        """Whether a result was written for an input since snapshot() gave `before`."""
        after = self.snapshot(file_path)
        return any(before.get(name) != mtime for name, mtime in after.items())
