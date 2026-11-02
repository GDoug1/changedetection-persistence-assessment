import os
import shutil
from pathlib import Path
from loguru import logger

from .persistence import create_tombstone_dir

_SENTINEL = object()


class TagsDict(dict):
    """Dict subclass that removes the corresponding tag directory on deletion."""

    def __init__(self, *args, datastore_path: str | os.PathLike, **kwargs) -> None:
        self._datastore_path = Path(datastore_path)
        super().__init__(*args, **kwargs)

    def __delitem__(self, key: str) -> None:
        tombstone_dir = self.stage_directory_deletion(key)
        super().__delitem__(key)
        if tombstone_dir:
            self.cleanup_tombstone(tombstone_dir, key)

    def stage_directory_deletion(self, key: str):
        """Atomically move a tag directory aside before removing its mapping."""
        tag_dir = self._datastore_path / key
        if not os.path.lexists(tag_dir):
            return None

        tombstone_dir = Path(create_tombstone_dir(self._datastore_path, 'tag'))
        try:
            os.rename(tag_dir, tombstone_dir / 'tag')
        except Exception:
            try:
                tombstone_dir.rmdir()
            except OSError:
                logger.exception(f"Could not remove empty tag tombstone directory {tombstone_dir}")
            raise
        return tombstone_dir

    @staticmethod
    def cleanup_tombstone(tombstone_dir, key: str) -> bool:
        try:
            shutil.rmtree(tombstone_dir)
            return True
        except FileNotFoundError:
            return True
        except OSError as e:
            logger.warning(f"Tag {key!r} was deleted, but tombstone cleanup is pending at {tombstone_dir}: {e}")
            return False

    def pop(self, key: str, default=_SENTINEL):
        """Remove and return tag, deleting its tag.json file. Raises KeyError if missing and no default given."""
        if key in self:
            value = self[key]
            del self[key]
            return value
        if default is _SENTINEL:
            raise KeyError(key)
        return default
