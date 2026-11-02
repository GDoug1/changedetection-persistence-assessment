import shutil

from changedetectionio.strtobool import strtobool

from changedetectionio.validate_url import is_safe_valid_url

from flask import (
    flash,
    has_request_context
)
from flask_babel import gettext

from ..model import App, Watch
from copy import deepcopy
from os import path, unlink
import json
import hashlib
import os
import re
import secrets
import sys
import tempfile
import time
import uuid as uuid_builder
from threading import RLock
from pathlib import Path
from contextlib import nullcontext
from loguru import logger
from blinker import signal

from ..model.Tags import TagsDict
from ..model.persistence import (create_tombstone_dir, HISTORY_CLEAR_TRANSACTION_MARKER,
                                WATCH_RESULT_TRANSACTION_MARKER, WATCH_STORAGE_IDENTITY,
                                TOMBSTONE_DIR_PREFIX)

# Try to import orjson for faster JSON serialization
try:
    import orjson

    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

from ..processors import get_custom_watch_obj_for_processor, find_processors

# Import the base class and helpers
from .file_saving_datastore import (FileSavingDataStore, load_all_watches, load_all_tags, load_watch_from_file,
                                    save_json_atomic)
from .updates import DatastoreUpdatesMixin

# Because the server will run as a daemon and wont know the URL for notification links when firing off a notification
BASE_URL_NOT_SET_TEXT = '("Base URL" not set - see settings - notifications)'

dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])


# Is there an existing library to ensure some data store (JSON etc) is in sync with CRUD methods?
# Open a github issue if you know something :)
# https://stackoverflow.com/questions/6190468/how-to-trigger-function-on-value-change
_TAG_UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)


class ChangeDetectionStore(DatastoreUpdatesMixin, FileSavingDataStore):
    __version_check = True

    def __init__(self, datastore_path="/datastore", include_default_watches=True, version_tag="0.0.0"):
        # Initialize parent class
        super().__init__()
        self._tag_reference_lock = RLock()

        # Should only be active for docker
        # logging.basicConfig(filename='/dev/stdout', level=logging.INFO)
        self.datastore_path = datastore_path
        self.start_time = time.time()
        self.save_version_copy_json_db(version_tag)
        self.reload_state(datastore_path=datastore_path, include_default_watches=include_default_watches, version_tag=version_tag)

    def save_version_copy_json_db(self, version_tag):
        """
        Create version-tagged backup of changedetection.json.

        This is called on version upgrades to preserve a backup in case
        the new version has issues.
        """
        import re

        version_text = re.sub(r'\D+', '-', version_tag)
        db_path = os.path.join(self.datastore_path, "changedetection.json")
        db_path_version_backup = os.path.join(self.datastore_path, f"changedetection-{version_text}.json")

        if not os.path.isfile(db_path_version_backup) and os.path.isfile(db_path):
            from shutil import copyfile
            logger.info(f"Backing up changedetection.json due to new version to '{db_path_version_backup}'.")
            copyfile(db_path, db_path_version_backup)

    def _load_settings(self, filename="changedetection.json"):
        """
        Load settings from storage.

        File backend implementation: reads from changedetection.json

        Returns:
            dict: Settings data loaded from storage
        """
        changedetection_json = os.path.join(self.datastore_path, filename)

        logger.info(f"Loading settings from {changedetection_json}")

        if HAS_ORJSON:
            with open(changedetection_json, 'rb') as f:
                return orjson.loads(f.read())
        else:
            with open(changedetection_json, 'r', encoding='utf-8') as f:
                return json.load(f)

    def _apply_settings(self, settings_data):
        """
        Apply loaded settings data to internal data structure.

        Args:
            settings_data: Dictionary loaded from changedetection.json
        """
        # Apply top-level fields
        if 'app_guid' in settings_data:
            self.__data['app_guid'] = settings_data['app_guid']
        if 'build_sha' in settings_data:
            self.__data['build_sha'] = settings_data['build_sha']
        if 'version_tag' in settings_data:
            self.__data['version_tag'] = settings_data['version_tag']

        # Apply settings sections
        if 'settings' in settings_data:
            if 'headers' in settings_data['settings']:
                self.__data['settings']['headers'].update(settings_data['settings']['headers'])
            if 'requests' in settings_data['settings']:
                self.__data['settings']['requests'].update(settings_data['settings']['requests'])
            if 'application' in settings_data['settings']:
                self.__data['settings']['application'].update(settings_data['settings']['application'])

                # Use our Tags dict with cleanup helpers etc
                # @todo Same for Watches
                existing_tags = settings_data.get('settings', {}).get('application', {}).get('tags') or {}
                self.__data['settings']['application']['tags'] = TagsDict(existing_tags, datastore_path=self.datastore_path)

        # More or less for the old format which had this data in the one url-watches.json
        # cant hurt to leave it here,
        if 'watching' in settings_data:
            self.__data['watching'].update(settings_data['watching'])

    def _rehydrate_tags(self):
        """Rehydrate tag entities from stored data into Tag objects with restock_diff processor."""
        from ..model import Tag

        for uuid, tag in self.__data['settings']['application']['tags'].items():
            # Force processor to restock_diff for override functionality (technical debt)
            tag['processor'] = 'restock_diff'

            self.__data['settings']['application']['tags'][uuid] = Tag.model(
                datastore_path=self.datastore_path,
                __datastore=self.__data,
                __datastore_owner=self,
                default=tag
            )
            logger.info(f"Tag: {uuid} {tag['title']}")

    def _rehydrate_watches(self):
        """Rehydrate watch entities from stored data (converts dicts to Watch objects)."""
        watch_count = len(self.__data.get('watching', {}))
        if watch_count == 0:
            return

        logger.info(f"Rehydrating {watch_count} watches...")
        watching_rehydrated = {}
        for uuid, watch_dict in self.__data.get('watching', {}).items():
            if isinstance(watch_dict, dict):
                watching_rehydrated[uuid] = self.rehydrate_entity(uuid, watch_dict)
            else:
                logger.error(f"Watch UUID {uuid} already rehydrated")

        self.__data['watching'] = watching_rehydrated
        logger.success(f"Rehydrated {watch_count} watches into Watch objects")


    def _sync_directory(self, directory):
        """Best-effort directory sync; unsupported platforms retain atomic but weaker durability."""
        try:
            fd = os.open(str(directory), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except (OSError, AttributeError) as e:
            logger.warning(f"Directory fsync unavailable for {directory}: {e}")

    @staticmethod
    def _sync_file(filepath):
        """Flush file contents before advancing a crash-recovery transaction state."""
        with open(filepath, 'r+b') as stream:
            os.fsync(stream.fileno())

    def _write_history_clear_manifest(self, manifest_path, manifest):
        """Atomically persist and sync transaction intent/state before destructive transitions."""
        save_json_atomic(str(manifest_path), manifest, label="history clear transaction", max_size_mb=1)
        with open(manifest_path, 'r+b') as manifest_file:
            os.fsync(manifest_file.fileno())
        self._sync_directory(Path(manifest_path).parent)

    def _write_watch_result_manifest(self, manifest_path, manifest):
        save_json_atomic(str(manifest_path), manifest, label="watch result transaction", max_size_mb=10)
        self._sync_file(manifest_path)
        self._sync_directory(Path(manifest_path).parent)

    @staticmethod
    def _write_bytes_atomic(filepath, contents):
        filepath = Path(filepath)
        fd, temporary = tempfile.mkstemp(prefix='result-', suffix='.tmp', dir=filepath.parent)
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(contents)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, filepath)
        except Exception:
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise

    def _rollback_watch_result_transaction(self, transaction_dir, manifest, watch_dir):
        payload = transaction_dir / 'payload'
        index_path = watch_dir / manifest['index_name']
        index_backup = payload / 'index.old'
        if manifest['had_index']:
            self._write_bytes_atomic(index_path, index_backup.read_bytes())
        else:
            try:
                index_path.unlink()
            except FileNotFoundError:
                pass

        snapshot_root = payload / 'snapshots'
        original_names = set(manifest.get('snapshot_files', []))
        prefix = manifest['snapshot_id'] + '.'
        for path in watch_dir.iterdir():
            if path.is_file() and path.name.startswith(prefix) and path.name not in original_names:
                path.unlink()
        for name in original_names:
            backup = snapshot_root / name
            if backup.is_file():
                self._write_bytes_atomic(watch_dir / name, backup.read_bytes())
        self._sync_directory(watch_dir)

    def _recover_watch_result_transactions(self):
        root = Path(self.datastore_path)
        if not root.exists():
            return
        for transaction_dir in root.iterdir():
            if (not transaction_dir.name.startswith(f'{TOMBSTONE_DIR_PREFIX}result-')
                    or not transaction_dir.is_dir() or transaction_dir.is_symlink()):
                continue
            manifest_path = transaction_dir / 'manifest.json'
            if not manifest_path.is_file():
                # No original files are moved by this protocol before the manifest
                # exists, so an empty/partial transaction directory is safe to discard.
                try:
                    shutil.rmtree(transaction_dir)
                    self._sync_directory(root)
                    continue
                except OSError as e:
                    logger.critical(f"Could not clean incomplete watch-result transaction {transaction_dir}: {e}")
                    raise RuntimeError(f"Incomplete watch-result transaction needs manual recovery: {transaction_dir}") from e
            try:
                with open(manifest_path, 'r', encoding='utf-8') as stream:
                    manifest = json.load(stream)
                watch_uuid = manifest.get('watch_uuid')
                transaction_id = manifest.get('transaction_id')
                watch_dir = root / watch_uuid
                if (manifest.get('version') not in (1, 2)
                        or transaction_dir.name != f'{TOMBSTONE_DIR_PREFIX}result-{transaction_id}'
                        or not self._is_safe_path_component(watch_uuid)
                        or not self._is_safe_path_component(transaction_id)
                        or not self._is_safe_path_component(manifest.get('snapshot_id'))
                        or not self._is_safe_path_component(manifest.get('index_name'))
                        or not isinstance(manifest.get('watch_identity'), str)
                        or len(manifest['watch_identity']) != (32 if manifest.get('version') == 2 else 64)
                        or not isinstance(manifest.get('snapshot_files'), list)
                        or any(not self._is_safe_path_component(name)
                               for name in manifest['snapshot_files'])
                        or not isinstance(manifest.get('had_index'), bool)):
                    raise ValueError("Invalid watch-result transaction manifest")
                watch_json = watch_dir / 'watch.json'
                if not watch_json.is_file():
                    raise OSError(f"Watch {watch_uuid} is missing during result recovery")
                watch_data = json.loads(watch_json.read_text(encoding='utf-8'))
                marker = watch_data.get(WATCH_RESULT_TRANSACTION_MARKER)
                committed = manifest.get('state') == 'committed' or marker == transaction_id
                if marker and marker != transaction_id:
                    raise ValueError(f"Watch {watch_uuid} has a conflicting result transaction marker")
                if committed:
                    if marker == transaction_id:
                        if (manifest.get('version') == 2
                                and watch_data.get(WATCH_STORAGE_IDENTITY) != manifest['watch_identity']):
                            raise ValueError(f"Watch {watch_uuid} identity changed during result recovery")
                        watch_data.pop(WATCH_RESULT_TRANSACTION_MARKER, None)
                        save_json_atomic(str(watch_json), watch_data, label='recovered watch metadata')
                        self._sync_file(watch_json)
                        self._sync_directory(watch_dir)
                else:
                    if manifest.get('version') == 2:
                        identity_matches = watch_data.get(WATCH_STORAGE_IDENTITY) == manifest['watch_identity']
                    else:
                        identity_matches = hashlib.sha256(watch_json.read_bytes()).hexdigest() == manifest['watch_identity']
                    if not identity_matches:
                        raise ValueError(f"Watch {watch_uuid} identity changed during result recovery")
                    self._rollback_watch_result_transaction(transaction_dir, manifest, watch_dir)
                shutil.rmtree(transaction_dir)
                self._sync_directory(root)
            except Exception as e:
                logger.critical(f"Watch-result recovery failed for {transaction_dir}; preserving recovery data: {e}")
                raise RuntimeError(f"Watch-result recovery failed for {transaction_dir}: {e}") from e

    def _recover_restore_transactions(self):
        """Restore an entity directory moved aside by an interrupted backup restore.

        A published destination is kept in place. Prepared transactions with an old
        directory retain that directory as a tombstone until an operator can inspect
        the ambiguous publication state; if publication never happened, the old
        directory is atomically restored before entity loading.
        """
        root = Path(self.datastore_path)
        if not root.exists():
            return
        for transaction_dir in root.iterdir():
            if (not transaction_dir.name.startswith(f'{TOMBSTONE_DIR_PREFIX}restore-')
                    or not transaction_dir.is_dir() or transaction_dir.is_symlink()):
                continue
            previous_dir = transaction_dir / 'previous'
            manifest_path = transaction_dir / 'manifest.json'
            try:
                manifest = None
                if manifest_path.is_file():
                    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
                    kind = manifest.get('kind')
                    entity_uuid = manifest.get('uuid')
                    prefix = f'{TOMBSTONE_DIR_PREFIX}restore-{kind}-'
                    if (manifest.get('version') != 1 or kind not in ('watch', 'tag')
                            or not self._is_safe_path_component(entity_uuid)
                            or not transaction_dir.name.startswith(prefix)
                            or manifest.get('state') not in ('prepared', 'committed')):
                        raise ValueError("Invalid backup-restore recovery manifest")
                elif previous_dir.is_dir() and not previous_dir.is_symlink():
                    # Recover tombstones created before the journal format was added.
                    metadata_name = 'watch.json' if 'watch' in transaction_dir.name else 'tag.json'
                    metadata_path = previous_dir / metadata_name
                    entity_data = json.loads(metadata_path.read_text(encoding='utf-8'))
                    entity_uuid = entity_data.get('uuid')
                    kind = 'watch' if metadata_name == 'watch.json' else 'tag'
                    if not self._is_safe_path_component(entity_uuid):
                        raise ValueError("Cannot identify legacy backup-restore tombstone")
                    manifest = {'kind': kind, 'uuid': entity_uuid, 'state': 'prepared'}
                else:
                    # No original directory was moved, so this transaction contains
                    # no sole copy of entity data. Remove only an empty journal.
                    if not any(transaction_dir.iterdir()):
                        transaction_dir.rmdir()
                        self._sync_directory(root)
                    continue

                destination = root / entity_uuid
                if previous_dir.is_dir() and not previous_dir.is_symlink():
                    if not os.path.lexists(destination):
                        os.replace(previous_dir, destination)
                        self._sync_directory(root)
                        self._sync_directory(transaction_dir)
                        logger.warning(f"Recovered previous {kind} directory for {entity_uuid} after interrupted restore")
                        shutil.rmtree(transaction_dir)
                        self._sync_directory(root)
                    elif manifest.get('state') == 'committed':
                        # The replacement metadata was committed; the old directory is
                        # now only cleanup data.
                        shutil.rmtree(transaction_dir)
                        self._sync_directory(root)
                    else:
                        logger.error(f"Preserving ambiguous prepared restore for {entity_uuid} at {transaction_dir}; "
                                     f"both the destination and previous directory exist")
                elif os.path.lexists(destination) and manifest.get('state') == 'committed':
                    shutil.rmtree(transaction_dir)
                    self._sync_directory(root)
                elif os.path.lexists(destination):
                    # A new entity was published without a previous directory. Keep
                    # it loadable and retain the journal for manual inspection.
                    logger.error(f"Preserving prepared restore journal for {entity_uuid} at {transaction_dir}")
                else:
                    raise OSError(f"Restore transaction {transaction_dir} has no recoverable entity directory")
            except Exception as e:
                logger.critical(f"Backup-restore recovery failed for {transaction_dir}; preserving data: {e}")
                raise RuntimeError(f"Backup-restore recovery failed for {transaction_dir}: {e}") from e

    @staticmethod
    def _is_safe_path_component(value):
        return (isinstance(value, str) and value not in ('', '.', '..')
                and '/' not in value and '\\' not in value and ':' not in value)

    @staticmethod
    def _is_history_clear_artifact(name, history_files):
        import re

        fixed_artifacts = {
            'last-checksum.txt', 'last-screenshot.png', 'last-error.txt',
            'last-error-screenshot.png', 'elements.deflate', 'elements-error.deflate',
            'last-fetched.br', 'thumbnail.jpeg',
        }
        known_history_indexes = {'history.txt'} | {
            f'history-{processor_name}.txt'
            for _processor, processor_name in find_processors()
        }
        return (
            name in history_files
            or name in fixed_artifacts
            or name in known_history_indexes
            or name.startswith('favicon.')
            or (name.startswith('change-summary-') and name.endswith('.txt'))
            or (name.startswith('step_') and name.endswith('.jpeg'))
            or re.fullmatch(r'\d+\.(?:html\.br|pdf)', name) is not None
            or re.fullmatch(r'[0-9a-fA-F]{32}\.[A-Za-z0-9.]+', name) is not None
        )

    def _validate_history_clear_manifest(self, transaction_dir, manifest):
        if not isinstance(manifest, dict) or manifest.get('version') not in (1, 2, 3):
            raise ValueError("Unsupported history clear transaction manifest")
        transaction_id = manifest.get('transaction_id')
        watch_uuid = manifest.get('watch_uuid')
        if (not self._is_safe_path_component(transaction_id)
                or transaction_dir.name != f"{TOMBSTONE_DIR_PREFIX}history-{transaction_id}"):
            raise ValueError("Invalid history clear transaction identifier")
        if not self._is_safe_path_component(watch_uuid):
            raise ValueError("Invalid watch UUID in history clear transaction")
        if manifest.get('tombstone_location') != transaction_dir.name:
            raise ValueError("History clear transaction points to a different tombstone")
        if manifest.get('state') not in ('prepared', 'committed'):
            raise ValueError("Invalid history clear transaction state")
        identity = manifest.get('watch_identity')
        if manifest.get('version') in (2, 3) and (
                not isinstance(identity, str)
                or len(identity) != (32 if manifest.get('version') == 3 else 64)
                or any(char not in '0123456789abcdef' for char in identity)):
            raise ValueError("Invalid original watch identity in history clear transaction")
        files = manifest.get('files')
        if not isinstance(files, list):
            raise ValueError("Invalid history clear transaction file list")

        names = set()
        for item in files:
            if not isinstance(item, dict):
                raise ValueError("Invalid history clear transaction file entry")
            name = item.get('name')
            if (not self._is_safe_path_component(name) or name == 'watch.json'
                    or name in names
                    or item.get('original_path') != f"{watch_uuid}/{name}"
                    or item.get('staged_path') != f"{transaction_dir.name}/payload/{name}"):
                raise ValueError("Unsafe path in history clear transaction manifest")
            names.add(name)
        return transaction_id, watch_uuid, files

    @staticmethod
    def _restore_staged_history_file(staged_path, original_path):
        """Restore without replacing a file that appeared after the interrupted clear."""
        if os.path.lexists(original_path):
            same_staged_file = False
            if os.path.islink(staged_path) and os.path.islink(original_path):
                same_staged_file = os.readlink(staged_path) == os.readlink(original_path)
            elif not os.path.islink(staged_path) and not os.path.islink(original_path):
                try:
                    same_staged_file = os.path.samefile(staged_path, original_path)
                except OSError:
                    same_staged_file = False
            if same_staged_file:
                # A crash may occur after the no-overwrite link was created but before
                # its staged name was removed. Treat that state as an already restored file.
                os.unlink(staged_path)
                return
            raise FileExistsError(f"Refusing to overwrite {original_path} during history recovery")
        if os.path.islink(staged_path):
            os.symlink(os.readlink(staged_path), original_path,
                       target_is_directory=os.path.isdir(staged_path))
        else:
            # Staging is under the datastore root on the same filesystem as watch dirs.
            # A hard link creates the destination atomically and fails if it already exists.
            os.link(staged_path, original_path)
        os.unlink(staged_path)

    def _finish_history_clear_transaction(self, transaction_dir, manifest, committed):
        _, watch_uuid, files = self._validate_history_clear_manifest(transaction_dir, manifest)
        watch_dir = Path(self.datastore_path) / watch_uuid
        payload_dir = transaction_dir / 'payload'

        if not (watch_dir / 'watch.json').is_file():
            raise OSError(f"Cannot recover history clear for {watch_uuid}: watch.json is missing")

        if not committed:
            if manifest.get('version') not in (2, 3):
                raise OSError(f"Cannot safely roll back legacy history clear for {watch_uuid}: identity is unavailable")
            current_watch_json = watch_dir / 'watch.json'
            current_identity = hashlib.sha256(current_watch_json.read_bytes()).hexdigest()
            if manifest.get('version') == 3:
                current_watch_data = json.loads(current_watch_json.read_text(encoding='utf-8'))
                identity_matches = current_watch_data.get(WATCH_STORAGE_IDENTITY) == manifest['watch_identity']
            else:
                identity_matches = current_identity == manifest['watch_identity']
            if not identity_matches:
                raise OSError(f"Cannot roll back history clear for {watch_uuid}: watch identity changed")

        for item in files:
            name = item['name']
            staged_path = payload_dir / name
            original_path = watch_dir / name
            if not os.path.lexists(staged_path):
                if not os.path.lexists(original_path) and not committed:
                    raise FileNotFoundError(f"Both staged and original history file are missing: {original_path}")
                continue
            if committed:
                if os.path.isdir(staged_path) and not os.path.islink(staged_path):
                    raise OSError(f"Unexpected directory in history tombstone: {staged_path}")
                os.unlink(staged_path)
            else:
                self._restore_staged_history_file(staged_path, original_path)
        self._sync_directory(payload_dir)
        self._sync_directory(watch_dir)

        if payload_dir.exists():
            payload_dir.rmdir()
            self._sync_directory(transaction_dir)
        manifest_path = transaction_dir / 'manifest.json'
        if manifest_path.exists():
            manifest_path.unlink()
            self._sync_directory(transaction_dir)
        for temporary_manifest in transaction_dir.glob('json-*.tmp'):
            temporary_manifest.unlink()
        transaction_dir.rmdir()
        self._sync_directory(Path(self.datastore_path))
        return True

    def _resolve_pending_history_clears_for_watch(self, watch_uuid, watch):
        """Finish or roll back any earlier clear before deleting or clearing this watch again."""
        root = Path(self.datastore_path)
        for transaction_dir in root.iterdir():
            if (not transaction_dir.name.startswith(f'{TOMBSTONE_DIR_PREFIX}history-')
                    or not transaction_dir.is_dir() or transaction_dir.is_symlink()):
                continue
            manifest_path = transaction_dir / 'manifest.json'
            if not manifest_path.is_file():
                legacy_watch_json = transaction_dir / 'watch.json'
                if legacy_watch_json.is_file():
                    with open(legacy_watch_json, 'r', encoding='utf-8') as stream:
                        legacy_watch = json.load(stream)
                    if legacy_watch.get('uuid') == watch_uuid:
                        raise RuntimeError(
                            f"Unresolved legacy history-clear tombstone for {watch_uuid}: {transaction_dir}"
                        )
                continue

            with open(manifest_path, 'r', encoding='utf-8') as stream:
                manifest = json.load(stream)
            transaction_id, transaction_watch_uuid, _ = self._validate_history_clear_manifest(
                transaction_dir, manifest
            )
            if transaction_watch_uuid != watch_uuid:
                continue

            watch_json = root / watch_uuid / 'watch.json'
            if not watch_json.is_file():
                raise OSError(f"Cannot resolve history clear for {watch_uuid}: watch.json is missing")
            with open(watch_json, 'r', encoding='utf-8') as stream:
                watch_data = json.load(stream)
            marker = watch_data.get(HISTORY_CLEAR_TRANSACTION_MARKER)
            if marker and marker != transaction_id:
                raise ValueError(f"Watch {watch_uuid} has a different active history-clear marker")

            committed = manifest['state'] == 'committed' or marker == transaction_id
            if (marker == transaction_id and manifest.get('version') == 3
                    and watch_data.get(WATCH_STORAGE_IDENTITY) != manifest.get('watch_identity')):
                raise ValueError(f"Watch {watch_uuid} identity changed during history-clear recovery")
            if not committed and manifest.get('version') not in (2, 3):
                raise ValueError(f"Cannot safely roll back legacy history clear for {watch_uuid}: identity is unavailable")
            if committed:
                if manifest['state'] != 'committed':
                    manifest['state'] = 'committed'
                    self._write_history_clear_manifest(manifest_path, manifest)
                if marker == transaction_id:
                    watch_data.pop(HISTORY_CLEAR_TRANSACTION_MARKER, None)
                    save_json_atomic(str(watch_json), watch_data, label='recovered watch metadata')
                    self._sync_file(watch_json)
                    self._sync_directory(watch_json.parent)

            self._finish_history_clear_transaction(transaction_dir, manifest, committed)
            if getattr(watch, '_history_clear_transaction_id', None) == transaction_id:
                watch._history_clear_transaction_id = None
            logger.info(f"Resolved pending {'committed' if committed else 'interrupted'} history clear "
                        f"{transaction_id} for watch {watch_uuid}")

    def _resolve_pending_watch_results_for_watch(self, watch_uuid, watch):
        """Resolve committed or interrupted check journals before another lifecycle mutation."""
        root = Path(self.datastore_path)
        watch_dir = root / watch_uuid
        for transaction_dir in root.iterdir():
            if (not transaction_dir.name.startswith(f'{TOMBSTONE_DIR_PREFIX}result-')
                    or not transaction_dir.is_dir() or transaction_dir.is_symlink()):
                continue
            manifest_path = transaction_dir / 'manifest.json'
            if not manifest_path.is_file():
                shutil.rmtree(transaction_dir)
                continue
            with open(manifest_path, 'r', encoding='utf-8') as stream:
                manifest = json.load(stream)
            if manifest.get('watch_uuid') != watch_uuid:
                continue
            transaction_id = manifest.get('transaction_id')
            with open(watch_dir / 'watch.json', 'r', encoding='utf-8') as stream:
                watch_data = json.load(stream)
            marker = watch_data.get(WATCH_RESULT_TRANSACTION_MARKER)
            if marker and marker != transaction_id:
                raise ValueError(f"Watch {watch_uuid} has a conflicting result transaction marker")
            committed = manifest.get('state') == 'committed' or marker == transaction_id
            if (marker == transaction_id and manifest.get('version') == 2
                    and watch_data.get(WATCH_STORAGE_IDENTITY) != manifest.get('watch_identity')):
                raise ValueError(f"Watch {watch_uuid} identity changed during result recovery")
            if committed:
                if marker == transaction_id:
                    watch_data.pop(WATCH_RESULT_TRANSACTION_MARKER, None)
                    save_json_atomic(str(watch_dir / 'watch.json'), watch_data,
                                     label='recovered watch metadata')
                    self._sync_file(watch_dir / 'watch.json')
                watch._watch_result_transaction_id = None
            else:
                if manifest.get('version') == 2:
                    identity_matches = watch_data.get(WATCH_STORAGE_IDENTITY) == manifest.get('watch_identity')
                else:
                    identity_matches = (
                        hashlib.sha256((watch_dir / 'watch.json').read_bytes()).hexdigest()
                        == manifest.get('watch_identity')
                    )
                if not identity_matches:
                    raise ValueError(f"Watch {watch_uuid} identity changed during result recovery")
                self._rollback_watch_result_transaction(transaction_dir, manifest, watch_dir)
                _ = watch.history
            shutil.rmtree(transaction_dir)
            self._sync_directory(root)

    def _recover_legacy_history_tombstone(self, transaction_dir):
        """Recover old pre-manifest history tombstones only when watch.json proves rollback is needed."""
        old_watch_json = transaction_dir / 'watch.json'
        if old_watch_json.is_file():
            with open(old_watch_json, 'r', encoding='utf-8') as stream:
                old_watch = json.load(stream)
            watch_uuid = old_watch.get('uuid')
            if not self._is_safe_path_component(watch_uuid):
                raise ValueError(f"Cannot identify legacy history tombstone {transaction_dir}")
            watch_dir = Path(self.datastore_path) / watch_uuid
            if (watch_dir / 'watch.json').exists():
                logger.warning(f"Leaving legacy history tombstone for {watch_uuid} for manual review: "
                               "the metadata commit state cannot be determined")
                return False
            watch_dir.mkdir(parents=True, exist_ok=True)
            for staged_path in transaction_dir.iterdir():
                if staged_path.name == 'watch.json':
                    continue
                self._restore_staged_history_file(staged_path, watch_dir / staged_path.name)
            self._restore_staged_history_file(old_watch_json, watch_dir / 'watch.json')
            transaction_dir.rmdir()
            logger.warning(f"Restored legacy interrupted history clear for watch {watch_uuid}")
            return True

        # A crash while atomically writing the first manifest happens before any data moves.
        entries = list(transaction_dir.iterdir())
        safe_manifest_temps = [p for p in entries if p.name.startswith('json-') and p.name.endswith('.tmp')]
        other_entries = [p for p in entries if p not in safe_manifest_temps and not (p.name == 'payload' and p.is_dir())]
        if not other_entries:
            for path in safe_manifest_temps:
                path.unlink()
            payload = transaction_dir / 'payload'
            if payload.exists():
                payload.rmdir()
            transaction_dir.rmdir()
            return True

        logger.error(f"Preserving unrecognized history tombstone for manual recovery: {transaction_dir}")
        return False

    def _recover_history_clear_transactions(self):
        """Recover history-clear journals before the normal watch loader sees the datastore."""
        root = Path(self.datastore_path)
        if not root.exists():
            return
        for transaction_dir in root.iterdir():
            if (not transaction_dir.name.startswith(f'{TOMBSTONE_DIR_PREFIX}history-')
                    or not transaction_dir.is_dir() or transaction_dir.is_symlink()):
                continue
            manifest_path = transaction_dir / 'manifest.json'
            if not manifest_path.is_file():
                self._recover_legacy_history_tombstone(transaction_dir)
                continue
            try:
                with open(manifest_path, 'r', encoding='utf-8') as stream:
                    manifest = json.load(stream)
                transaction_id, watch_uuid, _ = self._validate_history_clear_manifest(transaction_dir, manifest)
                watch_json = root / watch_uuid / 'watch.json'
                marker = None
                if watch_json.is_file():
                    with open(watch_json, 'r', encoding='utf-8') as stream:
                        watch_data = json.load(stream)
                    marker = watch_data.get(HISTORY_CLEAR_TRANSACTION_MARKER)
                if marker and marker != transaction_id:
                    raise ValueError(f"Watch {watch_uuid} has a different active history-clear marker")
                committed = manifest['state'] == 'committed' or marker == transaction_id
                if (marker == transaction_id and manifest.get('version') == 3
                        and watch_data.get(WATCH_STORAGE_IDENTITY) != manifest['watch_identity']):
                    raise ValueError(f"Watch {watch_uuid} identity changed during history-clear recovery")
                if not committed and manifest.get('version') not in (2, 3):
                    raise ValueError(f"Cannot safely roll back legacy history clear for {watch_uuid}: identity is unavailable")
                if committed:
                    if manifest['state'] != 'committed':
                        manifest['state'] = 'committed'
                        self._write_history_clear_manifest(manifest_path, manifest)
                    if marker == transaction_id:
                        watch_data.pop(HISTORY_CLEAR_TRANSACTION_MARKER, None)
                        save_json_atomic(str(watch_json), watch_data, label='recovered watch metadata')
                        with open(watch_json, 'r+b') as stream:
                            os.fsync(stream.fileno())
                        self._sync_directory(watch_json.parent)
                self._finish_history_clear_transaction(transaction_dir, manifest, committed)
                logger.info(f"Recovered {'committed' if committed else 'interrupted'} history clear "
                            f"{transaction_id} for watch {watch_uuid}")
            except Exception as e:
                logger.critical(f"History-clear recovery failed for {transaction_dir}; preserving staged data: {e}")
                raise RuntimeError(f"History-clear recovery failed for {transaction_dir}: {e}") from e


    def _load_state(self, main_settings_filename="changedetection.json"):
        """
        Load complete datastore state from storage.

        Orchestrates loading of settings, watches, and tags using polymorphic methods.
        """
        # Load settings
        settings_data = self._load_settings(filename=main_settings_filename)
        self._apply_settings(settings_data)

        # Restore any old entity directory moved aside before an interrupted restore.
        self._recover_restore_transactions()
        # Repair any interrupted history clear before watch.json files are loaded.
        self._recover_history_clear_transactions()
        self._recover_watch_result_transactions()

        # Load watches, scan them from the disk
        self._load_watches()
        self._rehydrate_watches()

        # Load tags from individual tag.json files
        # These will override any tags in settings (migration path)
        self._load_tags()

        # Rehydrate any remaining tags from settings (legacy/fallback)
        self._rehydrate_tags()

    def reload_state(self, datastore_path, include_default_watches, version_tag):
        """
        Load datastore from storage or create new one.

        Supports two scenarios:
        1. NEW format: changedetection.json exists → load and run updates if needed
        2. EMPTY: No changedetection.json → create new OR trigger migration from legacy

        Note: Legacy url-watches.json migration happens in update_26, not here.
        """
        logger.info(f"Datastore path is '{datastore_path}'")

        # CRITICAL: Update datastore_path (was using old path from __init__)
        self.datastore_path = datastore_path

        # Initialize data structure
        self.__data = App.model(datastore_path=datastore_path)
        self.json_store_path = os.path.join(self.datastore_path, "changedetection.json")

        # Base definition for all watchers (deepcopy part of #569)
        self.generic_definition = deepcopy(Watch.model(datastore_path=datastore_path, __datastore=self.__data, default={}))

        # Load build SHA if available (Docker deployments)
        if path.isfile('changedetectionio/source.txt'):
            with open('changedetectionio/source.txt') as f:
                self.__data['build_sha'] = f.read()

        # Check if datastore already exists
        changedetection_json = os.path.join(self.datastore_path, "changedetection.json")
        changedetection_json_old_schema = os.path.join(self.datastore_path, "url-watches.json")

        if os.path.exists(changedetection_json):
            # Run schema updates if needed
            # Pass current schema version from loaded datastore (defaults to 0 if not set)
            # Load existing datastore (changedetection.json + watch.json files)
            logger.info("Loading existing datastore")
            self._load_state()
            current_schema = self.data['settings']['application'].get('schema_version', 0)
            self.run_updates(current_schema_version=current_schema)

        # Legacy datastore detected - trigger migration, even works if the schema is much before the migration step.
        elif os.path.exists(changedetection_json_old_schema):

            logger.critical(f"Legacy datastore detected at {changedetection_json_old_schema}, loading and running updates")
            self._load_state(main_settings_filename="url-watches.json")
            # update 26 will load the whole old config from disk to __data
            current_schema = self.__data['settings']['application'].get('schema_version', 0)
            self.run_updates(current_schema_version=current_schema)
            # Probably tags were also shifted to disk and many other changes, so best to reload here.
            self._load_state()

        else:
            # No datastore yet - check if this is a fresh install or legacy migration
            self.init_fresh_install(include_default_watches=include_default_watches,
                                    version_tag=version_tag)
            # Maybe they copied a bunch of watch subdirs across too
            self._load_state()

        # Runs for every datastore - new, existing or migrated. These used to run on every startup, until the
        # move to per-watch files put them inside init_fresh_install(), where they only ran for brand-new
        # installs (so e.g. removepassword.lock did nothing).
        self._after_load(version_tag)

    def _after_load(self, version_tag):
        """Startup housekeeping once the datastore is loaded: the running version, missing IDs and tokens,
        and the password reset lockfile."""
        # Set version tag
        self.__data['version_tag'] = version_tag

        # Validate proxies.json if it exists
        _ = self.proxy_list  # Just to test parsing

        # Ensure app_guid exists (for datastores loaded from existing files)
        if 'app_guid' not in self.__data:
            if "pytest" in sys.modules or "PYTEST_CURRENT_TEST" in os.environ:
                self.__data['app_guid'] = "test-" + str(uuid_builder.uuid4())
            else:
                self.__data['app_guid'] = str(uuid_builder.uuid4())
            if not self.commit():
                raise OSError("Could not persist application GUID during startup")

        # Ensure RSS access token exists
        if not self.__data['settings']['application'].get('rss_access_token'):
            secret = secrets.token_hex(16)
            self.__data['settings']['application']['rss_access_token'] = secret
            if not self.commit():
                raise OSError("Could not persist RSS access token during startup")

        # Ensure API access token exists
        if not self.__data['settings']['application'].get('api_access_token'):
            secret = secrets.token_hex(16)
            self.__data['settings']['application']['api_access_token'] = secret
            if not self.commit():
                raise OSError("Could not persist API access token during startup")

        # Handle password reset lockfile
        password_reset_lockfile = os.path.join(self.datastore_path, "removepassword.lock")
        if path.isfile(password_reset_lockfile):
            if self.remove_password():
                unlink(password_reset_lockfile)


    def init_fresh_install(self, include_default_watches, version_tag):
      # Generate app_guid FIRST (required for all operations)
        if "pytest" in sys.modules or "PYTEST_CURRENT_TEST" in os.environ:
            self.__data['app_guid'] = "test-" + str(uuid_builder.uuid4())
        else:
            self.__data['app_guid'] = str(uuid_builder.uuid4())

        # Generate RSS access token
        self.__data['settings']['application']['rss_access_token'] = secrets.token_hex(16)

        # Generate API access token
        self.__data['settings']['application']['api_access_token'] = secrets.token_hex(16)
        logger.warning(f"No datastore found, creating new datastore at {self.datastore_path}")

        # Set schema version to latest (no updates needed)
        latest_update_available = self.get_updates_available().pop()
        logger.info(f"Marking fresh install to schema version {latest_update_available}")
        self.__data['settings']['application']['schema_version'] = latest_update_available

        # Add default watches if requested
        if include_default_watches:
            self.add_watch(
                url='https://news.ycombinator.com/',
                tag='Tech news',
                extras={'fetch_backend': 'html_requests'}
            )
            self.add_watch(
                url='https://changedetection.io/CHANGELOG.txt',
                tag='changedetection.io',
                extras={'fetch_backend': 'html_requests'}
            )

        # Create changedetection.json immediately
        try:
            self._save_settings()
            logger.info("Created changedetection.json for new datastore")
        except Exception as e:
            logger.error(f"Failed to create initial changedetection.json: {e}")



    def rehydrate_entity(self, uuid, entity, processor_override=None):
        """Set the dict back to the dict Watch object"""
        entity['uuid'] = uuid

        if processor_override:
            watch_class = get_custom_watch_obj_for_processor(processor_override)
            entity['processor'] = processor_override
        else:
            watch_class = get_custom_watch_obj_for_processor(entity.get('processor'))

        if entity.get('processor') != 'text_json_diff':
            logger.trace(f"Loading Watch object '{watch_class.__module__}.{watch_class.__name__}' for UUID {uuid}")

        entity = watch_class(datastore_path=self.datastore_path, __datastore=self.__data,
                             __datastore_owner=self, default=entity)
        return entity

    # ============================================================================
    # FileSavingDataStore Abstract Method Implementations
    # ============================================================================

    def _watch_exists(self, uuid):
        """Check if watch exists in datastore."""
        return uuid in self.__data['watching']

    def _get_watch_dict(self, uuid):
        """Get watch as dictionary."""
        return dict(self.__data['watching'][uuid])

    def _build_settings_data(self):
        """
        Build settings data structure for saving.

        Tags behavior depends on schema version:
        - Before update_28 (schema < 28): Tags saved in settings for migration
        - After update_28 (schema >= 28): Tags excluded from settings (in individual files)

        Returns:
            dict: Settings data ready for serialization
        """
        import copy

        # Deep copy settings to avoid modifying the original
        settings_copy = copy.deepcopy(self.__data['settings'])

        # Is saved as {uuid}/tag.json
        settings_copy['application']['tags'] = {}

        return {
            'note': 'Settings file - watches are in {uuid}/watch.json, tags are in {uuid}/tag.json',
            'app_guid': self.__data.get('app_guid'),
            'settings': settings_copy,
            'build_sha': self.__data.get('build_sha'),
            'version_tag': self.__data.get('version_tag')
        }

    def _save_settings(self):
        """
        Save settings to storage.

        File backend implementation: saves to changedetection.json
        Implementation of abstract method from FileSavingDataStore.
        Uses the generic save_json_atomic helper.

        Raises:
            OSError: If disk is full or other I/O error
        """
        settings_data = self._build_settings_data()
        changedetection_json = os.path.join(self.datastore_path, "changedetection.json")
        save_json_atomic(changedetection_json, settings_data, label="settings")

    def _load_watches(self):
        """
        Load all watches from storage.

        File backend implementation: reads individual watch.json files
        Implementation of abstract method from FileSavingDataStore.
        Delegates to helper function and stores results in internal data structure.
        """

        # Store loaded data
        # @note this will also work for the old legacy format because self.__data['watching'] should already have them loaded by this point.
        self.__data['watching'].update(load_all_watches(
            self.datastore_path,
            self.rehydrate_entity
        ))
        logger.debug(f"Loaded {len(self.__data['watching'])} watches")

    def _load_tags(self):
        """
        Load all tags from storage.

        File backend implementation: reads individual tag.json files.
        Tags loaded from files override any tags in settings (migration path).
        """
        from ..model import Tag

        def rehydrate_tag(uuid, entity_dict):
            """Rehydrate tag as Tag object with forced restock_diff processor."""
            entity_dict['uuid'] = uuid
            entity_dict['processor'] = 'restock_diff'  # Force processor for override functionality

            return Tag.model(
                datastore_path=self.datastore_path,
                __datastore=self.__data,
                __datastore_owner=self,
                default=entity_dict
            )

        tags = load_all_tags(
            self.datastore_path,
            rehydrate_tag
        )

        # Override settings tags with loaded tags
        # This ensures tag.json files take precedence over settings
        if tags:
            self.__data['settings']['application']['tags'].update(tags)
            logger.info(f"Loaded {len(tags)} tags from individual tag.json files")

    def _delete_watch(self, uuid):
        """
        Move a watch directory to a private tombstone directory.

        The rename is the logical deletion boundary. Tombstone cleanup happens only
        after the watch has been unregistered and datastore locks are released.

        Args:
            uuid: Watch UUID to delete
        """
        watch_dir = os.path.join(self.datastore_path, uuid)
        if not os.path.lexists(watch_dir):
            return None

        tombstone_dir = create_tombstone_dir(self.datastore_path, 'watch')
        try:
            os.rename(watch_dir, os.path.join(tombstone_dir, 'watch'))
        except Exception:
            try:
                os.rmdir(tombstone_dir)
            except OSError:
                logger.exception(f"Could not remove empty watch tombstone directory {tombstone_dir}")
            raise
        return tombstone_dir

    @staticmethod
    def _cleanup_watch_tombstone(tombstone_dir, uuid):
        try:
            shutil.rmtree(tombstone_dir)
            return True
        except FileNotFoundError:
            return True
        except OSError as e:
            logger.warning(f"Watch {uuid} was deleted, but tombstone cleanup is pending at {tombstone_dir}: {e}")
            return False

    # ============================================================================
    # Watch Management Methods
    # ============================================================================

    def set_last_viewed(self, uuid, timestamp, send_signal=True):
        logger.debug(f"Setting watch UUID: {uuid} last viewed to {int(timestamp)}")
        saved = self.mutate_watch(uuid, lambda watch: watch.update({'last_viewed': int(timestamp)}))
        if not saved:
            return False

        # Bulk callers (mark-all-viewed) pass send_signal=False and emit one summary event
        # afterwards instead. Each signal fans out to handle_watch_update(), which rescans
        # EVERY watch twice (errored_count + unread_changes_count), takes the queue and
        # worker-pool locks, and broadcasts 3 socket events — so signalling per watch makes
        # a bulk mark O(n^2) with 3n emits, and floods every connected browser with n
        # row updates it will immediately re-render anyway.
        if send_signal:
            watch_check_update = signal('watch_check_update')
            if watch_check_update:
                watch_check_update.send(watch_uuid=uuid)
        return True

    def remove_password(self):
        application = self.__data['settings']['application']
        had_password_key = 'password' in application
        previous = application.get('password')
        self.__data['settings']['application']['password'] = False
        if not self.commit():
            if had_password_key:
                application['password'] = previous
            else:
                application.pop('password', None)
            return False
        return True

    def clear_all_last_checksums(self):
        """
        Delete all last-checksum.txt files to force reprocessing of all watches.

        This should be called when global settings change, since watches inherit
        configuration and need to reprocess even if their individual watch dict
        hasn't been modified.

        Note: We delete the checksum file rather than setting was_edited=True because:
        - was_edited is not persisted across restarts
        - File deletion ensures reprocessing works across app restarts
        """
        deleted_count = 0
        for uuid in self.__data['watching'].keys():
            watch = self.__data['watching'][uuid]
            if watch.data_dir:
                checksum_file = os.path.join(watch.data_dir, 'last-checksum.txt')
                if os.path.isfile(checksum_file):
                    try:
                        os.remove(checksum_file)
                        deleted_count += 1
                        logger.debug(f"Cleared checksum for watch {uuid}")
                    except OSError as e:
                        logger.warning(f"Failed to delete checksum file for {uuid}: {e}")

        logger.info(f"Cleared {deleted_count} checksum files to force reprocessing")
        return deleted_count

    def clear_checksums_for_tag(self, tag_uuid):
        """
        Delete last-checksum.txt files for all watches using a specific tag.

        This should be called when a tag configuration is edited, since watches
        inherit tag settings and need to reprocess.

        Args:
            tag_uuid: UUID of the tag that was modified

        Returns:
            int: Number of checksum files deleted
        """
        deleted_count = 0
        for uuid, watch in self.__data['watching'].items():
            if watch.get('tags') and tag_uuid in watch['tags']:
                if watch.data_dir:
                    checksum_file = os.path.join(watch.data_dir, 'last-checksum.txt')
                    if os.path.isfile(checksum_file):
                        try:
                            os.remove(checksum_file)
                            deleted_count += 1
                            logger.debug(f"Cleared checksum for watch {uuid} (tag {tag_uuid})")
                        except OSError as e:
                            logger.warning(f"Failed to delete checksum file for {uuid}: {e}")

        logger.info(f"Cleared {deleted_count} checksum files for tag {tag_uuid}")
        return deleted_count

    def commit(self):
        """
        Save settings immediately to disk using atomic write.

        Uses atomic write pattern (temp file + rename) for crash safety.

        Returns True only when the settings file was written successfully.
        """
        try:
            self._save_settings()
            logger.debug("Committed settings")
            return True
        except Exception as e:
            logger.error(f"Failed to commit settings: {e}")
            return False

    def update_watch(self, uuid, update_obj):
        # Fetch before taking the per-watch lock. All paths that need both locks use
        # watch-lock -> datastore-lock ordering to avoid inversion with commit().
        from ..model.Watch import current_check_result
        active_result = current_check_result()
        expected_watch = active_result[0] if active_result and active_result[0].get('uuid') == uuid else None
        with self.lock:
            watch = self.__data['watching'].get(uuid)
        if not watch or (expected_watch is not None and watch is not expected_watch):
            return False

        tag_guard = self._tag_reference_lock if update_obj and 'tags' in update_obj else nullcontext()
        with watch.mutation_lock:
            with tag_guard:
                with self.lock:
                    # It may have been removed while this caller waited for the watch lock.
                    if self.__data['watching'].get(uuid) is not watch:
                        return False
                    if (active_result and active_result[0] is watch
                            and not self._watch_check_token_current_locked(watch, active_result[1])):
                        return False
                    previous_watch_state = watch.snapshot_for_rollback()

                # In python 3.9 we have the |= dict operator, but that still will lose data on nested structures...
                for dict_key, d in self.generic_definition.items():
                    if isinstance(d, dict):
                        if update_obj is not None and dict_key in update_obj:
                            watch[dict_key].update(update_obj[dict_key])
                            del update_obj[dict_key]

                # Processor results continue to merge into the watch model.
                watch.update(update_obj)

                # Immediate save while still excluding other mutations of this watch.
                if not watch.commit():
                    watch.restore_after_failed_commit(previous_watch_state)
                    return False
                return True

    def apply_watch_result(self, uuid, watch, token, update_obj, contents, timestamp, snapshot_id):
        """Persist one check's metadata and history entry as a recoverable transaction.

        Caller performs network fetch and processing before this method. Lock order is
        watch mutation lock then datastore lock, matching commit/delete paths.
        """
        import copy

        if not isinstance(snapshot_id, str) or not self._is_safe_path_component(snapshot_id):
            return False
        watch.mutation_lock.acquire()
        transaction_dir = None
        manifest = None
        previous_state = None
        metadata_committed = False
        try:
            with self.lock:
                if (self.__data['watching'].get(uuid) is not watch
                        or not self._watch_check_token_current_locked(watch, token)):
                    return False
                previous_state = watch.snapshot_for_rollback()

            if not watch.commit():
                return False
            watch_dir = Path(watch.data_dir)
            if not watch.ensure_data_dir_exists():
                return False
            index_path = watch_dir / watch.history_index_filename
            had_index = index_path.is_file()
            transaction_id = uuid_builder.uuid4().hex
            transaction_dir = Path(self.datastore_path) / f'{TOMBSTONE_DIR_PREFIX}result-{transaction_id}'
            payload_dir = transaction_dir / 'payload'
            snapshot_backup_dir = payload_dir / 'snapshots'
            transaction_dir.mkdir(mode=0o700)
            payload_dir.mkdir()
            snapshot_backup_dir.mkdir()
            if had_index:
                shutil.copyfile(index_path, payload_dir / 'index.old')
                self._sync_file(payload_dir / 'index.old')
            snapshot_files = []
            for existing in watch_dir.glob(f'{snapshot_id}.*'):
                if existing.is_file() and not existing.is_symlink():
                    shutil.copyfile(existing, snapshot_backup_dir / existing.name)
                    snapshot_files.append(existing.name)
            manifest = {
                'version': 2,
                'transaction_id': transaction_id,
                'watch_uuid': uuid,
                'watch_identity': watch._watch_storage_identity,
                'state': 'prepared',
                'snapshot_id': snapshot_id,
                'snapshot_files': snapshot_files,
                'index_name': watch.history_index_filename,
                'had_index': had_index,
            }
            self._write_watch_result_manifest(transaction_dir / 'manifest.json', manifest)

            snapshot_name = watch.save_history_blob(contents=contents, timestamp=timestamp,
                                                    snapshot_id=snapshot_id, append_index=False)
            if snapshot_name is False:
                raise OSError("History snapshot could not be written")
            watch.append_history_index_entry(timestamp, snapshot_name)

            with self.lock:
                if (self.__data['watching'].get(uuid) is not watch
                        or not self._watch_check_token_current_locked(watch, token)):
                    raise RuntimeError("Watch changed before result metadata could be committed")
            for key, value in copy.deepcopy(update_obj or {}).items():
                default_value = self.generic_definition.get(key)
                if isinstance(default_value, dict) and isinstance(value, dict):
                    current_value = watch.get(key)
                    if isinstance(current_value, dict):
                        merged = dict(current_value)
                        merged.update(value)
                        watch[key] = merged
                        continue
                watch[key] = value
            watch._watch_result_transaction_id = transaction_id
            if not watch.commit():
                raise OSError(f"Could not persist result metadata for {uuid}")
            metadata_committed = True

            manifest['state'] = 'committed'
            self._write_watch_result_manifest(transaction_dir / 'manifest.json', manifest)
            watch._watch_result_transaction_id = None
            if not watch.commit():
                # The durable result still carries its transaction marker; startup
                # will remove it and finish journal cleanup.
                watch._watch_result_transaction_id = transaction_id
                logger.warning(f"Watch result committed for {uuid}; marker cleanup is pending")
                return True
            try:
                shutil.rmtree(transaction_dir)
                self._sync_directory(Path(self.datastore_path))
            except OSError as cleanup_error:
                logger.warning(f"Watch result committed for {uuid}; cleanup is pending at {transaction_dir}: "
                               f"{cleanup_error}")
            return True
        except Exception as e:
            if metadata_committed:
                logger.error(f"Watch result committed for {uuid}; journal finalization is pending: {e}")
                return True
            if previous_state is not None:
                watch.restore_after_failed_commit(previous_state)
            if transaction_dir and manifest is not None:
                try:
                    self._rollback_watch_result_transaction(transaction_dir, manifest, Path(watch.data_dir))
                    shutil.rmtree(transaction_dir)
                except Exception as rollback_error:
                    logger.critical(f"Could not roll back failed watch result for {uuid}; recovery data remains at "
                                    f"{transaction_dir}: {rollback_error}")
                finally:
                    _ = watch.history
            logger.error(f"Could not persist watch result for {uuid}: {e}")
            return False
        finally:
            watch.mutation_lock.release()

    def _is_entity_current_locked(self, uuid, entity):
        """Check a watch/tag mapping while ``self.lock`` is held by the caller."""
        from ..model.persistence import _determine_entity_type

        if _determine_entity_type(entity.__class__) == 'tag':
            collection = self.__data['settings']['application']['tags']
        else:
            collection = self.__data['watching']
        return collection.get(uuid) is entity

    def mutate_watch(self, uuid, mutation):
        """Apply and persist one watch mutation, restoring memory if persistence fails."""
        with self.lock:
            watch = self.__data['watching'].get(uuid)
        if watch is None:
            return False

        with watch.mutation_lock:
            with self._tag_reference_lock:
                with self.lock:
                    if self.__data['watching'].get(uuid) is not watch:
                        return False
                    previous_watch_state = watch.snapshot_for_rollback()
                try:
                    changed = mutation(watch)
                except Exception:
                    watch.restore_after_failed_commit(previous_watch_state)
                    raise
                if changed is False:
                    return True
                if not watch.commit():
                    watch.restore_after_failed_commit(previous_watch_state)
                    return False
                return True

    def mutate_tag(self, uuid, mutation):
        """Apply and persist one tag mutation, restoring memory if persistence fails."""
        with self.lock:
            tag = self.__data['settings']['application']['tags'].get(uuid)
        if tag is None:
            return False

        with tag.mutation_lock:
            with self.lock:
                if self.__data['settings']['application']['tags'].get(uuid) is not tag:
                    return False
                previous_tag_state = tag.snapshot_for_rollback()
            try:
                changed = mutation(tag)
            except Exception:
                tag.restore_after_failed_commit(previous_tag_state)
                raise
            if changed is False:
                return True
            if not tag.commit():
                tag.restore_after_failed_commit(previous_tag_state)
                return False
            return True

    def delete_tag(self, uuid, expected_tag=None):
        """Delete a tag only after serializing reference checks with all tag writes."""
        with self._tag_reference_lock:
            with self.lock:
                tag = self.__data['settings']['application']['tags'].get(uuid)
            if tag is None or (expected_tag is not None and tag is not expected_tag):
                return False

            tombstone_dir = None
            with tag.mutation_lock:
                with self.lock:
                    tags = self.__data['settings']['application']['tags']
                    if tags.get(uuid) is not tag:
                        return False
                    if any(uuid in (watch.get('tags') or []) for watch in self.__data['watching'].values()):
                        return False
                    try:
                        tombstone_dir = tags.stage_directory_deletion(uuid)
                    except OSError as e:
                        logger.error(f"Could not atomically stage tag {uuid} for deletion: {e}")
                        raise
                    dict.__delitem__(tags, uuid)

        if tombstone_dir:
            tags.cleanup_tombstone(tombstone_dir, uuid)
        return True

    def is_watch_current(self, uuid, watch):
        """Check that a locked watch or tag reference is still registered for its UUID."""
        with self.lock:
            return self._is_entity_current_locked(uuid, watch)

    def is_watch_unregistered(self, uuid):
        """Check that a watch UUID has no registered object (for explicit creation only)."""
        with self.lock:
            return uuid not in self.__data['watching']

    def _watch_check_token_current_locked(self, watch, token):
        return token == (watch._history_generation, watch._check_generation, watch._configuration_generation)

    def is_watch_check_token_current(self, uuid, watch, token):
        with self.lock:
            return (self.__data['watching'].get(uuid) is watch
                    and self._watch_check_token_current_locked(watch, token))

    def begin_watch_check(self, uuid, started_at):
        """Capture a watch identity/version before fetch without holding its lock during I/O."""
        with self.lock:
            watch = self.__data['watching'].get(uuid)
        if watch is None:
            return None, None

        with watch.mutation_lock:
            with self.lock:
                if self.__data['watching'].get(uuid) is not watch:
                    return None, None
                previous = watch.snapshot_for_rollback()
            previous_generation = watch._check_generation
            watch._check_generation += 1
            watch.update({'browser_steps_last_error_step': None, 'last_checked': started_at})
            if not watch.commit():
                watch.restore_after_failed_commit(previous)
                watch._check_generation = previous_generation
                return None, None
            token = (watch._history_generation, watch._check_generation, watch._configuration_generation)
            return watch, token

    def begin_watch_result_application(self, uuid, watch, token):
        """Acquire the watch lock for the result transaction, rejecting stale fetches."""
        watch.mutation_lock.acquire()
        try:
            with self.lock:
                if (self.__data['watching'].get(uuid) is not watch
                        or not self._watch_check_token_current_locked(watch, token)):
                    watch.mutation_lock.release()
                    return False
            return True
        except Exception:
            watch.mutation_lock.release()
            raise

    @staticmethod
    def end_watch_result_application(watch):
        watch.mutation_lock.release()

    def _has_invalid_new_tag_references_locked(self, watch):
        """Return whether this commit adds a tag UUID that is not currently registered."""
        previous = set(getattr(watch, '_persisted_tag_refs', set()))
        current = watch.get('tags') or []
        if isinstance(current, str):
            current = [current]
        added = set(current) - previous
        registered = self.__data['settings']['application'].get('tags', {})
        # Historical watch data can contain non-UUID labels in this field. Keep those
        # values compatible, while rejecting any UUID-shaped reference to a missing tag.
        return any(
            isinstance(tag_uuid, str)
            and _TAG_UUID_RE.fullmatch(tag_uuid)
            and tag_uuid not in registered
            for tag_uuid in added
        )

    @property
    def threshold_seconds(self):
        seconds = 0
        for m, n in Watch.mtable.items():
            x = self.__data['settings']['requests']['time_between_check'].get(m)
            if x:
                seconds += x * n
        return seconds

    @property
    def unread_changes_count(self):
        unread_changes_count = 0
        for uuid, watch in self.__data['watching'].items():
            if watch.history_n >= 2 and watch.viewed == False:
                unread_changes_count += 1

        return unread_changes_count

    @property
    def data(self):
        # Re #152, Return env base_url if not overriden
        # Re #148 - Some people have just {{ base_url }} in the body or title, but this may break some notification services
        #           like 'Join', so it's always best to atleast set something obvious so that they are not broken.

        active_base_url = BASE_URL_NOT_SET_TEXT
        if self.__data['settings']['application'].get('base_url'):
            active_base_url = self.__data['settings']['application'].get('base_url')
        elif os.getenv('BASE_URL'):
            active_base_url = os.getenv('BASE_URL')

        # I looked at various ways todo the following, but in the end just copying the dict seemed simplest/most reliable
        # even given the memory tradeoff - if you know a better way.. maybe return d|self.__data.. or something
        d = self.__data
        d['settings']['application']['active_base_url'] = active_base_url.strip('" ')
        return d

    # Delete a single watch by UUID
    def delete(self, uuid):
        """
        Delete a watch by UUID while coordinating with same-watch mutations and commits.

        Supports 'all' to delete all watches (mainly for testing).
        """
        if uuid == 'all':
            with self.lock:
                watches = list(self.__data['watching'].items())

            deleted_all = True
            for watch_uuid, watch in watches:
                if not self._delete_watch_under_watch_lock(watch_uuid, watch):
                    deleted_all = False

            # Mainly used in tests to allow all items to flush before running next test
            time.sleep(1)
            return deleted_all

        with self.lock:
            watch = self.__data['watching'].get(uuid)
        if watch is not None:
            return self._delete_watch_under_watch_lock(uuid, watch)
        return False

    def _delete_watch_under_watch_lock(self, uuid, watch):
        """Delete a watch without racing a same-watch commit or update."""
        tombstone_dir = None
        with watch.mutation_lock:
            with self.lock:
                if self.__data['watching'].get(uuid) is not watch:
                    return False

            try:
                self._resolve_pending_watch_results_for_watch(uuid, watch)
                # A prior clear may have committed but failed while removing its journal.
                # Resolve it before moving the watch directory out of the normal location.
                self._resolve_pending_history_clears_for_watch(uuid, watch)
            except Exception as e:
                logger.error(f"Failed to resolve pending history clear for watch {uuid}; refusing deletion: {e}")
                return False

            with self.lock:
                if self.__data['watching'].get(uuid) is not watch:
                    return False

                try:
                    tombstone_dir = self._delete_watch(uuid)
                except Exception as e:
                    logger.error(f"Failed to delete watch {uuid} from storage: {e}")
                    return False

                del self.__data['watching'][uuid]

        watch_delete_signal = signal('watch_deleted')
        if watch_delete_signal:
            watch_delete_signal.send(watch_uuid=uuid)
        if tombstone_dir:
            self._cleanup_watch_tombstone(tombstone_dir, uuid)
        return True

    # Clone a watch by UUID
    def clone(self, uuid):
        url = self.data['watching'][uuid].get('url')
        # No need to deepcopy here - add_watch() will deepcopy extras anyway (line 569)
        # Just pass a dict copy (with lock for thread safety)
        # NOTE: dict() is shallow copy but safe since add_watch() deepcopies it
        with self.lock:
            extras = dict(self.data['watching'][uuid])
        # None when add_watch() refused it (e.g. PAGE_WATCH_LIMIT), having already flashed why
        return self.add_watch(url=url, extras=extras)

    def url_exists(self, url):

        # Probably their should be dict...
        for watch in self.data['watching'].values():
            if watch['url'].lower() == url.lower():
                return True

        return False

    # Remove a watchs data but keep the entry (URL etc)
    def clear_watch_history(self, uuid, mutation=None):
        """Journal a history clear so startup can roll it back or finish it after a crash."""
        import pathlib

        with self.lock:
            watch = self.__data['watching'].get(uuid)
        if watch is None:
            return False

        with watch.mutation_lock:
            with self.lock:
                if self.__data['watching'].get(uuid) is not watch:
                    return False

            try:
                self._resolve_pending_watch_results_for_watch(uuid, watch)
                # Do not let a second clear replace or overlap an unresolved journal.
                self._resolve_pending_history_clears_for_watch(uuid, watch)
            except Exception as e:
                logger.error(f"Failed to resolve pending history clear for watch {uuid}; refusing new clear: {e}")
                return False

            with self.lock:
                if self.__data['watching'].get(uuid) is not watch:
                    return False
                previous_watch_state = watch.snapshot_for_rollback()

            transaction_id = uuid_builder.uuid4().hex
            transaction_dir = pathlib.Path(self.datastore_path) / f"{TOMBSTONE_DIR_PREFIX}history-{transaction_id}"
            payload_dir = transaction_dir / 'payload'
            manifest_path = transaction_dir / 'manifest.json'
            manifest = None
            metadata_committed = False
            try:
                # Persist a stable identity for legacy watches before staging files.
                if not watch.commit():
                    raise OSError(f"Could not persist watch identity for {uuid}")
                processor_names = [name for _processor, name in find_processors()]
                processor_config_files = {f"{name}.json" for name in processor_names}
                watch_dir = pathlib.Path(watch.data_dir)
                history_files = {pathlib.Path(path).name for path in watch.history.values()}
                files_to_stage = []
                for item in sorted(watch_dir.glob("*.*"), key=lambda candidate: candidate.name):
                    if (item.name == 'watch.json' or item.name in processor_config_files
                            or not (item.is_file() or item.is_symlink())):
                        continue
                    # Clear only known watch history/check artifacts. User-created sidecars
                    # and unrelated files remain in place, even if they have an extension.
                    if not self._is_history_clear_artifact(item.name, history_files):
                        continue
                    try:
                        stat = item.lstat()
                    except FileNotFoundError:
                        continue
                    files_to_stage.append((item, stat.st_dev, stat.st_ino, stat.st_size,
                                           stat.st_mtime_ns, stat.st_ctime_ns))

                transaction_dir.mkdir(mode=0o700)
                payload_dir.mkdir()
                self._sync_directory(Path(self.datastore_path))
                manifest = {
                    'version': 3,
                    'transaction_id': transaction_id,
                    'watch_uuid': uuid,
                    'watch_identity': watch._watch_storage_identity,
                    'tombstone_location': transaction_dir.name,
                    'state': 'prepared',
                    'files': [
                        {
                            'name': source.name,
                            'original_path': f'{uuid}/{source.name}',
                            'staged_path': f'{transaction_dir.name}/payload/{source.name}',
                        }
                        for source, *_ in files_to_stage
                    ],
                }
                # The durable intent must exist before the first original is moved.
                self._write_history_clear_manifest(manifest_path, manifest)

                for source, device, inode, size, modified_ns, changed_ns in files_to_stage:
                    try:
                        current = source.lstat()
                    except FileNotFoundError:
                        continue
                    if (current.st_dev, current.st_ino, current.st_size,
                            current.st_mtime_ns, current.st_ctime_ns) != (
                                device, inode, size, modified_ns, changed_ns):
                        continue
                    os.replace(source, payload_dir / source.name)
                    self._sync_directory(source.parent)
                    self._sync_directory(payload_dir)

                if mutation is not None and mutation(watch) is False:
                    raise RuntimeError("Watch history clearing mutation was rejected")
                watch.clear_watch()
                watch._history_clear_transaction_id = transaction_id
                if not watch.commit():
                    raise OSError(f"Could not persist cleared history for watch {uuid}")
                metadata_committed = True
                watch._history_generation += 1
                watch._check_generation += 1
                self._sync_file(watch_dir / 'watch.json')
                self._sync_directory(watch_dir)
            except Exception as e:
                if metadata_committed:
                    # The clear is committed. Keep the journal and marker so startup can
                    # finish cleanup; rolling files back now would contradict watch.json.
                    logger.error(f"History clear committed for {uuid}, but journal finalization is pending: {e}")
                    return True

                watch._history_clear_transaction_id = None
                state_restored = True
                try:
                    watch.restore_after_failed_commit(previous_watch_state)
                except Exception as restore_state_error:
                    state_restored = False
                    logger.exception(f"Could not restore in-memory watch {uuid} after history clear failure: "
                                     f"{restore_state_error}")

                restoration_error = None
                if manifest is not None:
                    try:
                        self._finish_history_clear_transaction(transaction_dir, manifest, committed=False)
                    except Exception as restore_error:
                        restoration_error = restore_error
                        logger.critical(f"History clear failed for watch {uuid}; recovery data remains at "
                                        f"{transaction_dir}: {restore_error}")

                if restoration_error is None and state_restored:
                    # clear_watch() refreshed these derived counters after staging emptied
                    # the index. Re-read now that rollback has restored the index file.
                    _ = watch.history
                    logger.error(f"Could not clear history for watch {uuid}; previous files and state were restored: {e}")
                elif restoration_error is None:
                    logger.critical(f"History clear failed for watch {uuid}; in-memory state restoration also failed: {e}")
                return False

            # A marker in watch.json disambiguates a crash between metadata replacement
            # and the journal's committed-state replacement.
            try:
                manifest['state'] = 'committed'
                self._write_history_clear_manifest(manifest_path, manifest)
            except Exception as e:
                logger.error(f"History clear committed for {uuid}; transaction recovery is pending at "
                             f"{transaction_dir}: {e}")
                return True

            watch._history_clear_transaction_id = None
            if not watch.commit():
                # Keep the marker in memory and the committed journal on disk. Recovery
                # will remove the marker and finish cleanup on the next startup.
                watch._history_clear_transaction_id = transaction_id
                logger.warning(f"History clear committed for {uuid}; transaction cleanup is pending at "
                               f"{transaction_dir}")
                return True
            try:
                self._sync_file(watch_dir / 'watch.json')
            except OSError as e:
                logger.warning(f"History clear committed for {uuid}; metadata sync is pending at "
                               f"{transaction_dir}: {e}")
                return True
            self._sync_directory(watch_dir)

            try:
                self._finish_history_clear_transaction(transaction_dir, manifest, committed=True)
            except Exception as e:
                logger.warning(f"History was cleared for watch {uuid}, but cleanup is pending at "
                               f"{transaction_dir}: {e}")

        watch_check_update = signal('watch_check_update')
        if watch_check_update:
            try:
                watch_check_update.send(watch_uuid=uuid)
            except Exception as e:
                logger.error(f"Watch {uuid} history was cleared, but update notification failed: {e}")

        return True

    @property
    def watch_limit(self):
        """Total watches allowed by PAGE_WATCH_LIMIT, or None when there is no limit.

        None means "unlimited" and is the normal case - the env var being absent, empty or
        unparseable all leave the limit switched off entirely. There is no default.
        """
        limit = os.getenv('PAGE_WATCH_LIMIT')
        if not limit:
            return None
        try:
            return int(limit)
        except ValueError:
            logger.warning(f"Invalid PAGE_WATCH_LIMIT value: {limit}, ignoring limit check")
            return None

    def watch_limit_reached(self):
        """True when the limit is set and leaves no room for another watch.

        add_watch() enforces this on its own, but it can only return None. Callers that can
        report something better - a 429 in the API, one flash instead of one per row in the
        importers - should check this first.
        """
        limit = self.watch_limit
        return limit is not None and len(self.__data['watching']) >= limit

    def watch_limit_message(self):
        """The single wording for a blocked add, so every UI surface says the same thing."""
        return gettext("Watch limit reached ({current}/{limit} watches). Cannot add more watches.").format(
            current=len(self.__data['watching']), limit=self.watch_limit
        )

    def _create_initial_watch_data_dir_locked(self, watch):
        """Create an unregistered watch directory while add_watch holds datastore.lock."""
        watch_uuid = watch.get('uuid')
        if self.__data['watching'].get(watch_uuid) is not None or os.path.lexists(watch.data_dir):
            raise FileExistsError(f"Watch directory or mapping already exists for {watch_uuid}")
        os.mkdir(watch.data_dir)
        return True

    def add_watch(self, url, tag='', extras=None, tag_uuids=None, save_immediately=True, seed_data_dir=None,
                  return_commit_status=False):
        """
        seed_data_dir: optional path to an existing directory (already in the watch's
        on-disk format, e.g. last-screenshot.png + elements.deflate) that becomes this
        watch's data_dir via a single rename(). Used to "promote" a temporary add-watch
        snapshot into a real watch without re-fetching. See make_temporary_watch_active_watch().
        """

        if extras is None:
            extras = {}

        if return_commit_status and not save_immediately:
            raise ValueError("return_commit_status requires save_immediately=True")

        def add_result(watch_uuid, saved=None, cleanup_complete=None, legacy_result=None):
            if return_commit_status:
                return watch_uuid, saved, cleanup_complete
            return watch_uuid if legacy_result is None else legacy_result

        # Incase these are copied across, assume it's a reference and deepcopy()
        apply_extras = deepcopy(extras)
        apply_extras['tags'] = [] if not apply_extras.get('tags') else apply_extras.get('tags')

        # Was it a share link? try to fetch the data
        if (url.startswith("https://changedetection.io/share/")):
            import requests

            try:
                r = requests.request(method="GET",
                                     url=url,
                                     # So we know to return the JSON instead of the human-friendly "help" page
                                     headers={'App-Guid': self.__data['app_guid']},
                                     timeout=5.0)  # 5 second timeout to prevent blocking
                res = r.json()

                # List of permissible attributes we accept from the wild internet
                for k in [
                    'body',
                    'browser_steps',
                    'css_filter',
                    'extract_text',
                    'headers',
                    'ignore_text',
                    'include_filters',
                    'method',
                    'paused',
                    'previous_md5',
                    'processor',
                    'subtractive_selectors',
                    'tag',
                    'tags',
                    'text_should_not_be_present',
                    'title',
                    'trigger_text',
                    'url',
                    'use_page_title_in_list',
                    'webdriver_js_execute_code',
                ]:
                    if res.get(k):
                        if k != 'css_filter':
                            apply_extras[k] = res[k]
                        else:
                            # We renamed the field and made it a list
                            apply_extras['include_filters'] = [res['css_filter']]

            except Exception as e:
                logger.error(f"Error fetching shared watch metadata: {type(e).__name__}")
                flash(gettext("Error fetching watch metadata"), 'error')
                return add_result(None, False, True, legacy_result=False)

        if not is_safe_valid_url(url):
            if has_request_context():
                flash(gettext('Watch protocol is not permitted or invalid URL format'), 'error')
            else:
                logger.error("add_watch: URL is not permitted or invalid, skipping.")
            return add_result(None, False, True)

        # Backstop for PAGE_WATCH_LIMIT - every add path funnels through here, so nothing can
        # get past the limit even if a caller forgets to pre-check watch_limit_reached().
        if self.watch_limit_reached():
            logger.error(f"Watch limit reached: {len(self.__data['watching'])}/{self.watch_limit} watches. Cannot add another watch")
            # The CLI (-u) and the API's background import thread have no request context,
            # where flash() raises instead of reporting anything
            if has_request_context():
                flash(self.watch_limit_message(), 'error')
            return add_result(None, False, True)

        if tag and type(tag) == str:
            # A comma separated string of tag *titles*, created when they don't exist yet.
            # An existing tag's UUID is accepted here too: the API documented this field as taking
            # a UUID for years, and honouring that beats creating a tag *titled* with the UUID.
            existing_tag_uuids = self.__data['settings']['application'].get('tags', {})

            for tag_name in tag.split(','):
                tag_name = tag_name.strip()
                if not tag_name:
                    continue

                if _TAG_UUID_RE.match(tag_name):
                    if tag_name in existing_tag_uuids:
                        apply_extras['tags'].append(tag_name)
                        continue
                    # UUID-shaped but no such tag, and no tag literally titled that either -
                    # a stale or foreign ID. Skip it rather than leave behind a group named
                    # after a UUID, which is never what the caller wanted.
                    if not self.tag_uuid_for_title(tag_name):
                        logger.warning(f"Tag '{tag_name}' looks like a UUID but no such tag exists, skipping")
                        continue

                tag_uuid = self.add_tag(tag_name)
                # add_tag() returns False for a title it won't create - never let that into the list,
                # a falsy entry blows up every lookup of watch['tags']
                if tag_uuid:
                    apply_extras['tags'].append(tag_uuid)

        # Or if UUIDs given directly
        if tag_uuids:
            for t in tag_uuids:
                apply_extras['tags'] = list(set(apply_extras['tags'] + [t.strip()]))

        # Make any uuids unique
        if apply_extras.get('tags'):
            apply_extras['tags'] = list(set(apply_extras.get('tags')))

        # 'processor' reaches here from callers that do NOT enum-validate it the way the API does:
        # /imports/import passes request.values through verbatim, and the share-link path above
        # takes it straight out of remote JSON. It later becomes a config filename
        # (f'{processor}.json'), so an unknown value is both a data-integrity problem and how
        # GHSA-mh42-m7cg-49fr escaped the watch directory. Drop it rather than store it; the
        # write paths are contained too, this stops it being persisted at all.
        if apply_extras.get('processor'):
            known_processors = [name for _module, name in find_processors()]
            if apply_extras['processor'] not in known_processors:
                logger.error(f"Ignoring unknown processor {apply_extras['processor']!r}; "
                             f"falling back to the default. Known: {known_processors}")
                del apply_extras['processor']

        # If the processor also has its own Watch implementation
        watch_class = get_custom_watch_obj_for_processor(apply_extras.get('processor'))
        new_watch = watch_class(datastore_path=self.datastore_path, __datastore=self.__data,
                                __datastore_owner=self, url=url)

        new_uuid = new_watch.get('uuid')

        logger.debug(f"Adding watch {new_uuid}")

        for k in ['uuid', 'history', 'last_checked', 'last_changed', 'newest_history_key', 'previous_md5', 'viewed']:
            if k in apply_extras:
                del apply_extras[k]

        if not apply_extras.get('date_created'):
            apply_extras['date_created'] = int(time.time())

        new_watch.update(apply_extras)
        # The first persisted watch has no saved tag-reference baseline yet.
        new_watch._persisted_tag_refs = set()

        # Import, share-link and API callers can hand over "don't use the default" with no interval of its own
        new_watch.ensure_valid_time_between_check()

        if save_immediately:
            seeded = bool(seed_data_dir and os.path.isdir(seed_data_dir))
            cleanup_complete = True
            with new_watch.mutation_lock:
                with self._tag_reference_lock:
                    with self.lock:
                        # Initial persistence is the sole allowed unregistered write. Hold
                        # the datastore lock from UUID reservation through registration.
                        while (new_uuid in self.__data['watching'] or os.path.exists(new_watch.data_dir)):
                            new_uuid = str(uuid_builder.uuid4())
                            new_watch['uuid'] = new_uuid

                        if self._has_invalid_new_tag_references_locked(new_watch):
                            logger.error(f"Refusing to create watch {new_uuid} with unregistered tag references")
                            return add_result(new_uuid, False, True, legacy_result=False)

                        try:
                            if seeded:
                                shutil.move(seed_data_dir, new_watch.data_dir)
                                logger.debug(f"Seeded watch {new_uuid} data_dir from {seed_data_dir}")
                            else:
                                self._create_initial_watch_data_dir_locked(new_watch)
                        except Exception as e:
                            logger.error(f"Failed to prepare initial watch directory for {new_uuid}: {e}")
                            if seeded:
                                cleanup_complete = False
                            elif os.path.isdir(new_watch.data_dir):
                                try:
                                    os.rmdir(new_watch.data_dir)
                                except OSError as cleanup_error:
                                    cleanup_complete = False
                                    logger.error(f"Failed to clean up initial watch directory "
                                                 f"{new_watch.data_dir}: {cleanup_error}")
                            return add_result(new_uuid, False, cleanup_complete, legacy_result=False)

                        if not new_watch._commit_locked(run_after_commit=False):
                            if not seeded and os.path.isdir(new_watch.data_dir):
                                try:
                                    os.rmdir(new_watch.data_dir)
                                except OSError as e:
                                    cleanup_complete = False
                                    logger.error(f"Failed to clean up uncommitted watch directory "
                                                 f"{new_watch.data_dir}: {e}")
                            elif seeded:
                                cleanup_complete = False
                            return add_result(new_uuid, False, cleanup_complete, legacy_result=False)

                        new_watch._persisted_tag_refs = set(new_watch.get('tags') or [])
                        self.__data['watching'][new_uuid] = new_watch

            new_watch._after_commit()
            logger.debug(f"Saved new watch {new_uuid}")
        else:
            seeded = bool(seed_data_dir and os.path.isdir(seed_data_dir))
            with new_watch.mutation_lock:
                with self._tag_reference_lock:
                    with self.lock:
                        if self._has_invalid_new_tag_references_locked(new_watch):
                            logger.error(f"Refusing to register watch {new_uuid} with unregistered tag references")
                            return add_result(new_uuid, False, True)
                        while (new_uuid in self.__data['watching'] or os.path.exists(new_watch.data_dir)):
                            new_uuid = str(uuid_builder.uuid4())
                            new_watch['uuid'] = new_uuid
                        if seeded:
                            shutil.move(seed_data_dir, new_watch.data_dir)
                            logger.debug(f"Seeded watch {new_uuid} data_dir from {seed_data_dir}")
                        else:
                            self._create_initial_watch_data_dir_locked(new_watch)
                        self.__data['watching'][new_uuid] = new_watch

        logger.debug(f"Added watch {new_uuid}")

        return add_result(new_uuid, True, True)

    # Where add-watch-ui parks freshly-fetched (but not-yet-saved) snapshots.
    TEMPORARY_WATCH_DIR = "temporary"

    def get_temporary_watch_dir(self, temp_uuid):
        """Resolve datastore_path/temporary/{temp_uuid}, rejecting anything that would
        escape the temporary/ directory (the temp_uuid arrives from a client POST field)."""
        if not temp_uuid or not re.match(r'^[0-9a-fA-F-]{36}$', str(temp_uuid)):
            return None
        base = os.path.realpath(os.path.join(self.datastore_path, self.TEMPORARY_WATCH_DIR))
        target = os.path.realpath(os.path.join(base, str(temp_uuid)))
        if os.path.dirname(target) != base:
            logger.warning(f"Rejected temporary watch path outside {base}: {temp_uuid}")
            return None
        return target

    def make_temporary_watch_active_watch(self, temp_uuid, url, tag='', extras=None):
        """Promote a temporary add-watch snapshot into a real watch.

        The snapshot directory (datastore_path/temporary/{temp_uuid}, already holding
        last-screenshot.png + elements.deflate in final on-disk format) is renamed into
        place as the new watch's data_dir - no re-fetch, no copy. If the temp_uuid is
        missing/expired/invalid we fall back to a normal add_watch() so the UI still works.

        Settings the snapshot recorded for itself in its own watch.json (currently just
        fetch_backend - the browser that rendered the preview) win over the posted form,
        because they describe what actually fetched. add_watch() -> commit() rewrites that
        file in full once the directory has been promoted.
        """
        seed_dir = self.get_temporary_watch_dir(temp_uuid)
        if not (seed_dir and os.path.isdir(seed_dir)):
            seed_dir = None

        extras = dict(extras or {})
        seed_watch_json = os.path.join(seed_dir, "watch.json") if seed_dir else None
        # isfile() first - a snapshot parked before this file existed is normal, not an error
        if seed_watch_json and os.path.isfile(seed_watch_json):
            seed_watch = load_watch_from_file(seed_watch_json, temp_uuid, self.rehydrate_entity)
            if seed_watch and seed_watch.get('fetch_backend'):
                extras['fetch_backend'] = seed_watch.get('fetch_backend')
                logger.debug(f"Promoting temporary watch {temp_uuid} with its recorded "
                             f"fetch_backend '{extras['fetch_backend']}'")

        return self.add_watch(url=url, tag=tag, extras=extras, seed_data_dir=seed_dir)

    def cleanup_temporary_watches(self, ttl_seconds=3600):
        """Remove orphaned snapshot dirs (user clicked Go but never submitted)."""
        base = os.path.join(self.datastore_path, self.TEMPORARY_WATCH_DIR)
        if not os.path.isdir(base):
            return
        now = time.time()
        for name in os.listdir(base):
            d = os.path.join(base, name)
            try:
                if os.path.isdir(d) and (now - os.path.getmtime(d)) > ttl_seconds:
                    shutil.rmtree(d, ignore_errors=True)
                    logger.debug(f"Cleaned orphaned temporary watch {name}")
            except Exception as e:
                logger.error(f"Failed cleaning temporary watch {d}: {e}")

    def _watch_resource_exists(self, watch_uuid, resource_name):
        """
        Check if a watch-related resource exists.

        File backend implementation: checks if file exists in watch directory.

        Args:
            watch_uuid: Watch UUID
            resource_name: Name of resource (e.g., "last-screenshot.png")

        Returns:
            bool: True if resource exists
        """
        resource_path = os.path.join(self.datastore_path, watch_uuid, resource_name)
        return path.isfile(resource_path)

    def visualselector_data_is_ready(self, watch_uuid):
        """
        Check if visual selector data (screenshot + elements) is ready.

        Returns:
            bool: True if both screenshot and elements data exist
        """
        has_screenshot = self._watch_resource_exists(watch_uuid, "last-screenshot.png")
        has_elements = self._watch_resource_exists(watch_uuid, "elements.deflate")
        return has_screenshot and has_elements

    # Old sync_to_json and save_datastore methods removed - now handled by FileSavingDataStore parent class

    @property
    def proxy_list(self):
        proxy_list = {}
        proxy_list_file = os.path.join(self.datastore_path, 'proxies.json')

        # Load from external config file
        if path.isfile(proxy_list_file):
            if HAS_ORJSON:
                # orjson.loads() expects UTF-8 encoded bytes #3611
                with open(os.path.join(self.datastore_path, "proxies.json"), 'rb') as f:
                    proxy_list = orjson.loads(f.read())
            else:
                with open(os.path.join(self.datastore_path, "proxies.json"), encoding='utf-8') as f:
                    proxy_list = json.load(f)

        # Mapping from UI config if available
        extras = self.data['settings']['requests'].get('extra_proxies')
        if extras:
            i = 0
            for proxy in extras:
                i += 0
                if proxy.get('proxy_name') and proxy.get('proxy_url'):
                    k = "ui-" + str(i) + proxy.get('proxy_name')
                    proxy_list[k] = {'label': proxy.get('proxy_name'), 'url': proxy.get('proxy_url')}

        if proxy_list and strtobool(os.getenv('ENABLE_NO_PROXY_OPTION', 'True')):
            proxy_list["no-proxy"] = {'label': gettext("No proxy"), 'url': ''}

        return proxy_list if len(proxy_list) else None

    def get_preferred_proxy_for_watch(self, uuid):
        """
        Returns the preferred proxy by ID key
        :param uuid: UUID
        :return: proxy "key" id
        """

        if self.proxy_list is None:
            return None

        # If it's a valid one
        watch = self.data['watching'].get(uuid)

        if strtobool(os.getenv('ENABLE_NO_PROXY_OPTION', 'True')) and watch.get('proxy') == "no-proxy":
            return None

        if watch.get('proxy') and watch.get('proxy') in list(self.proxy_list.keys()):
            return watch.get('proxy')

        # not valid (including None), try the system one
        else:
            system_proxy_id = self.data['settings']['requests'].get('proxy')
            # Is not None and exists
            if self.proxy_list.get(system_proxy_id):
                return system_proxy_id

        # Fallback - Did not resolve anything, or doesnt exist, use the first available
        if system_proxy_id is None or not self.proxy_list.get(system_proxy_id):
            first_default = list(self.proxy_list)[0]
            return first_default

        return None

    @property
    def has_extra_headers_file(self):
        filepath = os.path.join(self.datastore_path, 'headers.txt')
        return os.path.isfile(filepath)

    def get_all_base_headers(self):
        headers = {}
        # Global app settings
        headers.update(self.data['settings'].get('headers', {}))

        return headers

    def get_all_headers_in_textfile_for_watch(self, uuid):
        from ..model.App import parse_headers_from_text_file
        headers = {}

        # Global in /datastore/headers.txt
        filepath = os.path.join(self.datastore_path, 'headers.txt')
        try:
            if os.path.isfile(filepath):
                headers.update(parse_headers_from_text_file(filepath))
        except Exception as e:
            logger.error(f"ERROR reading headers.txt at {filepath} {str(e)}")

        watch = self.data['watching'].get(uuid)
        if watch:

            # In /datastore/xyz-xyz/headers.txt
            filepath = os.path.join(watch.data_dir, 'headers.txt')
            try:
                if os.path.isfile(filepath):
                    headers.update(parse_headers_from_text_file(filepath))
            except Exception as e:
                logger.error(f"ERROR reading headers.txt at {filepath} {str(e)}")

            # In /datastore/tag-name.txt
            tags = self.get_all_tags_for_watch(uuid=uuid)
            for tag_uuid, tag in tags.items():
                fname = "headers-" + re.sub(r'[\W_]', '', tag.get('title')).lower().strip() + ".txt"
                filepath = os.path.join(self.datastore_path, fname)
                try:
                    if os.path.isfile(filepath):
                        headers.update(parse_headers_from_text_file(filepath))
                except Exception as e:
                    logger.error(f"ERROR reading headers.txt at {filepath} {str(e)}")

        return headers

    def get_tag_overrides_for_watch(self, uuid, attr):
        tags = self.get_all_tags_for_watch(uuid=uuid)
        ret = []

        if tags:
            for tag_uuid, tag in tags.items():
                if attr in tag and tag[attr]:
                    ret = [*ret, *tag[attr]]

        return ret

    def tag_uuid_for_title(self, title):
        """UUID of the tag with this title (case/space insensitive), or None. Creates nothing."""
        n = title.strip().lower()
        if not n:
            return None

        for uuid, tag in self.__data['settings']['application'].get('tags', {}).items():
            if n == tag.get('title', '').lower().strip():
                return uuid

        return None

    def add_tag(self, title):
        # If name exists, return that
        n = title.strip().lower()
        logger.debug(f">>> Adding new tag - '{n}'")
        if not n:
            return False

        existing_uuid = self.tag_uuid_for_title(title)
        if existing_uuid:
            logger.warning(f"Tag '{title}' already exists, skipping creation.")
            return existing_uuid

        # Eventually almost everything todo with a watch will apply as a Tag
        # So we use the same model as a Watch
        with self.lock:
            from ..model import Tag
            new_tag = Tag.model(
                datastore_path=self.datastore_path,
                __datastore=self.__data,
                __datastore_owner=self,
                default={
                    'title': title.strip(),
                    'date_created': int(time.time())
                }
            )

            new_uuid = new_tag.get('uuid')
            while (new_uuid in self.__data['settings']['application']['tags']
                   or os.path.exists(new_tag.data_dir)):
                new_uuid = str(uuid_builder.uuid4())
                new_tag['uuid'] = new_uuid

            self.__data['settings']['application']['tags'][new_uuid] = new_tag

        # Save tag to its own tag.json file instead of settings
        if not new_tag.commit():
            with new_tag.mutation_lock:
                with self.lock:
                    tags = self.__data['settings']['application']['tags']
                    if tags.get(new_uuid) is new_tag:
                        del tags[new_uuid]
                # UUIDs are generated for this attempt; only remove a directory if it
                # was absent before registration and this commit created it.
                try:
                    shutil.rmtree(new_tag.data_dir)
                except FileNotFoundError:
                    pass
                except OSError as e:
                    logger.error(f"Failed to clean up unsaved tag directory {new_uuid}: {e}")
            return False
        return new_uuid

    def get_all_tags_for_watch(self, uuid):
        """This should be in Watch model but Watch doesn't have access to datastore, not sure how to solve that yet"""
        watch = self.data['watching'].get(uuid)
        if not watch:
            return {}

        # Start with manually assigned tags
        result = dictfilt(self.__data['settings']['application']['tags'], watch.get('tags', []))

        # Additionally include any tag whose url_match_pattern matches this watch's URL
        watch_url = watch.get('url', '')
        if watch_url:
            for tag_uuid, tag in self.__data['settings']['application']['tags'].items():
                if tag_uuid not in result and tag.matches_url(watch_url):
                    result[tag_uuid] = tag

        return result

    @property
    def extra_browsers(self):
        res = []
        p = list(filter(
            lambda s: (s.get('browser_name') and s.get('browser_connection_url')),
            self.__data['settings']['requests'].get('extra_browsers', [])))
        if p:
            for i in p:
                res.append(("extra_browser_" + i['browser_name'], i['browser_name']))

        return res

    def tag_exists_by_name(self, tag_name):
        # Check if any tag dictionary has a 'title' attribute matching the provided tag_name
        tags = self.__data['settings']['application']['tags'].values()
        return next((v for v in tags if v.get('title', '').lower() == tag_name.lower()),
                    None)

    def any_watches_have_processor_by_name(self, processor_name):
        for watch in self.data['watching'].values():
            if watch.get('processor') == processor_name:
                return True
        return False

    def search_watches_for_url(self, query, tag_limit=None, partial=False):
        """Search watches by URL, title, or error messages

        Args:
            query (str): Search term to match against watch URLs, titles, and error messages
            tag_limit (str, optional): Optional tag name to limit search results
            partial: (bool, optional): sub-string matching

        Returns:
            list: List of UUIDs of watches that match the search criteria
        """
        matching_uuids = []
        query = query.lower().strip()
        tag = self.tag_exists_by_name(tag_limit) if tag_limit else False

        for uuid, watch in self.data['watching'].items():
            # Filter by tag if requested
            if tag_limit:
                if not tag.get('uuid') in watch.get('tags', []):
                    continue

            # Search in URL, title, or error messages
            if partial:
                if ((watch.get('title') and query in watch.get('title').lower()) or
                        query in watch.get('url', '').lower() or
                        (watch.get('last_error') and query in watch.get('last_error').lower())):
                    matching_uuids.append(uuid)
            else:
                if ((watch.get('title') and query == watch.get('title').lower()) or
                        query == watch.get('url', '').lower() or
                        (watch.get('last_error') and query == watch.get('last_error').lower())):
                    matching_uuids.append(uuid)

        return matching_uuids

    def get_unique_notification_tokens_available(self):
        # Ask each type of watch if they have any extra notification token to add to the validation
        extra_notification_tokens = {}
        watch_processors_checked = set()

        for watch_uuid, watch in self.__data['watching'].items():
            processor = watch.get('processor')
            if processor not in watch_processors_checked:
                extra_notification_tokens.update(watch.extra_notification_token_values())
                watch_processors_checked.add(processor)

        return extra_notification_tokens

    def get_unique_notification_token_placeholders_available(self):
        # The actual description of the tokens, could be combined with get_unique_notification_tokens_available instead of doing this twice
        extra_notification_tokens = []
        watch_processors_checked = set()

        for watch_uuid, watch in self.__data['watching'].items():
            processor = watch.get('processor')
            if processor not in watch_processors_checked:
                extra_notification_tokens += watch.extra_notification_token_placeholder_info()
                watch_processors_checked.add(processor)

        return extra_notification_tokens

    def add_notification_url(self, notification_url):

        logger.debug(f">>> Adding new notification_url - '{notification_url}'")

        notification_urls = self.data['settings']['application'].get('notification_urls', [])

        if notification_url in notification_urls:
            return notification_url

        with self.lock:
            notification_urls = self.__data['settings']['application'].get('notification_urls', [])

            if notification_url in notification_urls:
                return notification_url

            # Append and update the datastore
            notification_urls.append(notification_url)
            self.__data['settings']['application']['notification_urls'] = notification_urls

        if not self.commit():
            with self.lock:
                urls = self.__data['settings']['application'].get('notification_urls', [])
                if notification_url in urls:
                    urls.remove(notification_url)
            return False
        return notification_url

    # Schema update methods moved to store/updates.py (DatastoreUpdatesMixin)
    # This includes: get_updates_available(), run_updates(), and update_1() through update_26()
