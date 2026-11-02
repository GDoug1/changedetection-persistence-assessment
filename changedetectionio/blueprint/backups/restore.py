import io
import json
import os
import re
import shutil
import tempfile
import threading
import zipfile
import uuid as uuid_builder
from contextlib import nullcontext

from flask import Blueprint, render_template, flash, url_for, redirect, request
from flask_babel import gettext, lazy_gettext as _l
from wtforms import Form, BooleanField, SubmitField
from flask_wtf.file import MultipleFileField, FileAllowed
from loguru import logger

from changedetectionio.flask_app import login_optionally_required

# Maximum total size of the uploaded zip file(s). Override via env var MAX_RESTORE_UPLOAD_MB.
_MAX_UPLOAD_BYTES = int(os.getenv("MAX_RESTORE_UPLOAD_MB", 256)) * 1024 * 1024
# Maximum total uncompressed size of all entries (zip-bomb guard). Override via MAX_RESTORE_DECOMPRESSED_MB.
_MAX_DECOMPRESSED_BYTES = int(os.getenv("MAX_RESTORE_DECOMPRESSED_MB", 1024)) * 1024 * 1024
# Only top-level directories whose name is a valid UUID are treated as watch/tag entries.
_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
    re.IGNORECASE,
)


def _rollback_directory_replacement(datastore_path, dst_dir, backup_root, kind):
    """Move failed replacement aside, then restore the prior directory if present."""
    failed_root = None
    if os.path.lexists(dst_dir):
        failed_root = tempfile.mkdtemp(prefix=f'.cdio-tombstone-failed-{kind}-', dir=datastore_path)
        os.replace(dst_dir, os.path.join(failed_root, 'failed'))
    previous = os.path.join(backup_root, 'previous') if backup_root else None
    if previous and os.path.lexists(previous):
        os.replace(previous, dst_dir)
    if failed_root:
        try:
            shutil.rmtree(failed_root)
        except OSError as e:
            logger.error(f"Restore: failed {kind} data retained at {failed_root}: {e}")
    if backup_root and os.path.isdir(backup_root):
        try:
            shutil.rmtree(backup_root)
        except OSError as e:
            logger.error(f"Restore: recovery journal retained at {backup_root}: {e}")


def _write_restore_manifest(datastore, backup_root, kind, uuid, state='prepared'):
    from changedetectionio.store.file_saving_datastore import save_json_atomic

    manifest_path = os.path.join(backup_root, 'manifest.json')
    save_json_atomic(manifest_path, {
        'version': 1,
        'kind': kind,
        'uuid': uuid,
        'state': state,
    }, label=f'{kind} restore transaction', max_size_mb=1)
    datastore._sync_file(manifest_path)
    datastore._sync_directory(backup_root)
    datastore._sync_directory(datastore.datastore_path)
    return manifest_path


def _discard_restore_journal_without_previous(backup_root):
    """Remove a failed pre-publication journal only when it holds no old entity."""
    if not backup_root or os.path.lexists(os.path.join(backup_root, 'previous')):
        return
    try:
        shutil.rmtree(backup_root)
    except OSError as e:
        logger.error(f"Restore: incomplete journal retained at {backup_root}: {e}")


class RestoreForm(Form):
    zip_file = MultipleFileField(_l('Backup .ZIP file(s)'), validators=[
        FileAllowed(['zip'], _l('Must be a .zip backup file!'))
    ], render_kw={'accept': '.zip'})
    include_groups = BooleanField(_l('Include groups'), default=True)
    include_groups_replace_existing = BooleanField(_l('Replace existing groups of the same UUID'), default=True)
    include_watches = BooleanField(_l('Include watches'), default=True)
    include_watches_replace_existing = BooleanField(_l('Replace existing watches of the same UUID'), default=True)
    submit = SubmitField(_l('Restore backup'))


def import_from_zip(zip_stream, datastore, include_groups, include_groups_replace, include_watches, include_watches_replace):
    """
    Extract and import watches and groups from a backup zip stream.

    Mirrors the store's _load_watches / _load_tags loading pattern:
      - UUID dirs with tag.json  → Tag.model + tag_obj.commit()
      - UUID dirs with watch.json → rehydrate_entity + watch_obj.commit()

    Returns a dict with counts: restored_groups, skipped_groups, restored_watches, skipped_watches.
    Raises zipfile.BadZipFile if the stream is not a valid zip.
    """
    from changedetectionio.model import Tag

    restored_groups = 0
    skipped_groups = 0
    restored_watches = 0
    skipped_watches = 0

    current_tags = datastore.data['settings']['application'].get('tags', {})
    current_watches = datastore.data['watching']

    with tempfile.TemporaryDirectory() as tmpdir:
        logger.debug(f"Restore: extracting zip to {tmpdir}")
        with zipfile.ZipFile(zip_stream, 'r') as zf:
            total_uncompressed = sum(m.file_size for m in zf.infolist())
            if total_uncompressed > _MAX_DECOMPRESSED_BYTES:
                raise ValueError(
                    f"Backup archive decompressed size ({total_uncompressed // (1024 * 1024)} MB) "
                    f"exceeds the {_MAX_DECOMPRESSED_BYTES // (1024 * 1024)} MB limit"
                )
            resolved_dest = os.path.realpath(tmpdir)
            for member in zf.infolist():
                member_dest = os.path.realpath(os.path.join(resolved_dest, member.filename))
                if not member_dest.startswith(resolved_dest + os.sep) and member_dest != resolved_dest:
                    raise ValueError(f"Zip Slip path traversal detected in backup archive: {member.filename!r}")
                zf.extract(member, tmpdir)
        logger.debug("Restore: zip extracted, scanning UUID directories")

        for entry in os.scandir(tmpdir):
            if not entry.is_dir():
                continue

            uuid = entry.name
            if not _UUID_RE.match(uuid):
                logger.warning(f"Restore: skipping non-UUID directory {uuid!r}")
                continue
            tag_json_path = os.path.join(entry.path, 'tag.json')
            watch_json_path = os.path.join(entry.path, 'watch.json')

            # --- Tags (groups) ---
            if include_groups and os.path.exists(tag_json_path):
                if uuid in current_tags and not include_groups_replace:
                    logger.debug(f"Restore: skipping existing group {uuid} (replace not requested)")
                    skipped_groups += 1
                    continue

                try:
                    with open(tag_json_path, 'r', encoding='utf-8') as f:
                        tag_data = json.load(f)
                except (json.JSONDecodeError, IOError) as e:
                    logger.error(f"Restore: failed to read tag.json for {uuid}: {e}")
                    continue

                title = tag_data.get('title', uuid)
                logger.debug(f"Restore: importing group '{title}' ({uuid})")

                # Mirror _load_tags: set uuid and force processor
                tag_data['uuid'] = uuid
                tag_data['processor'] = 'restock_diff'
                stage_root = tempfile.mkdtemp(prefix='.cdio-restore-stage-tag-', dir=datastore.datastore_path)
                stage_dir = os.path.join(stage_root, uuid)
                backup_root = None
                previous_tag = None
                restore_journal_committed = False
                tag_obj = Tag.model(
                    datastore_path=datastore.datastore_path,
                    __datastore=datastore.data,
                    __datastore_owner=datastore,
                    default=tag_data
                )
                try:
                    shutil.copytree(entry.path, stage_dir)
                    with open(os.path.join(stage_dir, 'tag.json'), 'r', encoding='utf-8') as stream:
                        staged_tag_data = json.load(stream)
                    if not isinstance(staged_tag_data, dict) or staged_tag_data.get('uuid') not in (None, uuid):
                        raise ValueError(f"Invalid staged tag metadata for {uuid}")

                    with datastore.lock:
                        previous_tag = current_tags.get(uuid)
                    tag_lock = previous_tag.mutation_lock if previous_tag else tag_obj.mutation_lock
                    with tag_lock:
                        with datastore.lock:
                            if current_tags.get(uuid) is not previous_tag:
                                skipped_groups += 1
                                continue
                            dst_dir = os.path.join(datastore.datastore_path, uuid)
                            backup_root = tempfile.mkdtemp(prefix='.cdio-tombstone-restore-tag-',
                                                           dir=datastore.datastore_path)
                            _write_restore_manifest(datastore, backup_root, 'tag', uuid)
                            if os.path.lexists(dst_dir):
                                os.replace(dst_dir, os.path.join(backup_root, 'previous'))
                                datastore._sync_directory(datastore.datastore_path)
                                datastore._sync_directory(backup_root)
                            try:
                                os.replace(stage_dir, dst_dir)
                                current_tags[uuid] = tag_obj
                            except Exception:
                                _rollback_directory_replacement(datastore.datastore_path, dst_dir,
                                                                backup_root, 'tag')
                                raise
                        if not tag_obj.commit():
                            with datastore.lock:
                                if current_tags.get(uuid) is tag_obj:
                                    if previous_tag is None:
                                        dict.pop(current_tags, uuid, None)
                                    else:
                                        current_tags[uuid] = previous_tag
                            _rollback_directory_replacement(datastore.datastore_path, dst_dir,
                                                            backup_root, 'tag')
                            raise OSError(f"Restore: tag {uuid} could not be committed")
                        try:
                            _write_restore_manifest(datastore, backup_root, 'tag', uuid, state='committed')
                            restore_journal_committed = True
                        except Exception as journal_error:
                            logger.error(f"Restore: tag {uuid} was committed; restore journal finalization is "
                                         f"pending at {backup_root}: {journal_error}")
                    if backup_root and restore_journal_committed:
                        try:
                            shutil.rmtree(backup_root)
                            datastore._sync_directory(datastore.datastore_path)
                        except OSError as cleanup_error:
                            logger.error(f"Restore: tag {uuid} was restored, but old data cleanup is pending at "
                                         f"{backup_root}: {cleanup_error}")
                except Exception as e:
                    logger.error(f"Restore: failed to restore tag {uuid}: {e}")
                    _discard_restore_journal_without_previous(backup_root)
                    skipped_groups += 1
                    continue
                finally:
                    if os.path.isdir(stage_root):
                        shutil.rmtree(stage_root, ignore_errors=True)
                restored_groups += 1
                logger.success(f"Restore: group '{title}' ({uuid}) restored")

            # --- Watches ---
            elif include_watches and os.path.exists(watch_json_path):
                if uuid in current_watches and not include_watches_replace:
                    logger.debug(f"Restore: skipping existing watch {uuid} (replace not requested)")
                    skipped_watches += 1
                    continue

                try:
                    with open(watch_json_path, 'r', encoding='utf-8') as f:
                        watch_data = json.load(f)
                except (json.JSONDecodeError, IOError) as e:
                    logger.error(f"Restore: failed to read watch.json for {uuid}: {e}")
                    continue

                url = watch_data.get('url', uuid)
                logger.debug(f"Restore: importing watch '{url}' ({uuid})")

                stage_root = tempfile.mkdtemp(prefix='.cdio-restore-stage-watch-', dir=datastore.datastore_path)
                stage_dir = os.path.join(stage_root, uuid)
                backup_root = None
                previous_watch = None
                watch_obj = None
                restore_journal_committed = False
                try:
                    shutil.copytree(entry.path, stage_dir)
                    with open(os.path.join(stage_dir, 'watch.json'), 'r', encoding='utf-8') as stream:
                        staged_watch_data = json.load(stream)
                    if not isinstance(staged_watch_data, dict) or staged_watch_data.get('uuid') not in (None, uuid):
                        raise ValueError(f"Invalid staged watch metadata for {uuid}")
                    watch_data['uuid'] = uuid
                    with datastore.lock:
                        previous_watch = current_watches.get(uuid)
                    watch_obj = datastore.rehydrate_entity(uuid, watch_data)
                    # A restored archive is a replacement entity even when it uses
                    # the same UUID and carries a copy of the former identity token.
                    watch_obj._watch_storage_identity = uuid_builder.uuid4().hex
                    watch_obj.ensure_valid_time_between_check()
                    watch_lock = previous_watch.mutation_lock if previous_watch else watch_obj.mutation_lock
                    with watch_lock:
                        with datastore._tag_reference_lock:
                            with datastore.lock:
                                if current_watches.get(uuid) is not previous_watch:
                                    skipped_watches += 1
                                    continue
                                tag_refs = watch_data.get('tags') or []
                                if isinstance(tag_refs, str):
                                    tag_refs = [tag_refs]
                                registered_tags = datastore.data['settings']['application'].get('tags', {})
                                if any(tag_uuid not in registered_tags for tag_uuid in tag_refs):
                                    logger.error(f"Restore: watch {uuid} references an unregistered tag; skipping")
                                    skipped_watches += 1
                                    continue
                                if previous_watch is not None:
                                    datastore._resolve_pending_watch_results_for_watch(uuid, previous_watch)
                                    datastore._resolve_pending_history_clears_for_watch(uuid, previous_watch)
                                dst_dir = os.path.join(datastore.datastore_path, uuid)
                                backup_root = tempfile.mkdtemp(prefix='.cdio-tombstone-restore-watch-',
                                                               dir=datastore.datastore_path)
                                _write_restore_manifest(datastore, backup_root, 'watch', uuid)
                                if os.path.lexists(dst_dir):
                                    os.replace(dst_dir, os.path.join(backup_root, 'previous'))
                                    datastore._sync_directory(datastore.datastore_path)
                                    datastore._sync_directory(backup_root)
                                try:
                                    os.replace(stage_dir, dst_dir)
                                    current_watches[uuid] = watch_obj
                                except Exception:
                                    _rollback_directory_replacement(datastore.datastore_path, dst_dir,
                                                                    backup_root, 'watch')
                                    raise
                            if not watch_obj.commit():
                                with datastore.lock:
                                    if current_watches.get(uuid) is watch_obj:
                                        if previous_watch is None:
                                            current_watches.pop(uuid, None)
                                        else:
                                            current_watches[uuid] = previous_watch
                                _rollback_directory_replacement(datastore.datastore_path, dst_dir,
                                                                backup_root, 'watch')
                                raise OSError(f"Restore: watch {uuid} could not be committed")
                            try:
                                _write_restore_manifest(datastore, backup_root, 'watch', uuid, state='committed')
                                restore_journal_committed = True
                            except Exception as journal_error:
                                logger.error(f"Restore: watch {uuid} was committed; restore journal finalization is "
                                             f"pending at {backup_root}: {journal_error}")
                    if backup_root and restore_journal_committed:
                        try:
                            shutil.rmtree(backup_root)
                            datastore._sync_directory(datastore.datastore_path)
                        except OSError as cleanup_error:
                            logger.error(f"Restore: watch {uuid} was restored, but old data cleanup is pending at "
                                         f"{backup_root}: {cleanup_error}")
                except Exception as e:
                    logger.error(f"Restore: failed to restore watch {uuid}: {e}")
                    _discard_restore_journal_without_previous(backup_root)
                    skipped_watches += 1
                    continue
                finally:
                    if os.path.isdir(stage_root):
                        shutil.rmtree(stage_root, ignore_errors=True)
                restored_watches += 1
                logger.success(f"Restore: watch '{url}' ({uuid}) restored")

        logger.debug(f"Restore: scan complete - groups {restored_groups} restored / {skipped_groups} skipped, "
                     f"watches {restored_watches} restored / {skipped_watches} skipped")

    # Persist changedetection.json (includes the updated tags dict)
    logger.debug("Restore: committing datastore settings")
    settings_persisted = datastore.commit()
    if not settings_persisted:
        logger.error("Restore: entity files were processed, but datastore settings could not be committed")

    return {
        'restored_groups': restored_groups,
        'skipped_groups': skipped_groups,
        'restored_watches': restored_watches,
        'skipped_watches': skipped_watches,
        'settings_persisted': settings_persisted,
    }


def import_from_zips(zip_streams, **kwargs):
    """
    Import several backup zips one after another, in upload order (a later zip wins for a shared UUID
    when replace is requested). A failure in one zip is logged and does not stop the rest.

    zip_streams is a list of (filename, stream) tuples, remaining kwargs are passed to import_from_zip().
    """
    for filename, zip_stream in zip_streams:
        logger.info(f"Restore: importing {filename!r}")
        try:
            import_from_zip(zip_stream=zip_stream, **kwargs)
        except Exception as e:
            logger.error(f"Restore: failed to import {filename!r}: {e}")


def construct_restore_blueprint(datastore):
    restore_blueprint = Blueprint('restore', __name__, template_folder="templates")
    restore_threads = []

    @restore_blueprint.route("/restore", methods=['GET'])
    @login_optionally_required
    def restore():
        form = RestoreForm()
        return render_template("backup_restore.html",
                               form=form,
                               restore_running=any(t.is_alive() for t in restore_threads),
                               max_upload_mb=_MAX_UPLOAD_BYTES // (1024 * 1024),
                               max_decompressed_mb=_MAX_DECOMPRESSED_BYTES // (1024 * 1024))

    @restore_blueprint.route("/restore/start", methods=['POST'])
    @login_optionally_required
    def backups_restore_start():
        if any(t.is_alive() for t in restore_threads):
            flash(gettext("A restore is already running, check back in a few minutes"), "error")
            return redirect(url_for('backups.restore.restore'))

        zip_files = [f for f in request.files.getlist('zip_file') if f and f.filename]
        if not zip_files:
            flash(gettext("No file uploaded"), "error")
            return redirect(url_for('backups.restore.restore'))

        if any(not f.filename.lower().endswith('.zip') for f in zip_files):
            flash(gettext("File must be a .zip backup file"), "error")
            return redirect(url_for('backups.restore.restore'))

        # Reject oversized uploads before reading the stream into memory.
        content_length = request.content_length
        if content_length and content_length > _MAX_UPLOAD_BYTES:
            flash(gettext("Backup file is too large (max %(mb)s MB)", mb=_MAX_UPLOAD_BYTES // (1024 * 1024)), "error")
            return redirect(url_for('backups.restore.restore'))

        # Read into memory now — the request stream is gone once we return.
        # The size limit applies to all files combined; read one byte beyond what is left
        # so we can detect truncated-but-still-oversized streams.
        zip_streams = []
        remaining = _MAX_UPLOAD_BYTES
        for zip_file in zip_files:
            try:
                raw = zip_file.read(remaining + 1)
                if len(raw) > remaining:
                    flash(gettext("Backup file is too large (max %(mb)s MB)", mb=_MAX_UPLOAD_BYTES // (1024 * 1024)), "error")
                    return redirect(url_for('backups.restore.restore'))
                remaining -= len(raw)
                zip_bytes = io.BytesIO(raw)
                with zipfile.ZipFile(zip_bytes):  # quick validity check before spawning
                    pass
                zip_bytes.seek(0)
            except zipfile.BadZipFile:
                flash(gettext("Invalid or corrupted zip file"), "error")
                return redirect(url_for('backups.restore.restore'))
            zip_streams.append((zip_file.filename, zip_bytes))

        include_groups = request.form.get('include_groups') == 'y'
        include_groups_replace = request.form.get('include_groups_replace_existing') == 'y'
        include_watches = request.form.get('include_watches') == 'y'
        include_watches_replace = request.form.get('include_watches_replace_existing') == 'y'

        restore_thread = threading.Thread(
            target=import_from_zips,
            kwargs={
                'zip_streams': zip_streams,
                'datastore': datastore,
                'include_groups': include_groups,
                'include_groups_replace': include_groups_replace,
                'include_watches': include_watches,
                'include_watches_replace': include_watches_replace,
            },
            daemon=True,
            name="BackupRestore"
        )
        restore_thread.start()
        restore_threads[:] = [t for t in restore_threads if t.is_alive()]
        restore_threads.append(restore_thread)
        flash(gettext("Restore started in background, check back in a few minutes."))
        return redirect(url_for('backups.restore.restore'))

    return restore_blueprint
