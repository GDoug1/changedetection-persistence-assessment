"""Regression coverage for processor-specific configuration persistence."""

import json
import os
import pickle
from pathlib import Path
import threading

import pytest

from changedetectionio.processors import get_resolved_processor_config, save_processor_config
from changedetectionio.processors.restock_diff.processor import perform_site_check
from changedetectionio.store import ChangeDetectionStore


@pytest.fixture
def datastore(tmp_path):
    store = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    yield store
    store.stop_thread = True


def _add_restock_processor(datastore):
    watch_uuid = datastore.add_watch(
        url='https://example.com/product',
        extras={'processor': 'restock_diff'},
    )
    return watch_uuid, perform_site_check(datastore, watch_uuid)


def test_restock_partial_updates_preserve_siblings_and_explicit_values(datastore):
    _, processor = _add_restock_processor(datastore)

    assert processor.update_extra_watch_config('restock_diff.json', {
        'restock_diff': {
            'in_stock_processing': 'all_changes',
            'follow_price_changes': True,
            'price_change_min': 10.0,
        }
    })

    assert processor.update_extra_watch_config('restock_diff.json', {
        'restock_diff': {
            'price_change_min': None,
            'follow_price_changes': False,
            'price_change_threshold_percent': 0,
            'extension_field': '',
        }
    })

    saved = processor.get_extra_watch_config('restock_diff.json')['restock_diff']
    assert saved == {
        'in_stock_processing': 'all_changes',
        'follow_price_changes': False,
        'price_change_min': None,
        'price_change_threshold_percent': 0,
        'extension_field': '',
    }


def test_empty_restock_object_clears_and_merge_false_replaces(datastore):
    _, processor = _add_restock_processor(datastore)
    initial = {
        'restock_diff': {'follow_price_changes': True, 'price_change_min': 10.0},
        'other_processor_metadata': 'preserved by partial merge',
    }
    assert processor.update_extra_watch_config('restock_diff.json', initial)

    assert processor.update_extra_watch_config('restock_diff.json', {'restock_diff': {}})
    after_clear = processor.get_extra_watch_config('restock_diff.json')
    assert after_clear['restock_diff'] == {}
    assert after_clear['other_processor_metadata'] == 'preserved by partial merge'

    replacement = {'restock_diff': {'in_stock_processing': 'off'}}
    assert processor.update_extra_watch_config('restock_diff.json', replacement, merge=False)
    assert processor.get_extra_watch_config('restock_diff.json') == replacement


def test_atomic_write_failure_preserves_previous_config_and_reports_failure(datastore, monkeypatch):
    watch_uuid, _ = _add_restock_processor(datastore)
    original_config = {'restock_diff': {'follow_price_changes': True, 'price_change_min': 10.0}}
    assert save_processor_config(datastore, watch_uuid, original_config)

    config_path = os.path.join(datastore.data['watching'][watch_uuid].data_dir, 'restock_diff.json')
    with open(config_path, 'rb') as f:
        original_bytes = f.read()

    from changedetectionio.store import file_saving_datastore

    def fail_replace(_source, _destination):
        raise OSError('simulated atomic replacement failure')

    monkeypatch.setattr(file_saving_datastore.os, 'replace', fail_replace)

    assert not save_processor_config(datastore, watch_uuid, {
        'restock_diff': {'follow_price_changes': False, 'price_change_min': 25.0}
    })

    with open(config_path, 'rb') as f:
        assert f.read() == original_bytes
    assert json.loads(original_bytes) == original_config


def test_saved_processor_config_reloads_from_datastore(datastore, tmp_path):
    watch_uuid, _ = _add_restock_processor(datastore)
    expected = {
        'in_stock_processing': 'all_changes',
        'follow_price_changes': False,
        'price_change_min': 0,
    }
    assert save_processor_config(datastore, watch_uuid, {'restock_diff': expected})

    reloaded_datastore = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        reloaded_watch = reloaded_datastore.data['watching'][watch_uuid]
        config, source = get_resolved_processor_config(reloaded_datastore, reloaded_watch)
        assert source == 'watch'
        assert config == expected
    finally:
        reloaded_datastore.stop_thread = True


def test_watch_commit_returns_true_on_success(datastore):
    watch_uuid = datastore.add_watch(url='https://example.com/saved')
    watch = datastore.data['watching'][watch_uuid]

    assert watch.commit() is True


def test_watch_commit_returns_false_on_write_failure(datastore, monkeypatch):
    watch_uuid = datastore.add_watch(url='https://example.com/failed')
    watch = datastore.data['watching'][watch_uuid]

    def fail_save(*_args, **_kwargs):
        raise OSError('simulated watch write failure')

    monkeypatch.setattr(watch, '_save_to_disk', fail_save)

    assert watch.commit() is False


def test_watch_commit_returns_false_without_a_datastore_path(datastore):
    watch_uuid = datastore.add_watch(url='https://example.com/missing-path')
    watch = datastore.data['watching'][watch_uuid]
    watch._datastore_path = None

    assert watch.commit() is False


def test_initial_watch_creation_persists_before_registration(datastore):
    watch_uuid, saved, cleanup_complete = datastore.add_watch(
        url='https://example.com/initial-create',
        return_commit_status=True,
    )

    assert saved is True
    assert cleanup_complete is True
    assert datastore.data['watching'][watch_uuid].get('uuid') == watch_uuid
    with open(Path(datastore.datastore_path) / watch_uuid / 'watch.json', encoding='utf-8') as watch_file:
        assert json.load(watch_file)['uuid'] == watch_uuid


def test_stale_direct_commit_after_delete_cannot_recreate_watch(datastore, tmp_path):
    watch_uuid = datastore.add_watch(url='https://example.com/deleted')
    stale_watch = datastore.data['watching'][watch_uuid]
    watch_dir = Path(stale_watch.data_dir)

    datastore.delete(watch_uuid)
    stale_watch['title'] = 'stale update'

    assert stale_watch.commit() is False
    assert not watch_dir.exists()

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert watch_uuid not in reloaded.data['watching']
    finally:
        reloaded.stop_thread = True


def test_failed_watch_directory_delete_keeps_registered_watch_and_disk_data(datastore, tmp_path, monkeypatch):
    watch_uuid = datastore.add_watch(url='https://example.com/delete-failure')
    watch = datastore.data['watching'][watch_uuid]
    watch_path = Path(watch.data_dir) / 'watch.json'
    with open(watch_path, encoding='utf-8') as watch_file:
        original_data = json.load(watch_file)
    original_data.pop('__cdio_watch_storage_identity__', None)

    def fail_delete(_uuid):
        raise OSError('simulated filesystem deletion failure')

    monkeypatch.setattr(datastore, '_delete_watch', fail_delete)

    assert datastore.delete(watch_uuid) is False
    assert datastore.data['watching'][watch_uuid] is watch
    assert watch_path.exists()

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert reloaded.data['watching'][watch_uuid] == original_data
    finally:
        reloaded.stop_thread = True


def test_stale_direct_commit_cannot_overwrite_same_uuid_replacement(datastore, tmp_path):
    watch_uuid = datastore.add_watch(
        url='https://example.com/replaced',
        extras={'title': 'Original'},
    )
    stale_watch = datastore.data['watching'][watch_uuid]
    replacement = datastore.rehydrate_entity(watch_uuid, dict(stale_watch))
    replacement['title'] = 'Replacement'
    with datastore.lock:
        datastore.data['watching'][watch_uuid] = replacement
    assert replacement.commit() is True

    stale_watch['title'] = 'Stale overwrite'
    assert stale_watch.commit() is False
    with open(Path(replacement.data_dir) / 'watch.json', encoding='utf-8') as watch_file:
        assert json.load(watch_file)['title'] == 'Replacement'

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert reloaded.data['watching'][watch_uuid]['title'] == 'Replacement'
    finally:
        reloaded.stop_thread = True


def test_identity_lock_is_held_until_watch_write_finishes(datastore):
    watch_uuid = datastore.add_watch(url='https://example.com/lock-through-write')
    watch = datastore.data['watching'][watch_uuid]
    save_entered = threading.Event()
    allow_save = threading.Event()
    mapping_change_acquired = threading.Event()
    results = {}
    real_save = watch._save_to_disk

    def blocked_save(data, uuid):
        save_entered.set()
        assert allow_save.wait(5), 'test did not release the blocked save'
        return real_save(data, uuid)

    watch._save_to_disk = blocked_save

    def commit_watch():
        results['commit'] = watch.commit()

    def replace_mapping():
        with datastore.lock:
            mapping_change_acquired.set()
            datastore.data['watching'].pop(watch_uuid, None)

    commit_thread = threading.Thread(target=commit_watch)
    commit_thread.start()
    assert save_entered.wait(5), 'commit did not reach the disk write'
    replace_thread = threading.Thread(target=replace_mapping)
    replace_thread.start()
    assert not mapping_change_acquired.wait(0.2), 'mapping changed while the commit was writing'

    allow_save.set()
    commit_thread.join(5)
    replace_thread.join(5)
    assert not commit_thread.is_alive()
    assert not replace_thread.is_alive()
    assert results['commit'] is True
    assert mapping_change_acquired.is_set()


def test_update_watch_failure_restores_memory_and_reload(datastore, tmp_path, monkeypatch):
    watch_uuid = datastore.add_watch(
        url='https://example.com/update-failure',
        extras={'title': 'Saved title'},
    )
    watch = datastore.data['watching'][watch_uuid]
    watch_path = Path(watch.data_dir) / 'watch.json'
    original_bytes = watch_path.read_bytes()

    def fail_save(*_args, **_kwargs):
        raise OSError('simulated watch persistence failure')

    monkeypatch.setattr(watch, '_save_to_disk', fail_save)

    assert datastore.update_watch(watch_uuid, {'title': 'Unsaved title'}) is False
    assert watch['title'] == 'Saved title'
    assert watch_path.read_bytes() == original_bytes

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert reloaded.data['watching'][watch_uuid]['title'] == 'Saved title'
    finally:
        reloaded.stop_thread = True


def test_favicon_is_cleared_only_after_successful_url_commit(datastore):
    watch_uuid = datastore.add_watch(
        url='https://example.com/old-product',
        extras={'paused': True},
    )
    watch = datastore.data['watching'][watch_uuid]
    favicon_path = Path(watch.data_dir) / 'favicon.ico'
    favicon_path.write_bytes(b'cached favicon')

    watch.update({'url': 'https://example.com/new-product'})

    assert favicon_path.read_bytes() == b'cached favicon'
    assert watch.commit() is True
    assert not favicon_path.exists()


def test_partial_merge_preserves_unreadable_existing_config(datastore, monkeypatch):
    watch_uuid, processor = _add_restock_processor(datastore)
    assert save_processor_config(datastore, watch_uuid, {
        'restock_diff': {'follow_price_changes': True}
    })
    config_path = Path(datastore.data['watching'][watch_uuid].data_dir) / 'restock_diff.json'
    original_bytes = config_path.read_bytes()
    real_open = open

    def fail_config_read(file, mode='r', *args, **kwargs):
        if os.fspath(file) == str(config_path) and 'r' in mode:
            raise PermissionError('simulated unreadable config')
        return real_open(file, mode, *args, **kwargs)

    monkeypatch.setattr('builtins.open', fail_config_read)

    assert processor.update_extra_watch_config('restock_diff.json', {
        'restock_diff': {'price_change_min': 12.0}
    }) is False
    assert config_path.read_bytes() == original_bytes


def test_partial_merge_preserves_malformed_existing_config(datastore):
    watch_uuid, processor = _add_restock_processor(datastore)
    config_path = Path(datastore.data['watching'][watch_uuid].data_dir) / 'restock_diff.json'
    malformed_bytes = b'{"restock_diff": '
    config_path.write_bytes(malformed_bytes)

    assert processor.update_extra_watch_config('restock_diff.json', {
        'restock_diff': {'price_change_min': 12.0}
    }) is False
    assert config_path.read_bytes() == malformed_bytes


def test_save_json_atomic_completes_short_writes(tmp_path, monkeypatch):
    from changedetectionio.store import file_saving_datastore

    config_path = tmp_path / 'config.json'
    expected = {'setting': 'a value longer than each simulated write'}
    real_write = os.write
    write_calls = []

    def short_write(fd, data):
        chunk = bytes(data[:5])
        write_count = real_write(fd, chunk)
        write_calls.append(write_count)
        return write_count

    monkeypatch.setattr(file_saving_datastore.os, 'write', short_write)

    file_saving_datastore.save_json_atomic(str(config_path), expected, label='test config')

    assert len(write_calls) > 1
    assert json.loads(config_path.read_text(encoding='utf-8')) == expected


def test_save_json_atomic_zero_byte_write_preserves_existing_file(tmp_path, monkeypatch):
    from changedetectionio.store import file_saving_datastore

    config_path = tmp_path / 'config.json'
    config_path.write_text('{"old": true}', encoding='utf-8')
    original_bytes = config_path.read_bytes()
    monkeypatch.setattr(file_saving_datastore.os, 'write', lambda _fd, _data: 0)

    with pytest.raises(OSError, match='no bytes'):
        file_saving_datastore.save_json_atomic(str(config_path), {'new': True}, label='test config')

    assert config_path.read_bytes() == original_bytes


def test_save_json_atomic_write_exception_preserves_target_and_cleans_temp(tmp_path, monkeypatch):
    from changedetectionio.store import file_saving_datastore

    config_path = tmp_path / 'config.json'
    config_path.write_text('{"old": true}', encoding='utf-8')
    original_bytes = config_path.read_bytes()
    real_write = os.write
    calls = 0

    def partial_then_fail(fd, data):
        nonlocal calls
        calls += 1
        if calls == 1:
            return real_write(fd, bytes(data[:4]))
        raise OSError('simulated write error after partial progress')

    monkeypatch.setattr(file_saving_datastore.os, 'write', partial_then_fail)

    with pytest.raises(OSError, match='simulated write error'):
        file_saving_datastore.save_json_atomic(str(config_path), {'new': True}, label='test config')

    assert config_path.read_bytes() == original_bytes
    assert list(tmp_path.glob('json-*.tmp')) == []


@pytest.mark.parametrize('invalid_result', [lambda data: len(data) + 1, lambda data: float(len(data))])
def test_save_json_atomic_invalid_write_counts_preserve_target_and_clean_temp(tmp_path, monkeypatch, invalid_result):
    from changedetectionio.store import file_saving_datastore

    config_path = tmp_path / 'config.json'
    config_path.write_text('{"old": true}', encoding='utf-8')
    original_bytes = config_path.read_bytes()
    monkeypatch.setattr(file_saving_datastore.os, 'write', lambda _fd, data: invalid_result(data))

    with pytest.raises(OSError, match='invalid count'):
        file_saving_datastore.save_json_atomic(str(config_path), {'new': True}, label='test config')

    assert config_path.read_bytes() == original_bytes
    assert list(tmp_path.glob('json-*.tmp')) == []


def test_save_json_atomic_replace_failure_preserves_target_and_cleans_temp(tmp_path, monkeypatch):
    from changedetectionio.store import file_saving_datastore

    config_path = tmp_path / 'config.json'
    config_path.write_text('{"old": true}', encoding='utf-8')
    original_bytes = config_path.read_bytes()

    def fail_replace(_source, _destination):
        raise OSError('simulated replacement failure')

    monkeypatch.setattr(file_saving_datastore.os, 'replace', fail_replace)

    with pytest.raises(OSError, match='replacement failure'):
        file_saving_datastore.save_json_atomic(str(config_path), {'new': True}, label='test config')

    assert config_path.read_bytes() == original_bytes
    assert list(tmp_path.glob('json-*.tmp')) == []


@pytest.mark.parametrize('file_value', [[], 'bad', None])
def test_resolved_processor_config_fails_closed_for_non_object_json(datastore, file_value):
    watch_uuid, _ = _add_restock_processor(datastore)
    watch = datastore.data['watching'][watch_uuid]
    config_path = Path(watch.data_dir) / 'restock_diff.json'
    config_path.write_text(json.dumps(file_value), encoding='utf-8')

    config, source = get_resolved_processor_config(datastore, watch)

    assert source == 'watch'
    assert config == {}


@pytest.mark.parametrize('file_value', [{'restock_diff': {}}, {'restock_diff': {'enabled': False}}])
def test_resolved_processor_config_reads_dictionary_json(datastore, file_value):
    watch_uuid, _ = _add_restock_processor(datastore)
    watch = datastore.data['watching'][watch_uuid]
    config_path = Path(watch.data_dir) / 'restock_diff.json'
    config_path.write_text(json.dumps(file_value), encoding='utf-8')

    config, source = get_resolved_processor_config(datastore, watch)

    assert source == 'watch'
    assert config == file_value.get('restock_diff', {})


def test_resolved_processor_config_fails_closed_for_malformed_or_missing_file(datastore):
    watch_uuid, _ = _add_restock_processor(datastore)
    watch = datastore.data['watching'][watch_uuid]
    config_path = Path(watch.data_dir) / 'restock_diff.json'
    config_path.write_text('{"restock_diff": ', encoding='utf-8')

    assert get_resolved_processor_config(datastore, watch) == ({}, 'watch')

    config_path.unlink()
    assert get_resolved_processor_config(datastore, watch) == ({}, 'watch')


def test_clear_history_write_failure_preserves_files_and_reports_failure(datastore, tmp_path, monkeypatch):
    watch_uuid = datastore.add_watch(
        url='https://example.com/history-clear-failure',
        extras={'title': 'Saved title', 'check_count': 7, 'last_checked': 123},
    )
    watch = datastore.data['watching'][watch_uuid]
    watch['last_checked'] = 123
    assert watch.commit() is True
    watch_dir = Path(watch.data_dir)
    history_files = {
        'history.txt': b'123,123.txt\n',
        '123.txt': b'previous snapshot',
        'last-screenshot.png': b'previous screenshot',
        'favicon.ico': b'previous favicon',
        'restock_diff.json': b'{"restock_diff":{"follow_price_changes":true}}',
    }
    for filename, contents in history_files.items():
        (watch_dir / filename).write_bytes(contents)
    assert watch.commit() is True
    original_watch_json = (watch_dir / 'watch.json').read_bytes()

    def fail_save(*_args, **_kwargs):
        raise OSError('simulated watch metadata write failure')

    monkeypatch.setattr(watch, '_save_to_disk', fail_save)

    assert datastore.clear_watch_history(watch_uuid) is False
    assert watch['title'] == 'Saved title'
    assert watch['check_count'] == 7
    assert watch['last_checked'] == 123
    assert (watch_dir / 'watch.json').read_bytes() == original_watch_json
    for filename, contents in history_files.items():
        assert (watch_dir / filename).read_bytes() == contents

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        reloaded_watch = reloaded.data['watching'][watch_uuid]
        assert reloaded_watch['title'] == 'Saved title'
        assert reloaded_watch.history
        assert (Path(reloaded_watch.data_dir) / '123.txt').read_bytes() == b'previous snapshot'
    finally:
        reloaded.stop_thread = True


def test_clear_history_staging_failure_restores_moved_files(datastore, tmp_path, monkeypatch):
    import changedetectionio.store as store_module

    watch_uuid = datastore.add_watch(url='https://example.com/history-stage-failure')
    watch = datastore.data['watching'][watch_uuid]
    watch_dir = Path(watch.data_dir)
    original_files = {
        'last-error.txt': b'first file',
        'last-screenshot.png': b'second file',
    }
    for filename, contents in original_files.items():
        (watch_dir / filename).write_bytes(contents)
    original_watch_json = (watch_dir / 'watch.json').read_bytes()

    real_replace = store_module.os.replace

    def fail_second_stage_move(source, destination):
        source_path = Path(source)
        destination_path = Path(destination)
        if (destination_path.parent.name == 'payload'
                and destination_path.parent.parent.name.startswith('.cdio-tombstone-history-')
                and source_path.name == 'last-screenshot.png'):
            raise OSError('simulated staging failure')
        return real_replace(source, destination)

    monkeypatch.setattr(store_module.os, 'replace', fail_second_stage_move)

    assert datastore.clear_watch_history(watch_uuid) is False
    assert (watch_dir / 'watch.json').read_bytes() == original_watch_json
    for filename, contents in original_files.items():
        assert (watch_dir / filename).read_bytes() == contents
    assert not list(Path(datastore.datastore_path).glob('.cdio-tombstone-history-*'))

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert reloaded.data['watching'][watch_uuid]['uuid'] == watch_uuid
        for filename, contents in original_files.items():
            assert (Path(reloaded.data['watching'][watch_uuid].data_dir) / filename).read_bytes() == contents
    finally:
        reloaded.stop_thread = True


def test_watch_delete_rename_failure_preserves_registered_watch_and_disk_data(datastore, tmp_path, monkeypatch):
    watch_uuid = datastore.add_watch(
        url='https://example.com/delete-rename-failure',
        extras={'title': 'Keep on disk'},
    )
    watch = datastore.data['watching'][watch_uuid]
    watch_path = Path(watch.data_dir) / 'watch.json'
    original_data = watch_path.read_bytes()

    def fail_rename(_uuid):
        raise OSError('simulated directory rename failure')

    monkeypatch.setattr(datastore, '_delete_watch', fail_rename)

    assert datastore.delete(watch_uuid) is False
    assert datastore.data['watching'][watch_uuid] is watch
    assert watch_path.read_bytes() == original_data

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert reloaded.data['watching'][watch_uuid]['title'] == 'Keep on disk'
    finally:
        reloaded.stop_thread = True


def test_watch_delete_tombstone_cleanup_failure_keeps_watch_logically_deleted(datastore, tmp_path, monkeypatch):
    import changedetectionio.store as store_module

    watch_uuid = datastore.add_watch(url='https://example.com/delete-cleanup-failure')
    watch = datastore.data['watching'][watch_uuid]
    watch_dir = Path(watch.data_dir)
    real_rmtree = store_module.shutil.rmtree

    def fail_tombstone_cleanup(path, *args, **kwargs):
        if Path(path).name.startswith('.cdio-tombstone-watch-'):
            raise PermissionError('simulated tombstone cleanup failure')
        return real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(store_module.shutil, 'rmtree', fail_tombstone_cleanup)

    assert datastore.delete(watch_uuid) is True
    assert watch_uuid not in datastore.data['watching']
    assert not watch_dir.exists()
    tombstones = list(Path(datastore.datastore_path).glob('.cdio-tombstone-watch-*'))
    assert len(tombstones) == 1
    assert (tombstones[0] / 'watch' / 'watch.json').exists()

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert watch_uuid not in reloaded.data['watching']
    finally:
        reloaded.stop_thread = True


def test_watch_delete_partial_cleanup_failure_does_not_resurrect_watch(datastore, tmp_path, monkeypatch):
    import changedetectionio.store as store_module

    watch_uuid = datastore.add_watch(url='https://example.com/delete-partial-cleanup')
    watch = datastore.data['watching'][watch_uuid]
    watch_dir = Path(watch.data_dir)
    real_rmtree = store_module.shutil.rmtree

    def partially_fail_tombstone_cleanup(path, *args, **kwargs):
        path = Path(path)
        if path.name.startswith('.cdio-tombstone-watch-'):
            (path / 'watch' / 'watch.json').unlink()
            raise OSError('simulated failure after partial tombstone cleanup')
        return real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(store_module.shutil, 'rmtree', partially_fail_tombstone_cleanup)

    assert datastore.delete(watch_uuid) is True
    assert watch_uuid not in datastore.data['watching']
    assert not watch_dir.exists()

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert watch_uuid not in reloaded.data['watching']
    finally:
        reloaded.stop_thread = True


def test_watch_delete_same_uuid_replacement_is_not_removed(datastore):
    watch_uuid = datastore.add_watch(
        url='https://example.com/same-uuid-replacement',
        extras={'title': 'Original'},
    )
    original = datastore.data['watching'][watch_uuid]
    replacement = datastore.rehydrate_entity(watch_uuid, dict(original))
    replacement['title'] = 'Replacement'
    with datastore.lock:
        datastore.data['watching'][watch_uuid] = replacement

    assert datastore._delete_watch_under_watch_lock(watch_uuid, original) is False
    assert datastore.data['watching'][watch_uuid] is replacement
    assert (Path(replacement.data_dir) / 'watch.json').exists()


def test_watch_delete_reload_ignores_tombstone_directories(datastore, tmp_path):
    tombstone_watch = Path(datastore.datastore_path) / '.cdio-tombstone-watch-test' / 'watch'
    tombstone_watch.mkdir(parents=True)
    watch_uuid = datastore.add_watch(url='https://example.com/tombstone-template')
    source_watch = datastore.data['watching'][watch_uuid]
    (tombstone_watch / 'watch.json').write_bytes((Path(source_watch.data_dir) / 'watch.json').read_bytes())

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert '.cdio-tombstone-watch-test' not in reloaded.data['watching']
        assert watch_uuid in reloaded.data['watching']
    finally:
        reloaded.stop_thread = True


def test_tag_delete_rename_failure_preserves_tag_and_disk_data(datastore, tmp_path, monkeypatch):
    import importlib
    tags_module = importlib.import_module('changedetectionio.model.Tags')

    tag_uuid = datastore.add_tag('Tag rename failure')
    tag = datastore.data['settings']['application']['tags'][tag_uuid]
    tag_path = Path(tag.data_dir) / 'tag.json'
    original_data = tag_path.read_bytes()
    real_rename = tags_module.os.rename

    def fail_tag_rename(source, destination):
        if Path(source) == Path(tag.data_dir):
            raise OSError('simulated tag directory rename failure')
        return real_rename(source, destination)

    monkeypatch.setattr(tags_module.os, 'rename', fail_tag_rename)

    with pytest.raises(OSError):
        datastore.delete_tag(tag_uuid)
    assert datastore.data['settings']['application']['tags'][tag_uuid] is tag
    assert tag_path.read_bytes() == original_data

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert reloaded.data['settings']['application']['tags'][tag_uuid]['title'] == 'Tag rename failure'
    finally:
        reloaded.stop_thread = True


def test_tag_delete_cleanup_failure_does_not_restore_deleted_tag(datastore, tmp_path, monkeypatch):
    import importlib
    tags_module = importlib.import_module('changedetectionio.model.Tags')

    tag_uuid = datastore.add_tag('Tag cleanup failure')
    tag = datastore.data['settings']['application']['tags'][tag_uuid]
    tag_dir = Path(tag.data_dir)
    real_rmtree = tags_module.shutil.rmtree

    def fail_tag_cleanup(path, *args, **kwargs):
        if Path(path).name.startswith('.cdio-tombstone-tag-'):
            raise PermissionError('simulated tag tombstone cleanup failure')
        return real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(tags_module.shutil, 'rmtree', fail_tag_cleanup)

    assert datastore.delete_tag(tag_uuid) is True
    assert tag_uuid not in datastore.data['settings']['application']['tags']
    assert not tag_dir.exists()
    assert len(list(Path(datastore.datastore_path).glob('.cdio-tombstone-tag-*'))) == 1

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert tag_uuid not in reloaded.data['settings']['application']['tags']
    finally:
        reloaded.stop_thread = True


def test_tag_delete_success_survives_fresh_datastore_reload(datastore, tmp_path):
    tag_uuid = datastore.add_tag('Tag deleted successfully')
    tag_dir = Path(datastore.data['settings']['application']['tags'][tag_uuid].data_dir)

    assert datastore.delete_tag(tag_uuid) is True
    assert tag_uuid not in datastore.data['settings']['application']['tags']
    assert not tag_dir.exists()

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert tag_uuid not in reloaded.data['settings']['application']['tags']
    finally:
        reloaded.stop_thread = True


def test_history_snapshot_write_waits_for_history_clear_staging(datastore, monkeypatch):
    from changedetectionio import store as store_module

    watch_uuid = datastore.add_watch(url='https://example.com/history-clear-concurrency')
    watch = datastore.data['watching'][watch_uuid]
    watch.save_history_blob('old snapshot', 100, 'old-snapshot')

    staging_started = threading.Event()
    continue_staging = threading.Event()
    writer_started = threading.Event()
    writer_finished = threading.Event()
    clear_result = {}
    real_replace = os.replace

    def hold_history_staging(source, destination):
        if '.cdio-tombstone-history-' in str(destination):
            staging_started.set()
            assert continue_staging.wait(5), 'test did not release history staging'
        return real_replace(source, destination)

    monkeypatch.setattr(store_module.os, 'replace', hold_history_staging)

    clear_thread = threading.Thread(
        target=lambda: clear_result.setdefault('saved', datastore.clear_watch_history(watch_uuid))
    )
    clear_thread.start()
    assert staging_started.wait(5), 'history clear did not begin staging'

    def write_new_snapshot():
        writer_started.set()
        watch.save_history_blob('new snapshot', 200, 'new-snapshot')
        writer_finished.set()

    writer_thread = threading.Thread(target=write_new_snapshot)
    writer_thread.start()
    assert writer_started.wait(5)
    assert not writer_finished.wait(0.1), 'snapshot write did not wait for the history clear lock'

    continue_staging.set()
    clear_thread.join(5)
    writer_thread.join(5)

    assert not clear_thread.is_alive()
    assert not writer_thread.is_alive()
    assert clear_result['saved'] is True
    assert writer_finished.is_set()
    assert list(watch.history) == ['200']
    assert watch.get_history_snapshot(timestamp='200') == 'new snapshot'


def _make_history_clear_recovery_watch(datastore):
    watch_uuid = datastore.add_watch(
        url='https://example.com/history-clear-recovery',
        extras={'last_checked': 123, 'check_count': 1},
    )
    watch = datastore.data['watching'][watch_uuid]
    assert watch.save_history_blob('pre-clear snapshot', 100, 'recovery-snapshot') is not False
    (Path(watch.data_dir) / 'last-screenshot.png').write_bytes(b'history screenshot')
    (Path(watch.data_dir) / 'custom-metadata.json').write_text('{"keep": true}', encoding='utf-8')
    (Path(watch.data_dir) / 'restock_diff.json').write_text(
        '{"restock_diff":{"keep":true}}', encoding='utf-8'
    )
    assert watch.commit() is True
    return watch_uuid


@pytest.mark.parametrize(
    ('crash_point', 'should_be_cleared'),
    [
        ('manifest', False),
        ('first_move', False),
        ('all_staged', False),
        ('metadata_commit', True),
        ('committed_manifest', True),
        ('partial_cleanup', True),
    ],
)
def test_history_clear_crash_recovery_is_idempotent(
        datastore, tmp_path, monkeypatch, crash_point, should_be_cleared):
    import importlib

    store_module = importlib.import_module('changedetectionio.store')
    watch_uuid = _make_history_clear_recovery_watch(datastore)
    watch = datastore.data['watching'][watch_uuid]
    watch_dir = Path(watch.data_dir)
    assert list(watch.history) == ['100']

    if crash_point == 'manifest':
        real_write_manifest = datastore._write_history_clear_manifest

        def crash_after_prepared_manifest(path, manifest):
            real_write_manifest(path, manifest)
            if manifest['state'] == 'prepared':
                raise SystemExit('simulated crash after manifest')

        monkeypatch.setattr(datastore, '_write_history_clear_manifest', crash_after_prepared_manifest)
    elif crash_point in ('first_move', 'all_staged'):
        real_replace = store_module.os.replace
        expected_moves = sum(
            1 for item in watch_dir.glob('*.*')
            if item.name in {'history.txt', 'recovery-snapshot.txt', 'last-screenshot.png'}
        )
        moved = 0

        def crash_during_staging(source, destination):
            nonlocal moved
            source_path, destination_path = Path(source), Path(destination)
            is_history_move = (
                source_path.parent == watch_dir
                and destination_path.parent.name == 'payload'
                and destination_path.parent.parent.name.startswith('.cdio-tombstone-history-')
            )
            result = real_replace(source, destination)
            if is_history_move:
                moved += 1
                if crash_point == 'first_move' and moved == 1:
                    raise SystemExit('simulated crash after first history move')
                if crash_point == 'all_staged' and moved == expected_moves:
                    raise SystemExit('simulated crash after all history moves')
            return result

        monkeypatch.setattr(store_module.os, 'replace', crash_during_staging)
    elif crash_point == 'metadata_commit':
        real_commit = watch.commit

        def crash_after_metadata_commit():
            result = real_commit()
            if result and getattr(watch, '_history_clear_transaction_id', None):
                raise SystemExit('simulated crash after metadata commit')
            return result

        monkeypatch.setattr(watch, 'commit', crash_after_metadata_commit)
    elif crash_point == 'committed_manifest':
        real_write_manifest = datastore._write_history_clear_manifest

        def crash_after_committed_manifest(path, manifest):
            real_write_manifest(path, manifest)
            if manifest['state'] == 'committed':
                raise SystemExit('simulated crash after committed manifest')

        monkeypatch.setattr(datastore, '_write_history_clear_manifest', crash_after_committed_manifest)
    else:
        real_unlink = store_module.os.unlink
        removed_staged_files = 0

        def crash_during_cleanup(path, *args, **kwargs):
            nonlocal removed_staged_files
            path_obj = Path(path)
            result = real_unlink(path, *args, **kwargs)
            if path_obj.parent.name == 'payload':
                removed_staged_files += 1
                if removed_staged_files == 1:
                    raise SystemExit('simulated crash during tombstone cleanup')
            return result

        monkeypatch.setattr(store_module.os, 'unlink', crash_during_cleanup)

    with pytest.raises(SystemExit, match='simulated crash'):
        datastore.clear_watch_history(watch_uuid)

    monkeypatch.undo()
    datastore.stop_thread = True
    recovered = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        recovered_watch = recovered.data['watching'][watch_uuid]
        recovered_dir = Path(recovered_watch.data_dir)
        assert recovered_watch['uuid'] == watch_uuid
        assert list(recovered_watch.history) == ([] if should_be_cleared else ['100'])
        assert (recovered_dir / 'custom-metadata.json').read_text(encoding='utf-8') == '{"keep": true}'
        assert json.loads((recovered_dir / 'restock_diff.json').read_text(encoding='utf-8')) == {
            'restock_diff': {'keep': True}
        }
        assert not list(Path(tmp_path).glob('.cdio-tombstone-history-*'))

        # A second startup pass sees no partial transaction and cannot change the result.
        recovered._recover_history_clear_transactions()
        assert list(recovered_watch.history) == ([] if should_be_cleared else ['100'])
    finally:
        recovered.stop_thread = True

    second_reload = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert list(second_reload.data['watching'][watch_uuid].history) == (
            [] if should_be_cleared else ['100']
        )
    finally:
        second_reload.stop_thread = True


def test_history_clear_rollback_recovery_refuses_to_overwrite_new_file(datastore, tmp_path, monkeypatch):
    import importlib

    store_module = importlib.import_module('changedetectionio.store')
    watch_uuid = _make_history_clear_recovery_watch(datastore)
    watch = datastore.data['watching'][watch_uuid]
    watch_dir = Path(watch.data_dir)
    real_replace = store_module.os.replace

    def crash_after_first_move(source, destination):
        source_path, destination_path = Path(source), Path(destination)
        result = real_replace(source, destination)
        if (source_path.parent == watch_dir and destination_path.parent.name == 'payload'):
            raise SystemExit('simulated crash after staging')
        return result

    monkeypatch.setattr(store_module.os, 'replace', crash_after_first_move)
    with pytest.raises(SystemExit, match='simulated crash'):
        datastore.clear_watch_history(watch_uuid)
    monkeypatch.undo()

    transaction_dir = next(Path(tmp_path).glob('.cdio-tombstone-history-*'))
    manifest = json.loads((transaction_dir / 'manifest.json').read_text(encoding='utf-8'))
    staged_name = manifest['files'][0]['name']
    (watch_dir / staged_name).write_bytes(b'newer file must survive')
    datastore.stop_thread = True

    with pytest.raises(RuntimeError, match='History-clear recovery failed'):
        ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)

    assert (watch_dir / staged_name).read_bytes() == b'newer file must survive'
    assert (transaction_dir / 'payload' / staged_name).exists()


@pytest.mark.parametrize('next_operation', ['delete', 'clear'])
def test_pending_committed_history_clear_is_resolved_before_lifecycle_change(
        datastore, tmp_path, monkeypatch, next_operation):
    watch_uuid = _make_history_clear_recovery_watch(datastore)
    watch = datastore.data['watching'][watch_uuid]
    real_finish = datastore._finish_history_clear_transaction

    def fail_committed_cleanup(transaction_dir, manifest, committed):
        if committed:
            raise OSError('simulated cleanup interruption')
        return real_finish(transaction_dir, manifest, committed)

    monkeypatch.setattr(datastore, '_finish_history_clear_transaction', fail_committed_cleanup)
    assert datastore.clear_watch_history(watch_uuid) is True
    pending_transactions = list(Path(tmp_path).glob('.cdio-tombstone-history-*'))
    assert len(pending_transactions) == 1
    assert watch['uuid'] == watch_uuid

    monkeypatch.setattr(datastore, '_finish_history_clear_transaction', real_finish)
    if next_operation == 'delete':
        assert datastore.delete(watch_uuid) is True
    else:
        assert datastore.clear_watch_history(watch_uuid) is True
        assert list(watch.history) == []

    assert not list(Path(tmp_path).glob('.cdio-tombstone-history-*'))
    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        if next_operation == 'delete':
            assert watch_uuid not in reloaded.data['watching']
        else:
            assert watch_uuid in reloaded.data['watching']
            assert list(reloaded.data['watching'][watch_uuid].history) == []
    finally:
        reloaded.stop_thread = True


def test_unresolved_legacy_history_tombstone_blocks_watch_deletion(datastore, tmp_path):
    watch_uuid = _make_history_clear_recovery_watch(datastore)
    watch = datastore.data['watching'][watch_uuid]
    active_watch_json = Path(watch.data_dir) / 'watch.json'
    snapshot_path = Path(watch.history['100'])
    legacy_tombstone = Path(tmp_path) / '.cdio-tombstone-history-legacy'
    legacy_tombstone.mkdir()
    (legacy_tombstone / 'watch.json').write_bytes(active_watch_json.read_bytes())
    (legacy_tombstone / snapshot_path.name).write_bytes(snapshot_path.read_bytes())

    assert datastore.delete(watch_uuid) is False
    assert datastore.data['watching'][watch_uuid] is watch
    assert active_watch_json.is_file()
    assert snapshot_path.read_text(encoding='utf-8') == 'pre-clear snapshot'
    assert (legacy_tombstone / 'watch.json').is_file()

    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        reloaded_watch = reloaded.data['watching'][watch_uuid]
        assert list(reloaded_watch.history) == ['100']
        assert snapshot_path.read_text(encoding='utf-8') == 'pre-clear snapshot'
        assert (legacy_tombstone / 'watch.json').is_file()
    finally:
        reloaded.stop_thread = True


def test_stale_watch_file_writes_after_delete_are_rejected_and_do_not_recreate_directory(datastore, tmp_path):
    watch_uuid = datastore.add_watch(url='https://example.com/stale-file-write')
    stale_watch = datastore.data['watching'][watch_uuid]
    stale_dir = Path(stale_watch.data_dir)

    assert datastore.delete(watch_uuid) is True
    assert not stale_dir.exists()
    assert stale_watch.save_history_blob('orphan', 200, 'orphan') is False
    assert stale_watch.save_screenshot(b'orphan screenshot') is False
    assert stale_watch.save_error_text('orphan error') is False
    assert not stale_dir.exists()

    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert watch_uuid not in reloaded.data['watching']
        assert not stale_dir.exists()
    finally:
        reloaded.stop_thread = True


def test_stale_same_uuid_watch_cannot_write_into_replacement_directory(datastore, tmp_path):
    watch_uuid = datastore.add_watch(url='https://example.com/stale-replacement-write')
    stale_watch = datastore.data['watching'][watch_uuid]
    replacement = datastore.rehydrate_entity(watch_uuid, dict(stale_watch))
    replacement['title'] = 'replacement'
    with datastore.lock:
        datastore.data['watching'][watch_uuid] = replacement
    assert replacement.commit() is True

    assert stale_watch.save_history_blob('orphan', 300, 'stale') is False
    assert stale_watch.save_screenshot(b'orphan screenshot') is False
    assert not (Path(replacement.data_dir) / 'stale.txt').exists()

    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        loaded = reloaded.data['watching'][watch_uuid]
        assert loaded['title'] == 'replacement'
        assert list(loaded.history) == []
        assert not (Path(loaded.data_dir) / 'stale.txt').exists()
    finally:
        reloaded.stop_thread = True


def test_history_recovery_refuses_identical_same_uuid_replacement(datastore, tmp_path, monkeypatch):
    import changedetectionio.store as store_module

    watch_uuid = _make_history_clear_recovery_watch(datastore)
    original = datastore.data['watching'][watch_uuid]
    original_dir = Path(original.data_dir)
    real_replace = store_module.os.replace

    def crash_after_first_stage(source, destination):
        result = real_replace(source, destination)
        source_path, destination_path = Path(source), Path(destination)
        if source_path.parent == original_dir and destination_path.parent.name == 'payload':
            raise SystemExit('simulated interrupted history clear')
        return result

    monkeypatch.setattr(store_module.os, 'replace', crash_after_first_stage)
    with pytest.raises(SystemExit, match='simulated interrupted'):
        datastore.clear_watch_history(watch_uuid)
    monkeypatch.undo()

    # A byte-for-byte copy of the old watch metadata used to pass the journal's
    # hash check. Replacement watches now receive a distinct persisted identity.
    replacement = datastore.rehydrate_entity(watch_uuid, dict(original))
    replacement._watch_storage_identity = 'f' * 32
    with datastore.lock:
        datastore.data['watching'][watch_uuid] = replacement
    assert replacement.commit() is True
    tombstone = next(Path(tmp_path).glob('.cdio-tombstone-history-*'))
    staged_files = list((tombstone / 'payload').iterdir())

    datastore.stop_thread = True
    with pytest.raises(RuntimeError, match='History-clear recovery failed'):
        ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)

    assert tombstone.is_dir()
    assert all(path.exists() for path in staged_files)
    loaded_json = json.loads((original_dir / 'watch.json').read_text(encoding='utf-8'))
    assert loaded_json['__cdio_watch_storage_identity__'] == 'f' * 32


def test_extract_history_csv_rejects_same_uuid_stale_watch(datastore, tmp_path):
    watch_uuid = datastore.add_watch(url='https://example.com/stale-csv')
    stale = datastore.data['watching'][watch_uuid]
    replacement = datastore.rehydrate_entity(watch_uuid, dict(stale))
    replacement._watch_storage_identity = 'e' * 32
    with datastore.lock:
        datastore.data['watching'][watch_uuid] = replacement
    assert replacement.commit() is True

    assert stale.extract_regex_from_all_history('.*') is False
    assert not (Path(replacement.data_dir) / f'report-{watch_uuid}.csv').exists()


def test_restock_migration_merges_existing_target_and_is_idempotent(datastore, tmp_path):
    watch_uuid = datastore.add_watch(url='https://example.com/restock-migration')
    watch = datastore.data['watching'][watch_uuid]
    watch['processor'] = 'restock_diff'
    watch['restock_settings'] = {'follow_price_changes': False, 'minimum_price_change': 0}
    assert watch.commit() is True
    target = Path(watch.data_dir) / 'restock_diff.json'
    target.write_text(json.dumps({'restock_diff': {'in_stock_processing': 'ignore', 'keep': None}}),
                      encoding='utf-8')

    datastore.update_30()

    result = json.loads(target.read_text(encoding='utf-8'))
    assert result['restock_diff'] == {
        'in_stock_processing': 'ignore', 'keep': None,
        'follow_price_changes': False, 'minimum_price_change': 0,
    }
    assert 'restock_settings' not in watch
    datastore.update_30()
    assert json.loads(target.read_text(encoding='utf-8')) == result


def test_restock_migration_write_failure_preserves_legacy_settings(datastore, tmp_path, monkeypatch):
    import importlib

    updates_module = importlib.import_module('changedetectionio.store.file_saving_datastore')
    watch_uuid = datastore.add_watch(url='https://example.com/restock-migration-failure')
    watch = datastore.data['watching'][watch_uuid]
    watch['processor'] = 'restock_diff'
    watch['restock_settings'] = {'follow_price_changes': True}
    assert watch.commit() is True
    previous_watch_json = (Path(watch.data_dir) / 'watch.json').read_bytes()

    def fail_atomic_save(*_args, **_kwargs):
        raise OSError('simulated processor config write failure')

    monkeypatch.setattr(updates_module, 'save_json_atomic', fail_atomic_save)
    with pytest.raises(OSError, match='simulated processor config write failure'):
        datastore.update_30()

    assert watch['restock_settings'] == {'follow_price_changes': True}
    assert (Path(watch.data_dir) / 'watch.json').read_bytes() == previous_watch_json
    assert not (Path(watch.data_dir) / 'restock_diff.json').exists()


def test_watch_result_transaction_persists_metadata_snapshot_and_index(datastore, tmp_path):
    watch_uuid = datastore.add_watch(url='https://example.com/result-transaction')
    watch, token = datastore.begin_watch_check(watch_uuid, 123)
    assert datastore.apply_watch_result(
        watch_uuid, watch, token,
        {'previous_md5': 'new-md5', 'last_checked': 123, 'check_count': 1},
        'new snapshot', '123', 'snapshot-123',
    ) is True

    assert watch['previous_md5'] == 'new-md5'
    assert watch.get_history_snapshot(timestamp='123') == 'new snapshot'
    assert list(watch.history) == ['123']
    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        loaded = reloaded.data['watching'][watch_uuid]
        assert loaded['previous_md5'] == 'new-md5'
        assert list(loaded.history) == ['123']
        assert loaded.get_history_snapshot(timestamp='123') == 'new snapshot'
    finally:
        reloaded.stop_thread = True


@pytest.mark.parametrize('failure', ['snapshot', 'index', 'metadata'])
def test_watch_result_transaction_rolls_back_required_write_failure(datastore, tmp_path, monkeypatch, failure):
    watch_uuid = datastore.add_watch(url=f'https://example.com/result-failure-{failure}')
    watch, token = datastore.begin_watch_check(watch_uuid, 123)
    original_commit = watch.commit
    if failure == 'snapshot':
        monkeypatch.setattr(watch, 'save_history_blob', lambda **_kwargs: False)
    elif failure == 'index':
        monkeypatch.setattr(watch, 'append_history_index_entry',
                            lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError('index failure')))
    else:
        def fail_metadata_commit():
            if watch._watch_result_transaction_id:
                return False
            return original_commit()
        monkeypatch.setattr(watch, 'commit', fail_metadata_commit)

    assert datastore.apply_watch_result(
        watch_uuid, watch, token,
        {'previous_md5': 'must-not-persist', 'last_checked': 123},
        'snapshot', '123', 'snapshot-123',
    ) is False
    assert watch.get('previous_md5') != 'must-not-persist'
    assert list(watch.history) == []
    assert not list(Path(watch.data_dir).glob('snapshot-123.*'))
    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        loaded = reloaded.data['watching'][watch_uuid]
        assert loaded.get('previous_md5') != 'must-not-persist'
        assert list(loaded.history) == []
        assert not list(Path(loaded.data_dir).glob('snapshot-123.*'))
    finally:
        reloaded.stop_thread = True


def test_watch_result_transaction_recovers_after_metadata_marker_commit(datastore, tmp_path, monkeypatch):
    watch_uuid = datastore.add_watch(url='https://example.com/result-crash-recovery')
    watch, token = datastore.begin_watch_check(watch_uuid, 123)
    original_commit = watch.commit

    def crash_after_result_metadata_commit():
        saved = original_commit()
        if saved and watch._watch_result_transaction_id:
            raise SystemExit('simulated result transaction crash')
        return saved

    monkeypatch.setattr(watch, 'commit', crash_after_result_metadata_commit)
    with pytest.raises(SystemExit, match='simulated result transaction crash'):
        datastore.apply_watch_result(
            watch_uuid, watch, token,
            {'previous_md5': 'crash-md5', 'last_checked': 123},
            'crash snapshot', '123', 'snapshot-123',
        )
    monkeypatch.undo()
    datastore.stop_thread = True

    recovered = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        loaded = recovered.data['watching'][watch_uuid]
        assert loaded['previous_md5'] == 'crash-md5'
        assert list(loaded.history) == ['123']
        assert loaded.get_history_snapshot(timestamp='123') == 'crash snapshot'
        assert loaded._watch_result_transaction_id is None
        assert not list(Path(tmp_path).glob('.cdio-tombstone-result-*'))
    finally:
        recovered.stop_thread = True


def test_remove_password_reports_failed_settings_persistence_and_restores_memory(datastore, monkeypatch):
    application = datastore.data['settings']['application']
    application['password'] = 'existing-hash'
    monkeypatch.setattr(datastore, 'commit', lambda: False)

    assert datastore.remove_password() is False
    assert application['password'] == 'existing-hash'


def _watch_backup_zip(watch_uuid, url):
    import io
    import zipfile

    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as stream:
        stream.writestr(f'{watch_uuid}/watch.json', json.dumps({'uuid': watch_uuid, 'url': url}))
    archive.seek(0)
    return archive


def _tag_backup_zip(tag_uuid, title):
    import io
    import zipfile

    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as stream:
        stream.writestr(f'{tag_uuid}/tag.json', json.dumps({'uuid': tag_uuid, 'title': title}))
    archive.seek(0)
    return archive


def test_backup_restore_publishes_watch_and_survives_reload(datastore, tmp_path):
    import uuid
    from changedetectionio.blueprint.backups.restore import import_from_zip

    watch_uuid = str(uuid.uuid4())
    result = import_from_zip(
        _watch_backup_zip(watch_uuid, 'https://example.com/restored'), datastore,
        include_groups=False, include_groups_replace=False,
        include_watches=True, include_watches_replace=True,
    )
    assert result['restored_watches'] == 1
    assert result['settings_persisted'] is True
    assert datastore.data['watching'][watch_uuid]['url'] == 'https://example.com/restored'
    datastore.stop_thread = True

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert reloaded.data['watching'][watch_uuid]['url'] == 'https://example.com/restored'
    finally:
        reloaded.stop_thread = True


def test_backup_restore_failed_watch_commit_retains_previous_entity(datastore, tmp_path, monkeypatch):
    import uuid
    from changedetectionio.blueprint.backups.restore import import_from_zip
    from changedetectionio.model.Watch import model as WatchModel

    watch_uuid = datastore.add_watch(url='https://example.com/original')
    previous = datastore.data['watching'][watch_uuid]
    previous_dir = Path(previous.data_dir)
    original_commit = WatchModel.commit

    def fail_imported_watch(self):
        if self.get('uuid') == watch_uuid and self is not previous:
            return False
        return original_commit(self)

    monkeypatch.setattr(WatchModel, 'commit', fail_imported_watch)
    result = import_from_zip(
        _watch_backup_zip(watch_uuid, 'https://example.com/replacement'), datastore,
        include_groups=False, include_groups_replace=False,
        include_watches=True, include_watches_replace=True,
    )
    assert result['restored_watches'] == 0
    assert result['skipped_watches'] == 1
    assert datastore.data['watching'][watch_uuid] is previous
    assert previous['url'] == 'https://example.com/original'
    assert Path(previous.data_dir) == previous_dir
    assert json.loads((previous_dir / 'watch.json').read_text(encoding='utf-8'))['url'] == (
        'https://example.com/original'
    )


def test_backup_restore_copy_failure_retains_previous_watch_and_tag(datastore, tmp_path, monkeypatch):
    import io
    import uuid
    import zipfile
    import changedetectionio.blueprint.backups.restore as restore_module

    watch_uuid = datastore.add_watch(url='https://example.com/original')
    old_watch = datastore.data['watching'][watch_uuid]
    old_watch_json = (Path(old_watch.data_dir) / 'watch.json').read_bytes()
    tag_uuid = datastore.add_tag('Original tag')
    old_tag = datastore.data['settings']['application']['tags'][tag_uuid]
    old_tag_json = (Path(old_tag.data_dir) / 'tag.json').read_bytes()

    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as stream:
        stream.writestr(f'{watch_uuid}/watch.json', json.dumps({'uuid': watch_uuid, 'url': 'https://replacement'}))
        stream.writestr(f'{tag_uuid}/tag.json', json.dumps({'uuid': tag_uuid, 'title': 'Replacement tag'}))
    archive.seek(0)
    original_copytree = restore_module.shutil.copytree

    def fail_staging_copy(source, destination, *args, **kwargs):
        if '.cdio-restore-stage-' in str(destination):
            raise OSError('simulated staging copy failure')
        return original_copytree(source, destination, *args, **kwargs)

    monkeypatch.setattr(restore_module.shutil, 'copytree', fail_staging_copy)
    result = restore_module.import_from_zip(
        archive, datastore,
        include_groups=True, include_groups_replace=True,
        include_watches=True, include_watches_replace=True,
    )

    assert result['restored_watches'] == 0
    assert result['restored_groups'] == 0
    assert datastore.data['watching'][watch_uuid] is old_watch
    assert datastore.data['settings']['application']['tags'][tag_uuid] is old_tag
    assert (Path(old_watch.data_dir) / 'watch.json').read_bytes() == old_watch_json
    assert (Path(old_tag.data_dir) / 'tag.json').read_bytes() == old_tag_json


@pytest.mark.parametrize('kind', ['watch', 'tag'])
def test_backup_restore_publication_failure_restores_previous_directory(datastore, tmp_path, monkeypatch, kind):
    import changedetectionio.blueprint.backups.restore as restore_module

    if kind == 'watch':
        entity_uuid = datastore.add_watch(url='https://example.com/original-publication')
        previous = datastore.data['watching'][entity_uuid]
        archive = _watch_backup_zip(entity_uuid, 'https://example.com/replacement-publication')
        previous_path = Path(previous.data_dir) / 'watch.json'
        collection = datastore.data['watching']
    else:
        entity_uuid = datastore.add_tag('Original publication tag')
        previous = datastore.data['settings']['application']['tags'][entity_uuid]
        archive = _tag_backup_zip(entity_uuid, 'Replacement publication tag')
        previous_path = Path(previous.data_dir) / 'tag.json'
        collection = datastore.data['settings']['application']['tags']
    original_disk = previous_path.read_bytes()
    real_replace = restore_module.os.replace
    failed_once = False

    def fail_publication_once(source, destination):
        nonlocal failed_once
        source_path, destination_path = Path(source), Path(destination)
        is_publication = (source_path.parent.name.startswith('.cdio-restore-stage-')
                          and destination_path.parent == Path(datastore.datastore_path))
        if is_publication and not failed_once:
            failed_once = True
            raise OSError('simulated restore publication failure')
        return real_replace(source, destination)

    monkeypatch.setattr(restore_module.os, 'replace', fail_publication_once)
    from changedetectionio.blueprint.backups.restore import import_from_zip
    result = import_from_zip(
        archive, datastore,
        include_groups=True, include_groups_replace=True,
        include_watches=True, include_watches_replace=True,
    )
    assert failed_once
    assert result['restored_watches' if kind == 'watch' else 'restored_groups'] == 0
    assert collection[entity_uuid] is previous
    assert previous_path.read_bytes() == original_disk
    assert not list(Path(tmp_path).glob('.cdio-tombstone-restore-*'))


def test_backup_restore_failed_tag_commit_retains_previous_entity(datastore, tmp_path, monkeypatch):
    from changedetectionio.blueprint.backups.restore import import_from_zip
    from changedetectionio.model.Tag import model as TagModel

    tag_uuid = datastore.add_tag('Original tag commit failure')
    previous = datastore.data['settings']['application']['tags'][tag_uuid]
    previous_path = Path(previous.data_dir) / 'tag.json'
    previous_json = previous_path.read_bytes()
    original_commit = TagModel.commit

    def fail_imported_tag(self):
        if self.get('uuid') == tag_uuid and self is not previous:
            return False
        return original_commit(self)

    monkeypatch.setattr(TagModel, 'commit', fail_imported_tag)
    result = import_from_zip(
        _tag_backup_zip(tag_uuid, 'Replacement tag commit failure'), datastore,
        include_groups=True, include_groups_replace=True,
        include_watches=False, include_watches_replace=False,
    )
    assert result['restored_groups'] == 0
    assert result['skipped_groups'] == 1
    assert datastore.data['settings']['application']['tags'][tag_uuid] is previous
    assert previous_path.read_bytes() == previous_json


def test_backup_restore_journal_write_failure_keeps_previous_entity(datastore, tmp_path, monkeypatch):
    import changedetectionio.blueprint.backups.restore as restore_module
    from changedetectionio.blueprint.backups.restore import import_from_zip

    watch_uuid = datastore.add_watch(url='https://example.com/restore-manifest-failure')
    previous = datastore.data['watching'][watch_uuid]
    previous_path = Path(previous.data_dir) / 'watch.json'
    previous_json = previous_path.read_bytes()

    def fail_journal(*_args, **_kwargs):
        raise OSError('simulated restore manifest write failure')

    monkeypatch.setattr(restore_module, '_write_restore_manifest', fail_journal)
    result = import_from_zip(
        _watch_backup_zip(watch_uuid, 'https://example.com/should-not-publish'), datastore,
        include_groups=False, include_groups_replace=False,
        include_watches=True, include_watches_replace=True,
    )
    assert result['restored_watches'] == 0
    assert result['skipped_watches'] == 1
    assert datastore.data['watching'][watch_uuid] is previous
    assert previous_path.read_bytes() == previous_json
    assert not list(Path(tmp_path).glob('.cdio-tombstone-restore-*'))


def test_backup_restore_interruption_before_publication_recovers_previous_directory(datastore, tmp_path, monkeypatch):
    import changedetectionio.blueprint.backups.restore as restore_module
    from changedetectionio.blueprint.backups.restore import import_from_zip

    watch_uuid = datastore.add_watch(url='https://example.com/original-before-crash')
    previous = datastore.data['watching'][watch_uuid]
    previous_json = (Path(previous.data_dir) / 'watch.json').read_bytes()
    real_replace = restore_module.os.replace

    def crash_after_old_directory_move(source, destination):
        source_path, destination_path = Path(source), Path(destination)
        moved = (source_path == Path(datastore.data['watching'][watch_uuid].data_dir)
                 and destination_path.name == 'previous'
                 and destination_path.parent.name.startswith('.cdio-tombstone-restore-watch-'))
        result = real_replace(source, destination)
        if moved:
            raise SystemExit('simulated process interruption after preserving old watch')
        return result

    monkeypatch.setattr(restore_module.os, 'replace', crash_after_old_directory_move)
    with pytest.raises(SystemExit, match='simulated process interruption'):
        import_from_zip(
            _watch_backup_zip(watch_uuid, 'https://example.com/replacement-before-crash'), datastore,
            include_groups=False, include_groups_replace=False,
            include_watches=True, include_watches_replace=True,
        )
    monkeypatch.undo()
    datastore.stop_thread = True

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        loaded = reloaded.data['watching'][watch_uuid]
        assert loaded['url'] == 'https://example.com/original-before-crash'
        assert (Path(loaded.data_dir) / 'watch.json').read_bytes() == previous_json
        assert not list(Path(tmp_path).glob('.cdio-tombstone-restore-watch-*'))
    finally:
        reloaded.stop_thread = True


def test_backup_restore_interruption_after_publication_keeps_both_copies_and_loads_destination(
        datastore, tmp_path, monkeypatch):
    import changedetectionio.blueprint.backups.restore as restore_module
    from changedetectionio.blueprint.backups.restore import import_from_zip

    watch_uuid = datastore.add_watch(url='https://example.com/original-after-publish-crash')
    old_watch = datastore.data['watching'][watch_uuid]
    real_replace = restore_module.os.replace

    def crash_after_staged_directory_is_published(source, destination):
        result = real_replace(source, destination)
        source_path, destination_path = Path(source), Path(destination)
        if (source_path.parent.name.startswith('.cdio-restore-stage-watch-')
                and destination_path == Path(datastore.datastore_path) / watch_uuid):
            raise SystemExit('simulated interruption after restore publication')
        return result

    monkeypatch.setattr(restore_module.os, 'replace', crash_after_staged_directory_is_published)
    with pytest.raises(SystemExit, match='simulated interruption'):
        import_from_zip(
            _watch_backup_zip(watch_uuid, 'https://example.com/replacement-after-publish-crash'), datastore,
            include_groups=False, include_groups_replace=False,
            include_watches=True, include_watches_replace=True,
        )
    monkeypatch.undo()
    datastore.stop_thread = True

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        loaded = reloaded.data['watching'][watch_uuid]
        assert loaded['url'] == 'https://example.com/replacement-after-publish-crash'
        restore_tombstones = list(Path(tmp_path).glob('.cdio-tombstone-restore-watch-*'))
        assert len(restore_tombstones) == 1
        previous = json.loads((restore_tombstones[0] / 'previous' / 'watch.json').read_text(encoding='utf-8'))
        assert previous['url'] == 'https://example.com/original-after-publish-crash'
        assert old_watch['url'] == 'https://example.com/original-after-publish-crash'
        # Prepared state is ambiguous but repeated recovery is safe and idempotent.
        reloaded._recover_restore_transactions()
        assert reloaded.data['watching'][watch_uuid] is loaded
        assert (restore_tombstones[0] / 'previous' / 'watch.json').is_file()
    finally:
        reloaded.stop_thread = True


def test_ui_url_import_is_durable_before_reporting_success(datastore, tmp_path):
    from changedetectionio.blueprint.imports.importer import import_url_list

    importer = import_url_list()
    flashes = []
    importer.run(
        'https://example.com/one\nhttps://example.com/two',
        lambda message, category=None: flashes.append((message, category)),
        datastore,
    )
    assert len(importer.new_uuids) == 2
    assert all((Path(datastore.data['watching'][watch_uuid].data_dir) / 'watch.json').is_file()
               for watch_uuid in importer.new_uuids)
    datastore.stop_thread = True

    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert set(importer.new_uuids).issubset(reloaded.data['watching'])
    finally:
        reloaded.stop_thread = True


def test_all_four_ui_import_formats_survive_immediate_datastore_reload(datastore, tmp_path):
    import io
    from openpyxl import Workbook
    from changedetectionio.blueprint.imports.importer import (
        import_distill_io_json, import_url_list, import_xlsx_custom, import_xlsx_wachete,
    )

    flashes = []
    flash = lambda message, category=None: flashes.append((message, category))
    imported = []

    url_importer = import_url_list()
    url_importer.run('https://example.com/import-url-list', flash, datastore)
    imported.extend(url_importer.new_uuids)

    distill_config = {'selections': [{'frames': [{'includes': [], 'excludes': []}]}]}
    distill_importer = import_distill_io_json()
    distill_importer.run(json.dumps({'data': [{
        'uri': 'https://example.com/import-distill', 'config': json.dumps(distill_config), 'tags': [],
    }]}), flash, datastore)
    imported.extend(distill_importer.new_uuids)

    wachete_book = Workbook()
    wachete_book.active.append(['URL', 'Name'])
    wachete_book.active.append(['https://example.com/import-wachete', 'Wachete import'])
    wachete_data = io.BytesIO()
    wachete_book.save(wachete_data)
    wachete_data.seek(0)
    wachete_importer = import_xlsx_wachete()
    wachete_importer.run(wachete_data, flash, datastore)
    imported.extend(wachete_importer.new_uuids)

    custom_book = Workbook()
    custom_book.active.append(['https://example.com/import-custom'])
    custom_data = io.BytesIO()
    custom_book.save(custom_data)
    custom_data.seek(0)
    custom_importer = import_xlsx_custom()
    custom_importer.import_profile = {1: 'url'}
    custom_importer.run(custom_data, flash, datastore)
    imported.extend(custom_importer.new_uuids)

    assert len(imported) == 4
    assert all((Path(datastore.data['watching'][watch_uuid].data_dir) / 'watch.json').is_file()
               for watch_uuid in imported)
    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert set(imported).issubset(reloaded.data['watching'])
    finally:
        reloaded.stop_thread = True


@pytest.mark.parametrize('urls, expected_count', [
    ('https://example.com/import-good\nhttps://example.com/import-bad', 1),
    ('https://example.com/import-total-failure', 0),
])
def test_ui_url_import_counts_only_durably_saved_watches(datastore, tmp_path, monkeypatch, urls, expected_count):
    from changedetectionio.blueprint.imports.importer import import_url_list
    from changedetectionio.model.Watch import model as WatchModel

    original_commit = WatchModel._commit_locked

    def fail_selected_watch(self, *args, **kwargs):
        url = self.get('url', '')
        if 'import-bad' in url or 'total-failure' in url:
            return False
        return original_commit(self, *args, **kwargs)

    monkeypatch.setattr(WatchModel, '_commit_locked', fail_selected_watch)
    importer = import_url_list()
    importer.run(urls, lambda *_args: None, datastore)

    assert len(importer.new_uuids) == expected_count
    assert len(datastore.data['watching']) == expected_count
    persisted = {p.name for p in Path(tmp_path).iterdir()
                 if p.is_dir() and (p / 'watch.json').is_file()}
    assert set(importer.new_uuids) == persisted


def test_worker_restart_threshold_does_not_suppress_cancellation(datastore, monkeypatch):
    import asyncio
    import importlib
    import queue
    from types import SimpleNamespace

    worker_module = importlib.import_module('changedetectionio.worker')
    processors_module = importlib.import_module('changedetectionio.processors')
    watch_uuid = datastore.add_watch(url='https://example.com/worker-cancellation')
    fetch_started = asyncio.Event()

    class FakeFetcher:
        async def quit(self, **_kwargs):
            return None

        def clear_content(self):
            return None

    class FakeHandler:
        fetcher = FakeFetcher()

        async def call_browser(self):
            fetch_started.set()
            await asyncio.Event().wait()

    fake_processor = SimpleNamespace(perform_site_check=lambda **_kwargs: FakeHandler())
    queue_item = SimpleNamespace(priority=1, item={'uuid': watch_uuid})

    class FakeQueue:
        async def async_get(self, **_kwargs):
            return queue_item

    app = SimpleNamespace(config=SimpleNamespace(exit=threading.Event()))
    monkeypatch.setenv('WORKER_MAX_JOBS', '1')
    monkeypatch.setenv('WORKER_MAX_RUNTIME', '3600')
    monkeypatch.setattr(processors_module, 'get_processor_module', lambda _name: fake_processor)
    monkeypatch.setattr(worker_module.worker_pool, 'claim_uuid_for_processing', lambda *_args: True)
    monkeypatch.setattr(worker_module.worker_pool, 'release_uuid_from_processing', lambda *_args, **_kw: None)
    monkeypatch.setattr(worker_module, 'apply_update_handler_alter', lambda handler, *_args: handler)
    monkeypatch.setattr(worker_module, 'apply_update_finalize', lambda **_kwargs: None)

    async def run_worker_and_cancel():
        task = asyncio.create_task(worker_module.async_update_worker(
            worker_id=999, q=FakeQueue(), notification_q=queue.Queue(),
            app=app, datastore=datastore, executor=None,
        ))
        assert await asyncio.wait_for(fetch_started.wait(), timeout=3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)

    asyncio.run(run_worker_and_cancel())


@pytest.mark.parametrize('fail_snapshot', [False, True])
def test_actual_worker_commits_result_before_notifying(datastore, tmp_path, monkeypatch, fail_snapshot):
    import asyncio
    import importlib
    import queue
    from types import SimpleNamespace

    worker_module = importlib.import_module('changedetectionio.worker')
    processors_module = importlib.import_module('changedetectionio.processors')
    watch_uuid = datastore.add_watch(url='https://example.com/worker-end-to-end')
    watch = datastore.data['watching'][watch_uuid]
    assert watch.save_history_blob('previous content', 100, 'previous-md5') is not False
    watch['previous_md5'] = 'previous-md5'
    watch['check_count'] = 1
    assert watch.commit() is True
    original_save_snapshot = watch.save_history_blob
    if fail_snapshot:
        monkeypatch.setattr(watch, 'save_history_blob', lambda **_kwargs: False)

    class FakeFetcher:
        content = '<html><title>New title</title><body>new content</body></html>'
        raw_content = None
        screenshot = None
        xpath_data = None
        favicon_blob = None

        async def quit(self, **_kwargs):
            return None

        def clear_content(self):
            return None

        def get_all_headers(self):
            return {'content-type': 'text/html', 'server': 'test-server'}

    class FakeHandler:
        fetcher = FakeFetcher()
        screenshot = None
        xpath_data = None

        async def call_browser(self):
            return None

        def run_changedetection(self, watch):
            return True, {'previous_md5': 'new-md5'}, 'new content'

    fake_processor = SimpleNamespace(perform_site_check=lambda **_kwargs: FakeHandler())
    item = SimpleNamespace(priority=1, item={'uuid': watch_uuid})

    class FakeQueue:
        async def async_get(self, **_kwargs):
            return item

    app = SimpleNamespace(config=SimpleNamespace(exit=threading.Event()))
    notifications = []
    async def send_notification(uuid, *_args):
        notifications.append(uuid)

    monkeypatch.setenv('WORKER_MAX_JOBS', '1')
    monkeypatch.setenv('WORKER_MAX_RUNTIME', '3600')
    monkeypatch.setattr(processors_module, 'get_processor_module', lambda _name: fake_processor)
    monkeypatch.setattr(worker_module.worker_pool, 'claim_uuid_for_processing', lambda *_args: True)
    monkeypatch.setattr(worker_module.worker_pool, 'release_uuid_from_processing', lambda *_args, **_kw: None)
    monkeypatch.setattr(worker_module, 'apply_update_handler_alter', lambda handler, *_args: handler)
    monkeypatch.setattr(worker_module, 'apply_update_finalize', lambda **_kwargs: None)
    monkeypatch.setattr(worker_module, 'send_content_changed_notification', send_notification)

    result = asyncio.run(worker_module.async_update_worker(
        worker_id=1000, q=FakeQueue(), notification_q=queue.Queue(),
        app=app, datastore=datastore, executor=None,
    ))

    assert result == 'restart'
    if fail_snapshot:
        monkeypatch.setattr(watch, 'save_history_blob', original_save_snapshot)
        assert notifications == []
        assert watch['previous_md5'] == 'previous-md5'
        assert list(watch.history) == ['100']
    else:
        assert notifications == [watch_uuid]
        assert watch['previous_md5'] == 'new-md5'
        assert len(watch.history) == 2

    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        loaded = reloaded.data['watching'][watch_uuid]
        if fail_snapshot:
            assert loaded['previous_md5'] == 'previous-md5'
            assert list(loaded.history) == ['100']
        else:
            assert loaded['previous_md5'] == 'new-md5'
            assert len(loaded.history) == 2
            assert loaded.get_history_snapshot(timestamp=list(loaded.history)[-1]) == 'new content'
    finally:
        reloaded.stop_thread = True


def test_worker_result_started_before_history_clear_is_discarded(datastore, tmp_path):
    from changedetectionio.model.Watch import bind_current_check_result, reset_current_check_result

    watch_uuid = datastore.add_watch(url='https://example.com/worker-clear-token')
    watch, token = datastore.begin_watch_check(watch_uuid, 200)
    assert watch is not None
    check_context = bind_current_check_result(watch, token)
    try:
        assert datastore.clear_watch_history(watch_uuid) is True
        assert datastore.begin_watch_result_application(watch_uuid, watch, token) is False
        assert watch.save_history_blob('stale worker result', 200, 'stale-worker') is False
    finally:
        reset_current_check_result(check_context)
    assert not (Path(watch.data_dir) / 'stale-worker.txt').exists()

    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        loaded = reloaded.data['watching'][watch_uuid]
        assert loaded['last_checked'] == 0
        assert list(loaded.history) == []
        assert not (Path(loaded.data_dir) / 'stale-worker.txt').exists()
    finally:
        reloaded.stop_thread = True


def test_worker_result_waiting_before_application_is_invalidated_by_clear(datastore, tmp_path):
    watch_uuid = datastore.add_watch(url='https://example.com/worker-clear-race')
    watch, token = datastore.begin_watch_check(watch_uuid, 200)
    ready = threading.Event()
    apply_result = threading.Event()
    result = {}

    def worker_finishes_fetch():
        ready.set()
        assert apply_result.wait(5)
        result['applied'] = datastore.begin_watch_result_application(watch_uuid, watch, token)
        if result['applied']:
            try:
                watch.save_history_blob('stale worker result', 200, 'stale-worker')
            finally:
                datastore.end_watch_result_application(watch)

    worker = threading.Thread(target=worker_finishes_fetch)
    worker.start()
    assert ready.wait(5)
    assert datastore.clear_watch_history(watch_uuid) is True
    apply_result.set()
    worker.join(5)

    assert not worker.is_alive()
    assert result['applied'] is False
    assert not (Path(watch.data_dir) / 'stale-worker.txt').exists()
    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert list(reloaded.data['watching'][watch_uuid].history) == []
    finally:
        reloaded.stop_thread = True


def test_worker_result_is_rejected_after_delete_or_same_uuid_replacement(datastore, tmp_path):
    deleted_uuid = datastore.add_watch(url='https://example.com/worker-deleted')
    deleted_watch, deleted_token = datastore.begin_watch_check(deleted_uuid, 200)
    deleted_dir = Path(deleted_watch.data_dir)
    assert datastore.delete(deleted_uuid)
    assert datastore.begin_watch_result_application(deleted_uuid, deleted_watch, deleted_token) is False
    assert deleted_watch.save_history_blob('stale', 200, 'deleted-worker') is False
    assert not deleted_dir.exists()

    replaced_uuid = datastore.add_watch(url='https://example.com/worker-replaced')
    old_watch, old_token = datastore.begin_watch_check(replaced_uuid, 300)
    replacement = datastore.rehydrate_entity(replaced_uuid, dict(old_watch))
    replacement['title'] = 'replacement after fetch started'
    with datastore.lock:
        datastore.data['watching'][replaced_uuid] = replacement
    assert replacement.commit() is True
    assert old_watch.save_history_blob('stale', 300, 'stale-worker') is False
    assert datastore.begin_watch_result_application(replaced_uuid, old_watch, old_token) is False
    assert list(replacement.history) == []

    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert deleted_uuid not in reloaded.data['watching']
        loaded_replacement = reloaded.data['watching'][replaced_uuid]
        assert loaded_replacement['title'] == 'replacement after fetch started'
        assert list(loaded_replacement.history) == []
        assert not (Path(loaded_replacement.data_dir) / 'stale-worker.txt').exists()
    finally:
        reloaded.stop_thread = True


def test_newer_worker_result_invalidates_older_token_and_persists_snapshot(datastore, tmp_path):
    watch_uuid = datastore.add_watch(url='https://example.com/worker-newer-check')
    watch, old_token = datastore.begin_watch_check(watch_uuid, 100)
    same_watch, new_token = datastore.begin_watch_check(watch_uuid, 200)
    assert same_watch is watch
    assert datastore.begin_watch_result_application(watch_uuid, watch, old_token) is False
    assert datastore.begin_watch_result_application(watch_uuid, watch, new_token) is True
    try:
        assert watch.save_history_blob('newer result', 200, 'newer-result') is not False
    finally:
        datastore.end_watch_result_application(watch)

    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        loaded = reloaded.data['watching'][watch_uuid]
        assert list(loaded.history) == ['200']
        assert loaded.get_history_snapshot(timestamp='200') == 'newer result'
    finally:
        reloaded.stop_thread = True


def test_watch_pickle_round_trip_initializes_lock_before_item_restoration():
    from multiprocessing.reduction import ForkingPickler
    from changedetectionio.model import Watch

    watch = Watch.model(datastore_path='unused', default={
        'uuid': 'pickle-watch',
        'url': 'https://example.com/pickle',
        'processor': 'restock_diff',
        'restock': {'price': {'current': 12.5}},
        'history_metadata': {'count': 3, 'last': '123'},
    }, __datastore={'settings': {'application': {}}})
    watch._datastore = {'must_not_be_pickled': object()}
    watch._datastore_owner = object()
    original_lock = watch.mutation_lock

    for serialized in (pickle.dumps(watch), bytes(ForkingPickler.dumps(watch))):
        restored = pickle.loads(serialized)
        assert restored['restock']['price'] == {'current': 12.5}
        assert restored['history_metadata'] == {'count': 3, 'last': '123'}
        assert not hasattr(restored, '_datastore')
        assert not hasattr(restored, '_datastore_owner')
        assert restored.mutation_lock is not original_lock
        with restored.mutation_lock:
            restored['lock_ready'] = True
        assert restored['lock_ready'] is True


def test_tag_reference_creation_and_deletion_share_a_serialization_boundary(datastore, tmp_path):
    tag_uuid = datastore.add_tag('Concurrent tag')
    watch_uuid = datastore.add_watch(url='https://example.com/tag-concurrency')
    mutation_ready = threading.Event()
    allow_commit = threading.Event()
    delete_started = threading.Event()
    mutation_result = {}
    delete_result = {}

    def add_tag_reference(watch):
        watch.update({'tags': [tag_uuid]})
        mutation_ready.set()
        assert allow_commit.wait(5)
        return True

    mutation = threading.Thread(
        target=lambda: mutation_result.setdefault('saved', datastore.mutate_watch(watch_uuid, add_tag_reference))
    )
    mutation.start()
    assert mutation_ready.wait(5)

    def delete_tag():
        delete_started.set()
        delete_result['deleted'] = datastore.delete_tag(tag_uuid)

    deletion = threading.Thread(target=delete_tag)
    deletion.start()
    assert delete_started.wait(5)
    assert 'deleted' not in delete_result
    allow_commit.set()
    mutation.join(5)
    deletion.join(5)

    assert not mutation.is_alive() and not deletion.is_alive()
    assert mutation_result['saved'] is True
    assert delete_result['deleted'] is False
    assert datastore.data['watching'][watch_uuid]['tags'] == [tag_uuid]
    watch_disk = json.loads((Path(datastore.data['watching'][watch_uuid].data_dir) / 'watch.json').read_text())
    assert watch_disk['tags'] == [tag_uuid]

    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert tag_uuid in reloaded.data['settings']['application']['tags']
        assert reloaded.data['watching'][watch_uuid]['tags'] == [tag_uuid]
    finally:
        reloaded.stop_thread = True


def test_tag_delete_wins_before_bulk_reference_additions_and_leaves_no_dangling_refs(
        datastore, tmp_path, monkeypatch):
    import importlib

    tags_module = importlib.import_module('changedetectionio.model.Tags')
    tag_uuid = datastore.add_tag('Tag deleted before bulk add')
    watch_uuids = [
        datastore.add_watch(url='https://example.com/tag-bulk-one'),
        datastore.add_watch(url='https://example.com/tag-bulk-two'),
    ]
    stage_started = threading.Event()
    finish_deletion = threading.Event()
    bulk_attempted = threading.Event()
    delete_result = {}
    bulk_result = {}
    tags = datastore.data['settings']['application']['tags']
    real_stage = tags.stage_directory_deletion

    def pause_after_reference_inspection(uuid):
        stage_started.set()
        assert finish_deletion.wait(5)
        return real_stage(uuid)

    monkeypatch.setattr(tags, 'stage_directory_deletion', pause_after_reference_inspection)
    deletion = threading.Thread(target=lambda: delete_result.setdefault('deleted', datastore.delete_tag(tag_uuid)))
    deletion.start()
    assert stage_started.wait(5)

    def bulk_assign_tags():
        bulk_attempted.set()
        bulk_result['results'] = [
            datastore.mutate_watch(uuid, lambda watch: watch.update({'tags': [tag_uuid]}))
            for uuid in watch_uuids
        ]

    bulk = threading.Thread(target=bulk_assign_tags)
    bulk.start()
    assert bulk_attempted.wait(5)
    assert 'results' not in bulk_result
    finish_deletion.set()
    deletion.join(5)
    bulk.join(5)

    assert not deletion.is_alive() and not bulk.is_alive()
    assert delete_result['deleted'] is True
    assert bulk_result['results'] == [False, False]
    assert tag_uuid not in datastore.data['settings']['application']['tags']
    for uuid in watch_uuids:
        watch = datastore.data['watching'][uuid]
        assert tag_uuid not in watch['tags']
        disk = json.loads((Path(watch.data_dir) / 'watch.json').read_text(encoding='utf-8'))
        assert tag_uuid not in disk['tags']

    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert tag_uuid not in reloaded.data['settings']['application']['tags']
        for uuid in watch_uuids:
            assert tag_uuid not in reloaded.data['watching'][uuid]['tags']
    finally:
        reloaded.stop_thread = True


def test_same_uuid_replacement_tag_cannot_be_deleted_by_stale_reference(datastore, tmp_path):
    from changedetectionio.model import Tag

    tag_uuid = datastore.add_tag('Original tag object')
    original = datastore.data['settings']['application']['tags'][tag_uuid]
    replacement = Tag.model(
        datastore_path=datastore.datastore_path,
        __datastore=datastore.data,
        __datastore_owner=datastore,
        default=dict(original),
    )
    replacement['title'] = 'Replacement tag object'
    with datastore.lock:
        datastore.data['settings']['application']['tags'][tag_uuid] = replacement
    assert replacement.commit() is True

    assert datastore.delete_tag(tag_uuid, expected_tag=original) is False
    assert datastore.data['settings']['application']['tags'][tag_uuid] is replacement
    assert json.loads((Path(replacement.data_dir) / 'tag.json').read_text(encoding='utf-8'))['title'] == (
        'Replacement tag object'
    )

    datastore.stop_thread = True
    reloaded = ChangeDetectionStore(datastore_path=str(tmp_path), include_default_watches=False)
    try:
        assert reloaded.data['settings']['application']['tags'][tag_uuid]['title'] == 'Replacement tag object'
    finally:
        reloaded.stop_thread = True
