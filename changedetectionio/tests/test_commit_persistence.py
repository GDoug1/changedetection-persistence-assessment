#!/usr/bin/env python3
"""
Tests for immediate commit-based persistence system.

Tests cover:
- Watch.commit() persistence to disk
- Concurrent commit safety (race conditions)
- Processor config separation
- Data loss prevention (settings, tags, watch modifications)
"""

import json
import os
import threading
import time
from pathlib import Path
from flask import url_for
from .util import wait_for_all_checks


# ==============================================================================
# 2. Commit() Persistence Tests
# ==============================================================================

def test_watch_commit_persists_to_disk(client, live_server):
    """Test that watch.commit() actually writes to watch.json immediately"""
    datastore = client.application.config.get('DATASTORE')

    # Create a watch
    uuid = datastore.add_watch(url='http://example.com', extras={'title': 'Original Title'})
    watch = datastore.data['watching'][uuid]

    # Modify and commit
    watch['title'] = 'Modified Title'
    watch['paused'] = True
    watch.commit()

    # Read directly from disk (bypass datastore cache)
    watch_json_path = os.path.join(watch.data_dir, 'watch.json')
    assert os.path.exists(watch_json_path), "watch.json should exist on disk"

    with open(watch_json_path, 'r') as f:
        disk_data = json.load(f)

    assert disk_data['title'] == 'Modified Title', "Title should be persisted to disk"
    assert disk_data['paused'] == True, "Paused state should be persisted to disk"
    assert disk_data['uuid'] == uuid, "UUID should match"


def test_watch_commit_survives_reload(client, live_server):
    """Test that committed changes survive datastore reload"""
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config.get('DATASTORE')
    datastore_path = datastore.datastore_path

    # Create and modify a watch
    uuid = datastore.add_watch(url='http://example.com', extras={'title': 'Test Watch'})
    watch = datastore.data['watching'][uuid]
    watch['title'] = 'Persisted Title'
    watch['paused'] = True
    watch['tags'] = ['tag-1', 'tag-2']
    watch.commit()

    # Simulate app restart - create new datastore instance
    datastore2 = ChangeDetectionStore(datastore_path=datastore_path)
    datastore2.reload_state(
        datastore_path=datastore_path,
        include_default_watches=False,
        version_tag='test'
    )

    # Check data survived
    assert uuid in datastore2.data['watching'], "Watch should exist after reload"
    reloaded_watch = datastore2.data['watching'][uuid]
    assert reloaded_watch['title'] == 'Persisted Title', "Title should survive reload"
    assert reloaded_watch['paused'] == True, "Paused state should survive reload"
    assert reloaded_watch['tags'] == ['tag-1', 'tag-2'], "Tags should survive reload"


def test_watch_commit_atomic_on_crash(client, live_server):
    """Test that atomic writes prevent corruption (temp file pattern)"""
    datastore = client.application.config.get('DATASTORE')

    uuid = datastore.add_watch(url='http://example.com', extras={'title': 'Original'})
    watch = datastore.data['watching'][uuid]

    # First successful commit
    watch['title'] = 'First Save'
    watch.commit()

    # Verify watch.json exists and is valid
    watch_json_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_json_path, 'r') as f:
        data = json.load(f)  # Should not raise JSONDecodeError
        assert data['title'] == 'First Save'

    # Second commit - even if interrupted, original file should be intact
    # (atomic write uses temp file + rename, so original is never corrupted)
    watch['title'] = 'Second Save'
    watch.commit()

    with open(watch_json_path, 'r') as f:
        data = json.load(f)
        assert data['title'] == 'Second Save'


def test_multiple_watches_commit_independently(client, live_server):
    """Test that committing one watch doesn't affect others"""
    datastore = client.application.config.get('DATASTORE')

    # Create multiple watches
    uuid1 = datastore.add_watch(url='http://example1.com', extras={'title': 'Watch 1'})
    uuid2 = datastore.add_watch(url='http://example2.com', extras={'title': 'Watch 2'})
    uuid3 = datastore.add_watch(url='http://example3.com', extras={'title': 'Watch 3'})

    watch1 = datastore.data['watching'][uuid1]
    watch2 = datastore.data['watching'][uuid2]
    watch3 = datastore.data['watching'][uuid3]

    # Modify and commit only watch2
    watch2['title'] = 'Modified Watch 2'
    watch2['paused'] = True
    watch2.commit()

    # Read all from disk
    def read_watch_json(uuid):
        watch = datastore.data['watching'][uuid]
        path = os.path.join(watch.data_dir, 'watch.json')
        with open(path, 'r') as f:
            return json.load(f)

    data1 = read_watch_json(uuid1)
    data2 = read_watch_json(uuid2)
    data3 = read_watch_json(uuid3)

    # Only watch2 should have changes
    assert data1['title'] == 'Watch 1', "Watch 1 should be unchanged"
    assert data1['paused'] == False, "Watch 1 should not be paused"

    assert data2['title'] == 'Modified Watch 2', "Watch 2 should be modified"
    assert data2['paused'] == True, "Watch 2 should be paused"

    assert data3['title'] == 'Watch 3', "Watch 3 should be unchanged"
    assert data3['paused'] == False, "Watch 3 should not be paused"


# ==============================================================================
# 3. Concurrency/Race Condition Tests
# ==============================================================================

def test_concurrent_watch_commits_dont_corrupt(client, live_server):
    """Test that simultaneous commits to same watch don't corrupt JSON"""
    datastore = client.application.config.get('DATASTORE')

    uuid = datastore.add_watch(url='http://example.com', extras={'title': 'Test'})
    watch = datastore.data['watching'][uuid]

    errors = []

    def modify_and_commit(field, value):
        try:
            watch[field] = value
            watch.commit()
        except Exception as e:
            errors.append(e)

    # Run 10 concurrent commits
    threads = []
    for i in range(10):
        t = threading.Thread(target=modify_and_commit, args=('title', f'Title {i}'))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Should not have any errors
    assert len(errors) == 0, f"Expected no errors, got: {errors}"

    # JSON file should still be valid (not corrupted)
    watch_json_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_json_path, 'r') as f:
        data = json.load(f)  # Should not raise JSONDecodeError
        assert data['uuid'] == uuid, "UUID should still be correct"
        assert 'Title' in data['title'], "Title should contain 'Title'"


def test_concurrent_modifications_during_commit(client, live_server):
    """Test that modifying watch during commit doesn't cause RuntimeError"""
    datastore = client.application.config.get('DATASTORE')

    uuid = datastore.add_watch(url='http://example.com', extras={'title': 'Test'})
    watch = datastore.data['watching'][uuid]

    errors = []
    stop_flag = threading.Event()

    def keep_modifying():
        """Continuously modify watch"""
        try:
            i = 0
            while not stop_flag.is_set():
                watch['title'] = f'Title {i}'
                watch['paused'] = i % 2 == 0
                i += 1
                time.sleep(0.001)
        except Exception as e:
            errors.append(('modifier', e))

    def keep_committing():
        """Continuously commit watch"""
        try:
            for _ in range(20):
                watch.commit()
                time.sleep(0.005)
        except Exception as e:
            errors.append(('committer', e))

    # Start concurrent modification and commits
    modifier = threading.Thread(target=keep_modifying)
    committer = threading.Thread(target=keep_committing)

    modifier.start()
    committer.start()

    committer.join()
    stop_flag.set()
    modifier.join()

    # Should not have RuntimeError from dict changing during iteration
    runtime_errors = [e for source, e in errors if isinstance(e, RuntimeError)]
    assert len(runtime_errors) == 0, f"Should not have RuntimeError, got: {runtime_errors}"


def test_datastore_lock_protects_commit_snapshot(client, live_server):
    """Test that datastore.lock prevents race conditions during deepcopy"""
    datastore = client.application.config.get('DATASTORE')

    uuid = datastore.add_watch(url='http://example.com', extras={'title': 'Test'})
    watch = datastore.data['watching'][uuid]

    # Add some complex nested data
    watch['browser_steps'] = [
        {'operation': 'click', 'selector': '#foo'},
        {'operation': 'wait', 'seconds': 5}
    ]

    errors = []
    commits_succeeded = [0]

    def rapid_commits():
        try:
            for i in range(50):
                watch['title'] = f'Title {i}'
                watch.commit()
                commits_succeeded[0] += 1
                time.sleep(0.001)
        except Exception as e:
            errors.append(e)

    # Multiple threads doing rapid commits
    threads = [threading.Thread(target=rapid_commits) for _ in range(3)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Expected no errors, got: {errors}"
    assert commits_succeeded[0] == 150, f"Expected 150 commits, got {commits_succeeded[0]}"

    # Final JSON should be valid
    watch_json_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_json_path, 'r') as f:
        data = json.load(f)
        assert data['uuid'] == uuid


# ==============================================================================
# 4. Processor Config Separation Tests
# ==============================================================================

def test_processor_config_never_in_watch_json(client, live_server):
    """Test that processor_config_* fields are filtered out of watch.json"""
    datastore = client.application.config.get('DATASTORE')

    uuid = datastore.add_watch(
        url='http://example.com',
        extras={
            'title': 'Test Watch',
            'processor': 'restock_diff'
        }
    )

    watch = datastore.data['watching'][uuid]

    # Try to set processor config fields (these should be filtered during commit)
    watch['processor_config_price_threshold'] = 10.0
    watch['processor_config_some_setting'] = 'value'
    watch['processor_config_another'] = {'nested': 'data'}
    watch.commit()

    # Read watch.json from disk
    watch_json_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_json_path, 'r') as f:
        data = json.load(f)

    # Verify processor_config_* fields are NOT in watch.json
    for key in data.keys():
        assert not key.startswith('processor_config_'), \
            f"Found {key} in watch.json - processor configs should be in separate file!"

    # Normal fields should still be there
    assert data['title'] == 'Test Watch'
    assert data['processor'] == 'restock_diff'


def test_api_post_saves_processor_config_separately(client, live_server):
    """Test that API POST saves processor configs to {processor}.json"""
    import json
    from changedetectionio.processors import extract_processor_config_from_form_data

    # Get API key
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    # Create watch via API with processor config
    response = client.post(
        url_for("createwatch"),
        data=json.dumps({
            'url': 'http://example.com',
            'processor': 'restock_diff',
            'processor_config_price_threshold': 10.0,
            'processor_config_in_stock_only': True
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key}
    )

    assert response.status_code in (200, 201), f"Expected 200/201, got {response.status_code}"
    uuid = response.json.get('uuid')
    assert uuid, "Should return UUID"

    datastore = client.application.config.get('DATASTORE')
    watch = datastore.data['watching'][uuid]

    # Check that processor config file exists
    processor_config_path = os.path.join(watch.data_dir, 'restock_diff.json')
    assert os.path.exists(processor_config_path), "Processor config file should exist"

    with open(processor_config_path, 'r') as f:
        config = json.load(f)

    # Verify fields are saved WITHOUT processor_config_ prefix
    assert config.get('price_threshold') == 10.0, "Should have price_threshold (no prefix)"
    assert config.get('in_stock_only') == True, "Should have in_stock_only (no prefix)"
    assert 'processor_config_price_threshold' not in config, "Should NOT have prefixed keys"


def test_api_put_saves_processor_config_separately(client, live_server):
    """Test that API PUT updates processor configs in {processor}.json"""
    import json
    datastore = client.application.config.get('DATASTORE')

    # Get API key
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    # Create watch
    uuid = datastore.add_watch(
        url='http://example.com',
        extras={'processor': 'restock_diff'}
    )

    # Update via API with processor config
    response = client.put(
        url_for("watch", uuid=uuid),
        data=json.dumps({
            'processor_config_price_threshold': 15.0,
            'processor_config_min_stock': 5
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key}
    )

    # PUT might return different status codes, 200 or 204 are both OK
    assert response.status_code in (200, 204), f"Expected 200/204, got {response.status_code}: {response.data}"

    watch = datastore.data['watching'][uuid]

    # Check processor config file
    processor_config_path = os.path.join(watch.data_dir, 'restock_diff.json')
    assert os.path.exists(processor_config_path), "Processor config file should exist"

    with open(processor_config_path, 'r') as f:
        config = json.load(f)

    assert config.get('price_threshold') == 15.0, "Should have updated price_threshold"
    assert config.get('min_stock') == 5, "Should have min_stock"


def test_ui_edit_saves_processor_config_separately(client, live_server):
    """Test that processor_config_* fields never appear in watch.json (even from UI)"""
    datastore = client.application.config.get('DATASTORE')

    # Create watch
    uuid = datastore.add_watch(
        url='http://example.com',
        extras={'processor': 'text_json_diff', 'title': 'Test'}
    )

    watch = datastore.data['watching'][uuid]

    # Simulate someone accidentally trying to set processor_config fields directly
    watch['processor_config_should_not_save'] = 'test_value'
    watch['processor_config_another_field'] = 123
    watch['normal_field'] = 'this_should_save'
    watch.commit()

    # Check watch.json has NO processor_config_* fields (main point of this test)
    watch_json_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_json_path, 'r') as f:
        watch_data = json.load(f)

    for key in watch_data.keys():
        assert not key.startswith('processor_config_'), \
            f"Found {key} in watch.json - processor configs should be filtered during commit"

    # Verify normal fields still save
    assert watch_data['normal_field'] == 'this_should_save', "Normal fields should save"
    assert watch_data['title'] == 'Test', "Original fields should still be there"


def test_browser_steps_normalized_to_empty_list(client, live_server):
    """Test that meaningless browser_steps are normalized to [] during commit"""
    datastore = client.application.config.get('DATASTORE')

    uuid = datastore.add_watch(url='http://example.com')
    watch = datastore.data['watching'][uuid]

    # Set browser_steps to meaningless values
    watch['browser_steps'] = [
        {'operation': 'Choose one', 'selector': ''},
        {'operation': 'Goto site', 'selector': ''},
        {'operation': '', 'selector': '#foo'}
    ]
    watch.commit()

    # Read from disk
    watch_json_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_json_path, 'r') as f:
        data = json.load(f)

    # Should be normalized to empty list
    assert data['browser_steps'] == [], "Meaningless browser_steps should be normalized to []"


# ==============================================================================
# 5. Data Loss Prevention Tests
# ==============================================================================

def test_settings_persist_after_update(client, live_server):
    """Test that settings updates are committed and survive restart"""
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config.get('DATASTORE')
    datastore_path = datastore.datastore_path

    # Update settings directly (bypass form validation issues)
    datastore.data['settings']['application']['empty_pages_are_a_change'] = True
    datastore.data['settings']['application']['fetch_backend'] = 'html_requests'
    datastore.data['settings']['requests']['time_between_check']['minutes'] = 120
    datastore.commit()

    # Simulate restart
    datastore2 = ChangeDetectionStore(datastore_path=datastore_path)
    datastore2.reload_state(
        datastore_path=datastore_path,
        include_default_watches=False,
        version_tag='test'
    )

    # Verify settings survived
    assert datastore2.data['settings']['application']['empty_pages_are_a_change'] == True, "empty_pages_are_a_change should persist"
    assert datastore2.data['settings']['application']['fetch_backend'] == 'html_requests', "fetch_backend should persist"
    assert datastore2.data['settings']['requests']['time_between_check']['minutes'] == 120, "time_between_check should persist"


def test_tag_mute_persists(client, live_server):
    """Test that tag mute/unmute operations persist"""
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config.get('DATASTORE')
    datastore_path = datastore.datastore_path

    # Add a tag
    tag_uuid = datastore.add_tag('Test Tag')

    # Mute the tag
    response = client.post(url_for("tags.mute", uuid=tag_uuid))
    assert response.status_code == 302  # Redirect

    # Verify muted in memory
    assert datastore.data['settings']['application']['tags'][tag_uuid]['notification_muted'] == True

    # Simulate restart
    datastore2 = ChangeDetectionStore(datastore_path=datastore_path)
    datastore2.reload_state(
        datastore_path=datastore_path,
        include_default_watches=False,
        version_tag='test'
    )

    # Verify mute state survived
    assert tag_uuid in datastore2.data['settings']['application']['tags']
    assert datastore2.data['settings']['application']['tags'][tag_uuid]['notification_muted'] == True


def test_tag_delete_removes_from_watches(client, live_server):
    """Test that deleting a tag removes it from all watches"""
    datastore = client.application.config.get('DATASTORE')

    # Create a tag
    tag_uuid = datastore.add_tag('Test Tag')

    # Create watches with this tag
    uuid1 = datastore.add_watch(url='http://example1.com')
    uuid2 = datastore.add_watch(url='http://example2.com')
    uuid3 = datastore.add_watch(url='http://example3.com')

    watch1 = datastore.data['watching'][uuid1]
    watch2 = datastore.data['watching'][uuid2]
    watch3 = datastore.data['watching'][uuid3]

    watch1['tags'] = [tag_uuid]
    watch1.commit()
    watch2['tags'] = [tag_uuid, 'other-tag']
    watch2.commit()
    # watch3 has no tags

    # Delete the tag
    response = client.post(url_for("tags.delete", uuid=tag_uuid))
    assert response.status_code == 302

    # Wait for background thread to complete
    time.sleep(1)

    # Tag should be removed from settings
    assert tag_uuid not in datastore.data['settings']['application']['tags']

    # Tag should be removed from watches and persisted
    def check_watch_tags(uuid):
        watch = datastore.data['watching'][uuid]
        watch_json_path = os.path.join(watch.data_dir, 'watch.json')
        with open(watch_json_path, 'r') as f:
            return json.load(f)['tags']

    assert tag_uuid not in check_watch_tags(uuid1), "Tag should be removed from watch1"
    assert tag_uuid not in check_watch_tags(uuid2), "Tag should be removed from watch2"
    assert 'other-tag' in check_watch_tags(uuid2), "Other tags should remain in watch2"
    assert check_watch_tags(uuid3) == [], "Watch3 should still have empty tags"


def test_watch_pause_unpause_persists(client, live_server):
    """Test that pause/unpause operations commit and persist"""
    datastore = client.application.config.get('DATASTORE')

    # Get API key
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    uuid = datastore.add_watch(url='http://example.com')
    watch = datastore.data['watching'][uuid]

    # Pause via API
    response = client.get(url_for("watch", uuid=uuid, paused='paused'), headers={'x-api-key': api_key})
    assert response.status_code == 200

    # Check persisted to disk
    watch_json_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_json_path, 'r') as f:
        data = json.load(f)
    assert data['paused'] == True, "Pause should be persisted"

    # Unpause
    response = client.get(url_for("watch", uuid=uuid, paused='unpaused'), headers={'x-api-key': api_key})
    assert response.status_code == 200

    with open(watch_json_path, 'r') as f:
        data = json.load(f)
    assert data['paused'] == False, "Unpause should be persisted"


def test_watch_mute_unmute_persists(client, live_server):
    """Test that mute/unmute operations commit and persist"""
    datastore = client.application.config.get('DATASTORE')

    # Get API key
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    uuid = datastore.add_watch(url='http://example.com')
    watch = datastore.data['watching'][uuid]

    # Mute via API
    response = client.get(url_for("watch", uuid=uuid, muted='muted'), headers={'x-api-key': api_key})
    assert response.status_code == 200

    # Check persisted to disk
    watch_json_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_json_path, 'r') as f:
        data = json.load(f)
    assert data['notification_muted'] == True, "Mute should be persisted"

    # Unmute
    response = client.get(url_for("watch", uuid=uuid, muted='unmuted'), headers={'x-api-key': api_key})
    assert response.status_code == 200

    with open(watch_json_path, 'r') as f:
        data = json.load(f)
    assert data['notification_muted'] == False, "Unmute should be persisted"


def test_ui_watch_edit_persists_all_fields(client, live_server):
    """Test that UI watch edit form persists all modified fields"""
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config.get('DATASTORE')
    datastore_path = datastore.datastore_path

    # Create watch
    uuid = datastore.add_watch(url='http://example.com')

    # Edit via UI with multiple field changes
    response = client.post(
        url_for("ui.ui_edit.edit_page", uuid=uuid),
        data={
            'url': 'http://updated-example.com',
            'title': 'Updated Watch Title',
            'time_between_check-hours': '2',
            'time_between_check-minutes': '30',
            'include_filters': '#content',
            'fetch_backend': 'html_requests',
            'method': 'POST',
            'ignore_text': 'Advertisement\nTracking'
        },
        follow_redirects=True
    )

    assert b"Updated watch" in response.data or b"Saved" in response.data

    # Simulate restart
    datastore2 = ChangeDetectionStore(datastore_path=datastore_path)
    datastore2.reload_state(
        datastore_path=datastore_path,
        include_default_watches=False,
        version_tag='test'
    )

    # Verify all fields survived
    watch = datastore2.data['watching'][uuid]
    assert watch['url'] == 'http://updated-example.com'
    assert watch['title'] == 'Updated Watch Title'
    assert watch['time_between_check']['hours'] == 2
    assert watch['time_between_check']['minutes'] == 30
    assert watch['fetch_backend'] == 'html_requests'
    assert watch['method'] == 'POST'


def test_ui_watch_edit_does_not_report_success_after_watch_write_failure(client, live_server, monkeypatch):
    from changedetectionio import processors
    from changedetectionio.store import file_saving_datastore

    datastore = client.application.config['DATASTORE']
    test_url = url_for('test_endpoint', _external=True)
    uuid = datastore.add_watch(
        url=test_url,
        extras={'processor': 'restock_diff', 'title': 'Original title', 'paused': True},
    )
    watch = datastore.data['watching'][uuid]
    assert processors.save_processor_config(datastore, uuid, {
        'restock_diff': {'follow_price_changes': True}
    })
    watch_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_path, 'rb') as watch_file:
        original_watch = watch_file.read()
    real_save_entity_atomic = file_saving_datastore.save_entity_atomic

    def fail_watch_save(*args, **kwargs):
        if kwargs.get('entity_type') == 'watch':
            raise OSError('simulated watch write failure')
        return real_save_entity_atomic(*args, **kwargs)

    monkeypatch.setattr(file_saving_datastore, 'save_entity_atomic', fail_watch_save)
    response = client.post(
        url_for('ui.ui_edit.edit_page', uuid=uuid),
        data={
            'url': test_url,
            'title': 'Unsaved title',
            'processor': 'restock_diff',
            'processor_config_restock_diff-follow_price_changes': '',
            'tags': '',
            'headers': '',
            'fetch_backend': 'html_requests',
            'time_between_check_use_default': 'y',
        },
        follow_redirects=True,
    )

    assert b'Processor configuration was saved' in response.data
    assert b'Updated watch.' not in response.data
    assert datastore.data['watching'][uuid]['title'] == 'Original title'
    with open(watch_path, 'rb') as watch_file:
        assert watch_file.read() == original_watch
    with open(os.path.join(watch.data_dir, 'restock_diff.json'), encoding='utf-8') as config_file:
        assert json.load(config_file)['restock_diff']['follow_price_changes'] is False


def test_ui_watch_edit_does_not_report_success_after_processor_config_failure(client, live_server, monkeypatch):
    from changedetectionio.store import file_saving_datastore

    datastore = client.application.config['DATASTORE']
    test_url = url_for('test_endpoint', _external=True)
    uuid = datastore.add_watch(
        url=test_url,
        extras={'processor': 'restock_diff', 'title': 'Original title', 'paused': True},
    )
    watch = datastore.data['watching'][uuid]
    from changedetectionio.processors import save_processor_config
    assert save_processor_config(datastore, uuid, {
        'restock_diff': {'follow_price_changes': True}
    })
    watch_path = os.path.join(watch.data_dir, 'watch.json')
    config_path = os.path.join(watch.data_dir, 'restock_diff.json')
    with open(watch_path, 'rb') as watch_file:
        original_watch = watch_file.read()
    with open(config_path, 'rb') as config_file:
        original_config = config_file.read()
    real_replace = file_saving_datastore.os.replace

    def fail_processor_replace(source, destination):
        if os.path.basename(destination) == 'restock_diff.json':
            raise OSError('simulated processor config replacement failure')
        return real_replace(source, destination)

    monkeypatch.setattr(file_saving_datastore.os, 'replace', fail_processor_replace)
    response = client.post(
        url_for('ui.ui_edit.edit_page', uuid=uuid),
        data={
            'url': test_url,
            'title': 'Unsaved title',
            'processor': 'restock_diff',
            'processor_config_restock_diff-follow_price_changes': '',
            'tags': '',
            'headers': '',
            'fetch_backend': 'html_requests',
            'time_between_check_use_default': 'y',
        },
        follow_redirects=True,
    )

    assert b'Could not save processor configuration' in response.data
    assert b'Updated watch.' not in response.data
    assert datastore.data['watching'][uuid]['title'] == 'Original title'
    with open(watch_path, 'rb') as watch_file:
        assert watch_file.read() == original_watch
    with open(config_path, 'rb') as config_file:
        assert config_file.read() == original_config


def test_ui_failed_url_update_preserves_favicon_and_edit_flag(client, live_server, monkeypatch):
    from changedetectionio.store import file_saving_datastore

    datastore = client.application.config['DATASTORE']
    old_url = url_for('test_endpoint', _external=True)
    uuid = datastore.add_watch(url=old_url, extras={'title': 'Original title', 'paused': True})
    watch = datastore.data['watching'][uuid]
    watch.reset_watch_edited_flag()
    favicon_path = os.path.join(watch.data_dir, 'favicon.ico')
    favicon_bytes = b'cached favicon bytes'
    with open(favicon_path, 'wb') as favicon_file:
        favicon_file.write(favicon_bytes)
    watch_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_path, 'rb') as watch_file:
        original_watch_bytes = watch_file.read()

    real_save_entity_atomic = file_saving_datastore.save_entity_atomic

    def fail_watch_save(*args, **kwargs):
        if kwargs.get('entity_type') == 'watch':
            raise OSError('simulated watch write failure')
        return real_save_entity_atomic(*args, **kwargs)

    monkeypatch.setattr(file_saving_datastore, 'save_entity_atomic', fail_watch_save)
    response = client.post(
        url_for('ui.ui_edit.edit_page', uuid=uuid),
        data={
            'url': 'https://example.com/new-product',
            'title': 'Unsaved title',
            'tags': '',
            'headers': '',
            'fetch_backend': 'html_requests',
            'time_between_check_use_default': 'y',
        },
        follow_redirects=True,
    )

    assert b'Watch settings could not be saved' in response.data
    assert datastore.data['watching'][uuid]['url'] == old_url
    assert datastore.data['watching'][uuid].was_edited is False
    assert datastore.data['watching'][uuid]._pending_favicon_clear is False
    assert open(favicon_path, 'rb').read() == favicon_bytes
    with open(watch_path, 'rb') as watch_file:
        assert watch_file.read() == original_watch_bytes


def test_ui_edit_rejects_watch_deleted_before_save_lock(client, live_server):
    """A stale edit form must report deletion without recreating watch data."""
    import sys
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    datastore_path = datastore.datastore_path
    app = client.application
    watch_uuid = datastore.add_watch(
        url=url_for('test_endpoint', _external=True),
        extras={'title': 'Before delete', 'paused': True},
    )
    test_url = url_for('test_endpoint', _external=True)
    watch = datastore.data['watching'][watch_uuid]
    watch_dir = watch.data_dir
    at_watch_lock = threading.Event()
    continue_request = threading.Event()
    request_thread_name = 'stale-ui-edit'

    class RequestGateRLock:
        def __init__(self):
            self.lock = threading.RLock()
            self.request_acquisitions = 0

        def __enter__(self):
            # Deepcopy re-enters this lock through Watch.__setitem__; gate only the route's
            # lock contexts so the deletion lands before the POST save lock is acquired.
            if (threading.current_thread().name == request_thread_name
                    and sys._getframe(1).f_code.co_name == 'edit_page'):
                self.request_acquisitions += 1
                if self.request_acquisitions == 2:
                    at_watch_lock.set()
                    if not continue_request.wait(10):
                        raise AssertionError('test did not release stale UI edit')
            self.lock.acquire()
            return self

        def __exit__(self, *_args):
            self.lock.release()

    watch._mutation_lock = RequestGateRLock()
    endpoint = url_for('ui.ui_edit.edit_page', uuid=watch_uuid)
    responses = {}

    def submit_edit():
        with app.test_client() as thread_client:
            responses['response'] = thread_client.post(
                endpoint,
                data={
                    'url': test_url,
                    'title': 'Stale UI update',
                    'processor': 'text_json_diff',
                    'tags': '',
                    'headers': '',
                    'fetch_backend': 'html_requests',
                    'time_between_check_use_default': 'y',
                },
            )

    request_thread = threading.Thread(target=submit_edit, name=request_thread_name)
    request_thread.start()
    assert at_watch_lock.wait(10), 'UI edit did not reach the per-watch lock'

    datastore.delete(watch_uuid)
    assert watch_uuid not in datastore.data['watching']
    assert not os.path.exists(watch_dir)

    continue_request.set()
    request_thread.join(10)
    assert not request_thread.is_alive(), 'stale UI edit did not finish'
    response = responses['response']
    assert response.status_code == 302
    assert b'Updated watch' not in response.data
    assert watch_uuid not in datastore.data['watching']
    assert not os.path.exists(watch_dir)

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert watch_uuid not in reloaded.data['watching']
    finally:
        reloaded.stop_thread = True


def test_tag_api_put_and_mute_report_write_failure_and_restore_state(client, datastore_path, monkeypatch):
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    tag_uuid = datastore.add_tag('Persistent tag')
    tag = datastore.data['settings']['application']['tags'][tag_uuid]
    original_title = tag['title']
    tag_path = os.path.join(tag.data_dir, 'tag.json')
    original_bytes = open(tag_path, 'rb').read()

    def fail_save(*_args, **_kwargs):
        raise OSError('simulated tag write failure')

    monkeypatch.setattr(tag, '_save_to_disk', fail_save)

    response = client.put(
        url_for('tag', uuid=tag_uuid),
        data=json.dumps({'title': 'Unsaved title'}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert response.status_code == 500
    assert tag['title'] == original_title
    assert open(tag_path, 'rb').read() == original_bytes

    response = client.get(
        url_for('tag', uuid=tag_uuid, muted='muted'),
        headers={'x-api-key': api_key},
    )
    assert response.status_code == 500
    assert tag['notification_muted'] is False
    assert open(tag_path, 'rb').read() == original_bytes

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        restored_tag = reloaded.data['settings']['application']['tags'][tag_uuid]
        assert restored_tag['title'] == original_title
        assert restored_tag['notification_muted'] is False
    finally:
        reloaded.stop_thread = True


def test_ui_edit_redirects_if_watch_is_deleted_after_initial_snapshot(client, monkeypatch):
    import changedetectionio.processors as processors

    datastore = client.application.config['DATASTORE']
    watch_uuid = datastore.add_watch(
        url='https://example.com/ui-delete-after-snapshot',
        extras={'paused': True},
    )
    watch_dir = datastore.data['watching'][watch_uuid].data_dir
    snapshot_ready = threading.Event()
    resume_request = threading.Event()
    real_find_processors = processors.find_processors

    def gate_after_snapshot():
        snapshot_ready.set()
        assert resume_request.wait(5), 'test did not release UI edit after snapshot'
        return real_find_processors()

    monkeypatch.setattr(processors, 'find_processors', gate_after_snapshot)
    app = client.application
    response_holder = {}
    endpoint = url_for('ui.ui_edit.edit_page', uuid=watch_uuid)

    def request_edit():
        with app.test_client() as thread_client:
            response_holder['response'] = thread_client.get(endpoint, follow_redirects=True)

    request_thread = threading.Thread(target=request_edit)
    request_thread.start()
    assert snapshot_ready.wait(5), 'UI edit did not pass its initial snapshot'
    datastore.delete(watch_uuid)
    resume_request.set()
    request_thread.join(10)

    assert not request_thread.is_alive()
    response = response_holder['response']
    assert response.status_code == 200
    assert b'This watch no longer exists' in response.data
    assert watch_uuid not in datastore.data['watching']
    assert not os.path.exists(watch_dir)


def test_ui_edit_redirects_if_watch_is_replaced_before_later_lookup(client, datastore_path, monkeypatch):
    import changedetectionio.processors as processors
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    watch_uuid = datastore.add_watch(
        url='https://example.com/ui-replace-after-snapshot',
        extras={'paused': True, 'title': 'Original'},
    )
    old_watch = datastore.data['watching'][watch_uuid]
    replacement = datastore.rehydrate_entity(watch_uuid, dict(old_watch))
    snapshot_ready = threading.Event()
    resume_request = threading.Event()
    real_find_processors = processors.find_processors

    def gate_after_snapshot():
        snapshot_ready.set()
        assert resume_request.wait(5), 'test did not release UI edit after snapshot'
        return real_find_processors()

    monkeypatch.setattr(processors, 'find_processors', gate_after_snapshot)
    app = client.application
    response_holder = {}
    endpoint = url_for('ui.ui_edit.edit_page', uuid=watch_uuid)

    def request_edit():
        with app.test_client() as thread_client:
            response_holder['response'] = thread_client.get(endpoint, follow_redirects=True)

    request_thread = threading.Thread(target=request_edit)
    request_thread.start()
    assert snapshot_ready.wait(5), 'UI edit did not pass its initial snapshot'
    replacement['title'] = 'Replacement'
    with datastore.lock:
        datastore.data['watching'][watch_uuid] = replacement
    assert replacement.commit() is True
    resume_request.set()
    request_thread.join(10)

    assert not request_thread.is_alive()
    response = response_holder['response']
    assert response.status_code == 200
    assert b'This watch no longer exists' in response.data

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert reloaded.data['watching'][watch_uuid]['title'] == 'Replacement'
    finally:
        reloaded.stop_thread = True


def test_ui_edit_reports_deletion_after_commit_before_response(client, monkeypatch):
    import changedetectionio.blueprint.browser_steps as browser_steps

    datastore = client.application.config['DATASTORE']
    watch_uuid = datastore.add_watch(
        url='https://example.com/ui-delete-after-commit',
        extras={'paused': True, 'title': 'Before'},
    )
    watch_dir = datastore.data['watching'][watch_uuid].data_dir
    commit_finished = threading.Event()
    resume_request = threading.Event()
    response_holder = {}

    def pause_before_response(_uuid):
        commit_finished.set()
        assert resume_request.wait(5), 'test did not release response after commit'

    monkeypatch.setattr(browser_steps, 'cleanup_session_for_watch', pause_before_response)
    app = client.application
    endpoint = url_for('ui.ui_edit.edit_page', uuid=watch_uuid)

    def post_edit():
        with app.test_client() as thread_client:
            response_holder['response'] = thread_client.post(
                endpoint,
                data={
                    'url': 'https://example.com/ui-delete-after-commit',
                    'title': 'Saved before deletion',
                    'tags': '',
                    'headers': '',
                    'fetch_backend': 'html_requests',
                    'time_between_check_use_default': 'y',
                },
                follow_redirects=True,
            )

    request_thread = threading.Thread(target=post_edit)
    request_thread.start()
    assert commit_finished.wait(10), 'UI edit did not reach post-commit response preparation'
    assert os.path.isfile(os.path.join(watch_dir, 'watch.json'))
    datastore.delete(watch_uuid)
    resume_request.set()
    request_thread.join(10)

    assert not request_thread.is_alive()
    response = response_holder['response']
    assert response.status_code == 200
    assert b'This watch was deleted before the edit could finish' in response.data
    assert b'Updated watch.' not in response.data
    assert watch_uuid not in datastore.data['watching']
    assert not os.path.exists(watch_dir)


def test_ui_clear_history_does_not_flash_success_when_commit_fails(client, datastore_path, monkeypatch):
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    watch_uuid = datastore.add_watch(
        url='https://example.com/ui-history-clear-failure',
        extras={'paused': True, 'title': 'Saved title', 'check_count': 4},
    )
    watch = datastore.data['watching'][watch_uuid]
    watch_dir = Path(watch.data_dir)
    history_path = watch_dir / 'history.txt'
    snapshot_path = watch_dir / '123.txt'
    history_path.write_bytes(b'123,123.txt\n')
    snapshot_path.write_bytes(b'previous snapshot')
    original_watch_json = (watch_dir / 'watch.json').read_bytes()

    def fail_save(*_args, **_kwargs):
        raise OSError('simulated watch commit failure')

    monkeypatch.setattr(watch, '_save_to_disk', fail_save)

    response = client.post(
        url_for('ui.clear_watch_history', uuid=watch_uuid),
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b'Could not clear snapshot history' in response.data
    assert b'Cleared snapshot history for watch' not in response.data
    assert history_path.read_bytes() == b'123,123.txt\n'
    assert snapshot_path.read_bytes() == b'previous snapshot'
    assert (watch_dir / 'watch.json').read_bytes() == original_watch_json

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert reloaded.data['watching'][watch_uuid]['title'] == 'Saved title'
        assert (Path(reloaded.data['watching'][watch_uuid].data_dir) / '123.txt').read_bytes() == b'previous snapshot'
    finally:
        reloaded.stop_thread = True


def test_bulk_clear_history_reports_failed_watches(client, monkeypatch):
    from threading import Event
    from changedetectionio.blueprint import ui as ui_module

    datastore = client.application.config['DATASTORE']
    first_uuid = datastore.add_watch(url='https://example.com/bulk-history-clear-first')
    failed_uuid = datastore.add_watch(url='https://example.com/bulk-history-clear-failed')
    calls = []
    completed = Event()
    captured_logs = []
    original_clear = datastore.clear_watch_history
    original_error = ui_module.logger.error

    def partial_clear(uuid, *args, **kwargs):
        calls.append(uuid)
        result = uuid != failed_uuid
        if len(calls) == 2:
            completed.set()
        if result:
            return original_clear(uuid, *args, **kwargs)
        return False

    def capture_error(message, *args, **kwargs):
        if 'Background: Cleared history for' in str(message):
            captured_logs.append(str(message))
        return original_error(message, *args, **kwargs)

    monkeypatch.setattr(datastore, 'clear_watch_history', partial_clear)
    monkeypatch.setattr(ui_module.logger, 'error', capture_error)

    response = client.post(
        url_for('ui.clear_all_history'),
        data={'confirmtext': 'clear'},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b'History clearing started in background' in response.data
    assert completed.wait(5), 'bulk clear worker did not finish processing both watches'
    for _ in range(50):
        if captured_logs:
            break
        time.sleep(0.01)
    assert len(calls) == 2
    assert captured_logs
    assert '1/2' in captured_logs[0]
    assert failed_uuid in captured_logs[0]
    assert first_uuid in calls


def test_processor_switch_does_not_report_success_when_history_clear_fails(client, datastore_path, monkeypatch):
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    watch_uuid = datastore.add_watch(
        url='https://example.com/processor-switch-clear-failure',
        extras={'paused': True, 'processor': 'text_json_diff'},
    )
    watch = datastore.data['watching'][watch_uuid]
    watch_dir = Path(watch.data_dir)
    history_path = watch_dir / 'history.txt'
    history_path.write_bytes(b'123,123.txt\n')
    (watch_dir / '123.txt').write_bytes(b'previous snapshot')
    original_watch_json = (watch_dir / 'watch.json').read_bytes()

    def fail_save(*_args, **_kwargs):
        raise OSError('simulated processor switch persistence failure')

    monkeypatch.setattr(watch, '_save_to_disk', fail_save)

    response = client.get(
        url_for('ui.ui_edit.edit_page', uuid=watch_uuid, switch_processor='restock_diff'),
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b'Could not save the processor change or clear its history' in response.data
    assert b'Switched to mode' not in response.data
    assert watch['processor'] == 'text_json_diff'
    assert history_path.read_bytes() == b'123,123.txt\n'
    assert (watch_dir / '123.txt').read_bytes() == b'previous snapshot'
    assert (watch_dir / 'watch.json').read_bytes() == original_watch_json

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert reloaded.data['watching'][watch_uuid]['processor'] == 'text_json_diff'
        assert (Path(reloaded.data['watching'][watch_uuid].data_dir) / '123.txt').read_bytes() == b'previous snapshot'
    finally:
        reloaded.stop_thread = True
