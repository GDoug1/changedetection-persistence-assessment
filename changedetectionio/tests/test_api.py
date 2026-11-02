#!/usr/bin/env python3

import time
import threading
from flask import url_for
from .util import live_server_setup, wait_for_all_checks, delete_all_watches
import os

import json
import uuid


def set_original_response(datastore_path):
    test_return_data = """<html>
       <body>
     Some initial text<br>
     <p>Which is across multiple lines</p>
     <br>
     So let's see what happens.  <br>
     <div id="sametext">Some text thats the same</div>
     <div id="changetext">Some text that will change</div>
     </body>
     </html>
    """

    with open(os.path.join(datastore_path, "endpoint-content.txt"), "w") as f:
        f.write(test_return_data)
    return None


def set_modified_response(datastore_path):
    test_return_data = """<html>
       <body>
     Some initial text<br>
     <p>which has this one new line</p>
     <br>
     So let's see what happens.  <br>
     <div id="sametext">Some text thats the same</div>
     <div id="changetext">Some text that changes</div>
     </body>
     </html>
    """

    with open(os.path.join(datastore_path, "endpoint-content.txt"), "w") as f:
        f.write(test_return_data)

    return None

def _time_schedule_limit(timezone, start_time, disabled_day):
    """A full, non-default time_schedule_limit structure as the API sends/returns it"""
    schedule = {'enabled': True}
    for day in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'):
        schedule[day] = {'enabled': day != disabled_day, 'start_time': start_time,
                         'duration': {'hours': '8', 'minutes': '30'}}
    schedule['timezone'] = timezone
    return schedule


def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False


# def test_setup(client, live_server, measure_memory_usage, datastore_path):
   #  live_server_setup(live_server) # Setup on conftest per function


def test_api_simple(client, live_server, measure_memory_usage, datastore_path):


    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    # Create a watch
    set_original_response(datastore_path=datastore_path)

    # Validate bad URL
    test_url = url_for('test_endpoint', _external=True )
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": "h://xxxxxxxxxom"}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    assert res.status_code == 400

    # Create new
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": test_url, 'tag': "One, Two", "title": "My test URL"}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )

    assert is_valid_uuid(res.json.get('uuid'))
    watch_uuid = res.json.get('uuid')
    assert res.status_code == 201

    wait_for_all_checks(client)

    # Verify its in the list and that recheck worked
    res = client.get(
        url_for("createwatch", tag="OnE"),
        headers={'x-api-key': api_key}
    )
    assert watch_uuid in res.json.keys()
    before_recheck_info = res.json[watch_uuid]

    assert before_recheck_info['last_checked'] != 0

    #705 `last_changed` should be zero on the first check
    assert before_recheck_info['last_changed'] == 0
    assert before_recheck_info['title'] == 'My test URL'
    assert isinstance(before_recheck_info['link'], str), "link must be a plain string, not a tuple or list"
    assert before_recheck_info['link'] == test_url

    # Check the limit by tag doesnt return anything when nothing found
    res = client.get(
        url_for("createwatch", tag="Something else entirely"),
        headers={'x-api-key': api_key}
    )
    assert len(res.json) == 0
    time.sleep(2)
    wait_for_all_checks(client)
    set_modified_response(datastore_path=datastore_path)
    # Trigger recheck of all ?recheck_all=1
    res = client.get(
        url_for("createwatch", recheck_all='1'),
        headers={'x-api-key': api_key},
    )
    wait_for_all_checks(client)

    time.sleep(2)
    # Did the recheck fire?
    res = client.get(
        url_for("createwatch"),
        headers={'x-api-key': api_key},
    )
    after_recheck_info = res.json[watch_uuid]
    assert after_recheck_info['last_checked'] != before_recheck_info['last_checked']
    assert after_recheck_info['last_changed'] != 0

    # #2877 When run in a slow fetcher like playwright etc
    assert after_recheck_info['last_changed'] ==  after_recheck_info['last_checked']

    # Check history index list
    res = client.get(
        url_for("watchhistory", uuid=watch_uuid),
        headers={'x-api-key': api_key},
    )
    watch_history = res.json
    assert len(res.json) == 2, "Should have two history entries (the original and the changed)"

    # Fetch a snapshot by timestamp, check the right one was found
    res = client.get(
        url_for("watchsinglehistory", uuid=watch_uuid, timestamp=list(res.json.keys())[-1]),
        headers={'x-api-key': api_key},
    )
    assert b'which has this one new line' in res.data

    # Fetch a snapshot by 'latest'', check the right one was found
    res = client.get(
        url_for("watchsinglehistory", uuid=watch_uuid, timestamp='latest'),
        headers={'x-api-key': api_key},
    )
    assert b'which has this one new line' in res.data
    assert b'<div id' not in res.data

    # Fetch the HTML of the latest one
    res = client.get(
        url_for("watchsinglehistory", uuid=watch_uuid, timestamp='latest')+"?html=1",
        headers={'x-api-key': api_key},
    )
    assert b'which has this one new line' in res.data
    assert b'<div id' in res.data


    # Fetch the difference between two versions (default text format)
    res = client.get(
        url_for("watchhistorydiff", uuid=watch_uuid, from_timestamp='previous', to_timestamp='latest'),
        headers={'x-api-key': api_key},
    )
    assert b'(changed) Which is across' in res.data
    assert b'Some text thats the same' in res.data

    # Fetch the difference between two versions (default text format)
    res = client.get(
        url_for("watchhistorydiff", uuid=watch_uuid, from_timestamp='previous', to_timestamp='latest')+"?changesOnly=true",
        headers={'x-api-key': api_key},
    )
    assert b'Some text thats the same' not in res.data

    # Test htmlcolor format
    res = client.get(
        url_for("watchhistorydiff", uuid=watch_uuid, from_timestamp='previous', to_timestamp='latest')+'?format=htmlcolor',
        headers={'x-api-key': api_key},
    )
    assert b'aria-label="Changed text" title="Changed text">Which is across multiple lines' in res.data

    # Test html format
    res = client.get(
        url_for("watchhistorydiff", uuid=watch_uuid, from_timestamp='previous', to_timestamp='latest')+'?format=html',
        headers={'x-api-key': api_key},
    )
    assert res.status_code == 200
    assert b'<br>' in res.data

    # Test markdown format
    res = client.get(
        url_for("watchhistorydiff", uuid=watch_uuid, from_timestamp='previous', to_timestamp='latest')+'?format=markdown',
        headers={'x-api-key': api_key},
    )
    assert res.status_code == 200

    # Test new diff preference parameters
    # Test removed=false (should hide removed content)
    res = client.get(
        url_for("watchhistorydiff", uuid=watch_uuid, from_timestamp='previous', to_timestamp='latest')+'?removed=false',
        headers={'x-api-key': api_key},
    )
    # Should not contain removed content indicator
    assert b'(removed)' not in res.data
    # Should still contain added content
    assert b'(added)' in res.data or b'which has this one new line' in res.data

    # Test added=false (should hide added content)
    # Note: The test data has replacements, not pure additions, so we test differently
    res = client.get(
        url_for("watchhistorydiff", uuid=watch_uuid, from_timestamp='previous', to_timestamp='latest')+'?added=false&replaced=false',
        headers={'x-api-key': api_key},
    )
    # With both added and replaced disabled, should have minimal content
    # Should not contain added indicators
    assert b'(added)' not in res.data

    # Test replaced=false (should hide replaced/changed content)
    res = client.get(
        url_for("watchhistorydiff", uuid=watch_uuid, from_timestamp='previous', to_timestamp='latest')+'?replaced=false',
        headers={'x-api-key': api_key},
    )
    # Should not contain changed content indicator
    assert b'(changed)' not in res.data

    # Test type=diffWords for word-level diff
    res = client.get(
        url_for("watchhistorydiff", uuid=watch_uuid, from_timestamp='previous', to_timestamp='latest')+'?type=diffWords&format=htmlcolor',
        headers={'x-api-key': api_key},
    )
    # Should contain HTML formatted diff
    assert res.status_code == 200
    assert len(res.data) > 0

    # Test combined parameters: show only additions with word diff
    res = client.get(
        url_for("watchhistorydiff", uuid=watch_uuid, from_timestamp='previous', to_timestamp='latest')+'?removed=false&replaced=false&type=diffWords',
        headers={'x-api-key': api_key},
    )
    assert res.status_code == 200
    # Should not contain removed or changed markers
    assert b'(removed)' not in res.data
    assert b'(changed)' not in res.data


    # Fetch the whole watch
    res = client.get(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key}
    )
    watch = res.json
    # @todo how to handle None/default global values?
    assert watch['history_n'] == 2, "Found replacement history section, which is in its own API"

    assert watch.get('viewed') == False
    # Loading the most recent snapshot should force viewed to become true
    client.get(url_for("ui.ui_diff.diff_history_page", uuid="first"), follow_redirects=True)

    time.sleep(3)
    # Fetch the whole watch again, viewed should be true
    res = client.get(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key}
    )
    watch = res.json
    assert watch.get('viewed') == True

    # basic systeminfo check
    res = client.get(
        url_for("systeminfo"),
        headers={'x-api-key': api_key},
    )
    assert res.json.get('watch_count') == 1
    assert res.json.get('uptime') > 0.5

    ######################################################
    # Mute and Pause, check it worked
    res = client.get(
        url_for("watch", uuid=watch_uuid, paused='paused'),
        headers={'x-api-key': api_key}
    )
    assert b'OK' in res.data
    res = client.get(
        url_for("watch", uuid=watch_uuid,  muted='muted'),
        headers={'x-api-key': api_key}
    )
    assert b'OK' in res.data
    res = client.get(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key}
    )
    assert res.json.get('paused') == True
    assert res.json.get('notification_muted') == True

    # Now unpause, unmute
    res = client.get(
        url_for("watch", uuid=watch_uuid,  muted='unmuted'),
        headers={'x-api-key': api_key}
    )
    assert b'OK' in res.data
    res = client.get(
        url_for("watch", uuid=watch_uuid, paused='unpaused'),
        headers={'x-api-key': api_key}
    )
    assert b'OK' in res.data
    res = client.get(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key}
    )
    assert res.json.get('paused') == 0
    assert res.json.get('notification_muted') == 0
    ######################################################





    # Finally delete the watch
    res = client.delete(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key},
    )
    assert res.status_code == 204

    # Check via a relist
    res = client.get(
        url_for("createwatch"),
        headers={'x-api-key': api_key}
    )
    assert len(res.json) == 0, "Watch list should be empty"

def test_api_delete_watch_history(client, live_server, measure_memory_usage, datastore_path):
    """DELETE /api/v1/watch/<uuid>/history should wipe the snapshots but keep the watch (#4397)"""

    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    set_original_response(datastore_path=datastore_path)
    test_url = url_for('test_endpoint', _external=True)

    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": test_url}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    assert res.status_code == 201
    watch_uuid = res.json.get('uuid')
    wait_for_all_checks(client)

    # A second snapshot so we know we're clearing more than one
    set_modified_response(datastore_path=datastore_path)
    client.get(url_for("watch", uuid=watch_uuid, recheck='1'), headers={'x-api-key': api_key})
    wait_for_all_checks(client)

    res = client.get(
        url_for("watchhistory", uuid=watch_uuid),
        headers={'x-api-key': api_key},
    )
    assert len(res.json) == 2, "Should have two history entries before clearing"

    # Unknown watch UUID should 404 and not blow up
    res = client.delete(
        url_for("watchhistory", uuid='4d8b5b4a-8e0b-4d4a-9f57-3f2b1c0d9e11'),
        headers={'x-api-key': api_key},
    )
    assert res.status_code == 404

    # Requires the API key
    res = client.delete(url_for("watchhistory", uuid=watch_uuid))
    assert res.status_code == 403

    # Pause it first - clearing resets last_checked to 0 which otherwise makes the ticker
    # queue an instant recheck, and that would race with the assertions below
    client.get(url_for("watch", uuid=watch_uuid, paused='paused'), headers={'x-api-key': api_key})

    # Now really clear it
    res = client.delete(
        url_for("watchhistory", uuid=watch_uuid),
        headers={'x-api-key': api_key},
    )
    assert res.status_code == 204

    res = client.get(
        url_for("watchhistory", uuid=watch_uuid),
        headers={'x-api-key': api_key},
    )
    assert res.json == {}, "History should be empty after DELETE"

    # The watch itself must survive, with its state reset
    res = client.get(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key}
    )
    assert res.status_code == 200
    assert res.json.get('url') == test_url
    assert res.json.get('history_n') == 0
    assert res.json.get('last_checked') == 0
    assert res.json.get('previous_md5') == False

    # And a snapshot fetch now has nothing to give
    res = client.get(
        url_for("watchsinglehistory", uuid=watch_uuid, timestamp='latest'),
        headers={'x-api-key': api_key},
    )
    assert res.status_code == 404

    delete_all_watches(client)


def test_roundtrip_API(client, live_server, measure_memory_usage, datastore_path):
    """
    Test the full round trip, this way we test the default Model fits back into OpenAPI spec
    :param client:
    :param live_server:
    :param measure_memory_usage:
    :param datastore_path:
    :return:
    """
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    set_original_response(datastore_path=datastore_path)
    test_url = url_for('test_endpoint', _external=True)

    # Create new
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": test_url}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )

    assert res.status_code == 201
    uuid = res.json.get('uuid')

    # Now fetch it and send it back

    res = client.get(
        url_for("watch", uuid=uuid),
        headers={'x-api-key': api_key}
    )

    watch=res.json
    original_schedule = watch['time_schedule_limit']
    assert original_schedule['monday']['duration'] == {'hours': '24', 'minutes': '00'}, \
        "Default time_schedule_limit structure is returned in full"

    # Be sure that 'readOnly' values are never updated in the real watch
    watch['last_changed'] = 454444444444
    watch['date_created'] = 454444444444

    # Exercise the new extract_lines_containing field
    watch['extract_lines_containing'] = ['celsius', 'temperature']

    # HTTP PUT ( UPDATE an existing watch )
    res = client.put(
        url_for("watch", uuid=uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps(watch),
    )
    if res.status_code != 200:
        print(f"\n=== PUT failed with {res.status_code} ===")
        print(f"Error: {res.data}")
    assert res.status_code == 200, "HTTP PUT update was sent OK"

    res = client.get(
        url_for("watch", uuid=uuid),
        headers={'x-api-key': api_key}
    )
    last_changed = res.json.get('last_changed')
    assert last_changed != 454444444444
    assert last_changed != "454444444444"

    date_created = res.json.get('date_created')
    assert date_created != 454444444444
    assert date_created != "454444444444"

    assert res.json.get('extract_lines_containing') == ['celsius', 'temperature'], \
        "extract_lines_containing should be persisted and returned via API"
    assert res.json.get('time_schedule_limit') == original_schedule, \
        "time_schedule_limit survives the GET -> PUT -> GET round trip unchanged"


def test_api_strips_internal_fields(client, live_server, measure_memory_usage, datastore_path):
    """
    Internal/transient fields must never cross the API boundary in either direction:
      1. `__`-prefixed keys (e.g. `__check_status` set by the worker for UI status)
      2. System-managed fields not in the OpenAPI spec (see SYSTEM_MANAGED_NON_SPEC_FIELDS):
         `last_check_status`, `last_filter_config_hash`, `_llm_*`, `llm_*`, etc.

    GET responses must strip them. PUT/POST payloads must silently discard them.
    Without this, a client that round-trips GET → PUT trips the unknown-field validator.
    """
    from changedetectionio.model.schema_utils import SYSTEM_MANAGED_NON_SPEC_FIELDS

    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')
    datastore = live_server.app.config['DATASTORE']

    set_original_response(datastore_path=datastore_path)
    test_url = url_for('test_endpoint', _external=True)

    # Create
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": test_url}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    assert res.status_code == 201
    watch_uuid = res.json.get('uuid')

    wait_for_all_checks(client)

    # Force both a transient __-prefixed and a system-managed field onto the watch,
    # simulating worker/processor-set state.
    watch_obj = datastore.data['watching'][watch_uuid]
    watch_obj['__check_status'] = 'Fetching...'
    watch_obj['last_check_status'] = 200
    watch_obj['_llm_result'] = {'summary': 'cached llm output'}
    watch_obj['last_filter_config_hash'] = 'abc123'

    # --- GET must strip all internal fields ---
    res = client.get(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key},
    )
    assert res.status_code == 200
    assert not any(k.startswith('__') for k in res.json.keys()), \
        f"No __-prefixed field should leak into API responses; got keys: {list(res.json.keys())}"
    leaked_system_fields = SYSTEM_MANAGED_NON_SPEC_FIELDS & set(res.json.keys())
    assert not leaked_system_fields, \
        f"System-managed non-spec fields must not appear in GET response; leaked: {leaked_system_fields}"

    # --- PUT must accept (and silently drop) those same internal fields ---
    # This is the key round-trip property: a client should be able to PUT back what it just GET'd.
    # Use the actual GET response as the payload (the realistic round-trip case).
    payload = dict(res.json)
    payload['__check_status'] = 'attacker-supplied value'   # not in the GET, but a client could add it
    payload['last_check_status'] = 999                       # ditto
    payload['_llm_result'] = 'attacker overwrite'
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps(payload),
    )
    assert res.status_code == 200, \
        f"PUT round-tripping GET response plus internal fields should succeed (got {res.status_code}: {res.data!r})"

    # Internal fields must not have been overwritten by the PUT
    assert watch_obj.get('__check_status') == 'Fetching...', \
        "PUT must not overwrite __-prefixed fields"
    assert watch_obj.get('_llm_result') == {'summary': 'cached llm output'}, \
        "PUT must not overwrite system-managed non-spec fields"

    # --- POST must also silently discard internal fields ---
    # Use unique sentinel values so we can distinguish "POST persisted my value" from
    # "the worker concurrently re-set the field while processing the new watch".
    attacker_check_status = 'attacker-sentinel-__check_status-9f7c'
    attacker_llm_result = 'attacker-sentinel-_llm_result-9f7c'
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": test_url + "?2",
            "__check_status": attacker_check_status,
            "_llm_result": attacker_llm_result,
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True,
    )
    assert res.status_code == 201, \
        f"POST with internal fields should succeed (got {res.status_code}: {res.data!r})"
    new_uuid = res.json.get('uuid')
    new_watch = datastore.data['watching'][new_uuid]
    # If POST had persisted the attacker payload these specific sentinel values would remain.
    # The worker may legitimately re-set __check_status with its own status string, that's fine.
    assert new_watch.get('__check_status') != attacker_check_status, \
        "POST must not persist __-prefixed fields from input"
    assert new_watch.get('_llm_result') != attacker_llm_result, \
        "POST must not persist system-managed fields from input"

    delete_all_watches(client)


def test_access_denied(client, live_server, measure_memory_usage, datastore_path):
    # `config_api_token_enabled` Should be On by default
    res = client.get(
        url_for("createwatch")
    )
    assert res.status_code == 403

    res = client.get(
        url_for("createwatch"),
        headers={'x-api-key': "something horrible"}
    )
    assert res.status_code == 403

    # Disable config_api_token_enabled and it should work
    res = client.post(
        url_for("settings.settings_page"),
        data={
            "requests-time_between_check-minutes": 180,
            "application-fetch_backend": "html_requests",
            "application-api_access_token_enabled": ""
        },
        follow_redirects=True
    )

    assert b"Settings updated." in res.data

    res = client.get(
        url_for("createwatch")
    )
    assert res.status_code == 200

    # Cleanup everything
    delete_all_watches(client)

    res = client.post(
        url_for("settings.settings_page"),
        data={
            "requests-time_between_check-minutes": 180,
            "application-fetch_backend": "html_requests",
            "application-api_access_token_enabled": "y"
        },
        follow_redirects=True
    )
    assert b"Settings updated." in res.data

def test_api_watch_PUT_update(client, live_server, measure_memory_usage, datastore_path):

    from changedetectionio.model.frozen import FrozenDict
    datastore = live_server.app.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    # Create a watch
    set_original_response(datastore_path=datastore_path)
    test_url = url_for('test_endpoint', _external=True)
    created_schedule = _time_schedule_limit(timezone='Europe/Berlin', start_time='09:30', disabled_day='sunday')

    # Create new
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": test_url,
                         'tag': "One, Two",
                         "title": "My test URL",
                         'headers': {'cookie': 'yum'},
                         'time_schedule_limit': created_schedule,
                         "conditions": [
                             {
                                 "field": "page_filtered_text",
                                 "operator": "contains_regex",
                                 "value": "."  # contains anything
                             }
                         ],
                         "conditions_match_logic": "ALL",
                         }
                        ),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )

    if res.status_code != 201:
        print(f"\n=== POST createwatch failed with {res.status_code} ===")
        print(f"Response: {res.data}")
    assert res.status_code == 201

    wait_for_all_checks(client)
    # Get a listing, it will be the first one
    res = client.get(
        url_for("createwatch"),
        headers={'x-api-key': api_key}
    )

    watch_uuid = list(res.json.keys())[0]
    assert not res.json[watch_uuid].get('viewed'), 'A newly created watch can only be unviewed'

    # Check in the edit page just to be sure
    res = client.get(
        url_for("ui.ui_edit.edit_page", uuid=watch_uuid),
    )
    assert b"cookie: yum" in res.data, "'cookie: yum' found in 'headers' section"
    assert b"One" in res.data, "Tag 'One' was found"
    assert b"Two" in res.data, "Tag 'Two' was found"

    # HTTP PUT ( UPDATE an existing watch )
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({
            "title": "new title",
            'time_between_check': {'minutes': 552},
            'headers': {'cookie': 'all eaten'},
            'last_viewed': int(time.time())
        }),
    )
    assert res.status_code == 200, "HTTP PUT update was sent OK"

    # HTTP GET single watch, title should be updated
    res = client.get(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key}
    )
    assert res.json.get('title') == 'new title'
    assert res.json.get('viewed'), 'With the timestamp greater than "changed" a watch can be updated to viewed'
    assert res.json.get('time_schedule_limit') == created_schedule, \
        "time_schedule_limit from POST is returned exactly, and a PUT without it leaves it alone"

    # time_schedule_limit is stored as a shared read-only value (model/frozen.py), identical schedules
    # are the same object in memory. A second watch with the same schedule must never be affected by
    # changes to the first, and each watch.json must hold exactly its own schedule.
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": test_url + "?second", 'time_schedule_limit': created_schedule}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 201, res.data
    other_uuid = res.json['uuid']
    watch = datastore.data['watching'][watch_uuid]
    other_watch = datastore.data['watching'][other_uuid]
    shared = other_watch['time_schedule_limit']
    assert isinstance(shared, FrozenDict)
    assert watch['time_schedule_limit'] is shared, "Identical schedules share one object"

    # An in-place write is refused, it can't reach the other watch through the shared object
    try:
        watch['time_schedule_limit']['monday']['enabled'] = False
        assert False, "In-place write to a shared time_schedule_limit should raise"
    except TypeError:
        pass
    assert other_watch['time_schedule_limit']['monday']['enabled'] is True

    # HTTP PUT a different time_schedule_limit on the first watch only
    updated_schedule = _time_schedule_limit(timezone='Pacific/Kiritimati', start_time='22:15', disabled_day='wednesday')
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({'time_schedule_limit': updated_schedule}),
    )
    assert res.status_code == 200, f"HTTP PUT time_schedule_limit was sent OK: {res.data}"

    for uuid, expected in ((watch_uuid, updated_schedule), (other_uuid, created_schedule)):
        # API returns exactly what was set, and nothing bled across
        res = client.get(url_for("watch", uuid=uuid), headers={'x-api-key': api_key})
        assert res.json.get('time_schedule_limit') == expected, f"API time_schedule_limit for {uuid}"
        live = datastore.data['watching'][uuid]['time_schedule_limit']
        assert isinstance(live, FrozenDict)
        assert live == expected, f"In-memory time_schedule_limit for {uuid}"

        # Saved to disk exactly, key order included, and loads back the same
        watch_json = os.path.join(datastore.data['watching'][uuid].data_dir, "watch.json")
        with open(watch_json) as f:
            on_disk = json.load(f)
        assert on_disk['time_schedule_limit'] == expected, f"watch.json time_schedule_limit for {uuid}"
        assert list(on_disk['time_schedule_limit'].keys()) == list(expected.keys())
        reloaded = datastore.rehydrate_entity(uuid, on_disk)
        assert reloaded['time_schedule_limit'] == expected
        assert reloaded['time_schedule_limit'] is live, "Reloaded schedule rejoins the shared object"

    assert other_watch['time_schedule_limit'] is shared, "The untouched watch still holds the original object"
    assert watch['time_schedule_limit'] is not shared

    # Check in the edit page just to be sure
    res = client.get(
        url_for("ui.ui_edit.edit_page", uuid=watch_uuid),
    )
    assert b"new title" in res.data, "new title found in edit page"
    assert b"552" in res.data, "552 minutes found in edit page"
    assert b"One" in res.data, "Tag 'One' was found"
    assert b"Two" in res.data, "Tag 'Two' was found"
    assert b"cookie: all eaten" in res.data, "'cookie: all eaten' found in 'headers' section"

    ######################################################

    # HTTP PUT try a field that doesn't exist

    # HTTP PUT an update
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({"title": "new title", "some other field": "uh oh"}),
    )

    assert res.status_code == 400, "Should get error 400 when we give a field that doesnt exist"
    # Backend validation now rejects unknown fields with a clear error message
    assert (b'Unknown field' in res.data or
            b'Additional properties are not allowed' in res.data or
            b'Unevaluated properties are not allowed' in res.data or
            b'does not match any of the regexes' in res.data), \
            "Should reject unknown fields with validation error"


    # Try a XSS URL
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({
            'url': 'javascript:alert(document.domain)'
        }),
    )
    assert res.status_code == 400

    # Cleanup everything
    delete_all_watches(client)


def test_api_import(client, live_server, measure_memory_usage, datastore_path):

    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    # Test 1: Basic import with tag
    res = client.post(
        url_for("import") + "?tag=import-test",
        data='https://website1.com\r\nhttps://website2.com',
        # We removed  'content-type': 'text/plain', the Import API should assume this if none is set #3547 #3542
        headers={'x-api-key': api_key},
        follow_redirects=True
    )

    assert res.status_code == 200
    assert len(res.json) == 2
    res = client.get(url_for("watchlist.index"))
    assert b"https://website1.com" in res.data
    assert b"https://website2.com" in res.data

    # Should see the new tag in the tag/groups list
    res = client.get(url_for('tags.tags_overview_page'))
    assert b'import-test' in res.data

    # Test 2: Import with watch configuration fields (issue #3845)
    # Test string field (include_filters), boolean (paused), and processor
    import urllib.parse
    params = urllib.parse.urlencode({
        'tag': 'config-test',
        'include_filters': 'div.content',
        'paused': 'true',
        'processor': 'text_json_diff',
        'title': 'Imported with Config'
    })

    res = client.post(
        url_for("import") + "?" + params,
        data='https://website3.com',
        headers={'x-api-key': api_key},
        follow_redirects=True
    )

    assert res.status_code == 200
    assert len(res.json) == 1
    uuid = res.json[0]

    # Verify the configuration was applied
    watch = live_server.app.config['DATASTORE'].data['watching'][uuid]
    assert watch['include_filters'] == ['div.content'], "include_filters should be set as array"
    assert watch['paused'] == True, "paused should be True"
    assert watch['processor'] == 'text_json_diff', "processor should be set"
    assert watch['title'] == 'Imported with Config', "title should be set"

    # Test 3: Import with array field (notification_urls) - using valid Apprise format
    params = urllib.parse.urlencode({
        'tag': 'notification-test',
        'notification_urls': 'mailto://test@example.com,mailto://admin@example.com'
    })

    res = client.post(
        url_for("import") + "?" + params,
        data='https://website4.com',
        headers={'x-api-key': api_key},
        follow_redirects=True
    )

    assert res.status_code == 200
    uuid = res.json[0]
    watch = live_server.app.config['DATASTORE'].data['watching'][uuid]
    assert isinstance(watch['notification_urls'], list), "notification_urls must be stored as a list"
    assert len(watch['notification_urls']) == 2, "notification_urls should have 2 entries"
    assert 'mailto://test@example.com' in watch['notification_urls'], "notification_urls should contain first email"
    assert 'mailto://admin@example.com' in watch['notification_urls'], "notification_urls should contain second email"

    # Test 4: Import with object field (time_between_check)
    import json
    time_config = json.dumps({"hours": 2, "minutes": 30})
    params = urllib.parse.urlencode({
        'tag': 'schedule-test',
        'time_between_check': time_config
    })

    res = client.post(
        url_for("import") + "?" + params,
        data='https://website5.com',
        headers={'x-api-key': api_key},
        follow_redirects=True
    )

    assert res.status_code == 200
    uuid = res.json[0]
    watch = live_server.app.config['DATASTORE'].data['watching'][uuid]
    assert watch['time_between_check']['hours'] == 2, "time_between_check hours should be 2"
    assert watch['time_between_check']['minutes'] == 30, "time_between_check minutes should be 30"

    # Test 5: Import with invalid processor (should fail)
    res = client.post(
        url_for("import") + "?processor=invalid_processor",
        data='https://website6.com',
        headers={'x-api-key': api_key},
        follow_redirects=True
    )

    assert res.status_code == 400, "Should reject invalid processor"
    assert b"Invalid processor" in res.data, "Error message should mention invalid processor"

    # Test 6: Import with invalid field (should fail)
    res = client.post(
        url_for("import") + "?unknown_field=value",
        data='https://website7.com',
        headers={'x-api-key': api_key},
        follow_redirects=True
    )

    assert res.status_code == 400, "Should reject unknown field"
    assert b"Unknown watch configuration parameter" in res.data, "Error message should mention unknown parameter"

    # Test 7: Import with complex nested array (browser_steps) - array of objects
    browser_steps = json.dumps([
        {"operation": "wait", "selector": "5", "optional_value": ""},
        {"operation": "click", "selector": "button.submit", "optional_value": ""}
    ])
    params = urllib.parse.urlencode({
        'tag': 'browser-test',
        'browser_steps': browser_steps
    })

    res = client.post(
        url_for("import") + "?" + params,
        data='https://website8.com',
        headers={'x-api-key': api_key},
        follow_redirects=True
    )

    assert res.status_code == 200, "Should accept browser_steps array"
    uuid = res.json[0]
    watch = live_server.app.config['DATASTORE'].data['watching'][uuid]
    assert len(watch['browser_steps']) == 2, "Should have 2 browser steps"
    assert watch['browser_steps'][0]['operation'] == 'wait', "First step should be wait"
    assert watch['browser_steps'][1]['operation'] == 'click', "Second step should be click"
    assert watch['browser_steps'][1]['selector'] == 'button.submit', "Second step selector should be button.submit"

    # Cleanup
    delete_all_watches(client)


def test_api_import_small_synchronous(client, live_server, measure_memory_usage, datastore_path):
    """Test that small imports (< threshold) are processed synchronously"""
    from changedetectionio.api.Import import IMPORT_SWITCH_TO_BACKGROUND_THRESHOLD

    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    # Use local test endpoint to avoid network delays
    test_url_base = url_for('test_endpoint', _external=True)

    # Create URLs: threshold - 1 to stay under limit
    num_urls = min(5, IMPORT_SWITCH_TO_BACKGROUND_THRESHOLD - 1)  # Use small number for faster test
    urls = '\n'.join([f'{test_url_base}?id=small-{i}' for i in range(num_urls)])

    # Import small batch
    res = client.post(
        url_for("import") + "?tag=small-test",
        data=urls,
        headers={'x-api-key': api_key},
        follow_redirects=True
    )

    # Should return 200 OK with UUID list (synchronous)
    assert res.status_code == 200, f"Should return 200 for small imports, got {res.status_code}"
    assert isinstance(res.json, list), "Response should be a list of UUIDs"
    assert len(res.json) == num_urls, f"Should return {num_urls} UUIDs, got {len(res.json)}"

    # Verify all watches were created immediately
    for uuid in res.json:
        assert uuid in live_server.app.config['DATASTORE'].data['watching'], \
            f"Watch {uuid} should exist immediately after synchronous import"
        watch = live_server.app.config['DATASTORE'].data['watching'][uuid]
        with open(os.path.join(watch.data_dir, 'watch.json'), encoding='utf-8') as watch_file:
            assert json.load(watch_file)['uuid'] == uuid

    print(f"\n✓ Successfully created {num_urls} watches synchronously")


def test_api_import_large_background(client, live_server, measure_memory_usage, datastore_path):
    """Test that large imports (>= threshold) are processed in background thread"""
    from changedetectionio.api.Import import IMPORT_SWITCH_TO_BACKGROUND_THRESHOLD
    import time

    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    # Use local test endpoint to avoid network delays
    test_url_base = url_for('test_endpoint', _external=True)

    # Create URLs: threshold + 10 to trigger background processing
    num_urls = IMPORT_SWITCH_TO_BACKGROUND_THRESHOLD + 10
    urls = '\n'.join([f'{test_url_base}?id=bulk-{i}' for i in range(num_urls)])

    # Import large batch
    res = client.post(
        url_for("import") + "?tag=bulk-test",
        data=urls,
        headers={'x-api-key': api_key},
        follow_redirects=True
    )

    # Should return 202 Accepted (background processing)
    assert res.status_code == 202, f"Should return 202 for large imports, got {res.status_code}"
    assert b"background" in res.data.lower(), "Response should mention background processing"

    # Extract expected count from response
    response_json = res.json
    assert 'count' in response_json, "Response should include count"
    assert response_json['count'] == num_urls, f"Count should be {num_urls}, got {response_json['count']}"

    # Wait for background thread to complete (with timeout)
    max_wait = 10  # seconds
    wait_interval = 0.5
    elapsed = 0
    watches_created = 0

    while elapsed < max_wait:
        time.sleep(wait_interval)
        elapsed += wait_interval

        # Count how many watches have been created
        watches_created = len([
            uuid for uuid, watch in live_server.app.config['DATASTORE'].data['watching'].items()
            if 'id=bulk-' in watch['url']
        ])

        if watches_created == num_urls:
            break

    # Verify all watches were created
    assert watches_created == num_urls, \
        f"Expected {num_urls} watches to be created, but found {watches_created} after {elapsed}s"

    # Verify watches have correct configuration
    bulk_watches = [
        watch for watch in live_server.app.config['DATASTORE'].data['watching'].values()
        if 'id=bulk-' in watch['url']
    ]

    assert len(bulk_watches) == num_urls, "All bulk watches should exist"

    # Check that they have the correct tag
    datastore = live_server.app.config['DATASTORE']
    # Get UUIDs of bulk watches by filtering the datastore keys
    bulk_watch_uuids = [
        uuid for uuid, watch in live_server.app.config['DATASTORE'].data['watching'].items()
        if 'id=bulk-' in watch['url']
    ]
    for watch_uuid in bulk_watch_uuids:
        tags = datastore.get_all_tags_for_watch(uuid=watch_uuid)
        tag_names = [t['title'] for t in tags.values()]
        assert 'bulk-test' in tag_names, f"Watch {watch_uuid} should have 'bulk-test' tag"

    print(f"\n✓ Successfully created {num_urls} watches in background (took {elapsed}s)")


def test_api_import_sync_partial_persistence_failure_reports_items_and_reload(client, live_server, datastore_path, monkeypatch):
    from changedetectionio.store import ChangeDetectionStore, file_saving_datastore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    base_url = url_for('test_endpoint', _external=True)
    urls = [f'{base_url}?id=sync-fail', f'{base_url}?id=sync-success']
    real_save = file_saving_datastore.save_entity_atomic

    def fail_one_watch(*args, **kwargs):
        data_dict = args[2]
        if kwargs.get('entity_type') == 'watch' and 'id=sync-fail' in data_dict['url']:
            raise OSError('simulated imported watch write failure')
        return real_save(*args, **kwargs)

    monkeypatch.setattr(file_saving_datastore, 'save_entity_atomic', fail_one_watch)
    response = client.post(
        url_for('import') + '?paused=true',
        data='\n'.join(urls),
        headers={'x-api-key': api_key},
    )

    assert response.status_code == 207
    result = response.json
    assert result['status'] == 'partial_failure'
    assert result['requested_count'] == 2
    assert result['success_count'] == 1
    assert result['failure_count'] == 1
    assert len(result['added']) == 1
    assert len(result['failed']) == 1

    failed_item = result['failed'][0]
    failed_uuid = failed_item['uuid']
    from changedetectionio.api.Import import _redact_import_url
    assert failed_item['url'] == _redact_import_url(urls[0])
    assert failed_item['cleanup_complete'] is True
    assert failed_uuid not in datastore.data['watching']
    assert not os.path.exists(os.path.join(datastore.datastore_path, failed_uuid))

    success_uuid = result['added'][0]
    success_watch = datastore.data['watching'][success_uuid]
    with open(os.path.join(success_watch.data_dir, 'watch.json'), encoding='utf-8') as watch_file:
        assert json.load(watch_file)['uuid'] == success_uuid

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert failed_uuid not in reloaded.data['watching']
        assert success_uuid in reloaded.data['watching']
    finally:
        reloaded.stop_thread = True


def test_api_import_background_success_reports_persisted_count(client, live_server, datastore_path, monkeypatch):
    import importlib
    from changedetectionio.store import ChangeDetectionStore

    import_module = importlib.import_module('changedetectionio.api.Import')
    monkeypatch.setattr(import_module, 'IMPORT_SWITCH_TO_BACKGROUND_THRESHOLD', 0)
    real_import_batch = import_module._import_watch_batch
    completed = threading.Event()
    batch_result = {}

    def record_batch_result(*args, **kwargs):
        result = real_import_batch(*args, **kwargs)
        batch_result.update(result)
        completed.set()
        return result

    monkeypatch.setattr(import_module, '_import_watch_batch', record_batch_result)

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    test_url = f"{url_for('test_endpoint', _external=True)}?id=background-success"
    response = client.post(
        url_for('import') + '?paused=true',
        data=test_url,
        headers={'x-api-key': api_key},
    )

    assert response.status_code == 202
    assert response.json['queued_count'] == 1
    assert completed.wait(10), 'background import did not complete'
    assert batch_result['status'] == 'completed'
    assert batch_result['requested_count'] == 1
    assert batch_result['success_count'] == 1
    assert batch_result['failure_count'] == 0

    watch_uuid = batch_result['added'][0]
    watch = datastore.data['watching'][watch_uuid]
    with open(os.path.join(watch.data_dir, 'watch.json'), encoding='utf-8') as watch_file:
        assert json.load(watch_file)['uuid'] == watch_uuid

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert watch_uuid in reloaded.data['watching']
    finally:
        reloaded.stop_thread = True


def test_api_import_background_partial_failure_reports_counts_and_reload(client, live_server, datastore_path, monkeypatch):
    import importlib
    from changedetectionio.store import ChangeDetectionStore, file_saving_datastore

    import_module = importlib.import_module('changedetectionio.api.Import')
    monkeypatch.setattr(import_module, 'IMPORT_SWITCH_TO_BACKGROUND_THRESHOLD', 0)
    real_import_batch = import_module._import_watch_batch
    completed = threading.Event()
    batch_result = {}

    def record_batch_result(*args, **kwargs):
        result = real_import_batch(*args, **kwargs)
        batch_result.update(result)
        completed.set()
        return result

    monkeypatch.setattr(import_module, '_import_watch_batch', record_batch_result)
    real_save = file_saving_datastore.save_entity_atomic

    def fail_one_watch(*args, **kwargs):
        data_dict = args[2]
        if kwargs.get('entity_type') == 'watch' and 'id=background-fail' in data_dict['url']:
            raise OSError('simulated background watch write failure')
        return real_save(*args, **kwargs)

    monkeypatch.setattr(file_saving_datastore, 'save_entity_atomic', fail_one_watch)

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    base_url = url_for('test_endpoint', _external=True)
    urls = [f'{base_url}?id=background-fail', f'{base_url}?id=background-success']
    response = client.post(
        url_for('import') + '?paused=true',
        data='\n'.join(urls),
        headers={'x-api-key': api_key},
    )

    assert response.status_code == 202
    assert response.json['queued_count'] == 2
    assert completed.wait(10), 'background import did not complete'
    assert batch_result['status'] == 'partial_failure'
    assert batch_result['requested_count'] == 2
    assert batch_result['success_count'] == 1
    assert batch_result['failure_count'] == 1

    failed_item = batch_result['failed'][0]
    failed_uuid = failed_item['uuid']
    from changedetectionio.api.Import import _redact_import_url
    assert failed_item['url'] == _redact_import_url(urls[0])
    assert failed_item['cleanup_complete'] is True
    assert failed_uuid not in datastore.data['watching']
    assert not os.path.exists(os.path.join(datastore.datastore_path, failed_uuid))

    success_uuid = batch_result['added'][0]
    success_watch = datastore.data['watching'][success_uuid]
    with open(os.path.join(success_watch.data_dir, 'watch.json'), encoding='utf-8') as watch_file:
        assert json.load(watch_file)['uuid'] == success_uuid

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert failed_uuid not in reloaded.data['watching']
        assert success_uuid in reloaded.data['watching']
    finally:
        reloaded.stop_thread = True


def test_api_restock_processor_config(client, live_server, measure_memory_usage, datastore_path):
    """
    Test that processor_config_restock_diff is accepted by the API for watches using
    restock_diff processor, that its schema is validated (enum values, types), and that
    genuinely unknown fields are rejected with an error that originates from the
    OpenAPI spec validation layer.
    """
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')
    test_url = url_for('test_endpoint', _external=True)

    # Create a watch in restock_diff mode WITH processor_config in the POST body (matches the API docs example)
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": test_url,
            "processor": "restock_diff",
            "title": "Restock test",
            "processor_config_restock_diff": {
                "in_stock_processing": "in_stock_only",
                "follow_price_changes": True,
                "price_change_min": 8888888.0,
            }
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    assert res.status_code == 201
    watch_uuid = res.json.get('uuid')
    assert is_valid_uuid(watch_uuid)

    # Verify the value set on POST is reflected in the UI edit page (not just via PUT)
    res = client.get(url_for("ui.ui_edit.edit_page", uuid=watch_uuid))
    assert res.status_code == 200
    assert b'8888888' in res.data, "price_change_min set via POST should appear in the UI edit form"

    # Valid processor_config_restock_diff update via PUT should also be accepted
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({
            "processor_config_restock_diff": {
                "in_stock_processing": "all_changes",
                "follow_price_changes": False,
                "price_change_min": 8888888.0,
                "price_change_max": 9999999.0,
            }
        }),
    )
    assert res.status_code == 200, f"Valid processor_config_restock_diff should be accepted, got: {res.data}"

    # Verify the updated value is still reflected in the UI edit page
    res = client.get(url_for("ui.ui_edit.edit_page", uuid=watch_uuid))
    assert res.status_code == 200
    assert b'8888888' in res.data, "price_change_min set via PUT should appear in the UI edit form"

    # An invalid enum value inside processor_config_restock_diff should be rejected by the spec
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({
            "processor_config_restock_diff": {
                "in_stock_processing": "not_a_valid_enum_value"
            }
        }),
    )
    assert res.status_code == 400, "Invalid enum value in processor config should be rejected"
    assert b'Validation failed' in res.data, "Rejection should come from OpenAPI spec validation layer"

    # A completely unknown field should be rejected (either by OpenAPI spec validation or
    # the application-level field filter — both are acceptable gatekeepers)
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({"field_that_is_not_in_the_spec_at_all": "some value"}),
    )
    assert res.status_code == 400, "Unknown fields should be rejected"
    assert (b'Validation failed' in res.data or b'Unknown field' in res.data), \
        "Rejection should come from either the OpenAPI spec validation layer or application field filter"

    delete_all_watches(client)


def test_api_restock_processor_config_partial_updates_preserve_siblings(client, live_server):
    """Restock config PUTs merge provided nested settings, while {} clears the config."""
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')
    test_url = url_for('test_endpoint', _external=True)
    headers = {'content-type': 'application/json', 'x-api-key': api_key}

    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": test_url,
            "processor": "restock_diff",
            "paused": True,
            "processor_config_restock_diff": {
                "in_stock_processing": "all_changes",
                "follow_price_changes": True,
                "price_change_min": 10.0,
                "price_change_max": 100.0,
            }
        }),
        headers=headers,
    )
    assert res.status_code == 201
    watch_uuid = res.json['uuid']

    # A partial update to one existing setting must retain its siblings.
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        data=json.dumps({"processor_config_restock_diff": {"price_change_min": 20.0}}),
        headers=headers,
    )
    assert res.status_code == 200
    res = client.get(url_for("watch", uuid=watch_uuid), headers={'x-api-key': api_key})
    assert res.status_code == 200
    settings = res.json['processor_config_restock_diff']
    assert settings == {
        "in_stock_processing": "all_changes",
        "follow_price_changes": True,
        "price_change_min": 20.0,
        "price_change_max": 100.0,
    }

    # Adding a setting preserves existing settings, and 0 is retained as an explicit value.
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        data=json.dumps({"processor_config_restock_diff": {"price_change_threshold_percent": 0}}),
        headers=headers,
    )
    assert res.status_code == 200
    res = client.get(url_for("watch", uuid=watch_uuid), headers={'x-api-key': api_key})
    settings = res.json['processor_config_restock_diff']
    assert settings['in_stock_processing'] == 'all_changes'
    assert settings['follow_price_changes'] is True
    assert settings['price_change_min'] == 20.0
    assert settings['price_change_max'] == 100.0
    assert settings['price_change_threshold_percent'] == 0

    # Optional nulls and False are explicit updates, not omissions.
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        data=json.dumps({
            "processor_config_restock_diff": {
                "price_change_min": None,
                "follow_price_changes": False,
            }
        }),
        headers=headers,
    )
    assert res.status_code == 200
    res = client.get(url_for("watch", uuid=watch_uuid), headers={'x-api-key': api_key})
    settings = res.json['processor_config_restock_diff']
    assert settings['price_change_min'] is None
    assert settings['follow_price_changes'] is False
    assert settings['in_stock_processing'] == 'all_changes'
    assert settings['price_change_max'] == 100.0
    assert settings['price_change_threshold_percent'] == 0

    # An empty nested object preserves the existing full-clear behavior.
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        data=json.dumps({"processor_config_restock_diff": {}}),
        headers=headers,
    )
    assert res.status_code == 200
    res = client.get(url_for("watch", uuid=watch_uuid), headers={'x-api-key': api_key})
    assert res.json['processor_config_restock_diff'] == {}

    delete_all_watches(client)


def test_api_processor_config_save_failure_returns_server_error(client, live_server, monkeypatch):
    """API create/update requests must not report success when config persistence fails."""
    from changedetectionio import processors

    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')
    test_url = url_for('test_endpoint', _external=True)
    headers = {'content-type': 'application/json', 'x-api-key': api_key}
    monkeypatch.setattr(processors, 'save_processor_config', lambda *args, **kwargs: False)

    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": test_url,
            "processor": "restock_diff",
            "title": "Watch created before config failure",
            "paused": True,
            "processor_config_restock_diff": {"follow_price_changes": False},
        }),
        headers=headers,
    )
    assert res.status_code == 500
    watch_uuid = res.json['uuid']
    assert 'could not be saved' in res.json['error']
    assert res.json['watch_saved'] is True
    assert res.json['processor_config_saved'] is False
    datastore = client.application.config['DATASTORE']
    created_watch = datastore.data['watching'][watch_uuid]
    with open(os.path.join(created_watch.data_dir, 'watch.json'), encoding='utf-8') as watch_file:
        assert json.load(watch_file)['title'] == "Watch created before config failure"

    res = client.put(
        url_for("watch", uuid=watch_uuid),
        data=json.dumps({
            "title": "Watch fields saved despite config failure",
            "processor_config_restock_diff": {"price_change_min": 5.0},
        }),
        headers=headers,
    )
    assert res.status_code == 500
    assert res.json['watch_saved'] is True
    assert res.json['processor_config_saved'] is False
    assert 'Watch settings were saved' in res.json['error']
    watch = datastore.data['watching'][watch_uuid]
    with open(os.path.join(watch.data_dir, 'watch.json'), encoding='utf-8') as watch_file:
        assert json.load(watch_file)['title'] == "Watch fields saved despite config failure"

    delete_all_watches(client)


def test_api_put_reports_ordinary_watch_write_failure(client, live_server, monkeypatch):
    from changedetectionio.store import file_saving_datastore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    test_url = url_for('test_endpoint', _external=True)
    uuid = datastore.add_watch(
        url=test_url,
        extras={'processor': 'restock_diff', 'title': 'Original title', 'paused': True},
    )
    watch = datastore.data['watching'][uuid]
    watch_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_path, 'rb') as watch_file:
        original_bytes = watch_file.read()

    real_save_entity_atomic = file_saving_datastore.save_entity_atomic

    def fail_watch_save(*args, **kwargs):
        if kwargs.get('entity_type') == 'watch':
            raise OSError('simulated watch write failure')
        return real_save_entity_atomic(*args, **kwargs)

    monkeypatch.setattr(file_saving_datastore, 'save_entity_atomic', fail_watch_save)
    response = client.put(
        url_for('watch', uuid=uuid),
        data=json.dumps({
            'title': 'Unsaved title',
            'processor_config_restock_diff': {'price_change_min': 12.0},
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )

    assert response.status_code == 500
    assert response.json['watch_saved'] is False
    assert response.json['processor_config_saved'] is None
    assert 'could not be saved' in response.json['error']
    assert datastore.data['watching'][uuid]['title'] == 'Original title'
    with open(watch_path, 'rb') as watch_file:
        assert watch_file.read() == original_bytes
    assert not os.path.exists(os.path.join(watch.data_dir, 'restock_diff.json'))

    response = client.get(
        url_for('watch', uuid=uuid, paused='unpaused'),
        headers={'x-api-key': api_key},
    )
    assert response.status_code == 500
    assert response.json['watch_saved'] is False
    assert datastore.data['watching'][uuid]['paused'] is True

    delete_all_watches(client)


def test_api_put_failure_rollback_preserves_concurrent_same_watch_update(client, live_server, monkeypatch):
    """A later update waits for the failing save/rollback, then persists its own state."""
    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    test_url = url_for('test_endpoint', _external=True)
    uuid = datastore.add_watch(url=test_url, extras={'title': 'Original', 'paused': True})
    watch = datastore.data['watching'][uuid]

    first_commit_entered = threading.Event()
    let_first_fail = threading.Event()
    second_waiting_for_lock = threading.Event()
    responses = {}
    real_commit = watch.commit

    class ObservedRLock:
        def __init__(self):
            self.lock = threading.RLock()

        def __enter__(self):
            if threading.current_thread().name == 'second-watch-update':
                second_waiting_for_lock.set()
            self.lock.acquire()
            return self

        def __exit__(self, *_args):
            self.lock.release()

    watch._mutation_lock = ObservedRLock()

    def controlled_commit():
        if threading.current_thread().name == 'first-watch-update':
            first_commit_entered.set()
            assert let_first_fail.wait(10), 'test did not release the first failing commit'
            return False
        return real_commit()

    monkeypatch.setattr(watch, 'commit', controlled_commit)
    app = client.application
    endpoint = url_for('watch', uuid=uuid)

    def update_watch(thread_name, title):
        with app.test_client() as thread_client:
            response = thread_client.put(
                endpoint,
                data=json.dumps({'title': title}),
                headers={'content-type': 'application/json', 'x-api-key': api_key},
            )
            responses[thread_name] = response

    first = threading.Thread(target=update_watch, args=('first', 'First request'), name='first-watch-update')
    second = threading.Thread(target=update_watch, args=('second', 'Second request'), name='second-watch-update')
    first.start()
    assert first_commit_entered.wait(10), 'first request did not reach its commit'
    second.start()
    assert second_waiting_for_lock.wait(10), 'second request did not contend for the watch lock'
    assert watch['title'] == 'First request'

    let_first_fail.set()
    first.join(10)
    second.join(10)
    assert not first.is_alive()
    assert not second.is_alive()
    assert responses['first'].status_code == 500
    assert responses['second'].status_code == 200

    watch_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_path, encoding='utf-8') as watch_file:
        saved_watch = json.load(watch_file)
    assert watch['title'] == 'Second request'
    assert saved_watch['title'] == watch['title']

    delete_all_watches(client)


def test_api_rejects_stale_put_and_pause_mute_after_watch_deletion(client, live_server):
    """Requests that captured a watch before deletion must not recreate it on disk."""
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    datastore_path = datastore.datastore_path
    api_key = datastore.data['settings']['application'].get('api_access_token')
    app = client.application

    def assert_deleted_watch_request_is_rejected(operation, query_args=None):
        watch_uuid = datastore.add_watch(
            url=url_for('test_endpoint', _external=True),
            extras={'title': 'Before delete', 'paused': True},
        )
        watch = datastore.data['watching'][watch_uuid]
        watch_dir = watch.data_dir
        at_watch_lock = threading.Event()
        continue_request = threading.Event()
        request_thread_name = f'stale-watch-{operation}'

        class RequestGateRLock:
            def __init__(self):
                self.lock = threading.RLock()

            def __enter__(self):
                if threading.current_thread().name == request_thread_name:
                    at_watch_lock.set()
                    if not continue_request.wait(10):
                        raise AssertionError('test did not release stale request')
                self.lock.acquire()
                return self

            def __exit__(self, *_args):
                self.lock.release()

        watch._mutation_lock = RequestGateRLock()
        endpoint = url_for('watch', uuid=watch_uuid, **(query_args or {}))
        responses = {}

        def make_request():
            with app.test_client() as thread_client:
                if operation == 'put':
                    responses['response'] = thread_client.put(
                        endpoint,
                        data=json.dumps({'title': 'Stale update'}),
                        headers={'content-type': 'application/json', 'x-api-key': api_key},
                    )
                else:
                    responses['response'] = thread_client.get(
                        endpoint,
                        headers={'x-api-key': api_key},
                    )

        request_thread = threading.Thread(target=make_request, name=request_thread_name)
        request_thread.start()
        assert at_watch_lock.wait(10), 'request did not reach the per-watch lock'

        datastore.delete(watch_uuid)
        assert watch_uuid not in datastore.data['watching']
        assert not os.path.exists(watch_dir)

        continue_request.set()
        request_thread.join(10)
        assert not request_thread.is_alive(), 'stale API request did not finish'
        assert responses['response'].status_code == 404
        assert watch_uuid not in datastore.data['watching']
        assert not os.path.exists(watch_dir)

        reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
        try:
            assert watch_uuid not in reloaded.data['watching']
        finally:
            reloaded.stop_thread = True

    assert_deleted_watch_request_is_rejected('put')
    assert_deleted_watch_request_is_rejected('pause', {'paused': 'paused'})
    assert_deleted_watch_request_is_rejected('mute', {'muted': 'muted'})


def test_api_delete_reports_watch_storage_failure_without_removing_watch(client, datastore_path, monkeypatch):
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    watch_uuid = datastore.add_watch(
        url=url_for('test_endpoint', _external=True),
        extras={'title': 'Keep on disk', 'paused': True},
    )
    watch = datastore.data['watching'][watch_uuid]
    watch_path = os.path.join(watch.data_dir, 'watch.json')

    def fail_delete(_uuid):
        raise OSError('simulated filesystem deletion failure')

    monkeypatch.setattr(datastore, '_delete_watch', fail_delete)
    response = client.delete(url_for('watch', uuid=watch_uuid), headers={'x-api-key': api_key})

    assert response.status_code == 500
    assert response.json['watch_deleted'] is False
    assert datastore.data['watching'][watch_uuid] is watch
    assert os.path.isfile(watch_path)

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert reloaded.data['watching'][watch_uuid]['title'] == 'Keep on disk'
    finally:
        reloaded.stop_thread = True


def test_tag_delete_filesystem_failure_returns_error_and_does_not_resurrect(client, datastore_path, monkeypatch):
    import importlib
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    tag_uuid = datastore.add_tag('Tag delete filesystem failure')
    tag = datastore.data['settings']['application']['tags'][tag_uuid]
    tag_path = os.path.join(tag.data_dir, 'tag.json')
    with open(tag_path, 'rb') as tag_file:
        original_data = tag_file.read()

    tags_module = importlib.import_module('changedetectionio.model.Tags')
    real_rename = tags_module.os.rename

    def fail_tag_rename(source, destination):
        if os.path.normcase(os.path.abspath(source)) == os.path.normcase(os.path.abspath(tag.data_dir)):
            raise OSError('simulated tag rename failure')
        return real_rename(source, destination)

    monkeypatch.setattr(tags_module.os, 'rename', fail_tag_rename)

    response = client.delete(url_for('tag', uuid=tag_uuid), headers={'x-api-key': api_key})

    assert response.status_code == 500
    assert response.json['tag_deleted'] is False
    assert tag_uuid in datastore.data['settings']['application']['tags']
    with open(tag_path, 'rb') as tag_file:
        assert tag_file.read() == original_data

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert reloaded.data['settings']['application']['tags'][tag_uuid]['title'] == 'Tag delete filesystem failure'
    finally:
        reloaded.stop_thread = True


def test_api_failed_url_update_preserves_favicon_and_edit_flag(client, live_server, monkeypatch):
    from changedetectionio.store import file_saving_datastore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    old_url = url_for('test_endpoint', _external=True)
    uuid = datastore.add_watch(url=old_url, extras={'paused': True})
    watch = datastore.data['watching'][uuid]
    watch.reset_watch_edited_flag()
    favicon_path = os.path.join(watch.data_dir, 'favicon.ico')
    favicon_bytes = b'cached favicon bytes'
    with open(favicon_path, 'wb') as favicon_file:
        favicon_file.write(favicon_bytes)
    watch_path = os.path.join(watch.data_dir, 'watch.json')
    with open(watch_path, 'rb') as watch_file:
        original_watch_bytes = watch_file.read()

    def fail_watch_save(*_args, **_kwargs):
        raise OSError('simulated watch write failure')

    monkeypatch.setattr(file_saving_datastore, 'save_entity_atomic', fail_watch_save)
    response = client.put(
        url_for('watch', uuid=uuid),
        data=json.dumps({'url': 'https://example.com/new-product'}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )

    assert response.status_code == 500
    assert watch['url'] == old_url
    assert watch.was_edited is False
    assert watch._pending_favicon_clear is False
    assert open(favicon_path, 'rb').read() == favicon_bytes
    with open(watch_path, 'rb') as watch_file:
        assert watch_file.read() == original_watch_bytes

    delete_all_watches(client)


def test_api_post_reports_initial_watch_write_failure(client, live_server, monkeypatch):
    from changedetectionio.store import file_saving_datastore
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    existing_uuid = datastore.add_watch(
        url='https://example.com/existing',
        extras={'paused': True, 'title': 'Must remain'},
    )
    existing_watch = datastore.data['watching'][existing_uuid]
    existing_path = os.path.join(existing_watch.data_dir, 'watch.json')
    with open(existing_path, 'rb') as watch_file:
        existing_bytes = watch_file.read()

    def fail_watch_save(*_args, **kwargs):
        if kwargs.get('entity_type') == 'watch':
            raise OSError('simulated watch write failure')
        raise AssertionError('Unexpected non-watch entity write')

    test_url = url_for('test_endpoint', _external=True)
    with monkeypatch.context() as patcher:
        patcher.setattr(file_saving_datastore, 'save_entity_atomic', fail_watch_save)
        response = client.post(
            url_for('createwatch'),
            data=json.dumps({
                'url': test_url,
                'processor': 'restock_diff',
                'paused': True,
                'processor_config_restock_diff': {'follow_price_changes': False},
            }),
            headers={'content-type': 'application/json', 'x-api-key': api_key},
        )

    assert response.status_code == 500
    failed_uuid = response.json['uuid']
    assert failed_uuid
    assert response.json['watch_created'] is False
    assert response.json['watch_saved'] is False
    assert response.json['processor_config_saved'] is None
    assert response.json['cleanup_complete'] is True
    assert 'creation failed' in response.json['error'].lower()
    assert failed_uuid not in datastore.data['watching']
    assert not os.path.exists(os.path.join(datastore.datastore_path, failed_uuid))
    with open(existing_path, 'rb') as watch_file:
        assert watch_file.read() == existing_bytes

    reloaded = ChangeDetectionStore(datastore_path=datastore.datastore_path, include_default_watches=False)
    try:
        assert failed_uuid not in reloaded.data['watching']
    finally:
        reloaded.stop_thread = True

    # A retry of the same request is a single successful creation, not completion of
    # the failed in-memory object from the first attempt.
    response = client.post(
        url_for('createwatch'),
        data=json.dumps({
            'url': test_url,
            'processor': 'restock_diff',
            'paused': True,
            'processor_config_restock_diff': {'follow_price_changes': False},
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert response.status_code == 201
    successful_uuid = response.json['uuid']
    assert successful_uuid != failed_uuid
    assert sum(watch['url'] == test_url for watch in datastore.data['watching'].values()) == 1

    delete_all_watches(client)


def test_api_post_reports_incomplete_cleanup_after_initial_write_failure(client, live_server, monkeypatch):
    from changedetectionio.store import file_saving_datastore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    real_rmdir = os.rmdir

    def fail_cleanup(path, *args, **kwargs):
        if os.path.dirname(os.path.abspath(path)) == os.path.abspath(datastore.datastore_path):
            raise PermissionError('simulated cleanup failure')
        return real_rmdir(path, *args, **kwargs)

    def fail_watch_save(*_args, **kwargs):
        if kwargs.get('entity_type') == 'watch':
            raise OSError('simulated watch write failure')
        raise AssertionError('Unexpected non-watch entity write')

    with monkeypatch.context() as patcher:
        patcher.setattr(file_saving_datastore, 'save_entity_atomic', fail_watch_save)
        patcher.setattr(os, 'rmdir', fail_cleanup)
        response = client.post(
            url_for('createwatch'),
            data=json.dumps({
                'url': url_for('test_endpoint', _external=True),
                'processor': 'restock_diff',
                'paused': True,
            }),
            headers={'content-type': 'application/json', 'x-api-key': api_key},
        )

    assert response.status_code == 500
    assert response.json['watch_created'] is False
    assert response.json['watch_saved'] is False
    assert response.json['cleanup_complete'] is False
    assert 'cleanup was incomplete' in response.json['error'].lower()
    failed_uuid = response.json['uuid']
    assert failed_uuid not in datastore.data['watching']
    failed_dir = os.path.join(datastore.datastore_path, failed_uuid)
    assert os.path.isdir(failed_dir)
    real_rmdir(failed_dir)

    delete_all_watches(client)


def test_api_processor_atomic_replace_failure_preserves_config(client, live_server, monkeypatch):
    from changedetectionio import processors
    from changedetectionio.store import file_saving_datastore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    test_url = url_for('test_endpoint', _external=True)
    uuid = datastore.add_watch(url=test_url, extras={'processor': 'restock_diff', 'paused': True})
    assert processors.save_processor_config(datastore, uuid, {
        'restock_diff': {'follow_price_changes': True, 'price_change_min': 10.0}
    })
    watch = datastore.data['watching'][uuid]
    watch_path = os.path.join(watch.data_dir, 'watch.json')
    config_path = os.path.join(watch.data_dir, 'restock_diff.json')
    with open(config_path, 'rb') as config_file:
        original_config = config_file.read()
    real_replace = file_saving_datastore.os.replace

    def fail_processor_replace(source, destination):
        if os.path.basename(destination) == 'restock_diff.json':
            raise OSError('simulated processor config replacement failure')
        return real_replace(source, destination)

    monkeypatch.setattr(file_saving_datastore.os, 'replace', fail_processor_replace)
    response = client.put(
        url_for('watch', uuid=uuid),
        data=json.dumps({
            'title': 'Watch fields persisted',
            'processor_config_restock_diff': {'follow_price_changes': False},
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )

    assert response.status_code == 500
    assert response.json['watch_saved'] is True
    assert response.json['processor_config_saved'] is False
    assert 'Watch settings were saved' in response.json['error']
    with open(watch_path, encoding='utf-8') as watch_file:
        assert json.load(watch_file)['title'] == 'Watch fields persisted'
    with open(config_path, 'rb') as config_file:
        assert config_file.read() == original_config

    delete_all_watches(client)


def test_api_watch_get_returns_resolved_restock_processor_config(client, live_server, measure_memory_usage, datastore_path):
    """
    GET /api/v1/watch/{uuid} must include processor_config_restock_diff and
    processor_config_restock_diff_source in the response.

    Two cases:
    - Watch-level config only: source == 'watch', config reflects the watch's own settings.
    - Tag with overrides_watch=True: source == 'tag:<uuid>', config reflects the tag's settings
      regardless of what the watch itself has stored.
    """
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')
    test_url = url_for('test_endpoint', _external=True)

    # --- Case 1: watch-level config, no tag override ---
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": test_url,
            "processor": "restock_diff",
            "processor_config_restock_diff": {
                "in_stock_processing": "all_changes",
                "follow_price_changes": False,
                "price_change_min": 1.23,
            }
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 201
    watch_uuid = res.json.get('uuid')

    res = client.get(url_for("watch", uuid=watch_uuid), headers={'x-api-key': api_key})
    assert res.status_code == 200
    data = res.json
    assert 'processor_config_restock_diff' in data, "GET should include processor_config_restock_diff"
    assert 'processor_config_restock_diff_source' in data, "GET should include processor_config_restock_diff_source"
    assert data['processor_config_restock_diff_source'] == 'watch'
    assert data['processor_config_restock_diff'].get('in_stock_processing') == 'all_changes'
    assert data['processor_config_restock_diff'].get('follow_price_changes') == False
    assert data['processor_config_restock_diff'].get('price_change_min') == 1.23

    # --- Case 2: tag with overrides_watch=True overrides watch-level config ---
    res = client.post(
        url_for("tag"),
        data=json.dumps({
            "title": "Override tag",
            "overrides_watch": True,
            "processor_config_restock_diff": {
                "in_stock_processing": "in_stock_only",
                "follow_price_changes": True,
                "price_change_min": 999.0,
            }
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 201
    tag_uuid = res.json.get('uuid')

    # Assign the tag to the watch
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        data=json.dumps({"tags": [tag_uuid]}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 200

    res = client.get(url_for("watch", uuid=watch_uuid), headers={'x-api-key': api_key})
    assert res.status_code == 200
    data = res.json
    assert data['processor_config_restock_diff_source'] == f'tag:{tag_uuid}', \
        "Source should show the overriding tag UUID"
    assert data['processor_config_restock_diff'].get('in_stock_processing') == 'in_stock_only', \
        "Tag config should override watch-level config"
    assert data['processor_config_restock_diff'].get('price_change_min') == 999.0, \
        "Tag price_change_min should override watch-level value"

    # processor_config_restock_diff is readonly — PUT attempts to set the resolved field should be
    # silently ignored (the field is stripped before the watch is updated, same as other readOnly fields)
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        data=json.dumps({"processor_config_restock_diff": {"in_stock_processing": "off"}}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    # PUT with processor_config_restock_diff is still valid (sets watch-level config),
    # but the GET response continues to reflect the tag override
    assert res.status_code == 200
    res = client.get(url_for("watch", uuid=watch_uuid), headers={'x-api-key': api_key})
    data = res.json
    assert data['processor_config_restock_diff_source'] == f'tag:{tag_uuid}', \
        "Tag override should still be active after PUT"
    assert data['processor_config_restock_diff'].get('in_stock_processing') == 'in_stock_only', \
        "Tag config should still win after PUT attempted to change watch-level config"

    delete_all_watches(client)


def test_api_watch_get_non_object_processor_config_fails_closed(client, live_server):
    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    uuid = datastore.add_watch(
        url=url_for('test_endpoint', _external=True),
        extras={'processor': 'restock_diff', 'paused': True},
    )
    watch = datastore.data['watching'][uuid]
    config_path = os.path.join(watch.data_dir, 'restock_diff.json')
    with open(config_path, 'w', encoding='utf-8') as config_file:
        json.dump(['unexpected', 'array'], config_file)

    response = client.get(url_for('watch', uuid=uuid), headers={'x-api-key': api_key})

    assert response.status_code == 200
    assert response.json['processor_config_restock_diff'] == {}
    assert response.json['processor_config_restock_diff_source'] == 'watch'
    with open(config_path, encoding='utf-8') as config_file:
        assert json.load(config_file) == ['unexpected', 'array']

    delete_all_watches(client)


def test_api_watch_get_invalid_tag_processor_config_fails_closed_without_rewrite(client, datastore_path):
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    tag_uuid = datastore.add_tag('Malformed processor override')
    tag = datastore.data['settings']['application']['tags'][tag_uuid]
    tag.update({
        'overrides_watch': True,
        'processor_config_restock_diff': ['unexpected', 'array'],
    })
    assert tag.commit() is True

    watch_uuid = datastore.add_watch(
        url=url_for('test_endpoint', _external=True),
        extras={'processor': 'restock_diff', 'paused': True, 'tags': [tag_uuid]},
    )
    response = client.get(url_for('watch', uuid=watch_uuid), headers={'x-api-key': api_key})

    assert response.status_code == 200
    assert response.json['processor_config_restock_diff'] == {}
    assert response.json['processor_config_restock_diff_source'] == f'tag:{tag_uuid}'
    assert tag['processor_config_restock_diff'] == ['unexpected', 'array']

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert reloaded.data['settings']['application']['tags'][tag_uuid]['processor_config_restock_diff'] == [
            'unexpected', 'array'
        ]
    finally:
        reloaded.stop_thread = True

    delete_all_watches(client)


def test_api_import_errors_and_background_logs_redact_sensitive_url_data(client, monkeypatch):
    from loguru import logger
    import importlib

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    secret_url = (
        'https://assessment-user:assessment-password@example.com/product'
        '?access_token=assessment-token&signature=private-value'
    )
    safe_url = 'https://example.com/product'

    def fail_add_watch(url, **_kwargs):
        raise OSError(f"write failed for {url}; assessment-password private-value")

    monkeypatch.setattr(datastore, 'add_watch', fail_add_watch)
    response = client.post(url_for('import'), data=secret_url, headers={'x-api-key': api_key})
    assert response.status_code == 500
    assert response.json['failed'][0]['url'] == safe_url
    assert response.json['failed'][0]['error'] == 'Watch creation failed'
    public_body = response.get_data(as_text=True)
    for secret in ('assessment-user', 'assessment-password', 'assessment-token', 'private-value'):
        assert secret not in public_body

    import_module = importlib.import_module('changedetectionio.api.Import')
    monkeypatch.setattr(import_module, 'IMPORT_SWITCH_TO_BACKGROUND_THRESHOLD', 0)
    logged_messages = []
    log_written = threading.Event()

    def capture_log(message):
        line = str(message)
        logged_messages.append(line)
        if 'Background import failed for' in line:
            log_written.set()

    handler_id = logger.add(capture_log, format='{message}')
    try:
        response = client.post(url_for('import'), data=secret_url, headers={'x-api-key': api_key})
        assert response.status_code == 202
        assert log_written.wait(5), 'background import failure was not logged'
    finally:
        logger.remove(handler_id)

    combined_logs = '\n'.join(logged_messages)
    assert safe_url in combined_logs
    for secret in ('assessment-user', 'assessment-password', 'assessment-token', 'private-value'):
        assert secret not in combined_logs


def test_api_tag_cleanup_does_not_resurrect_watch_deleted_during_unlink(client, datastore_path, monkeypatch):
    from changedetectionio.store import ChangeDetectionStore

    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    tag_uuid = datastore.add_tag('Concurrent cleanup')
    watch_uuid = datastore.add_watch(
        url=url_for('test_endpoint', _external=True),
        extras={'paused': True, 'tags': [tag_uuid]},
    )
    stale_watch = datastore.data['watching'][watch_uuid]
    watch_dir = stale_watch.data_dir
    real_mutate_watch = datastore.mutate_watch

    def delete_then_mutate(uuid, mutation):
        if uuid == watch_uuid:
            datastore.delete(uuid)
        return real_mutate_watch(uuid, mutation)

    monkeypatch.setattr(datastore, 'mutate_watch', delete_then_mutate)
    response = client.delete(url_for('tag', uuid=tag_uuid), headers={'x-api-key': api_key})

    assert response.status_code == 500
    assert response.json['tag_deleted'] is False
    assert tag_uuid in datastore.data['settings']['application']['tags']
    assert watch_uuid not in datastore.data['watching']
    assert not os.path.exists(watch_dir)
    assert stale_watch.commit() is False
    assert not os.path.exists(watch_dir)

    reloaded = ChangeDetectionStore(datastore_path=datastore_path, include_default_watches=False)
    try:
        assert watch_uuid not in reloaded.data['watching']
        assert tag_uuid in reloaded.data['settings']['application']['tags']
    finally:
        reloaded.stop_thread = True


def test_api_processor_enum_lists_installed_processors(client, live_server, measure_memory_usage, datastore_path):
    """The static spec only lists the built-in processors, the merged spec must accept every installed
    processor (image_ssim_diff isn't in the static enum, neither is any plugin processor), and still
    reject unknown ones."""
    from changedetectionio.api import build_merged_spec_dict
    from changedetectionio.processors import find_processors

    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')
    headers = {'content-type': 'application/json', 'x-api-key': api_key}
    test_url = url_for('test_endpoint', _external=True)

    installed = {proc_name for _module, proc_name in find_processors()}
    spec_enum = build_merged_spec_dict()['components']['schemas']['WatchBase']['properties']['processor']['enum']
    assert set(spec_enum) == installed

    res = client.post(url_for("createwatch"), headers=headers,
                      data=json.dumps({"url": test_url, "processor": "image_ssim_diff", "paused": True}))
    assert res.status_code == 201, res.data

    res = client.post(url_for("createwatch"), headers=headers,
                      data=json.dumps({"url": test_url, "processor": "no_such_processor"}))
    assert res.status_code == 400

    delete_all_watches(client)


def test_api_conflict_UI_password(client, live_server, measure_memory_usage, datastore_path):


    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    # Enable password check and diff page access bypass
    res = client.post(
        url_for("settings.settings_page"),
        data={"application-password": "foobar", # password is now set! API should still work!
              "application-api_access_token_enabled": "y",
              "requests-time_between_check-minutes": 180,
              'application-fetch_backend': "html_requests"},
        follow_redirects=True
    )

    assert b"Password protection enabled." in res.data

    # Create a watch
    set_original_response(datastore_path=datastore_path)
    test_url = url_for('test_endpoint', _external=True)

    # Create new
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": test_url, "title": "My test URL" }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )

    assert res.status_code == 201


    wait_for_all_checks(client)
    url = url_for("createwatch")
    # Get a listing, it will be the first one
    res = client.get(
        url,
        headers={'x-api-key': api_key}
    )
    assert res.status_code == 200

    assert len(res.json)


def test_api_url_validation(client, live_server, measure_memory_usage, datastore_path):
    """
    Test URL validation for edge cases in both CREATE and UPDATE endpoints.
    Addresses security issues where empty/null/invalid URLs could bypass validation.

    This test ensures that:
    - CREATE endpoint rejects null, empty, and invalid URLs
    - UPDATE endpoint rejects attempts to change URL to null, empty, or invalid
    - UPDATE endpoint allows updating other fields without touching URL
    - URL validation properly checks protocol, format, and safety
    """

    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')
    set_original_response(datastore_path=datastore_path)
    test_url = url_for('test_endpoint', _external=True)

    # Test 1: CREATE with null URL should fail
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": None}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    assert res.status_code == 400, "Creating watch with null URL should fail"

    # Test 2: CREATE with empty string URL should fail
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": ""}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    assert res.status_code == 400, "Creating watch with empty string URL should fail"
    assert b'Invalid or unsupported URL' in res.data or b'required' in res.data.lower()

    # Test 3: CREATE with whitespace-only URL should fail
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": "   "}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    assert res.status_code == 400, "Creating watch with whitespace-only URL should fail"

    # Test 4: CREATE with invalid protocol should fail
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": "javascript:alert(1)"}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    assert res.status_code == 400, "Creating watch with javascript: protocol should fail"

    # Test 5: CREATE with missing protocol should fail
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": "example.com"}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    assert res.status_code == 400, "Creating watch without protocol should fail"

    # Test 6: CREATE with valid URL should succeed (baseline)
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": test_url, "title": "Valid URL test"}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    assert res.status_code == 201, "Creating watch with valid URL should succeed"
    assert is_valid_uuid(res.json.get('uuid'))
    watch_uuid = res.json.get('uuid')
    wait_for_all_checks(client)

    # Test 7: UPDATE to null URL should fail
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({"url": None}),
    )
    assert res.status_code == 400, "Updating watch URL to null should fail"
    # Accept either OpenAPI validation error or our custom validation error
    assert (b'URL cannot be null' in res.data or
            b'Validation failed' in res.data or
            b'validation error' in res.data.lower())

    # Test 8: UPDATE to empty string URL should fail
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({"url": ""}),
    )
    assert res.status_code == 400, "Updating watch URL to empty string should fail"
    # Accept either our custom validation error or OpenAPI/schema validation error
    assert b'URL cannot be empty' in res.data or b'OpenAPI validation' in res.data or b'Invalid or unsupported URL' in res.data

    # Test 9: UPDATE to whitespace-only URL should fail
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({"url": "   \t\n  "}),
    )
    assert res.status_code == 400, "Updating watch URL to whitespace should fail"
    # Accept either our custom validation error or generic validation error
    assert b'URL cannot be empty' in res.data or b'Invalid or unsupported URL' in res.data or b'validation' in res.data.lower()

    # Test 10: UPDATE to invalid protocol should fail (javascript:)
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({"url": "javascript:alert(document.domain)"}),
    )
    assert res.status_code == 400, "Updating watch URL to XSS attempt should fail"
    assert b'Invalid or unsupported URL' in res.data or b'protocol' in res.data.lower()

    # Test 11: UPDATE to file:// protocol should fail (unless ALLOW_FILE_URI is set)
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({"url": "file:///etc/passwd"}),
    )
    assert res.status_code == 400, "Updating watch URL to file:// should fail by default"

    # Test 12: UPDATE other fields without URL should succeed
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({"title": "Updated title without URL change"}),
    )
    assert res.status_code == 200, "Updating other fields without URL should succeed"

    # Test 13: Verify URL is still valid after non-URL update
    res = client.get(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key}
    )
    assert res.json.get('url') == test_url, "URL should remain unchanged"
    assert res.json.get('title') == "Updated title without URL change"

    # Test 14: UPDATE to valid different URL should succeed
    new_valid_url = test_url + "?new=param"
    res = client.put(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({"url": new_valid_url}),
    )
    assert res.status_code == 200, "Updating to valid different URL should succeed"

    # Test 15: Verify URL was actually updated
    res = client.get(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key}
    )
    assert res.json.get('url') == new_valid_url, "URL should be updated to new valid URL"

    # Test 16: CREATE with XSS in URL parameters should fail
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({"url": "http://example.com?xss=<script>alert(1)</script>"}),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
        follow_redirects=True
    )
    # This should fail because of suspicious characters check
    assert res.status_code == 400, "Creating watch with XSS in URL params should fail"

    # Cleanup
    client.delete(
        url_for("watch", uuid=watch_uuid),
        headers={'x-api-key': api_key},
    )
    delete_all_watches(client)


def test_api_time_between_check_validation(client, live_server, measure_memory_usage, datastore_path):
    """
    Test that time_between_check validation works correctly:
    - When time_between_check_use_default is false, at least one time value must be > 0
    - Values must be valid integers
    """
    import json
    from flask import url_for
    
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')
    
    # Test 1: time_between_check_use_default=false with NO time_between_check should fail
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": "https://example.com",
            "time_between_check_use_default": False
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 400, "Should fail when time_between_check_use_default=false with no time_between_check"
    assert b"At least one time interval" in res.data, "Error message should mention time interval requirement"
    
    # Test 2: time_between_check_use_default=false with ALL zeros should fail
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": "https://example.com",
            "time_between_check_use_default": False,
            "time_between_check": {
                "weeks": 0,
                "days": 0,
                "hours": 0,
                "minutes": 0,
                "seconds": 0
            }
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 400, "Should fail when all time values are 0"
    assert b"At least one time interval" in res.data, "Error message should mention time interval requirement"
    
    # Test 3: time_between_check_use_default=false with NULL values should fail
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": "https://example.com",
            "time_between_check_use_default": False,
            "time_between_check": {
                "weeks": None,
                "days": None,
                "hours": None,
                "minutes": None,
                "seconds": None
            }
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 400, "Should fail when all time values are null"
    assert b"At least one time interval" in res.data, "Error message should mention time interval requirement"
    
    # Test 4: time_between_check_use_default=false with valid hours should succeed
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": "https://example.com",
            "time_between_check_use_default": False,
            "time_between_check": {
                "hours": 2
            }
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 201, "Should succeed with valid hours value"
    uuid1 = res.json.get('uuid')
    
    # Test 5: time_between_check_use_default=false with valid minutes should succeed
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": "https://example2.com",
            "time_between_check_use_default": False,
            "time_between_check": {
                "minutes": 30
            }
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 201, "Should succeed with valid minutes value"
    uuid2 = res.json.get('uuid')
    
    # Test 6: time_between_check_use_default=true (or missing) with no time_between_check should succeed (uses defaults)
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": "https://example3.com",
            "time_between_check_use_default": True
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 201, "Should succeed when using default settings"
    uuid3 = res.json.get('uuid')
    
    # Test 7: Default behavior (no time_between_check_use_default field) should use defaults and succeed
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": "https://example4.com"
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 201, "Should succeed with default behavior (using global settings)"
    uuid4 = res.json.get('uuid')
    
    # Test 8: Verify integer type validation - string should fail (OpenAPI validation)
    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            "url": "https://example5.com",
            "time_between_check_use_default": False,
            "time_between_check": {
                "hours": "not_a_number"
            }
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 400, "Should fail when time value is not an integer"
    assert b"Validation failed" in res.data or b"not of type" in res.data, "Should mention validation/type error"
    
    # Cleanup
    for uuid in [uuid1, uuid2, uuid3, uuid4]:
        client.delete(
            url_for("watch", uuid=uuid),
            headers={'x-api-key': api_key},
        )


def test_api_recheck_with_errors_skips_paused(client, live_server, measure_memory_usage, datastore_path):
    """#4506 - ?with_errors=1 filters the list, ?recheck_all=1 only rechecks what was listed and never paused watches."""
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')
    datastore = live_server.app.config['DATASTORE']
    set_original_response(datastore_path=datastore_path)
    headers = {'content-type': 'application/json', 'x-api-key': api_key}

    def create(url, **extra):
        res = client.post(url_for("createwatch"), data=json.dumps({"url": url, **extra}), headers=headers)
        assert res.status_code == 201
        return res.json.get('uuid')

    ok_uuid = create(url_for('test_endpoint', _external=True))
    error_uuid = create(url_for('test_endpoint', status_code=403, _external=True))
    paused_uuid = create(url_for('test_endpoint', status_code=403, _external=True), paused=True)
    wait_for_all_checks(client)

    # Paused never got checked, give it an error so it would match with_errors=1
    datastore.data['watching'][paused_uuid]['last_error'] = "Something went wrong"
    assert not datastore.data['watching'][ok_uuid].get('last_error')
    assert datastore.data['watching'][error_uuid].get('last_error')

    res = client.get(url_for("createwatch", with_errors='1'), headers=headers)
    assert res.status_code == 200
    assert set(res.json.keys()) == {error_uuid, paused_uuid}

    def last_checked():
        return {u: datastore.data['watching'][u]['last_checked'] for u in (ok_uuid, error_uuid, paused_uuid)}

    # Only the erroring, non-paused watch should be rechecked
    before = last_checked()
    time.sleep(1.1)
    res = client.get(url_for("createwatch", with_errors='1', recheck_all='1'), headers=headers)
    assert res.status_code == 200
    assert 'queued 1 watches' in res.json['status']
    wait_for_all_checks(client)
    after = last_checked()
    assert after[error_uuid] != before[error_uuid]
    assert after[ok_uuid] == before[ok_uuid]
    assert after[paused_uuid] == before[paused_uuid]

    # Plain recheck_all still skips the paused watch
    time.sleep(1.1)
    res = client.get(url_for("createwatch", recheck_all='1'), headers=headers)
    assert 'queued 2 watches' in res.json['status']
    wait_for_all_checks(client)
    final = last_checked()
    assert final[ok_uuid] != after[ok_uuid]
    assert final[error_uuid] != after[error_uuid]
    assert final[paused_uuid] == before[paused_uuid]

    delete_all_watches(client)
