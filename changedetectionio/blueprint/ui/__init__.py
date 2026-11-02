import time
import threading
from blinker import signal
from flask import Blueprint, request, redirect, url_for, flash, render_template, session, current_app, abort
from flask_babel import gettext
from loguru import logger

from changedetectionio.store import ChangeDetectionStore
from changedetectionio.blueprint.ui.edit import construct_blueprint as construct_edit_blueprint
from changedetectionio.blueprint.ui.notification import construct_blueprint as construct_notification_blueprint
from changedetectionio.blueprint.ui.views import construct_blueprint as construct_views_blueprint
from changedetectionio.blueprint.ui.queue import construct_blueprint as construct_queue_blueprint
from changedetectionio.blueprint.ui import diff, preview

def _handle_operations(op, uuids, datastore, worker_pool, update_q, queuedWatchMetaData, watch_check_update, extra_data=None, emit_flash=True):
    """Apply a bulk operation to the given watch uuids.

    Returns a {'message': str, 'type': 'success'|'error'} result describing the outcome so callers
    (e.g. the socket.io handler) can surface server-driven feedback. When emit_flash is True the same
    message is also flashed for the classic form-POST + redirect path.
    """
    from flask import flash

    result_message = None
    result_type = 'success'
    failed_watch_uuids = []

    def persist(uuid, mutation):
        if datastore.mutate_watch(uuid, mutation):
            return True
        failed_watch_uuids.append(uuid)
        return False

    def persisted_message(template, succeeded):
        nonlocal result_type
        if failed_watch_uuids:
            result_type = 'error'
            return gettext("{}: {} succeeded, {} failed to save.").format(
                template, succeeded, len(failed_watch_uuids)
            )
        return gettext(template).format(succeeded)

    if op == 'delete':
        deleted = 0
        for uuid in uuids:
            if datastore.delete(uuid):
                deleted += 1
            else:
                failed_watch_uuids.append(uuid)
        result_message = persisted_message("{} watches deleted", deleted)

    elif op in ('pause', 'unpause', 'mute', 'unmute', 'clear-errors'):
        field, value = {
            'pause': ('paused', True), 'unpause': ('paused', False),
            'mute': ('notification_muted', True), 'unmute': ('notification_muted', False),
            'clear-errors': ('last_error', False),
        }[op]
        succeeded = sum(persist(uuid, lambda watch: watch.update({field: value})) for uuid in uuids)
        templates = {
            'pause': "{} watches paused", 'unpause': "{} watches unpaused",
            'mute': "{} watches muted", 'unmute': "{} watches un-muted",
            'clear-errors': "{} watches errors cleared",
        }
        result_message = persisted_message(templates[op], succeeded)

    elif op == 'mark-viewed':
        succeeded = 0
        for uuid in uuids:
            if datastore.set_last_viewed(uuid, int(time.time()), send_signal=False):
                succeeded += 1
            else:
                failed_watch_uuids.append(uuid)
        result_message = persisted_message("{} watches updated", succeeded)

    elif op == 'mark-unviewed':
        # Watches with fewer than two snapshots have nothing to mark unviewed.
        updated = 0
        for uuid in uuids:
            with datastore.lock:
                watch = datastore.data['watching'].get(uuid)
            if watch and watch.history_n >= 2:
                sorted_keys = sorted(watch.history.keys(), key=lambda x: int(x))
                if datastore.set_last_viewed(uuid, int(sorted_keys[-2]), send_signal=False):
                    updated += 1
                else:
                    failed_watch_uuids.append(uuid)
        result_message = persisted_message("{} watches updated", updated)

    elif op == 'recheck':
        queued = 0
        for uuid in uuids:
            if datastore.data['watching'].get(uuid):
                worker_pool.queue_item_async_safe(update_q, queuedWatchMetaData.PrioritizedItem(priority=1, item={'uuid': uuid}))
                queued += 1
        result_message = gettext("{} watches queued for rechecking").format(queued)

    elif op == 'clear-history':
        cleared = 0
        for uuid in uuids:
            if datastore.clear_watch_history(uuid):
                cleared += 1
            else:
                failed_watch_uuids.append(uuid)
        result_message = persisted_message("{} watches cleared/reset.", cleared)

    elif op == 'notification-default':
        from changedetectionio.notification import USE_SYSTEM_DEFAULT_NOTIFICATION_FORMAT_FOR_WATCH

        def set_notification_defaults(watch):
            watch.update({
                'notification_title': None,
                'notification_body': None,
                'notification_urls': [],
                'notification_format': USE_SYSTEM_DEFAULT_NOTIFICATION_FORMAT_FOR_WATCH,
            })

        succeeded = sum(persist(uuid, set_notification_defaults) for uuid in uuids)
        result_message = persisted_message("{} watches set to use default notification settings", succeeded)

    elif op == 'set-fetch-backend':
        from changedetectionio import content_fetchers
        valid = [f[0] for f in content_fetchers.available_fetchers()] + ['system']
        if extra_data in valid:
            succeeded = sum(persist(uuid, lambda watch: watch.update({'fetch_backend': extra_data})) for uuid in uuids)
            result_message = persisted_message("Browser / fetch method updated on {} watches", succeeded)
        else:
            result_message = gettext("Invalid browser / fetch method selected")
            result_type = 'error'

    elif op == 'set-proxy':
        valid = [''] + (list(datastore.proxy_list.keys()) if datastore.proxy_list else [])
        if (extra_data or '') in valid:
            succeeded = sum(persist(uuid, lambda watch: watch.update({'proxy': (extra_data or '')})) for uuid in uuids)
            result_message = persisted_message("Proxy updated on {} watches", succeeded)
        else:
            result_message = gettext("Invalid proxy selected")
            result_type = 'error'

    elif op == 'assign-tag':
        tag_uuid = datastore.add_tag(title=extra_data) if extra_data else False
        if tag_uuid:
            def assign_tag(watch):
                tags = watch.get('tags')
                if isinstance(tags, str):
                    tags = []
                watch.update({'tags': list(tags or []) + [tag_uuid]})

            succeeded = sum(persist(uuid, assign_tag) for uuid in uuids)
            result_message = persisted_message("{} watches were tagged", succeeded)
        else:
            result_message = gettext("Could not create tag for selected watches")
            result_type = 'error'

    if uuids:
        for uuid in uuids:
            watch_check_update.send(watch_uuid=uuid)

    # Classic form-POST path flashes (shown after the redirect); the socket path uses the returned value
    if result_message and emit_flash:
        if result_type == 'error':
            flash(result_message, 'error')
        else:
            flash(result_message)

    return {'message': result_message, 'type': result_type} if result_message else None

def construct_blueprint(datastore: ChangeDetectionStore, update_q, worker_pool, queuedWatchMetaData, watch_check_update):
    ui_blueprint = Blueprint('ui', __name__, template_folder="templates")
    
    # Register the edit blueprint
    edit_blueprint = construct_edit_blueprint(datastore, update_q, queuedWatchMetaData)
    ui_blueprint.register_blueprint(edit_blueprint)
    
    # Register the notification blueprint
    notification_blueprint = construct_notification_blueprint(datastore)
    ui_blueprint.register_blueprint(notification_blueprint)
    
    # Register the views blueprint
    views_blueprint = construct_views_blueprint(datastore, update_q, queuedWatchMetaData, watch_check_update)
    ui_blueprint.register_blueprint(views_blueprint)

    # Register diff and preview blueprints
    diff_blueprint = diff.construct_blueprint(datastore)
    ui_blueprint.register_blueprint(diff_blueprint)

    preview_blueprint = preview.construct_blueprint(datastore)
    ui_blueprint.register_blueprint(preview_blueprint)

    queue_blueprint = construct_queue_blueprint(datastore, update_q)
    ui_blueprint.register_blueprint(queue_blueprint)

    # Import the login decorator
    from changedetectionio.auth_decorator import login_optionally_required

    @ui_blueprint.route("/clear_history/<uuid_str:uuid>", methods=['POST'])
    @login_optionally_required
    def clear_watch_history(uuid):
        try:
            cleared = datastore.clear_watch_history(uuid)
        except KeyError:
            flash(gettext('Watch not found'), 'error')
        else:
            if cleared:
                flash(gettext("Cleared snapshot history for watch {}").format(uuid))
            else:
                flash(gettext("Could not clear snapshot history for watch {}.").format(uuid), 'error')
        return redirect(url_for('watchlist.index'))

    @ui_blueprint.route("/clear_history", methods=['GET', 'POST'])
    @login_optionally_required
    def clear_all_history():
        if request.method == 'POST':
            confirmtext = request.form.get('confirmtext', '')

            if confirmtext.strip().lower() == gettext('clear').strip().lower():
                # Run in background thread to avoid blocking
                def clear_history_background():
                    # Capture UUIDs first to avoid race conditions
                    with datastore.lock:
                        watch_uuids = list(datastore.data['watching'].keys())
                    logger.info(f"Background: Clearing history for {len(watch_uuids)} watches")
                    cleared_uuids = []
                    failed_uuids = []

                    for uuid in watch_uuids:
                        try:
                            if datastore.clear_watch_history(uuid):
                                cleared_uuids.append(uuid)
                            else:
                                failed_uuids.append(uuid)
                        except Exception as e:
                            failed_uuids.append(uuid)
                            logger.error(f"Error clearing history for watch {uuid}: {e}")

                    if failed_uuids:
                        logger.error(f"Background: Cleared history for {len(cleared_uuids)}/{len(watch_uuids)} "
                                     f"watches; failed {len(failed_uuids)}: {', '.join(failed_uuids)}")
                    else:
                        logger.info(f"Background: Completed clearing history for {len(cleared_uuids)} watches")

                # Start daemon thread
                threading.Thread(target=clear_history_background, daemon=True).start()

                flash(gettext("History clearing started in background"))
            else:
                flash(gettext('Incorrect confirmation text.'), 'error')

            return redirect(url_for('watchlist.index'))

        output = render_template("clear_all_history.html")
        return output

    # Clear all statuses, so we do not see the 'unviewed' class
    @ui_blueprint.route("/form/mark-all-viewed", methods=['POST'])
    @login_optionally_required
    def mark_all_viewed():
        # Save the current newest history as the most recently viewed. Operate on
        # the SAME set the watch list is currently showing (tag/processor/status/
        # search) — via the shared filter — so a filtered "Mark all viewed" only
        # touches what's in view, not every watch.
        from changedetectionio.blueprint.watchlist import filters as wl_filters
        list_filters = wl_filters.list_filters_from_args(datastore, request.args)
        now = int(time.time())

        # Runs SYNCHRONOUSLY, and must stay that way. Re #4021: this used to hand the work to a
        # background thread and redirect immediately, so the watch list re-rendered from a
        # datastore that was still being marked and showed rows as unviewed until a manual
        # refresh. The realtime events that would have corrected it were emitted while the
        # browser was mid-navigation with no socket connected, so they went nowhere.
        # It is cheap enough to do inline: the per-watch signal is suppressed below (that was
        # the actual cost, not the disk write, which measures ~0.05ms per watch).
        marked_count = 0
        try:
            for watch_uuid, watch in datastore.data['watching'].items():
                if not wl_filters.watch_matches_filters(datastore, watch, list_filters):
                    continue

                datastore.set_last_viewed(watch_uuid, now, send_signal=False)
                marked_count += 1

            logger.info(f"Marking complete: {marked_count} watches marked as viewed")
        except Exception as e:
            logger.error(f"Error marking as viewed: {e}")

        # One summary event instead of one per watch, so other open tabs refresh their counters.
        # This page doesn't need it - the redirect below re-renders it from the marked datastore.
        if marked_count:
            general_stats_update = signal('general_stats_update')
            if general_stats_update:
                general_stats_update.send()

        return redirect(url_for('watchlist.index', **wl_filters.filter_query_args(request.args)))

    @ui_blueprint.route("/delete", methods=['POST'])
    @login_optionally_required
    def form_delete():
        uuid = request.args.get('uuid')
        # More for testing, possible to return the first/only
        if uuid == 'first':
            uuid = list(datastore.data['watching'].keys()).pop()

        if uuid != 'all' and not uuid in datastore.data['watching'].keys():
            flash(gettext('The watch by UUID {} does not exist.').format(uuid), 'error')
            return redirect(url_for('watchlist.index'))

        if not datastore.delete(uuid):
            message = (
                gettext('Some watches could not be deleted from storage.') if uuid == 'all' else
                gettext('The watch could not be deleted from storage.')
            )
            flash(message, 'error')
            return redirect(url_for('watchlist.index'))
        flash(gettext('Deleted.'))

        return redirect(url_for('watchlist.index'))

    @ui_blueprint.route("/clone", methods=['POST'])
    @login_optionally_required
    def form_clone():
        uuid = request.args.get('uuid')

        if uuid == 'first':
            uuid = list(datastore.data['watching'].keys()).pop()

        new_uuid = datastore.clone(uuid)
        if not new_uuid:
            # Refused (e.g. PAGE_WATCH_LIMIT) - the reason is already flashed
            return redirect(url_for('watchlist.index'))

        if not datastore.data['watching'].get(uuid).get('paused'):
            worker_pool.queue_item_async_safe(update_q, queuedWatchMetaData.PrioritizedItem(priority=5, item={'uuid': new_uuid}))

        flash(gettext('Cloned, you are editing the new watch.'))

        return redirect(url_for("ui.ui_edit.edit_page", uuid=new_uuid))

    @ui_blueprint.route("/checknow", methods=['POST'])
    @login_optionally_required
    def form_watch_checknow():
        # Forced recheck will skip the 'skip if content is the same' rule (, 'reprocess_existing_data': True})))
        from changedetectionio.blueprint.watchlist import filters as wl_filters
        uuid = request.args.get('uuid')

        if uuid:
            # Single watch - check if already queued or running
            if worker_pool.is_watch_running(uuid) or uuid in update_q.get_queued_uuids():
                flash(gettext("Watch is already queued or being checked."))
            else:
                worker_pool.queue_item_async_safe(update_q, queuedWatchMetaData.PrioritizedItem(priority=1, item={'uuid': uuid}))
                flash(gettext("Queued 1 watch for rechecking."))
        else:
            # Multiple watches - operate on the SAME set the watch list is showing
            # (tag/processor/status/search) via the shared filter, skipping paused.
            list_filters = wl_filters.list_filters_from_args(datastore, request.args)
            watches_to_queue = []
            for k in sorted(datastore.data['watching'].items(), key=lambda item: item[1].get('last_checked', 0)):
                watch_uuid = k[0]
                watch = k[1]
                if not watch['paused'] and watch_uuid and wl_filters.watch_matches_filters(datastore, watch, list_filters):
                    watches_to_queue.append(watch_uuid)

            # If less than 20 watches, queue synchronously for immediate feedback
            if len(watches_to_queue) < 20:
                # Get already queued/running UUIDs once (efficient)
                queued_uuids = set(update_q.get_queued_uuids())
                running_uuids = set(worker_pool.get_running_uuids())

                # Filter out watches that are already queued or running
                watches_to_queue_filtered = []
                for watch_uuid in watches_to_queue:
                    if watch_uuid not in queued_uuids and watch_uuid not in running_uuids:
                        watches_to_queue_filtered.append(watch_uuid)

                # Queue only the filtered watches
                for watch_uuid in watches_to_queue_filtered:
                    worker_pool.queue_item_async_safe(update_q, queuedWatchMetaData.PrioritizedItem(priority=1, item={'uuid': watch_uuid}))

                # Provide feedback about skipped watches
                skipped_count = len(watches_to_queue) - len(watches_to_queue_filtered)
                if skipped_count > 0:
                    flash(gettext("Queued {count} watches for rechecking ({skipped_count} already queued or running).").format(
                        count=len(watches_to_queue_filtered), skipped_count=skipped_count))
                else:
                    if len(watches_to_queue_filtered) == 1:
                        flash(gettext("Queued 1 watch for rechecking."))
                    else:
                        flash(gettext("Queued {} watches for rechecking.").format(len(watches_to_queue_filtered)))
            else:
                # 20+ watches - queue in background thread to avoid blocking HTTP response
                # Capture queued/running state before background thread
                queued_uuids = set(update_q.get_queued_uuids())
                running_uuids = set(worker_pool.get_running_uuids())

                def queue_watches_background():
                    """Background thread to queue watches - discarded after completion."""
                    try:
                        queued_count = 0
                        skipped_count = 0
                        for watch_uuid in watches_to_queue:
                            # Check if already queued or running (state captured at start)
                            if watch_uuid not in queued_uuids and watch_uuid not in running_uuids:
                                worker_pool.queue_item_async_safe(update_q, queuedWatchMetaData.PrioritizedItem(priority=1, item={'uuid': watch_uuid}))
                                queued_count += 1
                            else:
                                skipped_count += 1

                        logger.info(f"Background queueing complete: {queued_count} watches queued, {skipped_count} skipped (already queued/running)")
                    except Exception as e:
                        logger.error(f"Error in background queueing: {e}")

                # Start background thread and return immediately
                thread = threading.Thread(target=queue_watches_background, daemon=True, name="QueueWatches-Background")
                thread.start()

                # Return immediately with approximate message
                flash(gettext("Queueing watches for rechecking in background..."))

        return redirect(url_for('watchlist.index', **wl_filters.filter_query_args(request.args)))

    @ui_blueprint.route("/form/checkbox-operations", methods=['POST'])
    @login_optionally_required
    def form_watch_list_checkbox_operations():
        op = request.form['op']
        uuids = [u.strip() for u in request.form.getlist('uuids') if u]
        extra_data = request.form.get('op_extradata', '').strip()
        _handle_operations(
            datastore=datastore,
            extra_data=extra_data,
            queuedWatchMetaData=queuedWatchMetaData,
            uuids=uuids,
            worker_pool=worker_pool,
            update_q=update_q,
            watch_check_update=watch_check_update,
            op=op,
        )

        return redirect(url_for('watchlist.index'))

    @ui_blueprint.route("/share-url/<uuid_str:uuid>", methods=['POST'])
    @login_optionally_required
    def form_share_put_watch(uuid):
        if not datastore.data['settings']['application']['ui'].get('use_share_watch'):
            abort(403, description="Access denied")

        """Given a watch UUID, upload the info and return a share-link
           the share-link can be imported/added"""
        import requests
        import json
        from copy import deepcopy


        # copy it to memory as trim off what we dont need (history)
        watch = deepcopy(datastore.data['watching'].get(uuid))
        # For older versions that are not a @property
        if (watch.get('history')):
            del (watch['history'])

        # for safety/privacy
        for k in list(watch.keys()):
            if k.startswith('notification_'):
                del watch[k]

        for r in['uuid', 'last_checked', 'last_changed']:
            if watch.get(r):
                del (watch[r])

        # Add the global stuff which may have an impact
        watch['ignore_text'] += datastore.data['settings']['application']['global_ignore_text']
        watch['subtractive_selectors'] += datastore.data['settings']['application']['global_subtractive_selectors']

        watch_json = json.dumps(watch)

        try:
            r = requests.request(method="POST",
                                 data={'watch': watch_json},
                                 url="https://changedetection.io/share/share",
                                 headers={'App-Guid': datastore.data['app_guid']})
            res = r.json()

            # Add to the flask session
            session['share-link'] = f"https://changedetection.io/share/{res['share_key']}"


        except Exception as e:
            logger.error(f"Error sharing -{str(e)}")
            flash(gettext("Could not share, something went wrong while communicating with the share server - {}").format(str(e)), 'error')

        return redirect(url_for('watchlist.index'))

    @ui_blueprint.route("/language/auto-detect", methods=['POST'])
    def delete_locale_language_session_var_if_it_exists():
        """Clear the session locale preference to auto-detect from browser Accept-Language header"""
        if 'locale' in session:
            session.pop('locale', None)
            # Refresh Flask-Babel to clear cached locale
            from flask_babel import refresh
            refresh()
            flash(gettext("Language set to auto-detect from browser"))

        # Check if there's a redirect parameter to return to the same page
        redirect_url = request.args.get('redirect')

        # If redirect is provided and safe, use it
        from changedetectionio.is_safe_url import is_safe_url
        if redirect_url and is_safe_url(redirect_url, current_app):
            return redirect(redirect_url)

        # Otherwise redirect to watchlist
        return redirect(url_for('watchlist.index'))

    return ui_blueprint
