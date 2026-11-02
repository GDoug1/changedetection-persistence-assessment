
from changedetectionio.strtobool import strtobool
from flask import Blueprint, flash, redirect, url_for
from changedetectionio.auth_decorator import login_optionally_required
from changedetectionio.store import ChangeDetectionStore
from changedetectionio import queuedWatchMetaData
from changedetectionio import worker_pool
from queue import PriorityQueue

PRICE_DATA_TRACK_ACCEPT = 'accepted'
PRICE_DATA_TRACK_REJECT = 'rejected'

def construct_blueprint(datastore: ChangeDetectionStore, update_q: PriorityQueue):

    price_data_follower_blueprint = Blueprint('price_data_follower', __name__)

    @price_data_follower_blueprint.route("/<uuid_str:uuid>/accept", methods=['POST'])
    @login_optionally_required
    def accept(uuid):
        def accept_price_tracking(watch):
            watch['track_ldjson_price_data'] = PRICE_DATA_TRACK_ACCEPT
            watch['processor'] = 'restock_diff'

        if not datastore.clear_watch_history(uuid, mutation=accept_price_tracking):
            flash("Could not save this watch update or clear its history. It may have been deleted.", "error")
            return redirect(url_for("watchlist.index"))
        worker_pool.queue_item_async_safe(update_q, queuedWatchMetaData.PrioritizedItem(priority=1, item={'uuid': uuid}))
        return redirect(url_for("watchlist.index"))

    @price_data_follower_blueprint.route("/<uuid_str:uuid>/reject", methods=['POST'])
    @login_optionally_required
    def reject(uuid):
        if not datastore.mutate_watch(uuid, lambda watch: watch.update({'track_ldjson_price_data': PRICE_DATA_TRACK_REJECT})):
            flash("Could not save this watch update. It may have been deleted.", "error")
        return redirect(url_for("watchlist.index"))


    return price_data_follower_blueprint


