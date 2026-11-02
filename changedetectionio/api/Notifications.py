from flask_restful import Resource, abort
from flask import request
from . import auth, validate_openapi_request

class Notifications(Resource):
    def __init__(self, **kwargs):
        # datastore is a black box dependency
        self.datastore = kwargs['datastore']

    @auth.check_token
    @validate_openapi_request('getNotifications')
    def get(self):
        """Return Notification URL List."""

        notification_urls = self.datastore.data.get('settings', {}).get('application', {}).get('notification_urls', [])        

        return {
                'notification_urls': notification_urls,
               }, 200
    
    @auth.check_token
    @validate_openapi_request('addNotifications')
    def post(self):
        """Create Notification URLs."""

        json_data = request.get_json()
        notification_urls = json_data.get("notification_urls", [])

        from wtforms import ValidationError
        try:
            validate_notification_urls(notification_urls)
        except ValidationError as e:
            return str(e), 400

        added_urls = []
        save_failed = False

        for url in notification_urls:
            clean_url = url.strip()
            added_url = self.datastore.add_notification_url(clean_url)
            if added_url:
                added_urls.append(added_url)
            else:
                save_failed = True

        if not added_urls:
            if save_failed:
                return {'error': 'Notification settings could not be saved.'}, 500
            return "No valid notification URLs were added", 400

        if save_failed:
            return {'notification_urls': added_urls,
                    'error': 'Some notification URLs could not be saved.'}, 207
        return {'notification_urls': added_urls}, 201
    
    @auth.check_token
    @validate_openapi_request('replaceNotifications')
    def put(self):
        """Replace Notification URLs."""
        json_data = request.get_json()
        notification_urls = json_data.get("notification_urls", [])

        from wtforms import ValidationError
        try:
            validate_notification_urls(notification_urls)
        except ValidationError as e:
            return str(e), 400
        
        if not isinstance(notification_urls, list):
            return "Invalid input format", 400

        clean_urls = [url.strip() for url in notification_urls if isinstance(url, str)]
        application = self.datastore.data['settings']['application']
        previous_urls = list(application.get('notification_urls', []))
        application['notification_urls'] = clean_urls
        if not self.datastore.commit():
            application['notification_urls'] = previous_urls
            return {'error': 'Notification settings could not be saved.'}, 500

        return {'notification_urls': clean_urls}, 200
        
    @auth.check_token
    @validate_openapi_request('deleteNotifications')
    def delete(self):
        """Delete Notification URLs."""

        json_data = request.get_json()
        urls_to_delete = json_data.get("notification_urls", [])
        if not isinstance(urls_to_delete, list):
            abort(400, message="Expected a list of notification URLs.")

        application = self.datastore.data['settings']['application']
        previous_urls = list(application.get('notification_urls', []))
        notification_urls = list(previous_urls)
        deleted = []

        for url in urls_to_delete:
            clean_url = url.strip()
            if clean_url in notification_urls:
                notification_urls.remove(clean_url)
                deleted.append(clean_url)

        if not deleted:
            abort(400, message="No matching notification URLs found.")

        application['notification_urls'] = notification_urls
        if not self.datastore.commit():
            application['notification_urls'] = previous_urls
            return {'error': 'Notification settings could not be saved.'}, 500

        return 'OK', 204
    
def validate_notification_urls(notification_urls):
    from changedetectionio.forms import ValidateAppRiseServers
    validator = ValidateAppRiseServers()
    class DummyForm: pass
    dummy_form = DummyForm()
    field = type("Field", (object,), {"data": notification_urls, "gettext": lambda self, x: x})()
    validator(dummy_form, field)
