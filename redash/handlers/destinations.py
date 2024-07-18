from flask import make_response, request
from flask_restful import abort
from sqlalchemy.exc import IntegrityError

from redash import models
from redash.destinations import (
    destinations,
    get_configuration_schema_for_destination_type,
)
from redash.handlers.base import BaseResource, get_object_or_404, require_fields
from redash.permissions import require_admin, require_permission
from redash.utils.configuration import ConfigurationContainer, ValidationError
from redash.tasks.destinations import SyncTask, enqueue_destination
from redash.serializers import serialize_destination

class DestinationTypeListResource(BaseResource):
    @require_admin
    def get(self):
        available_destinations = filter(lambda q: not q.deprecated and q.alert_enabled, destinations.values())
        return [q.to_dict() for q in available_destinations]


class DestinationResource(BaseResource):
    @require_admin
    def get(self, destination_id):
        destination = models.NotificationDestination.get_by_id_and_org(
            destination_id, self.current_org
        )
        d = destination.to_dict(all=True)
        self.record_event(
            {
                "action": "view",
                "object_id": destination_id,
                "object_type": "destination",
            }
        )
        return d

    @require_admin
    def post(self, destination_id):
        destination = models.NotificationDestination.get_by_id_and_org(
            destination_id, self.current_org
        )
        req = request.get_json(True)

        schema = get_configuration_schema_for_destination_type(req["type"])
        if schema is None:
            abort(400)

        try:
            destination.type = req["type"]
            destination.name = req["name"]
            destination.options.set_schema(schema)
            destination.options.update(req["options"])
            models.db.session.add(destination)
            models.db.session.commit()
        except ValidationError:
            abort(400)
        except IntegrityError as e:
            if "name" in str(e):
                abort(
                    400,
                    message="Alert Destination with the name {} already exists.".format(
                        req["name"]
                    ),
                )
            abort(500)

        return destination.to_dict(all=True)

    @require_admin
    def delete(self, destination_id):
        destination = models.NotificationDestination.get_by_id_and_org(
            destination_id, self.current_org
        )
        models.db.session.delete(destination)
        models.db.session.commit()

        self.record_event(
            {
                "action": "delete",
                "object_id": destination_id,
                "object_type": "destination",
            }
        )

        return make_response("", 204)


class DestinationListResource(BaseResource):
    def get(self):
        destinations = models.NotificationDestination.all(self.current_org)

        response = {}
        for ds in destinations:
            if ds.id in response:
                continue

            d = ds.to_dict()
            response[ds.id] = d

        self.record_event(
            {
                "action": "list",
                "object_id": "admin/destinations",
                "object_type": "destination",
            }
        )

        return list(response.values())

    @require_admin
    def post(self):
        req = request.get_json(True)
        require_fields(req, ("options", "name", "type"))

        schema = get_configuration_schema_for_destination_type(req["type"])
        if schema is None:
            abort(400)

        config = ConfigurationContainer(req["options"], schema)
        if not config.is_valid():
            abort(400)

        destination = models.NotificationDestination(
            org=self.current_org,
            name=req["name"],
            type=req["type"],
            options=config,
            user=self.current_user,
        )

        try:
            models.db.session.add(destination)
            models.db.session.commit()
        except IntegrityError as e:
            if "name" in str(e):
                abort(
                    400,
                    message="Alert Destination with the name {} already exists.".format(
                        req["name"]
                    ),
                )
            abort(500)

        return destination.to_dict(all=True)
    
class VizDestinationTypeListResource(BaseResource):
    def get(self):
        available_destinations = filter(lambda q: q.visualization_enabled, destinations.values())
        return [q.to_dict() for q in available_destinations]


class VizDestinationResource(BaseResource):
    @require_permission('view_query')
    def get(self, destination_id, visualization_id):
        """
        Retrieve a particular destination
        :param destination_id: ID of destination to fetch
        :param visualization_id:  ID of the visualization corresponding to the destination
        Responds with the :ref:`destination <destination-response-label>` contents.
        """
        visualization = get_object_or_404(models.Visualization.get_by_id_and_org, visualization_id, self.current_org)
        try:
            destination = get_object_or_404(models.Destination.get_by_id_and_org, destination_id, visualization, self.current_org)
        except ValueError:
            abort(400, message=u"No destination with destination ID: {d_id} exists for visualization ID: {v_id}")

        d = serialize_destination(destination)
        self.record_event({
            'action': 'view',
            'object_id': destination_id,
            'object_type': 'viz_destination',
        })
        return {"status": 200, "destination": d}

    @require_permission('edit_query')
    def post(self, destination_id, visualization_id):
        """
        Update a particular destination
        :param destination_id: ID of destination to update
        :param visualization_id:  ID of the visualization corresponding to the destination
        Responds with the updated :ref:`destination <destination-response-label>` contents.
        """
        visualization = get_object_or_404(models.Visualization.get_by_id_and_org, visualization_id, self.current_org)
        try:
            destination = get_object_or_404(models.Destination.get_by_id_and_org, destination_id, visualization, self.current_org)
        except ValueError:
            abort(400, message=u"No destination with destination ID: {d_id} exists for visualization ID: {v_id}")

        req = request.get_json(True)

        schema = get_configuration_schema_for_destination_type(req['type'])
        if schema is None:
            abort(400, message="No such destination exist")

        try:
            destination.type = req['type']
            destination.name = req['name']
            destination.last_modified_by = self.current_user
            destination.options.set_schema(schema)
            destination.options.update(req['options'])
            models.db.session.add(destination)
            models.db.session.commit()
        except ValidationError:
            abort(400)

        return {"status": 200, "destination": serialize_destination(destination)}

    @require_permission('edit_query')
    def delete(self, destination_id, visualization_id):
        """
        Delete a particular destination
        :param destination_id: ID of destination to delete
        :param visualization_id:  ID of the visualization corresponding to the destination
        Responds with the deleted :ref:`destination <destination-response-label>` contents.
        """
        visualization = get_object_or_404(models.Visualization.get_by_id_and_org, visualization_id, self.current_org)
        try:
            destination = get_object_or_404(models.Destination.get_by_id_and_org, destination_id, visualization, self.current_org)
        except ValueError:
            abort(400, message=u"No destination with destination ID: {d_id} exists for visualization ID: {v_id}")

        models.db.session.add(destination)
        destination.is_archived = True
        models.db.session.commit()

        self.record_event({
            'action': 'archive',
            'object_id': destination_id,
            'object_type': 'viz_destination'
        })

        return {"status": 200, "destination": serialize_destination(destination)}


class VizDestinationListResource(BaseResource):
    @require_permission('view_query')
    def get(self, query_id=None, visualization_id=None):
        """
        Retrieve all destinations for a particular visualization_id or query_id. If none is supplied it returns all available destinations

        :param query_id: ID of query whose destinations needs to be fetched
        :param visualization_id:  ID of the visualization whose destinations needs to be fetched

        Responds with the :ref:`destinations [<destination-response-label>]` contents.
        """
        query, visualization = None, None
        if query_id:
            query = get_object_or_404(models.Query.get_by_id_and_org, query_id, self.current_org)
        if visualization_id:
            visualization = get_object_or_404(models.Visualization.get_by_id_and_org, visualization_id, self.current_org)

        try:
            destinations = models.Destination.all(query=query, visualization=visualization)
        except ValueError:
            abort(422, message="Both visualization and query can not be null")
        response = {}
        for ds in destinations:
            if ds.id in response:
                continue

            d = serialize_destination(ds)
            response[ds.id] = d

        self.record_event({
            'action': 'list',
            'object_id': 'admin/viz_destinations',
            'object_type': 'viz_destination',
        })

        return {"status": 200, "destinations": response.values()}

    @require_permission('edit_query')
    def post(self, visualization_id=None):
        """
        Create a new destination
        :param visualization_id:  ID of the visualization corresponding to the destination
        Responds with the :ref:`destination <destination-response-label>` contents.
        """
        req = request.get_json(True)
        req['options']['last_sync_rows'] = -1
        req['options']['last_sync_columns'] = -1
        require_fields(req, ('options', 'name', 'type'))

        schema = get_configuration_schema_for_destination_type(req['type'])
        if schema is None:
            abort(400)

        config = ConfigurationContainer(req['options'], schema)
        if not config.is_valid():
            abort(400)

        destination = models.Destination(name=req['name'],
                                            type=req['type'],
                                            options=config,
                                            user=self.current_user,
                                            last_modified_by=self.current_user,
                                            visualization_id=visualization_id)

        try:
            models.db.session.add(destination)
            models.db.session.commit()
        except IntegrityError as e:
            if 'name' in e.message:
                abort(400, message=u"Visualization Destination with the name {} already exists.".format(req['name']))
            abort(500)
        return {"status": 200, "destination": serialize_destination(destination)}


class SyncResource(BaseResource):
    def post(self, destination_id):
        job = enqueue_destination(destination_id=destination_id,
                                    user_id=self.current_user.id,
                                    sync_type="MANUAL")
        return {'job': job.to_dict()}


class SyncJobResource(BaseResource):
    def get(self, job_id, destination_id=None):
        """
        Retrieve info about a running query job.
        """
        job = SyncTask(job_id=job_id)
        return {'job': job.to_dict()}

    def delete(self, job_id, destination_id=None):
        """
        Cancel a query job in progress.
        """
        job = SyncTask(job_id=job_id)

        job.cancel()
