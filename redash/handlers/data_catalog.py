import json
import logging
import requests

from flask import request
from flask_login.utils import login_required
from redash.handlers.base import BaseResource
from redash.settings import DATA_CATALOG_DATASET_API,DATA_CATALOG_ACCESS_KEY


class DataCatalogTableDetailsResource(BaseResource):
    @login_required
    def get(self, catalog_type):
        dataset_id = str(request.args.get("dataset_id"))
        # TODO: Abstract out catalog wise logic
        if catalog_type == "datahub":
            platform, table_name, origin = dataset_id[36:-1].split(",")
            url = DATA_CATALOG_DATASET_API.format(
                table_name=table_name, origin=origin, platform=platform
            )
            logging.info("Fetching dataset details from: {url}".format(url=url))
            headers = {
                "X-RestLi-Protocol-Version": "2.0.0", "X-RestLi-Method": "get",
                'Authorization': "Bearer " + DATA_CATALOG_ACCESS_KEY
            }
            response = requests.get(url, headers=headers)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logging.error("Failed to fetch dataset details: {error}".format(error=str(e)))
                return {}
            details = response.json()
            schema_metadata = ownership = dataset_properties = dataset_key = institutional_memory = editable_dataset_properties = {}
            if not details['aspects'].get('schemaMetadata',None):
                return {
                    "name": "",
                    "description": "",
                    "owners": [],
                    "lastRefresh": None,
                    "partitionKeys": "",
                    "properties": {},
                    "docs": [],
                    "error": "Dataset not present over Datahub yet, please reach out to the respective owner to get the catalog added"
                }
            for key in details['aspects'].keys():
                if key == 'schemaMetadata':
                    schema_metadata = details['aspects'][key]
                elif key == 'ownership':
                    ownership = details['aspects'][key]
                elif key == 'datasetProperties':
                    dataset_properties = details['aspects'][key]
                elif key == 'institutionalMemory':
                    institutional_memory = details['aspects'][key]
                elif key == 'datasetKey':
                    dataset_key = details['aspects'][key]
                elif key == 'editableDatasetProperties':
                    editable_dataset_properties = details['aspects'][key]
                else:
                    continue
            fields = schema_metadata.get('value',{}).get('fields',[])
            partition_keys = [field['fieldPath'] for field in fields if field.get('isPartitioningKey',False)]
            return {
                "name": dataset_key.get('value', {}).get('name'),
                "description": editable_dataset_properties.get('value', {}).get('description',"") or dataset_properties.get('value', {}).get('description', ""),
                "owners": [
                    owner_details['owner'].split(':')[-1]
                    for owner_details in ownership.get('value',{}).get('owners', [])
                ],
                "lastRefresh": schema_metadata.get('value',{}).get('lastModified',{}).get('time',None),
                "partitionKeys": ", ".join(partition_keys),
                "properties": dataset_properties.get('value',{}).get('customProperties',{}),
                "docs": institutional_memory.get('value',{}).get('elements',[]),
                "error": ""
            }