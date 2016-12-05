#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Resource class containing all logic for creating, checking, and updating resources."""
import csv
import logging
from os import unlink
from os.path import join
from typing import Optional, List, Tuple

from hdx.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_yaml, load_json
from hdx.utilities.path import script_dir_plus_file
from .hdxobject import HDXObject, HDXError

logger = logging.getLogger(__name__)


class Resource(HDXObject):
    """Resource class containing all logic for creating, checking, and updating resources.

    Args:
        configuration (Configuration): HDX Configuration
        initial_data (Optional[dict]): Initial resource metadata dictionary. Defaults to None.
    """
    def __init__(self, configuration: Configuration, initial_data: Optional[dict] = None):
        if not initial_data:
            initial_data = dict()
        super(Resource, self).__init__(configuration, initial_data)

    @staticmethod
    def actions() -> dict:
        """Dictionary of actions that can be performed on object

        Returns:
            dict: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'resource_show',
            'update': 'resource_update',
            'create': 'resource_create',
            'delete': 'resource_delete',
            'search': 'resource_search',
            'datastore_delete': 'datastore_delete',
            'datastore_create': 'datastore_create',
            'datastore_insert': 'datastore_insert',
            'datastore_upsert': 'datastore_upsert'
        }

    def update_from_yaml(self, path: str = join('config', 'hdx_resource_static.yml')) -> None:
        """Update resource metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_resource_static.yml.

        Returns:
            None
        """
        super(Resource, self).update_from_yaml(path)

    def update_from_json(self, path: str = join('config', 'hdx_resource_static.json')) -> None:
        """Update resource metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_resource_static.json.

        Returns:
            None
        """
        super(Resource, self).update_from_json(path)

    @staticmethod
    def read_from_hdx(configuration: Configuration, identifier: str) -> Optional['Resource']:
        """Reads the resource given by identifier from HDX and returns Resource object

        Args:
            configuration (Configuration): HDX Configuration
            identifier (str): Identifier of resource

        Returns:
            Optional[Resource]: Resource object if successful read, None if not
        """

        resource = Resource(configuration)
        result = resource._load_from_hdx('resource', identifier)
        if result:
            return resource
        return None

    def check_required_fields(self, ignore_fields: List[str] = list()) -> None:
        """Check that metadata for resource is complete

        Args:
            ignore_fields (List[str]): Any fields to ignore in the check. Default is empty list.

        Returns:
            None
        """
        self._check_required_fields('resource', ignore_fields)

    def update_in_hdx(self) -> None:
        """Check if resource exists in HDX and if so, update it

        Returns:
            None
        """
        self._update_in_hdx('resource', 'id')

    def create_in_hdx(self) -> None:
        """Check if resource exists in HDX and if so, update it, otherwise create it

        Returns:
            None
        """
        self._create_in_hdx('resource', 'id', 'name')

    def delete_from_hdx(self) -> None:
        """Deletes a resource from HDX

        Returns:
            None
        """
        self._delete_from_hdx('resource', 'id')

    @staticmethod
    def search_in_hdx(configuration: Configuration, query: str, **kwargs) -> List['Resource']:
        """Searches for resources in HDX. NOTE: Does not search dataset metadata!

        Args:
            configuration (Configuration): HDX Configuration
            query (str): Query
            **kwargs: See below
            order_by (str): A field on the Resource model that orders the results
            offset (int): Apply an offset to the query
            limit (int): Apply a limit to the query
        Returns:
            List[Resource]: List of resources resulting from query
        """

        resources = []
        resource = Resource(configuration)
        success, result = resource._read_from_hdx('resource', query, 'query', Resource.actions()['search'])
        if result:
            count = result.get('count', None)
            if count:
                for resourcedict in result['results']:
                    resource = Resource(configuration, resourcedict)
                    resources.append(resource)
        else:
            logger.debug(result)
        return resources

    def delete_datastore(self) -> None:
        """Delete a resource from the HDX datastore

        Returns:
            None
        """
        success, result = self._read_from_hdx('datastore', self.data['id'], 'resource_id',
                                              self.actions()['datastore_delete'],
                                              force=True)
        if not success:
            logger.debug(result)

    def download(self, folder: Optional[str] = None) -> Tuple[str, str]:
        """Download resource store to provided folder or temporary folder if no folder supplied

        Args:
            folder (str): Folder to download resource to. Defaults to None.

        Returns:
            Tuple[str, str]: (URL downloaded, Path to downloaded file)

        """
        # Download the resource
        url = self.data.get('url', None)
        if not url:
            raise HDXError('No URL to download!')
        logger.debug('Downloading %s' % url)
        with Download() as download:
            path = download.download_file(url, folder)
            return url, path

    def create_datastore(self, schema: List[dict] = None, primary_key: Optional[str] = None,
                         delete_first: int = 0) -> None:
        """Create a resource in the HDX datastore. If no schema is provided all fields are assumed to be text.

        Args:
            schema (List[dict]): List of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}. Defaults to None.
            primary_key (Optional[str]): Primary key of schema. Defaults to None.
            delete_first (int): Delete datastore before creation. 0 = No, 1 = Yes, 2 = If no primary key. Defaults to 0.

        Returns:
            None
        """
        if delete_first == 0:
            pass
        elif delete_first == 1:
            self.delete_datastore()
        elif delete_first == 2:
            if primary_key is None:
                self.delete_datastore()
        else:
            raise HDXError('delete_first must be 0, 1 or 2! (0 = No, 1 = Yes, 2 = Delete if no primary key)')
        # Download the resource
        url, path = self.download()

        f = None
        try:
            f = open(path, 'r')
            reader = csv.DictReader(f)
            if schema is None:
                schema = list()
                for fieldname in reader.fieldnames:
                    schema.append({'id': fieldname, 'type': 'text'})
            data = {'resource_id': self.data['id'], 'force': True, 'fields': schema, 'primary_key': primary_key}
            self._write_to_hdx('datastore_create', data, 'id')
            rows = [row for row in reader]
            chunksize = 1024
            offset = 0
            if primary_key is None:
                method = 'insert'
            else:
                method = 'upsert'
            logger.debug('Uploading data from %s to datastore' % url)
            while offset < len(rows):
                rowset = rows[offset:offset + chunksize]
                data = {'resource_id': self.data['id'], 'force': True, 'method': method, 'records': rowset}
                self._write_to_hdx('datastore_upsert', data, 'id')
                offset += chunksize
                logger.debug('Uploading: %s' % offset)
        except Exception as e:
            raise HDXError('Upload to datastore of %s failed!' % url) from e
        finally:
            if f:
                f.close()
            unlink(path)

    def create_datastore_from_dict_schema(self, data: dict, delete_first: int = 0) -> None:
        """Creates a resource in the HDX datastore from a YAML file containing a list of fields and types of
        form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key

        Args:
            data (dict): Dictionary containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}
            delete_first (int): Delete datastore before creation. 0 = No, 1 = Yes, 2 = If no primary key. Defaults to 0.

        Returns:
            None
        """
        schema = data['schema']
        primary_key = data.get('primary_key')
        self.create_datastore(schema, primary_key, delete_first)

    def create_datastore_from_yaml_schema(self, path: str, delete_first: int = 0) -> None:
        """Creates a resource in the HDX datastore from a YAML file containing a list of fields and types of
        form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key

        Args:
            path (str): Path to YAML file containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}
            delete_first (int): Delete datastore before creation. 0 = No, 1 = Yes, 2 = If no primary key. Defaults to 0.

        Returns:
            None
        """
        data = load_yaml(path)
        self.create_datastore_from_dict_schema(data, delete_first)

    def create_datastore_from_json_schema(self, path: str, delete_first: int = 0) -> None:
        """Creates a resource in the HDX datastore from a JSON file containing a list of fields and types of
        form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key

        Args:
            path (str): Path to JSON file containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}
            delete_first (int): Delete datastore before creation. 0 = No, 1 = Yes, 2 = If no primary key. Defaults to 0.

        Returns:
            None
        """
        data = load_json(path)
        self.create_datastore_from_dict_schema(data, delete_first)

    def create_datastore_for_topline(self, delete_first: int = 0):
        """Creates a resource in the HDX datastore using the built in YAML definition for a topline

        Args:
            delete_first (int): Delete datastore before creation. 0 = No, 1 = Yes, 2 = If no primary key. Defaults to 0.

        Returns:
            None
        """
        data = load_yaml(script_dir_plus_file(join('..', 'hdx_datasource_topline.yml'), Resource))
        self.create_datastore_from_dict_schema(data, delete_first)

    def update_datastore(self, schema: List[dict] = None, primary_key: Optional[str] = None) -> None:
        """Update a resource in the HDX datastore. If no schema is provided all fields are assumed to be text.

        Args:
            schema (List[dict]): List of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}. Defaults to None.
            primary_key (Optional[str]): Primary key of schema. Defaults to None.

        Returns:
            None
        """
        self.create_datastore(schema, primary_key, 2)

    def update_datastore_from_yaml_schema(self, path: str) -> None:
        """Update a resource in the HDX datastore from a YAML file containing a list of fields and types of
        form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key

        Args:
            path (str): Path to YAML file containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}

        Returns:
            None
        """
        self.create_datastore_from_yaml_schema(path, 2)

    def update_datastore_from_json_schema(self, path: str) -> None:
        """Update a resource in the HDX datastore from a JSON file containing a list of fields and types of
        form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key

        Args:
            path (str): Path to JSON file containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}

        Returns:
            None
        """
        self.create_datastore_from_json_schema(path, 2)

    def update_datastore_for_topline(self) -> None:
        """Update a resource in the HDX datastore using the built in YAML definition for a topline

        Returns:
            None
        """
        self.create_datastore_for_topline(2)
