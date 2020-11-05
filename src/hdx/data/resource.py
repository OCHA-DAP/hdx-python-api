# -*- coding: utf-8 -*-
"""Resource class containing all logic for creating, checking, and updating resources."""
import logging
from os import remove
from os.path import join
from typing import Optional, List, Tuple, Dict, Union, Any

from hdx.utilities import raisefrom, is_valid_uuid
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_yaml, load_json
from hdx.utilities.path import script_dir_plus_file

import hdx.data.dataset
import hdx.data.filestore_helper
from hdx.data.hdxobject import HDXObject, HDXError
from hdx.data.resource_view import ResourceView
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)


class Resource(HDXObject):
    """Resource class containing all logic for creating, checking, and updating resources.

    Args:
        initial_data (Optional[Dict]): Initial resource metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    def __init__(self, initial_data=None, configuration=None):
        # type: (Optional[Dict], Optional[Configuration]) -> None
        if not initial_data:
            initial_data = dict()
        super(Resource, self).__init__(initial_data, configuration=configuration)
        self.file_to_upload = None

    @staticmethod
    def actions():
        # type: () -> Dict[str, str]
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'resource_show',
            'update': 'resource_update',
            'create': 'resource_create',
            'delete': 'resource_delete',
            'search': 'resource_search',
            'patch': 'resource_patch',
            'datastore_delete': 'datastore_delete',
            'datastore_create': 'datastore_create',
            'datastore_insert': 'datastore_insert',
            'datastore_upsert': 'datastore_upsert',
            'datastore_search': 'datastore_search'
        }

    def update_from_yaml(self, path=join('config', 'hdx_resource_static.yml')):
        # type: (str) -> None
        """Update resource metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_resource_static.yml.

        Returns:
            None
        """
        super(Resource, self).update_from_yaml(path)

    def update_from_json(self, path=join('config', 'hdx_resource_static.json')):
        # type: (str) -> None
        """Update resource metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_resource_static.json.

        Returns:
            None
        """
        super(Resource, self).update_from_json(path)

    @classmethod
    def read_from_hdx(cls, identifier, configuration=None):
        # type: (str, Optional[Configuration]) -> Optional['Resource']
        """Reads the resource given by identifier from HDX and returns Resource object

        Args:
            identifier (str): Identifier of resource
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[Resource]: Resource object if successful read, None if not
        """

        if is_valid_uuid(identifier) is False:
            raise HDXError('%s is not a valid resource id!' % identifier)
        return cls._read_from_hdx_class('resource', identifier, configuration)

    def get_file_type(self):
        # type: () -> Optional[str]
        """Get the resource's file type

        Returns:
            Optional[str]: Resource's file type or None if it has not been set
        """
        return self.data.get('format')

    def set_file_type(self, file_type):
        # type: (str) -> None
        """Set the resource's file type

        Args:
            file_type (str): resource's file type

        Returns:
            None
        """
        self.data['format'] = file_type.lower()

    def get_file_to_upload(self):
        # type: () -> Optional[str]
        """Get the file uploaded

        Returns:
            Optional[str]: The file that will be or has been uploaded or None if there isn't one
        """
        return self.file_to_upload

    def set_file_to_upload(self, file_to_upload):
        # type: (str) -> None
        """Delete any existing url and set the file uploaded to the local path provided

        Args:
            file_to_upload (str): Local path to file to upload

        Returns:
            None
        """
        if 'url' in self.data:
            del self.data['url']
        self.file_to_upload = file_to_upload

    def check_url_filetoupload(self):
        # type: () -> None
        """Check if url or file to upload provided for resource and add resource_type and url_type if not supplied

        Returns:
            None
        """
        if self.file_to_upload is None:
            if 'url' in self.data:
                if 'resource_type' not in self.data:
                    self.data['resource_type'] = 'api'
                if 'url_type' not in self.data:
                    self.data['url_type'] = 'api'
            else:
                raise HDXError('Either a url or a file to upload must be supplied!')
        else:
            if 'url' in self.data:
                if self.data['url'] != hdx.data.filestore_helper.FilestoreHelper.temporary_url:
                    raise HDXError('Either a url or a file to upload must be supplied not both!')
            if 'resource_type' not in self.data:
                self.data['resource_type'] = 'file.upload'
            if 'url_type' not in self.data:
                self.data['url_type'] = 'upload'
            if 'tracking_summary' in self.data:
                del self.data['tracking_summary']

    def check_required_fields(self, ignore_fields=list()):
        # type: (List[str]) -> None
        """Check that metadata for resource is complete. The parameter ignore_fields should be set if required to
        any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        self.check_url_filetoupload()
        self._check_required_fields('resource', ignore_fields)

    def update_in_hdx(self, **kwargs):
        # type: (Any) -> None
        """Check if resource exists in HDX and if so, update it

        Args:
            **kwargs: See below
            operation (string): Operation to perform eg. patch. Defaults to update.

        Returns:
            None
        """
        self._check_load_existing_object('resource', 'id')
        if self.file_to_upload and 'url' in self.data:
            del self.data['url']
        self._merge_hdx_update('resource', 'id', self.file_to_upload, True, **kwargs)

    def create_in_hdx(self, **kwargs):
        # type: (Any) -> None
        """Check if resource exists in HDX and if so, update it, otherwise create it

        Returns:
            None
        """
        if 'ignore_check' not in kwargs:  # allow ignoring of field checks
            self.check_required_fields()
        id = self.data.get('id')
        if id and self._load_from_hdx('resource', id):
            logger.warning('%s exists. Updating %s' % ('resource', id))
            if self.file_to_upload and 'url' in self.data:
                del self.data['url']
            self._merge_hdx_update('resource', 'id', self.file_to_upload, True, **kwargs)
        else:
            self._save_to_hdx('create', 'name', self.file_to_upload, True)

    def delete_from_hdx(self):
        # type: () -> None
        """Deletes a resource from HDX

        Returns:
            None
        """
        self._delete_from_hdx('resource', 'id')

    def get_dataset(self):
        # type: () -> hdx.data.dataset.Dataset
        """Return dataset containing this resource

        Returns:
            hdx.data.dataset.Dataset: Dataset containing this resource
        """
        package_id = self.data.get('package_id')
        if package_id is None:
            raise HDXError('Resource has no package id!')
        return hdx.data.dataset.Dataset.read_from_hdx(package_id)

    @staticmethod
    def search_in_hdx(query, configuration=None, **kwargs):
        # type: (str, Optional[Configuration], Any) -> List['Resource']
        """Searches for resources in HDX. NOTE: Does not search dataset metadata!

        Args:
            query (str): Query
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs: See below
            order_by (str): A field on the Resource model that orders the results
            offset (int): Apply an offset to the query
            limit (int): Apply a limit to the query
        Returns:
            List[Resource]: List of resources resulting from query
        """

        resources = []
        resource = Resource(configuration=configuration)
        success, result = resource._read_from_hdx('resource', query, 'query', Resource.actions()['search'])
        if result:
            count = result.get('count', None)
            if count:
                for resourcedict in result['results']:
                    resource = Resource(resourcedict, configuration=configuration)
                    resources.append(resource)
        else:
            logger.debug(result)
        return resources

    def download(self, folder=None):
        # type: (Optional[str]) -> Tuple[str, str]
        """Download resource store to provided folder or temporary folder if no folder supplied

        Args:
            folder (Optional[str]): Folder to download resource to. Defaults to None.

        Returns:
            Tuple[str, str]: (URL downloaded, Path to downloaded file)

        """
        # Download the resource
        url = self.data.get('url', None)
        if not url:
            raise HDXError('No URL to download!')
        logger.debug('Downloading %s' % url)
        filename = self.data['name']
        format = '.%s' % self.data['format']
        if format not in filename:
            filename = '%s%s' % (filename, format)
        with Download(full_agent=self.configuration.get_user_agent()) as downloader:
            path = downloader.download_file(url, folder, filename)
            return url, path

    @staticmethod
    def get_all_resource_ids_in_datastore(configuration=None):
        # type: (Optional[Configuration]) -> List[str]
        """Get list of resources that have a datastore returning their ids.

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List[str]: List of resource ids that are in the datastore
        """
        resource = Resource(configuration=configuration)
        success, result = resource._read_from_hdx('datastore', '_table_metadata', 'resource_id',
                                                  Resource.actions()['datastore_search'], limit=10000)
        resource_ids = list()
        if not success:
            logger.debug(result)
        else:
            for record in result['records']:
                resource_ids.append(record['name'])
        return resource_ids

    def has_datastore(self):
        # type: () -> bool
        """Check if the resource has a datastore.

        Returns:
            bool: Whether the resource has a datastore or not
        """
        success, result = self._read_from_hdx('datastore', self.data['id'], 'resource_id',
                                              self.actions()['datastore_search'])
        if not success:
            logger.debug(result)
        else:
            if result:
                return True
        return False

    def delete_datastore(self):
        # type: () -> None
        """Delete a resource from the HDX datastore

        Returns:
            None
        """
        success, result = self._read_from_hdx('datastore', self.data['id'], 'resource_id',
                                              self.actions()['datastore_delete'],
                                              force=True)
        if not success:
            logger.debug(result)

    def create_datastore(self, schema=None, primary_key=None,
                         delete_first=0, path=None):
        # type: (Optional[List[Dict]], Optional[str], int, Optional[str]) -> None
        """For tabular data, create a resource in the HDX datastore which enables data preview in HDX. If no schema is provided
        all fields are assumed to be text. If path is not supplied, the file is first downloaded from HDX.

        Args:
            schema (List[Dict]): List of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}. Defaults to None.
            primary_key (Optional[str]): Primary key of schema. Defaults to None.
            delete_first (int): Delete datastore before creation. 0 = No, 1 = Yes, 2 = If no primary key. Defaults to 0.
            path (Optional[str]): Local path to file that was uploaded. Defaults to None.

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
        if path is None:
            # Download the resource
            url, path = self.download()
            delete_after_download = True
        else:
            url = path
            delete_after_download = False

        def convert_to_text(extended_rows):
            for number, headers, row in extended_rows:
                for i, val in enumerate(row):
                    row[i] = str(val)
                yield (number, headers, row)

        with Download(full_agent=self.configuration.get_user_agent()) as downloader:
            try:
                stream = downloader.get_tabular_stream(path, headers=1, post_parse=[convert_to_text],
                                                       bytes_sample_size=1000000)
                nonefieldname = False
                if schema is None:
                    schema = list()
                    for fieldname in stream.headers:
                        if fieldname is not None:
                            schema.append({'id': fieldname, 'type': 'text'})
                        else:
                            nonefieldname = True
                data = {'resource_id': self.data['id'], 'force': True, 'fields': schema, 'primary_key': primary_key}
                self._write_to_hdx('datastore_create', data, 'resource_id')
                if primary_key is None:
                    method = 'insert'
                else:
                    method = 'upsert'
                logger.debug('Uploading data from %s to datastore' % url)
                offset = 0
                chunksize = 100
                rowset = stream.read(keyed=True, limit=chunksize)
                while len(rowset) != 0:
                    if nonefieldname:
                        for row in rowset:
                            del row[None]
                    data = {'resource_id': self.data['id'], 'force': True, 'method': method, 'records': rowset}
                    self._write_to_hdx('datastore_upsert', data, 'resource_id')
                    rowset = stream.read(keyed=True, limit=chunksize)
                    logger.debug('Uploading: %s' % offset)
                    offset += chunksize
            except Exception as e:
                raisefrom(HDXError, 'Upload to datastore of %s failed!' % url, e)
            finally:
                if delete_after_download:
                    remove(path)

    def create_datastore_from_dict_schema(self, data, delete_first=0, path=None):
        # type: (dict, int, Optional[str]) -> None
        """For tabular data, create a resource in the HDX datastore which enables data preview in HDX from a dictionary
        containing a list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key.
        If path is not supplied, the file is first downloaded from HDX.

        Args:
            data (dict): Dictionary containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}
            delete_first (int): Delete datastore before creation. 0 = No, 1 = Yes, 2 = If no primary key. Defaults to 0.
            path (Optional[str]): Local path to file that was uploaded. Defaults to None.

        Returns:
            None
        """
        schema = data['schema']
        primary_key = data.get('primary_key')
        self.create_datastore(schema, primary_key, delete_first, path=path)

    def create_datastore_from_yaml_schema(self, yaml_path, delete_first=0,
                                          path=None):
        # type: (str, Optional[int], Optional[str]) -> None
        """For tabular data, create a resource in the HDX datastore which enables data preview in HDX from a YAML file
        containing a list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key.
        If path is not supplied, the file is first downloaded from HDX.

        Args:
            yaml_path (str): Path to YAML file containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}
            delete_first (int): Delete datastore before creation. 0 = No, 1 = Yes, 2 = If no primary key. Defaults to 0.
            path (Optional[str]): Local path to file that was uploaded. Defaults to None.

        Returns:
            None
        """
        data = load_yaml(yaml_path)
        self.create_datastore_from_dict_schema(data, delete_first, path=path)

    def create_datastore_from_json_schema(self, json_path, delete_first=0, path=None):
        # type: (str, int, Optional[str]) -> None
        """For tabular data, create a resource in the HDX datastore which enables data preview in HDX from a JSON file
        containing a list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key.
        If path is not supplied, the file is first downloaded from HDX.

        Args:
            json_path (str): Path to JSON file containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}
            delete_first (int): Delete datastore before creation. 0 = No, 1 = Yes, 2 = If no primary key. Defaults to 0.
            path (Optional[str]): Local path to file that was uploaded. Defaults to None.

        Returns:
            None
        """
        data = load_json(json_path)
        self.create_datastore_from_dict_schema(data, delete_first, path=path)

    def create_datastore_for_topline(self, delete_first=0, path=None):
        # type: (int, Optional[str]) -> None
        """For tabular data, create a resource in the HDX datastore which enables data preview in HDX using the built in
        YAML definition for a topline. If path is not supplied, the file is first downloaded from HDX.

        Args:
            delete_first (int): Delete datastore before creation. 0 = No, 1 = Yes, 2 = If no primary key. Defaults to 0.
            path (Optional[str]): Local path to file that was uploaded. Defaults to None.

        Returns:
            None
        """
        data = load_yaml(script_dir_plus_file(join('..', 'hdx_datasource_topline.yml'), Resource))
        self.create_datastore_from_dict_schema(data, delete_first, path=path)

    def update_datastore(self, schema=None, primary_key=None,
                         path=None):
        # type: (Optional[List[Dict]], Optional[str], Optional[str]) -> None
        """For tabular data, update a resource in the HDX datastore which enables data preview in HDX. If no schema is provided
        all fields are assumed to be text. If path is not supplied, the file is first downloaded from HDX.

        Args:
            schema (List[Dict]): List of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}. Defaults to None.
            primary_key (Optional[str]): Primary key of schema. Defaults to None.
            path (Optional[str]): Local path to file that was uploaded. Defaults to None.

        Returns:
            None
        """
        self.create_datastore(schema, primary_key, 2, path=path)

    def update_datastore_from_dict_schema(self, data, path=None):
        # type: (dict, Optional[str]) -> None
        """For tabular data, update a resource in the HDX datastore which enables data preview in HDX from a dictionary
        containing a list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key.
        If path is not supplied, the file is first downloaded from HDX.

        Args:
            data (dict): Dictionary containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}
            path (Optional[str]): Local path to file that was uploaded. Defaults to None.

        Returns:
            None
        """
        self.create_datastore_from_dict_schema(data, 2, path=path)

    def update_datastore_from_yaml_schema(self, yaml_path, path=None):
        # type: (str, Optional[str]) -> None
        """For tabular data, update a resource in the HDX datastore which enables data preview in HDX from a YAML file
        containing a list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key.
        If path is not supplied, the file is first downloaded from HDX.

        Args:
            yaml_path (str): Path to YAML file containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}
            path (Optional[str]): Local path to file that was uploaded. Defaults to None.

        Returns:
            None
        """
        self.create_datastore_from_yaml_schema(yaml_path, 2, path=path)

    def update_datastore_from_json_schema(self, json_path, path=None):
        # type: (str, Optional[str]) -> None
        """For tabular data, update a resource in the HDX datastore which enables data preview in HDX from a JSON file
        containing a list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'} and optionally a primary key.
        If path is not supplied, the file is first downloaded from HDX.

        Args:
            json_path (str): Path to JSON file containing list of fields and types of form {'id': 'FIELD', 'type': 'TYPE'}
            path (Optional[str]): Local path to file that was uploaded. Defaults to None.

        Returns:
            None
        """
        self.create_datastore_from_json_schema(json_path, 2, path=path)

    def update_datastore_for_topline(self, path=None):
        # type: (Optional[str]) -> None
        """For tabular data, update a resource in the HDX datastore which enables data preview in HDX using the built in YAML
        definition for a topline. If path is not supplied, the file is first downloaded from HDX.

        Args:
            path (Optional[str]): Local path to file that was uploaded. Defaults to None.

        Returns:
            None
        """
        self.create_datastore_for_topline(2, path=path)

    def get_resource_views(self):
        # type: () -> List[ResourceView]
        """Get any resource views in the resource

        Returns:
            List[ResourceView]: List of resource views
        """
        return ResourceView.get_all_for_resource(self.data['id'])

    def _get_resource_view(self, resource_view):
        # type: (Union[ResourceView,Dict]) -> ResourceView
        """Get resource view id

        Args:
            resource_view (Union[ResourceView,Dict]): ResourceView metadata from a ResourceView object or dictionary

        Returns:
            ResourceView: ResourceView object
        """
        if isinstance(resource_view, dict):
            resource_view = ResourceView(resource_view, configuration=self.configuration)
        if isinstance(resource_view, ResourceView):
            return resource_view
        raise HDXError('Type %s is not a valid resource view!' % type(resource_view).__name__)

    def add_update_resource_view(self, resource_view):
        # type: (Union[ResourceView,Dict]) -> None
        """Add new resource view in resource with new metadata

        Args:
            resource_view (Union[ResourceView,Dict]): Resource view metadata either from a ResourceView object or a dictionary

        Returns:
            None

        """
        resource_view = self._get_resource_view(resource_view)
        resource_view.create_in_hdx()

    def add_update_resource_views(self, resource_views):
        # type: (List[Union[ResourceView,Dict]]) -> None
        """Add new or update existing resource views in resource with new metadata.

        Args:
            resource_views (List[Union[ResourceView,Dict]]): A list of resource views metadata from ResourceView objects or dictionaries

        Returns:
            None
        """
        if not isinstance(resource_views, list):
            raise HDXError('ResourceViews should be a list!')
        for resource_view in resource_views:
            self.add_update_resource_view(resource_view)

    def reorder_resource_views(self, resource_views):
        # type: (List[Union[ResourceView,Dict,str]]) -> None
        """Order resource views in resource.

        Args:
            resource_views (List[Union[ResourceView,Dict,str]]): A list of either resource view ids or resource views metadata from ResourceView objects or dictionaries

        Returns:
            None
        """
        if not isinstance(resource_views, list):
            raise HDXError('ResourceViews should be a list!')
        ids = list()
        for resource_view in resource_views:
            if isinstance(resource_view, str):
                resource_view_id = resource_view
            else:
                resource_view_id = resource_view['id']
            if is_valid_uuid(resource_view_id) is False:
                raise HDXError('%s is not a valid resource view id!' % resource_view)
            ids.append(resource_view_id)
        _, result = self._read_from_hdx('resource view', self.data['id'], 'id',
                                        ResourceView.actions()['reorder'], order=ids)

    def delete_resource_view(self, resource_view):
        # type: (Union[ResourceView,Dict,str]) -> None
        """Delete a resource view from the resource and HDX

        Args:
            resource_view (Union[ResourceView,Dict,str]): Either a resource view id or resource view metadata either from a ResourceView object or a dictionary

        Returns:
            None
        """
        if isinstance(resource_view, str):
            if is_valid_uuid(resource_view) is False:
                raise HDXError('%s is not a valid resource view id!' % resource_view)
            resource_view = ResourceView({'id': resource_view}, configuration=self.configuration)
        else:
            resource_view = self._get_resource_view(resource_view)
            if 'id' not in resource_view:
                found = False
                title = resource_view.get('title')
                for rv in self.get_resource_views():
                    if resource_view['title'] == rv['title']:
                        resource_view = rv
                        found = True
                        break
                if not found:
                    raise HDXError('No resource views have title %s in this resource!' % title)
        resource_view.delete_from_hdx()

    def enable_dataset_preview(self):
        # type: () -> None
        """Enable dataset preview of resource

        Returns:
            None
        """
        self.data['dataset_preview_enabled'] = 'True'

    def disable_dataset_preview(self):
        # type: () -> None
        """Disable dataset preview of resource

        Returns:
            None
        """
        self.data['dataset_preview_enabled'] = 'False'

