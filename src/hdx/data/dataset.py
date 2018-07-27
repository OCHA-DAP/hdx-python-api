# -*- coding: utf-8 -*-
"""Dataset class containing all logic for creating, checking, and updating datasets and associated resources.
"""
import copy
import fnmatch
import logging
import sys
from datetime import datetime
from os.path import join
from typing import List, Union, Optional, Dict, Any

from dateutil import parser
from hdx.location.country import Country
from hdx.utilities import is_valid_uuid
from hdx.utilities import raisefrom
from hdx.utilities.dictandlist import merge_two_dictionaries
from six.moves import range

import hdx.data.organization
import hdx.data.resource
import hdx.data.showcase
import hdx.data.user
from hdx.data.hdxobject import HDXObject, HDXError
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.hdx_tagscleanup import Tags

logger = logging.getLogger(__name__)


class NotRequestableError(HDXError):
    pass


class Dataset(HDXObject):
    """Dataset class enabling operations on datasets and associated resources.

    Args:
        initial_data (Optional[Dict]): Initial dataset metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    max_attempts = 5
    max_int = sys.maxsize
    update_frequencies = {
        '-2': 'Adhoc',
        '-1': 'Never',
        '0': 'Live',
        '1': 'Every day',
        '7': 'Every week',
        '14': 'Every two weeks',
        '30': 'Every month',
        '90': 'Every three months',
        '180': 'Every six months',
        '365': 'Every year',
        'adhoc': '-2',
        'never': '-1',
        'live': '0',
        'every day': '1',
        'every week': '7',
        'every two weeks': '14',
        'every month': '30',
        'every three months': '90',
        'every six months': '180',
        'every year': '365',
        'daily': '1',
        'weekly': '7',
        'fortnightly': '14',
        'every other week': '14',
        'monthly': '30',
        'quarterly': '90',
        'semiannually': '180',
        'semiyearly': '180',
        'annually': '365',
        'yearly': '365'
    }

    def __init__(self, initial_data=None, configuration=None):
        # type: (Optional[Dict], Optional[Configuration]) -> None
        if not initial_data:
            initial_data = dict()
        super(Dataset, self).__init__(dict(), configuration=configuration)
        self.init_resources()
        # workaround: python2 IterableUserDict does not call __setitem__ in __init__,
        # while python3 collections.UserDict does
        for key in initial_data:
            self[key] = initial_data[key]

    @staticmethod
    def actions():
        # type: () -> Dict[str, str]
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'package_show',
            'update': 'package_update',
            'create': 'package_create',
            'delete': 'package_delete',
            'search': 'package_search',
            'reorder': 'package_resource_reorder',
            'list': 'package_list',
            'all': 'current_package_list_with_resources',
            'hxl': 'package_hxl_update'
        }

    def __setitem__(self, key, value):
        # type: (Any, Any) -> None
        """Set dictionary items but do not allow setting of resources

        Args:
            key (Any): Key in dictionary
            value (Any): Value to put in dictionary

        Returns:
            None
        """
        if key == 'resources':
            self.add_update_resources(value, ignore_datasetid=True)
            return
        super(Dataset, self).__setitem__(key, value)

    def separate_resources(self):
        # type: () -> None
        """Move contents of resources key in internal dictionary into self.resources

        Returns:
            None
        """
        self._separate_hdxobjects(self.resources, 'resources', 'name', hdx.data.resource.Resource)

    def init_resources(self):
        # type: () -> None
        """Initialise self.resources list

        Returns:
            None
        """
        self.resources = list()
        """:type : List[hdx.data.resource.Resource]"""

    def _get_resource_from_obj(self, resource):
        # type: (Union[hdx.data.resource.Resource,Dict,str]) -> hdx.data.resource.Resource
        """Add new or update existing resource in dataset with new metadata

        Args:
            resource (Union[hdx.data.resource.Resource,Dict,str]): Either resource id or resource metadata from a Resource object or a dictionary

        Returns:
            hdx.data.resource.Resource: Resource object
        """
        if isinstance(resource, str):
            if is_valid_uuid(resource) is False:
                raise HDXError('%s is not a valid resource id!' % resource)
            resource = hdx.data.resource.Resource.read_from_hdx(resource, configuration=self.configuration)
        elif isinstance(resource, dict):
            resource = hdx.data.resource.Resource(resource, configuration=self.configuration)
        if not isinstance(resource, hdx.data.resource.Resource):
            raise HDXError('Type %s cannot be added as a resource!' % type(resource).__name__)
        return resource

    def add_update_resource(self, resource, ignore_datasetid=False):
        # type: (Union[hdx.data.resource.Resource,Dict,str], bool) -> None
        """Add new or update existing resource in dataset with new metadata

        Args:
            resource (Union[hdx.data.resource.Resource,Dict,str]): Either resource id or resource metadata from a Resource object or a dictionary
            ignore_datasetid (bool): Whether to ignore dataset id in the resource

        Returns:
            None
        """
        resource = self._get_resource_from_obj(resource)
        if 'package_id' in resource:
            if not ignore_datasetid:
                raise HDXError('Resource %s being added already has a dataset id!' % (resource['name']))
        resource_updated = self._addupdate_hdxobject(self.resources, 'name', resource)
        resource_updated.set_file_to_upload(resource.get_file_to_upload())

    def add_update_resources(self, resources, ignore_datasetid=False):
        # type: (List[Union[hdx.data.resource.Resource,Dict,str]], bool) -> None
        """Add new or update existing resources with new metadata to the dataset

        Args:
            resources (List[Union[hdx.data.resource.Resource,Dict,str]]): A list of either resource ids or resources metadata from either Resource objects or dictionaries
            ignore_datasetid (bool): Whether to ignore dataset id in the resource. Defaults to False.

        Returns:
            None
        """
        if not isinstance(resources, list):
            raise HDXError('Resources should be a list!')
        for resource in resources:
            self.add_update_resource(resource, ignore_datasetid)

    def delete_resource(self, resource, delete=True):
        # type: (Union[hdx.data.resource.Resource,Dict,str]) -> bool
        """Delete a resource from the dataset and also from HDX by default

        Args:
            resource (Union[hdx.data.resource.Resource,Dict,str]): Either resource id or resource metadata from a Resource object or a dictionary
            delete (bool): Whetehr to delete the resource from HDX (not just the dataset). Defaults to True.

        Returns:
            bool: True if resource removed or False if not
        """
        if isinstance(resource, str):
            if is_valid_uuid(resource) is False:
                raise HDXError('%s is not a valid resource id!' % resource)
        return self._remove_hdxobject(self.resources, resource, delete=delete)

    def get_resources(self):
        # type: () -> List[hdx.data.resource.Resource]
        """Get dataset's resources

        Returns:
            List[hdx.data.resource.Resource]: list of Resource objects
        """
        return self.resources

    def get_resource(self, index=0):
        # type: (int) -> hdx.data.resource.Resource
        """Get one resource from dataset by index

        Args:
            index (int): Index of resource in dataset. Defaults to 0.

        Returns:
            hdx.data.resource.Resource: Resource object
        """
        return self.resources[index]

    def reorder_resources(self, resource_ids, hxl_update=True):
        # type: (List[str], bool) -> None
        """Reorder resources in dataset according to provided list.
        If only some resource ids are supplied then these are
        assumed to be first and the other resources will stay in
        their original order.

        Args:
            resource_ids (List[str]): List of resource ids
            hxl_update (bool): Whether to call package_hxl_update. Defaults to True.

        Returns:
            None
        """
        dataset_id = self.data.get('id')
        if not dataset_id:
            raise HDXError('Dataset has no id! It must be read, created or updated first.')
        data = {'id': dataset_id,
                'order': resource_ids}
        self._write_to_hdx('reorder', data, 'package_id')
        if hxl_update:
            self.hxl_update()

    def update_from_yaml(self, path=join('config', 'hdx_dataset_static.yml')):
        # type: (str) -> None
        """Update dataset metadata with static metadata from YAML file

        Args:
            path (str): Path to YAML dataset metadata. Defaults to config/hdx_dataset_static.yml.

        Returns:
            None
        """
        super(Dataset, self).update_from_yaml(path)
        self.separate_resources()

    def update_from_json(self, path=join('config', 'hdx_dataset_static.json')):
        # type: (str) -> None
        """Update dataset metadata with static metadata from JSON file

        Args:
            path (str): Path to JSON dataset metadata. Defaults to config/hdx_dataset_static.json.

        Returns:
            None
        """
        super(Dataset, self).update_from_json(path)
        self.separate_resources()

    @staticmethod
    def read_from_hdx(identifier, configuration=None):
        # type: (str, Optional[Configuration]) -> Optional['Dataset']
        """Reads the dataset given by identifier from HDX and returns Dataset object

        Args:
            identifier (str): Identifier of dataset
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[Dataset]: Dataset object if successful read, None if not
        """

        dataset = Dataset(configuration=configuration)
        result = dataset._dataset_load_from_hdx(identifier)
        if result:
            return dataset
        return None

    def _dataset_create_resources(self):
        # type: () -> None
        """Creates resource objects in dataset
        """

        if 'resources' in self.data:
            self.old_data['resources'] = self._copy_hdxobjects(self.resources, hdx.data.resource.Resource, 'file_to_upload')
            self.init_resources()
            self.separate_resources()

    def _dataset_load_from_hdx(self, id_or_name):
        # type: (str) -> bool
        """Loads the dataset given by either id or name from HDX

        Args:
            id_or_name (str): Either id or name of dataset

        Returns:
            bool: True if loaded, False if not
        """

        if not self._load_from_hdx('dataset', id_or_name):
            return False
        self._dataset_create_resources()
        return True

    def check_required_fields(self, ignore_fields=list(), allow_no_resources=False):
        # type: (List[str], bool) -> None
        """Check that metadata for dataset and its resources is complete. The parameter ignore_fields
        should be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].
            allow_no_resources (bool): Whether to allow no resources. Defaults to False.

        Returns:
            None
        """
        if self.is_requestable():
            self._check_required_fields('dataset-requestable', ignore_fields)
        else:
            self._check_required_fields('dataset', ignore_fields)
            if len(self.resources) == 0 and not allow_no_resources:
                raise HDXError('There are no resources! Please add at least one resource!')
            for resource in self.resources:
                ignore_fields = ['package_id']
                resource.check_required_fields(ignore_fields=ignore_fields)

    def _append_resource(self, filestore_resources, resource):
        # type: (List[hdx.data.Resource], hdx.data.Resource) -> None
        """Helper method to append resources adding a url for file upload resources (which will be replaced)

        Args:
            filestore_resources (List[hdx.data.Resource]): List of file upload resources
            resource (hdx.data.Resource): Resource to append

        Returns:
            None
        """
        if resource.get_file_to_upload():
            filestore_resources.append(resource)
            if 'url' in resource.data:
                resource_data = resource.data
            else:
                resource_data = copy.deepcopy(resource.data)
                resource_data['url'] = 'ignore'
        else:
            resource_data = resource.data
        self.data['resources'].append(resource_data)

    def _merge_add_old_resource(self, old_resource, ignore_fields, filestore_resources):
        # type: (hdx.data.Resource, List[str], List[hdx.data.Resource]) -> None
        """Helper method to append old resources

        Args:
            resource (hdx.data.Resource): Resource to append
            ignore_fields (List[str]): Fields to ignore
            filestore_resources (List[hdx.data.Resource]): List of file upload resources

        Returns:
            None
        """
        old_resource.check_required_fields(ignore_fields=ignore_fields)
        self.resources.append(old_resource)
        self._append_resource(filestore_resources, old_resource)

    def _merge_add_resource(self, resource, old_resource, filestore_resources, ignore_fields):
        # type: (hdx.data.Resource, hdx.data.Resource, List[hdx.data.Resource], List[str]) -> None
        """Helper method to append old resources

        Args:
            resource (hdx.data.Resource): Resource to append
            old_resource (hdx.data.Resource): Old resource to check file upload
            filestore_resources (List[hdx.data.Resource]): List of file upload resources
            ignore_fields (List[str]): Fields to ignore

        Returns:
            None
        """
        merge_two_dictionaries(resource, old_resource)
        file_to_upload = old_resource.get_file_to_upload()
        if file_to_upload:
            resource.set_file_to_upload(file_to_upload)
            filestore_resources.append(resource)
            if 'url' in resource.data:
                resource_data = resource.data
            else:
                resource_data = copy.deepcopy(resource.data)
                resource_data['url'] = 'ignore'
        else:
            resource_data = resource.data
        resource.check_required_fields(ignore_fields=ignore_fields)
        self.data['resources'].append(resource_data)

    def _dataset_merge_hdx_update(self, update_resources, update_resources_by_name,
                                  remove_additional_resources, hxl_update):
        # type: (bool, bool, bool, bool) -> None
        """Helper method to check if dataset or its resources exist and update them

        Args:
            update_resources (bool): Whether to update resources
            update_resources_by_name (bool): Compare resource names rather than position in list
            remove_additional_resources (bool): Remove additional resources found in dataset (if updating)
            hxl_update (bool): Whether to call package_hxl_update. Defaults to True.

        Returns:
            None
        """
        # 'old_data' here is the data we want to use for updating while 'data' is the data read from HDX
        merge_two_dictionaries(self.data, self.old_data)
        if 'resources' in self.data:
            del self.data['resources']
        old_resources = self.old_data.get('resources', None)
        filestore_resources = list()
        if update_resources and old_resources:
            ignore_fields = ['package_id']
            self.data['resources'] = list()
            if update_resources_by_name:
                resource_names = set()
                for resource in self.resources:
                    resource_name = resource['name']
                    resource_names.add(resource_name)
                    for old_resource in old_resources:
                        if resource_name == old_resource['name']:
                            logger.warning('Resource exists. Updating %s' % resource_name)
                            self._merge_add_resource(resource, old_resource, filestore_resources, ignore_fields)
                            break
                old_resource_names = set()
                for old_resource in old_resources:
                    old_resource_name = old_resource['name']
                    old_resource_names.add(old_resource_name)
                    if not old_resource_name in resource_names:
                        self._merge_add_old_resource(old_resource, ignore_fields, filestore_resources)
                resources_to_delete = list()
                for i, resource in enumerate(self.resources):
                    resource_name = resource['name']
                    if resource_name not in old_resource_names:
                        if remove_additional_resources:
                            logger.warning('Removing additional resource %s!' % resource_name)
                            resources_to_delete.append(i)
                        else:
                            resource.check_required_fields(ignore_fields=ignore_fields)
                            self.data['resources'].append(resource.data)
                if remove_additional_resources:
                    for i in sorted(resources_to_delete, reverse=True):
                        del self.resources[i]

            else:  # update resources by position
                for i, old_resource in enumerate(old_resources):
                    if len(self.resources) > i:
                        old_resource_name = old_resource['name']
                        resource = self.resources[i]
                        resource_name = resource['name']
                        logger.warning('Resource exists. Updating %s' % resource_name)
                        if resource_name != old_resource_name:
                            logger.warning('Changing resource name to: %s' % old_resource_name)
                        self._merge_add_resource(resource, old_resource, filestore_resources, ignore_fields)
                    else:
                        self._merge_add_old_resource(old_resource, ignore_fields, filestore_resources)
                resources_to_delete = list()
                for i, resource in enumerate(self.resources):
                    if len(old_resources) <= i:
                        if remove_additional_resources:
                            logger.warning('Removing additional resource %s!' % resource['name'])
                            resources_to_delete.append(i)
                        else:
                            resource.check_required_fields(ignore_fields=ignore_fields)
                            self.data['resources'].append(resource.data)
                if remove_additional_resources:
                    for i in sorted(resources_to_delete, reverse=True):
                        del self.resources[i]

        self._save_to_hdx('update', 'id')

        for resource in filestore_resources:
            for created_resource in self.data['resources']:
                if resource['name'] == created_resource['name']:
                    merge_two_dictionaries(resource.data, created_resource)
                    resource.update_in_hdx()
                    merge_two_dictionaries(created_resource, resource.data)
                    break
        self.init_resources()
        self.separate_resources()
        if hxl_update:
            self.hxl_update()

    def update_in_hdx(self, update_resources=True, update_resources_by_name=True,
                      remove_additional_resources=False, hxl_update=True):
        # type: (bool, bool, bool, bool) -> None
        """Check if dataset exists in HDX and if so, update it

        Args:
            update_resources (bool): Whether to update resources. Defaults to True.
            update_resources_by_name (bool): Compare resource names rather than position in list. Defaults to True.
            remove_additional_resources (bool): Remove additional resources found in dataset (if updating). Defaults to False.
            hxl_update (bool): Whether to call package_hxl_update. Defaults to True.

        Returns:
            None
        """
        loaded = False
        if 'id' in self.data:
            self._check_existing_object('dataset', 'id')
            if self._dataset_load_from_hdx(self.data['id']):
                loaded = True
            else:
                logger.warning('Failed to load dataset with id %s' % self.data['id'])
        if not loaded:
            self._check_existing_object('dataset', 'name')
            if not self._dataset_load_from_hdx(self.data['name']):
                raise HDXError('No existing dataset to update!')
        self._dataset_merge_hdx_update(update_resources=update_resources,
                                       update_resources_by_name=update_resources_by_name,
                                       remove_additional_resources=remove_additional_resources,
                                       hxl_update=hxl_update)

    def create_in_hdx(self, allow_no_resources=False, update_resources=True, update_resources_by_name=True,
                      remove_additional_resources=False, hxl_update=True):
        # type: (bool, bool, bool, bool, bool) -> None
        """Check if dataset exists in HDX and if so, update it, otherwise create it

        Args:
            allow_no_resources (bool): Whether to allow no resources. Defaults to False.
            update_resources (bool): Whether to update resources (if updating). Defaults to True.
            update_resources_by_name (bool): Compare resource names rather than position in list. Defaults to True.
            remove_additional_resources (bool): Remove additional resources found in dataset (if updating). Defaults to False.
            hxl_update (bool): Whether to call package_hxl_update. Defaults to True.

        Returns:
            None
        """
        self.check_required_fields(allow_no_resources=allow_no_resources)
        loadedid = None
        if 'id' in self.data:
            if self._dataset_load_from_hdx(self.data['id']):
                loadedid = self.data['id']
            else:
                logger.warning('Failed to load dataset with id %s' % self.data['id'])
        if not loadedid:
            if self._dataset_load_from_hdx(self.data['name']):
                loadedid = self.data['name']
        if loadedid:
            logger.warning('Dataset exists. Updating %s' % loadedid)
            self._dataset_merge_hdx_update(update_resources=update_resources,
                                           update_resources_by_name=update_resources_by_name,
                                           remove_additional_resources=remove_additional_resources,
                                           hxl_update=hxl_update)
            return

        filestore_resources = list()
        if self.resources:
            ignore_fields = ['package_id']
            self.data['resources'] = list()
            for resource in self.resources:
                resource.check_required_fields(ignore_fields=ignore_fields)
                self._append_resource(filestore_resources, resource)
        self._save_to_hdx('create', 'name')
        for resource in filestore_resources:
            for created_resource in self.data['resources']:
                if resource['name'] == created_resource['name']:
                    merge_two_dictionaries(resource.data, created_resource)
                    resource.update_in_hdx()
                    merge_two_dictionaries(created_resource, resource.data)
                    break
        self.init_resources()
        self.separate_resources()
        if hxl_update:
            self.hxl_update()

    def delete_from_hdx(self):
        # type: () -> None
        """Deletes a dataset from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('dataset', 'id')

    def hxl_update(self):
        # type: () -> None
        """Checks dataset for HXL in resources and updates tags and other metadata to trigger HXL preview.

        Returns:
            None
        """
        self._read_from_hdx('dataset', self.data['id'], action=self.actions()['hxl'])

    @classmethod
    def search_in_hdx(cls, query='*:*', configuration=None, page_size=1000, **kwargs):
        # type: (Optional[str], Optional[Configuration], Any) -> List['Dataset']
        """Searches for datasets in HDX

        Args:
            query (Optional[str]): Query (in Solr format). Defaults to '*:*'.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            page_size (int): Size of page to return. Defaults to 1000.
            **kwargs: See below
            fq (string): Any filter queries to apply
            sort (string): Sorting of the search results. Defaults to 'relevance asc, metadata_modified desc'.
            rows (int): Number of matching rows to return. Defaults to all datasets (sys.maxsize).
            start (int): Offset in the complete result for where the set of returned datasets should begin
            facet (string): Whether to enable faceted results. Default to True.
            facet.mincount (int): Minimum counts for facet fields should be included in the results
            facet.limit (int): Maximum number of values the facet fields return (- = unlimited). Defaults to 50.
            facet.field (List[str]): Fields to facet upon. Default is empty.
            use_default_schema (bool): Use default package schema instead of custom schema. Defaults to False.

        Returns:
            List[Dataset]: list of datasets resulting from query
        """

        dataset = Dataset(configuration=configuration)
        total_rows = kwargs.get('rows', cls.max_int)
        start = kwargs.get('start', 0)
        all_datasets = None
        attempts = 0
        while attempts < cls.max_attempts and all_datasets is None:  # if the count values vary for multiple calls, then must redo query
            all_datasets = list()
            counts = set()
            for page in range(total_rows // page_size + 1):
                pagetimespagesize = page * page_size
                kwargs['start'] = start + pagetimespagesize
                rows_left = total_rows - pagetimespagesize
                rows = min(rows_left, page_size)
                kwargs['rows'] = rows
                _, result = dataset._read_from_hdx('dataset', query, 'q', Dataset.actions()['search'], **kwargs)
                datasets = list()
                if result:
                    count = result.get('count', None)
                    if count:
                        counts.add(count)
                        no_results = len(result['results'])
                        for datasetdict in result['results']:
                            dataset = Dataset(configuration=configuration)
                            dataset.old_data = dict()
                            dataset.data = datasetdict
                            dataset._dataset_create_resources()
                            datasets.append(dataset)
                        all_datasets += datasets
                        if no_results < rows:
                            break
                    else:
                        break
                else:
                    logger.debug(result)
            if all_datasets and len(counts) != 1:  # Make sure counts are all same for multiple calls to HDX
                all_datasets = None
                attempts += 1
            else:
                ids = [dataset['id'] for dataset in all_datasets]  # check for duplicates (shouldn't happen)
                if len(ids) != len(set(ids)):
                    all_datasets = None
                    attempts += 1
        if attempts == cls.max_attempts and all_datasets is None:
            raise HDXError('Maximum attempts reached for searching for datasets!')
        return all_datasets

    @staticmethod
    def get_all_dataset_names(configuration=None, **kwargs):
        # type: (Optional[Configuration], Any) -> List[str]
        """Get all dataset names in HDX

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs: See below
            limit (int): Number of rows to return. Defaults to all dataset names.
            offset (int): Offset in the complete result for where the set of returned dataset names should begin

        Returns:
            List[str]: list of all dataset names in HDX
        """
        dataset = Dataset(configuration=configuration)
        dataset['id'] = 'all dataset names'  # only for error message if produced
        return dataset._write_to_hdx('list', kwargs, 'id')

    @classmethod
    def get_all_datasets(cls, configuration=None, page_size=1000, check_duplicates=True, **kwargs):
        # type: (Optional[Configuration], int, bool, Any) -> List['Dataset']
        """Get all datasets in HDX

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            page_size (int): Size of page to return. Defaults to 1000.
            check_duplicates (bool): Whether to check for duplicate datasets. Defaults to True.
            **kwargs: See below
            limit (int): Number of rows to return. Defaults to all datasets (sys.maxsize)
            offset (int): Offset in the complete result for where the set of returned datasets should begin

        Returns:
            List[Dataset]: list of all datasets in HDX
        """

        dataset = Dataset(configuration=configuration)
        dataset['id'] = 'all datasets'  # only for error message if produced
        total_rows = kwargs.get('limit', cls.max_int)
        start = kwargs.get('offset', 0)
        all_datasets = None
        attempts = 0
        while attempts < cls.max_attempts and all_datasets is None:  # if the dataset names vary for multiple calls, then must redo query
            all_datasets = list()
            for page in range(total_rows // page_size + 1):
                pagetimespagesize = page * page_size
                kwargs['offset'] = start + pagetimespagesize
                rows_left = total_rows - pagetimespagesize
                rows = min(rows_left, page_size)
                kwargs['limit'] = rows
                result = dataset._write_to_hdx('all', kwargs, 'id')
                datasets = list()
                if result:
                    no_results = len(result)
                    for datasetdict in result:
                        dataset = Dataset(configuration=configuration)
                        dataset.old_data = dict()
                        dataset.data = datasetdict
                        dataset._dataset_create_resources()
                        datasets.append(dataset)
                    all_datasets += datasets
                    if no_results < rows:
                        break
                else:
                    logger.debug(result)
            if check_duplicates:
                names_list = [dataset['name'] for dataset in all_datasets]
                names = set(names_list)
                if len(names_list) != len(names):  # check for duplicates (shouldn't happen)
                    all_datasets = None
                    attempts += 1
                # This check is no longer valid because of showcases being returned by package_list!
                # elif total_rows == max_int:
                #     all_names = set(Dataset.get_all_dataset_names())  # check dataset names match package_list
                #     if names != all_names:
                #         all_datasets = None
                #         attempts += 1
        if attempts == cls.max_attempts and all_datasets is None:
            raise HDXError('Maximum attempts reached for getting all datasets!')
        return all_datasets

    @staticmethod
    def get_all_resources(datasets):
        # type: (List['Dataset']) -> List[hdx.data.resource.Resource]
        """Get all resources from a list of datasets (such as returned by search)

        Args:
            datasets (List[Dataset]): list of datasets

        Returns:
            List[hdx.data.resource.Resource]: list of resources within those datasets
        """
        resources = []
        for dataset in datasets:
            for resource in dataset.get_resources():
                resources.append(resource)
        return resources

    def get_dataset_date_type(self):
        # type: () -> Optional[str]
        """Get type of dataset date (range, date) or None if no date is set

        Returns:
            Optional[str]: Type of dataset date (range, date) or None if no date is set
        """
        dataset_date = self.data.get('dataset_date', None)
        if dataset_date:
            if '-' in dataset_date:
                return 'range'
            else:
                return 'date'
        else:
            return None

    def get_dataset_date_as_datetime(self):
        # type: () -> Optional[datetime]
        """Get dataset date as datetime.datetime object. For range returns start date.

        Returns:
            Optional[datetime.datetime]: Dataset date in datetime object or None if no date is set
        """
        dataset_date = self.data.get('dataset_date', None)
        if dataset_date:
            if '-' in dataset_date:
                dataset_date = dataset_date.split('-')[0]
            return datetime.strptime(dataset_date, '%m/%d/%Y')
        else:
            return None

    def get_dataset_end_date_as_datetime(self):
        # type: () -> Optional[datetime]
        """Get dataset end date as datetime.datetime object.

        Returns:
            Optional[datetime.datetime]: Dataset date in datetime object or None if no date is set
        """
        dataset_date = self.data.get('dataset_date', None)
        if dataset_date:
            if '-' in dataset_date:
                dataset_date = dataset_date.split('-')[1]
                return datetime.strptime(dataset_date, '%m/%d/%Y')
        return None

    @staticmethod
    def _get_formatted_date(dataset_date, date_format=None):
        # type: (Optional[datetime], Optional[str]) -> Optional[str]
        """Get supplied dataset date as string in specified format. 
        If no format is supplied, an ISO 8601 string is returned.

        Args:
            dataset_date (Optional[datetime.datetime]): dataset date in datetime.datetime format 
            date_format (Optional[str]): Date format. None is taken to be ISO 8601. Defaults to None.

        Returns:
            Optional[str]: Dataset date string or None if no date is set
        """
        if dataset_date:
            if date_format:
                return dataset_date.strftime(date_format)
            else:
                return dataset_date.date().isoformat()
        else:
            return None

    def get_dataset_date(self, date_format=None):
        # type: (Optional[str]) -> Optional[str]
        """Get dataset date as string in specified format. For range returns start date.
        If no format is supplied, an ISO 8601 string is returned.

        Args:
            date_format (Optional[str]): Date format. None is taken to be ISO 8601. Defaults to None.

        Returns:
            Optional[str]: Dataset date string or None if no date is set
        """
        dataset_date = self.get_dataset_date_as_datetime()
        return self._get_formatted_date(dataset_date, date_format)

    def get_dataset_end_date(self, date_format=None):
        # type: (Optional[str]) -> Optional[str]
        """Get dataset date as string in specified format. For range returns start date.
        If no format is supplied, an ISO 8601 string is returned.

        Args:
            date_format (Optional[str]): Date format. None is taken to be ISO 8601. Defaults to None.

        Returns:
            Optional[str]: Dataset date string or None if no date is set
        """
        dataset_date = self.get_dataset_end_date_as_datetime()
        return self._get_formatted_date(dataset_date, date_format)

    def set_dataset_date_from_datetime(self, dataset_date, dataset_end_date=None):
        # type: (datetime) -> None
        """Set dataset date from datetime.datetime object

        Args:
            dataset_date (datetime.datetime): Dataset date
            dataset_end_date (Optional[datetime.datetime]): Dataset end date

        Returns:
            None
        """
        start_date = dataset_date.strftime('%m/%d/%Y')
        if dataset_end_date is None:
            self.data['dataset_date'] = start_date
        else:
            end_date = dataset_end_date.strftime('%m/%d/%Y')
            self.data['dataset_date'] = '%s-%s' % (start_date, end_date)

    @staticmethod
    def _parse_date(dataset_date, date_format):
        # type: (str, Optional[str]) -> datetime
        """Parse dataset date from string using specified format. If no format is supplied, the function will guess.
        For unambiguous formats, this should be fine.

        Args:
            dataset_date (str): Dataset date string
            date_format (Optional[str]): Date format. If None is given, will attempt to guess. Defaults to None.

        Returns:
            datetime.datetime
        """
        if date_format is None:
            try:
                return parser.parse(dataset_date)
            except (ValueError, OverflowError) as e:
                raisefrom(HDXError, 'Invalid dataset date!', e)
        else:
            try:
                return datetime.strptime(dataset_date, date_format)
            except ValueError as e:
                raisefrom(HDXError, 'Invalid dataset date!', e)

    def set_dataset_date(self, dataset_date, dataset_end_date=None, date_format=None):
        # type: (str, Optional[str]) -> None
        """Set dataset date from string using specified format. If no format is supplied, the function will guess.
        For unambiguous formats, this should be fine.

        Args:
            dataset_date (str): Dataset date string
            dataset_end_date (Optional[str]): Dataset end date string
            date_format (Optional[str]): Date format. If None is given, will attempt to guess. Defaults to None.

        Returns:
            None
        """
        parsed_date = self._parse_date(dataset_date, date_format)
        if dataset_end_date is None:
            self.set_dataset_date_from_datetime(parsed_date)
        else:
            parsed_end_date = self._parse_date(dataset_end_date, date_format)
            self.set_dataset_date_from_datetime(parsed_date, parsed_end_date)

    def set_dataset_year_range(self, dataset_year, dataset_end_year=None):
        # type: (Union[str, int], Optional[Union[str, int]]) -> None
        """Set dataset date as a range from year or start and end year.

        Args:
            dataset_year (Union[str, int]): Dataset year given as string or int
            dataset_end_year (Optional[Union[str, int]]): Dataset end year given as string or int

        Returns:
            None
        """
        if isinstance(dataset_year, int):
            dataset_date = '01/01/%d' % dataset_year
        elif isinstance(dataset_year, str):
            dataset_date = '01/01/%s' % dataset_year
        else:
            raise hdx.data.hdxobject.HDXError('dataset_year has type %s which is not supported!' % type(dataset_year).__name__)
        if dataset_end_year is None:
            dataset_end_year = dataset_year
        if isinstance(dataset_end_year, int):
            dataset_end_date = '31/12/%d' % dataset_end_year
        elif isinstance(dataset_end_year, str):
            dataset_end_date = '31/12/%s' % dataset_end_year
        else:
            raise hdx.data.hdxobject.HDXError('dataset_end_year has type %s which is not supported!' % type(dataset_end_year).__name__)
        self.set_dataset_date(dataset_date, dataset_end_date)

    @staticmethod
    def transform_update_frequency(frequency):
        # type: (str) -> str
        """Get numeric update frequency (as string since that is required field format) from textual representation or
        vice versa (eg. 'Every month' = '30', '30' = 'Every month')

        Args:
            frequency (str): Update frequency in one format

        Returns:
            str: Update frequency in alternative format
        """
        return Dataset.update_frequencies.get(frequency.lower())

    def get_expected_update_frequency(self):
        # type: () -> Optional[str]
        """Get expected update frequency (in textual rather than numeric form)

        Returns:
            Optional[str]: Update frequency in textual form or None if the update frequency doesn't exist or is blank.
        """
        days = self.data.get('data_update_frequency', None)
        if days:
            return Dataset.transform_update_frequency(days)
        else:
            return None

    def set_expected_update_frequency(self, update_frequency):
        # type: (str) -> None
        """Set expected update frequency

        Args:
            update_frequency (str): Update frequency

        Returns:
            None
        """
        try:
            int(update_frequency)
        except ValueError:
            update_frequency = Dataset.transform_update_frequency(update_frequency)
        if not update_frequency:
            raise HDXError('Invalid update frequency supplied!')
        self.data['data_update_frequency'] = update_frequency

    def get_tags(self):
        # type: () -> List[str]
        """Return the dataset's list of tags

        Returns:
            List[str]: list of tags or [] if there are none
        """
        return self._get_tags()

    def add_tag(self, tag):
        # type: (str) -> bool
        """Add a tag

        Args:
            tag (str): Tag to add

        Returns:
            bool: True if tag added or False if tag already present
        """
        return self._add_tag(tag)

    def add_tags(self, tags):
        # type: (List[str]) -> bool
        """Add a list of tag

        Args:
            tags (List[str]): list of tags to add

        Returns:
            bool: True if all tags added or False if any already present
        """
        return self._add_tags(tags)

    def remove_tag(self, tag):
        # type: (str) -> bool
        """Remove a tag

        Args:
            tag (str): Tag to remove

        Returns:
            bool: True if tag removed or False if not
        """
        return self._remove_hdxobject(self.data.get('tags'), tag, matchon='name')

    def is_subnational(self):
        # type: () -> bool
        """Return if the dataset is subnational

        Returns:
            bool: True if the dataset is subnational, False if not
        """
        return self.data['subnational'] == '1'

    def set_subnational(self, subnational):
        # type: (bool) -> None
        """Set if dataset is subnational or national

        Args:
            subnational (bool): True for subnational, False for national

        Returns:
            None
        """
        if subnational:
            self.data['subnational'] = '1'
        else:
            self.data['subnational'] = '0'

    def get_location(self, locations=None):
        # type: (Optional[List[str]]) -> List[str]
        """Return the dataset's location

        Args:
            locations (Optional[List[str]]): Valid locations list. Defaults to list downloaded from HDX.

        Returns:
            List[str]: list of locations or [] if there are none
        """
        countries = self.data.get('groups', None)
        if not countries:
            return list()
        return [Locations.get_location_from_HDX_code(x['name'], locations=locations,
                                                     configuration=self.configuration) for x in countries]

    def add_country_location(self, country, exact=True, locations=None, use_live=True):
        # type: (str, bool,Optional[List[str]]) -> bool
        """Add a country. If an iso 3 code is not provided, value is parsed and if it is a valid country name,
        converted to an iso 3 code. If the country is already added, it is ignored.

        Args:
            country (str): Country to add
            exact (bool): True for exact matching or False to allow fuzzy matching. Defaults to True.
            locations (Optional[List[str]]): Valid locations list. Defaults to list downloaded from HDX.
            use_live (bool): Try to get use latest country data from web rather than file in package. Defaults to True.

        Returns:
            bool: True if country added or False if country already present
        """
        iso3, match = Country.get_iso3_country_code_fuzzy(country, use_live=use_live)
        if iso3 is None:
            raise HDXError('Country: %s - cannot find iso3 code!' % country)
        return self.add_other_location(iso3, exact=exact,
                                       alterror='Country: %s with iso3: %s could not be found in HDX list!' %
                                                (country, iso3),
                                       locations=locations)

    def add_country_locations(self, countries, locations=None, use_live=True):
        # type: (List[str], Optional[List[str]]) -> bool
        """Add a list of countries. If iso 3 codes are not provided, values are parsed and where they are valid country
        names, converted to iso 3 codes. If any country is already added, it is ignored.

        Args:
            countries (List[str]): list of countries to add
            locations (Optional[List[str]]): Valid locations list. Defaults to list downloaded from HDX.
            use_live (bool): Try to get use latest country data from web rather than file in package. Defaults to True.

        Returns:
            bool: True if all countries added or False if any already present.
        """
        allcountriesadded = True
        for country in countries:
            if not self.add_country_location(country, locations=locations, use_live=use_live):
                allcountriesadded = False
        return allcountriesadded

    def add_region_location(self, region, locations=None, use_live=True):
        # type: (str, Optional[List[str]]) -> bool
        """Add all countries in a region. If a 3 digit UNStats M49 region code is not provided, value is parsed as a
        region name. If any country is already added, it is ignored.

        Args:
            region (str): M49 region, intermediate region or subregion to add
            locations (Optional[List[str]]): Valid locations list. Defaults to list downloaded from HDX.
            use_live (bool): Try to get use latest country data from web rather than file in package. Defaults to True.

        Returns:
            bool: True if all countries in region added or False if any already present.
        """
        return self.add_country_locations(Country.get_countries_in_region(region, exception=HDXError,
                                                                          use_live=use_live), locations=locations)

    def add_other_location(self, location, exact=True, alterror=None, locations=None):
        # type: (str, bool, Optional[str], Optional[List[str]]) -> bool
        """Add a location which is not a country or region. Value is parsed and compared to existing locations in
        HDX. If the location is already added, it is ignored.

        Args:
            location (str): Location to add
            exact (bool): True for exact matching or False to allow fuzzy matching. Defaults to True.
            alterror (Optional[str]): Alternative error message to builtin if location not found. Defaults to None.
            locations (Optional[List[str]]): Valid locations list. Defaults to list downloaded from HDX.

        Returns:
            bool: True if location added or False if location already present
        """
        hdx_code, match = Locations.get_HDX_code_from_location_partial(location, locations=locations,
                                                                       configuration=self.configuration)
        if hdx_code is None or (exact is True and match is False):
            if alterror is None:
                raise HDXError('Location: %s - cannot find in HDX!' % location)
            else:
                raise HDXError(alterror)
        groups = self.data.get('groups', None)
        hdx_code = hdx_code.lower()
        if groups:
            if hdx_code in [x['name'] for x in groups]:
                return False
        else:
            groups = list()
        groups.append({'name': hdx_code})
        self.data['groups'] = groups
        return True

    def remove_location(self, location):
        # type: (str) -> bool
        """Remove a location. If the location is already added, it is ignored.

        Args:
            location (str): Location to remove

        Returns:
            bool: True if location removed or False if not
        """
        res = self._remove_hdxobject(self.data.get('groups'), location, matchon='name')
        if not res:
            res = self._remove_hdxobject(self.data.get('groups'), location.upper(), matchon='name')
        if not res:
            res = self._remove_hdxobject(self.data.get('groups'), location.lower(), matchon='name')
        return res

    def get_maintainer(self):
        # type: () -> hdx.data.user.User
        """Get the dataset's maintainer.

         Returns:
             User: Dataset's maintainer
        """
        return hdx.data.user.User.read_from_hdx(self.data['maintainer'], configuration=self.configuration)

    def set_maintainer(self, maintainer):
        # type: (Union[hdx.data.user.User,Dict,str]) -> None
        """Set the dataset's maintainer.

         Args:
             maintainer (Union[User,Dict,str]): Either a user id or User metadata from a User object or dictionary.
         Returns:
             None
        """
        if isinstance(maintainer, hdx.data.user.User) or isinstance(maintainer, dict):
            if 'id' not in maintainer:
                maintainer = hdx.data.user.User.read_from_hdx(maintainer['name'], configuration=self.configuration)
            maintainer = maintainer['id']
        elif not isinstance(maintainer, str):
            raise HDXError('Type %s cannot be added as a maintainer!' % type(maintainer).__name__)
        if is_valid_uuid(maintainer) is False:
            raise HDXError('%s is not a valid user id for a maintainer!' % maintainer)
        self.data['maintainer'] = maintainer

    def get_organization(self):
        # type: () -> hdx.data.organization.Organization
        """Get the dataset's organization.

         Returns:
             Organization: Dataset's organization
        """
        return hdx.data.organization.Organization.read_from_hdx(self.data['owner_org'], configuration=self.configuration)

    def set_organization(self, organization):
        # type: (Union[hdx.data.organization.Organization,Dict,str]) -> None
        """Set the dataset's organization.

         Args:
             organization (Union[Organization,Dict,str]): Either an Organization id or Organization metadata from an Organization object or dictionary.
         Returns:
             None
        """
        if isinstance(organization, hdx.data.organization.Organization) or isinstance(organization, dict):
            if 'id' not in organization:
                organization = hdx.data.organization.Organization.read_from_hdx(organization['name'], configuration=self.configuration)
            organization = organization['id']
        elif not isinstance(organization, str):
            raise HDXError('Type %s cannot be added as a organization!' % type(organization).__name__)
        if is_valid_uuid(organization) is False and organization != 'hdx':
            raise HDXError('%s is not a valid organization id!' % organization)
        self.data['owner_org'] = organization

    def get_showcases(self):
        # type: () -> List[hdx.data.showcase.Showcase]
        """Get any showcases the dataset is in

        Returns:
            List[Showcase]: list of showcases
        """
        assoc_result, showcases_dicts = self._read_from_hdx('showcase', self.data['id'], fieldname='package_id',
                                                            action=hdx.data.showcase.Showcase.actions()['list_showcases'])
        showcases = list()
        if assoc_result:
            for showcase_dict in showcases_dicts:
                showcase = hdx.data.showcase.Showcase(showcase_dict, configuration=self.configuration)
                showcases.append(showcase)
        return showcases

    def _get_dataset_showcase_dict(self, showcase):
        # type: (Union[hdx.data.showcase.Showcase, Dict,str]) -> Dict
        """Get dataset showcase dict

        Args:
            showcase (Union[Showcase,Dict,str]): Either a showcase id or Showcase metadata from a Showcase object or dictionary

        Returns:
            dict: dataset showcase dict
        """
        if isinstance(showcase, hdx.data.showcase.Showcase) or isinstance(showcase, dict):
            if 'id' not in showcase:
                showcase = hdx.data.showcase.Showcase.read_from_hdx(showcase['name'])
            showcase = showcase['id']
        elif not isinstance(showcase, str):
            raise HDXError('Type %s cannot be added as a showcase!' % type(showcase).__name__)
        if is_valid_uuid(showcase) is False:
            raise HDXError('%s is not a valid showcase id!' % showcase)
        return {'package_id': self.data['id'], 'showcase_id': showcase}

    def add_showcase(self, showcase, showcases_to_check=None):
        # type: (Union[hdx.data.showcase.Showcase,Dict,str], List[hdx.data.showcase.Showcase]) -> bool
        """Add dataset to showcase

        Args:
            showcase (Union[Showcase,Dict,str]): Either a showcase id or showcase metadata from a Showcase object or dictionary
            showcases_to_check (List[Showcase]): list of showcases against which to check existence of showcase. Defaults to showcases containing dataset.

        Returns:
            bool: True if the showcase was added, False if already present
        """
        dataset_showcase = self._get_dataset_showcase_dict(showcase)
        if showcases_to_check is None:
            showcases_to_check = self.get_showcases()
        for showcase in showcases_to_check:
            if dataset_showcase['showcase_id'] == showcase['id']:
                return False
        showcase = hdx.data.showcase.Showcase({'id': dataset_showcase['showcase_id']}, configuration=self.configuration)
        showcase._write_to_hdx('associate', dataset_showcase, 'package_id')
        return True

    def add_showcases(self, showcases, showcases_to_check=None):
        # type: (List[Union[hdx.data.showcase.Showcase,Dict,str]], List[hdx.data.showcase.Showcase]) -> bool
        """Add dataset to multiple showcases

        Args:
            showcases (List[Union[Showcase,Dict,str]]): A list of either showcase ids or showcase metadata from Showcase objects or dictionaries
            showcases_to_check (List[Showcase]): list of showcases against which to check existence of showcase. Defaults to showcases containing dataset.

        Returns:
            bool: True if all showcases added or False if any already present
        """
        if showcases_to_check is None:
            showcases_to_check = self.get_showcases()
        allshowcasesadded = True
        for showcase in showcases:
            if not self.add_showcase(showcase, showcases_to_check=showcases_to_check):
                allshowcasesadded = False
        return allshowcasesadded

    def remove_showcase(self, showcase):
        # type: (Union[hdx.data.showcase.Showcase,Dict,str]) -> None
        """Remove dataset from showcase

        Args:
            showcase (Union[Showcase,Dict,str]): Either a showcase id string or showcase metadata from a Showcase object or dictionary

        Returns:
            None
        """
        dataset_showcase = self._get_dataset_showcase_dict(showcase)
        showcase = hdx.data.showcase.Showcase({'id': dataset_showcase['showcase_id']}, configuration=self.configuration)
        showcase._write_to_hdx('disassociate', dataset_showcase, 'package_id')

    def is_requestable(self):
        # type: () -> bool
        """Return whether the dataset is requestable or not

        Returns:
            bool: Whether the dataset is requestable or not
        """
        return self.data.get('is_requestdata_type', False)

    def set_requestable(self, requestable=True):
        # type: (bool) -> None
        """Set the dataset to be of type requestable or not

        Args:
            requestable (bool): Set whether dataset is requestable. Defaults to True.

        Returns:
            None
        """
        self.data['is_requestdata_type'] = requestable
        if requestable:
            self.data['private'] = False

    def get_fieldnames(self):
        # type: () -> List[str]
        """Return list of fieldnames in your data. Only applicable to requestable datasets.

        Returns:
            List[str]: List of field names
        """
        if not self.is_requestable():
            raise NotRequestableError('get_fieldnames is only applicable to requestable datasets!')
        return self._get_stringlist_from_commastring('field_names')

    def add_fieldname(self, fieldname):
        # type: (str) -> bool
        """Add a fieldname to list of fieldnames in your data. Only applicable to requestable datasets.

        Args:
            fieldname (str): fieldname to add

        Returns:
            bool: True if fieldname added or False if tag already present
        """
        if not self.is_requestable():
            raise NotRequestableError('add_fieldname is only applicable to requestable datasets!')
        return self._add_string_to_commastring('field_names', fieldname)

    def add_fieldnames(self, fieldnames):
        # type: (List[str]) -> bool
        """Add a list of fieldnames to list of fieldnames in your data. Only applicable to requestable datasets.

        Args:
            fieldnames (List[str]): list of fieldnames to add

        Returns:
            bool: True if all fieldnames added or False if any already present
        """
        if not self.is_requestable():
            raise NotRequestableError('add_fieldnames is only applicable to requestable datasets!')
        return self._add_strings_to_commastring('field_names', fieldnames)

    def remove_fieldname(self, fieldname):
        # type: (str) -> bool
        """Remove a fieldname. Only applicable to requestable datasets.

        Args:
            fieldname (str): Fieldname to remove

        Returns:
            bool: True if fieldname removed or False if not
        """
        if not self.is_requestable():
            raise NotRequestableError('remove_fieldname is only applicable to requestable datasets!')
        return self._remove_string_from_commastring('field_names', fieldname)

    def get_filetypes(self):
        # type: () -> List[str]
        """Return list of filetypes in your data

        Returns:
            List[str]: List of filetypes
        """
        if not self.is_requestable():
            return [resource.get_file_type() for resource in self.get_resources()]
        return self._get_stringlist_from_commastring('file_types')

    def add_filetype(self, filetype):
        # type: (str) -> bool
        """Add a filetype to list of filetypes in your data. Only applicable to requestable datasets.

        Args:
            filetype (str): filetype to add

        Returns:
            bool: True if filetype added or False if tag already present
        """
        if not self.is_requestable():
            raise NotRequestableError('add_filetype is only applicable to requestable datasets!')
        return self._add_string_to_commastring('file_types', filetype)

    def add_filetypes(self, filetypes):
        # type: (List[str]) -> bool
        """Add a list of filetypes to list of filetypes in your data. Only applicable to requestable datasets.

        Args:
            filetypes (List[str]): list of filetypes to add

        Returns:
            bool: True if all filetypes added or False if any already present
        """
        if not self.is_requestable():
            raise NotRequestableError('add_filetypes is only applicable to requestable datasets!')
        return self._add_strings_to_commastring('file_types', filetypes)

    def remove_filetype(self, filetype):
        # type: (str) -> bool
        """Remove a filetype

        Args:
            filetype (str): Filetype to remove

        Returns:
            bool: True if filetype removed or False if not
        """
        if not self.is_requestable():
            raise NotRequestableError('remove_filetype is only applicable to requestable datasets!')
        return self._remove_string_from_commastring('file_types', filetype)

    def clean_dataset_tags(self):
        # type: (Optional[str], int) -> Tuple[bool, bool]
        """Clean dataset tags according to tags cleanup spreadsheet and return if any changes occurred

        Returns:
            Tuple[bool, bool]: Returns (True if tags changed or False if not, True if error or False if not)
        """
        tags_dict, wildcard_tags = Tags.tagscleanupdicts()

        def delete_tag(tag):
            logger.info('%s - Deleting tag %s!' % (self.data['name'], tag))
            return self.remove_tag(tag), False

        def update_tag(tag, final_tags, wording, remove_existing=True):
            text = '%s - %s: %s -> ' % (self.data['name'], wording, tag)
            if not final_tags:
                logger.error('%snothing!' % text)
                return False, True
            tags_lower_five = final_tags[:5].lower()
            if tags_lower_five == 'merge' or tags_lower_five == 'split' or (
                    ';' not in final_tags and len(final_tags) > 50):
                logger.error('%s%s - Invalid final tag!' % (text, final_tags))
                return False, True
            if remove_existing:
                self.remove_tag(tag)
            tags = ', '.join(self.get_tags())
            if self.add_tags(final_tags.split(';')):
                logger.info('%s%s! Dataset tags: %s' % (text, final_tags, tags))
            else:
                logger.warning(
                    '%s%s - At least one of the tags already exists! Dataset tags: %s' % (text, final_tags, tags))
            return True, False

        def do_action(tag, tags_dict_key):
            whattodo = tags_dict[tags_dict_key]
            action = whattodo[u'action']
            final_tags = whattodo[u'final tags (semicolon separated)']
            if action == u'Delete':
                changed, error = delete_tag(tag)
            elif action == u'Merge':
                changed, error = update_tag(tag, final_tags, 'Merging')
            elif action == u'Fix spelling':
                changed, error = update_tag(tag, final_tags, 'Fixing spelling')
            elif action == u'Non English':
                changed, error = update_tag(tag, final_tags, 'Anglicising', remove_existing=False)
            else:
                changed = False
                error = False
            return changed, error

        def process_tag(tag):
            changed = False
            error = False
            if tag in tags_dict.keys():
                changed, error = do_action(tag, tag)
            else:
                for wildcard_tag in wildcard_tags:
                    if fnmatch.fnmatch(tag, wildcard_tag):
                        changed, error = do_action(tag, wildcard_tag)
                        break
            return changed, error

        anychange = False
        anyerror = False
        for tag in self.get_tags():
            changed, error = process_tag(tag)
            if changed:
                anychange = True
            if error:
                anyerror = True

        return anychange, anyerror

    def preview_off(self):
        # type: () -> None
        """Set dataset preview off

        Returns:
            None
        """
        self.data['dataset_preview'] = 'no_preview'
        for resource in self.resources:
            resource.disable_dataset_preview()

    def preview_resource(self):
        # type: () -> None
        """Set dataset preview on for an unspecified resource

        Returns:
            None
        """
        self.data['dataset_preview'] = 'resource_id'

    def set_quickchart_resource(self, resource):
        # type: (Union[hdx.data.resource.Resource,Dict,str,int]) -> bool
        """Set the resource that will be used for displaying QuickCharts in dataset preview

        Args:
            resource (Union[hdx.data.resource.Resource,Dict,str,int]): Either resource id or name, resource metadata from a Resource object or a dictionary or position

        Returns:
            bool: Returns True if resource for QuickCharts in dataset preview set or False if not
        """
        if isinstance(resource, int) and not isinstance(resource, bool):
            resource = self.get_resources()[resource]
        if isinstance(resource, hdx.data.resource.Resource) or isinstance(resource, dict):
            res = resource.get('id')
            if res is None:
                resource = resource['name']
            else:
                resource = res
        elif not isinstance(resource, str):
            raise hdx.data.hdxobject.HDXError('Resource id cannot be found in type %s!' % type(resource).__name__)
        if is_valid_uuid(resource) is True:
            search = 'id'
        else:
            search = 'name'
        changed = False
        for dataset_resource in self.resources:
            if dataset_resource[search] == resource:
                dataset_resource.enable_dataset_preview()
                self.preview_resource()
                changed = True
            else:
                dataset_resource.disable_dataset_preview()
        return changed