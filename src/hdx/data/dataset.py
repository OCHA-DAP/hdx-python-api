# -*- coding: utf-8 -*-
"""Dataset class containing all logic for creating, checking, and updating datasets.

It also handles resource and gallery items.
"""
import logging
import sys
from datetime import datetime
from os.path import join
from typing import List, Optional

from dateutil import parser
from six.moves import range

from hdx.data.galleryitem import GalleryItem
from hdx.data.hdxobject import HDXObject, HDXError
from hdx.data.resource import Resource
from hdx.utilities import raisefrom
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.location import Location

logger = logging.getLogger(__name__)

max_attempts = 5
page_size = 1000
max_int = sys.maxsize

class Dataset(HDXObject):
    """Dataset class enabling operations on datasets and associated resources and gallery items.

    Args:
        initial_data (Optional[dict]): Initial dataset metadata dictionary. Defaults to None.
        include_gallery (Optional[bool]): Whether to include gallery items in dataset. Defaults to True.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    update_frequencies = {
        '0': 'Never',
        '1': 'Every day',
        '7': 'Every week',
        '14': 'Every two weeks',
        '30': 'Every month',
        '90': 'Every three months',
        '180': 'Every six months',
        '365': 'Every year',
        'never': '0',
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

    def __init__(self, initial_data=None,
                 include_gallery=True, configuration=None):
        # type: (Optional[dict], Optional[bool], Optional[Configuration]) -> None
        if not initial_data:
            initial_data = dict()
        super(Dataset, self).__init__(dict(), configuration=configuration)
        # workaround: python2 IterableUserDict does not call __setitem__ in __init__,
        # while python3 collections.UserDict does
        for key in initial_data:
            self[key] = initial_data[key]
        self.include_gallery = include_gallery
        self.init_resources()
        self.init_gallery()

    @staticmethod
    def actions():
        # type: () -> dict
        """Dictionary of actions that can be performed on object

        Returns:
            dict: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'package_show',
            'update': 'package_update',
            'create': 'package_create',
            'delete': 'package_delete',
            'search': 'package_search',
            'list': 'package_list',
            'all': 'current_package_list_with_resources'
        }

    def __setitem__(self, key, value):
        # type: (Any, Any) -> None
        """Set dictionary items but do not allow setting of resources or gallery

        Args:
            key (Any): Key in dictionary
            value (Any): Value to put in dictionary

        Returns:
            None
        """
        if key == 'resources':
            raise HDXError('Add resource using add_update_resource or resources using add_update_resources!')
        if key == 'gallery':
            raise HDXError('Add gallery item using add_update_galleryitem or gallery using add_update_gallery!')
        super(Dataset, self).__setitem__(key, value)

    def separate_resources(self):
        # type: () -> None
        """Move contents of resources key in internal dictionary into self.resources

        Returns:
            None
        """
        self._separate_hdxobjects(self.resources, 'resources', 'name', Resource)

    def separate_gallery(self):
        """Move contents of gallery key in internal dictionary into self.gallery

        Returns:
            None
        """
        self._separate_hdxobjects(self.gallery, 'gallery', 'title', GalleryItem)

    def init_resources(self):
        # type: () -> None
        """Initialise self.resources list

        Returns:
            None
        """
        self.resources = list()
        """:type : List[Resource]"""

    def add_update_resource(self, resource):
        # type: (Any) -> None
        """Add new or update existing resource in dataset with new metadata

        Args:
            resource (Any): Resource metadata either from a Resource object or a dictionary

        Returns:
            None
        """
        if isinstance(resource, dict):
            resource = Resource(resource, configuration=self.configuration)
        if isinstance(resource, Resource):
            if 'package_id' in resource:
                raise HDXError("Resource %s being added already has a dataset id!" % (resource['name']))
            resource_updated = self._addupdate_hdxobject(self.resources, 'name', resource)
            resource_updated.set_file_to_upload(resource.get_file_to_upload())
            return
        raise HDXError("Type %s cannot be added as a resource!" % type(resource).__name__)

    def add_update_resources(self, resources):
        # type: (List[Any]) -> None
        """Add new or update existing resources with new metadata to the dataset

        Args:
            resources (List[Any]): Resources metadata from a list of either Resource objects or dictionaries

        Returns:
            None
        """
        if not isinstance(resources, list):
            raise HDXError('Resources should be a list!')
        for resource in resources:
            self.add_update_resource(resource)

    def delete_resource(self, identifier):
        # type: (str) -> None
        """Delete a resource from the dataset

        Args:
            identifier (str): Id of resource to delete

        Returns:
            None
        """
        for i, resource in enumerate(self.resources):
            resourceid = resource.get('id', None)
            if resourceid and resourceid == identifier:
                resource.delete_from_hdx()
                del self.resources[i]

    def get_resources(self):
        # type: () -> List[Resource]
        """Get dataset's resources

        Returns:
            List[Resource]: List of Resource objects
        """
        return self.resources

    def init_gallery(self):
        # type: () -> None
        """Initialise self.gallery list

        Returns:
            None
        """
        self.gallery = list()

    def add_update_galleryitem(self, galleryitem):
        # type: (Any) -> None
        """Add new or update existing gallery item in dataset with new metadata

        Args:
            galleryitem (Any): Gallery item metadata either from a GalleryItem object or a dictionary

        Returns:
            None

        """
        if isinstance(galleryitem, dict):
            galleryitem = GalleryItem(galleryitem, configuration=self.configuration)
        if isinstance(galleryitem, GalleryItem):
            if 'dataset_id' in galleryitem:
                raise HDXError("Gallery item %s being added already has a dataset id!" % (galleryitem['name']))
            self._addupdate_hdxobject(self.gallery, 'title', galleryitem)
            return
        raise HDXError("Type %s cannot be added as a gallery item!" % type(galleryitem).__name__)

    def add_update_gallery(self, gallery):
        """Add new or update existing gallery items with new metadata to the dataset

        Args:
            gallery (List[Any]): Gallery metadata from a list of either GalleryItem objects or dictionaries

        Returns:
            None
        """
        if not isinstance(gallery, list):
            raise HDXError('Gallery should be a list!')
        for galleryitem in gallery:
            self.add_update_galleryitem(galleryitem)

    def delete_galleryitem(self, identifier):
        # type: (str) -> None
        """Delete a gallery item from the dataset

        Args:
            identifier (str): Id of gallery item to delete

        Returns:
            None
        """
        for i, galleryitem in enumerate(self.gallery):
            galleryitemid = galleryitem.get('id', None)
            if galleryitemid and galleryitemid == identifier:
                galleryitem.delete_from_hdx()
                del self.gallery[i]

    def get_gallery(self):
        # type: () -> List[GalleryItem]
        """Get dataset's gallery

        Returns:
            List[GalleryItem]: List of GalleryItem objects
        """
        return self.gallery

    def update_from_yaml(self, path=join('config', 'hdx_dataset_static.yml')):
        # type: (Optional[str]) -> None
        """Update dataset metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_dataset_static.yml.

        Returns:
            None
        """
        super(Dataset, self).update_from_yaml(path)
        self.separate_resources()
        self.separate_gallery()

    def update_from_json(self, path=join('config', 'hdx_dataset_static.json')):
        # type: (Optional[str]) -> None
        """Update dataset metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_dataset_static.json.

        Returns:
            None
        """
        super(Dataset, self).update_from_json(path)
        self.separate_resources()
        self.separate_gallery()

    @staticmethod
    def read_from_hdx(identifier, include_gallery=True, configuration=None):
        # type: (str, Optional[bool], Optional[Configuration]) -> Optional['Dataset']
        """Reads the dataset given by identifier from HDX and returns Dataset object

        Args:
            identifier (str): Identifier of dataset
            include_gallery (Optional[bool]): Whether to include gallery items in dataset. Defaults to True.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[Dataset]: Dataset object if successful read, None if not
        """

        dataset = Dataset(include_gallery=include_gallery, configuration=configuration)
        result = dataset._dataset_load_from_hdx(identifier)
        if result:
            return dataset
        return None

    def _dataset_create_resources_gallery(self):
        # type: () -> None
        """Creates resource and gallery item objects in dataset
        """

        if 'resources' in self.data:
            self.old_data['resources'] = self._copy_hdxobjects(self.resources, Resource, 'file_to_upload')
            self.init_resources()
            self.separate_resources()
        if self.include_gallery:
            success, result = self._read_from_hdx('gallery', self.data['id'], 'id', GalleryItem.actions()['list'])
            if success:
                self.data['gallery'] = result
                self.old_data['gallery'] = self._copy_hdxobjects(self.gallery, GalleryItem)
                self.init_gallery()
                self.separate_gallery()

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
        self._dataset_create_resources_gallery()
        return True

    def check_required_fields(self, ignore_fields=list()):
        # type: (List[str]) -> None
        """Check that metadata for dataset and its resources and gallery is complete. The parameter ignore_fields
        should be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        self._check_required_fields('dataset', ignore_fields)

        for resource in self.resources:
            ignore_fields = [self.configuration['resource']['dataset_id']]
            resource.check_required_fields(ignore_fields=ignore_fields)
        for galleryitem in self.gallery:
            ignore_fields = [self.configuration['galleryitem']['dataset_id']]
            galleryitem.check_required_fields(ignore_fields=ignore_fields)

    def _dataset_merge_hdx_update(self, update_resources, update_gallery):
        # type: (bool, bool) -> None
        """Helper method to check if dataset or its resources or gallery items exist and update them

        Args:
            update_resources (bool): Whether to update resources
            update_gallery (bool): Whether to update gallery

        Returns:
            None
        """
        merge_two_dictionaries(self.data, self.old_data)
        if 'resources' in self.data:
            del self.data['resources']
        if 'gallery' in self.data:
            del self.data['gallery']
        old_resources = self.old_data.get('resources', None)
        filestore_resources = list()
        if update_resources and old_resources:
            ignore_fields = [self.configuration['resource']['dataset_id']]
            resource_names = set()
            for resource in self.resources:
                resource_name = resource['name']
                resource_names.add(resource_name)
                for old_resource in old_resources:
                    if resource_name == old_resource['name']:
                        logger.warning('Resource exists. Updating %s' % resource_name)
                        merge_two_dictionaries(resource, old_resource)
                        if old_resource.get_file_to_upload():
                            resource.set_file_to_upload(old_resource.get_file_to_upload())
                            filestore_resources.append(resource)
                        resource.check_required_fields(ignore_fields=ignore_fields)
                        break
            for old_resource in old_resources:
                if not old_resource['name'] in resource_names:
                    old_resource.check_required_fields(ignore_fields=ignore_fields)
                    self.resources.append(old_resource)
                    if old_resource.get_file_to_upload():
                        filestore_resources.append(old_resource)
        old_gallery = self.old_data.get('gallery', None)
        if self.resources:
            self.data['resources'] = self._convert_hdxobjects(self.resources)
        self._save_to_hdx('update', 'id')

        for resource in filestore_resources:
            for created_resource in self.data['resources']:
                if resource['name'] == created_resource['name']:
                    merge_two_dictionaries(resource.data, created_resource)
                    resource.update_in_hdx()
                    merge_two_dictionaries(created_resource, resource.data)
                    break

        if self.include_gallery and update_gallery and old_gallery:
            self.old_data['gallery'] = self._copy_hdxobjects(self.gallery, GalleryItem)
            ignore_fields = [self.configuration['galleryitem']['dataset_id']]
            galleryitem_titles = set()
            galleryitem_dataset_id = self.configuration['galleryitem']['dataset_id']
            for i, galleryitem in enumerate(self.gallery):
                galleryitem_title = galleryitem['title']
                galleryitem_titles.add(galleryitem_title)
                for old_galleryitem in old_gallery:
                    if galleryitem_title == old_galleryitem['title']:
                        logger.warning('Gallery item exists. Updating %s' % galleryitem_title)
                        merge_two_dictionaries(galleryitem, old_galleryitem)
                        galleryitem.check_required_fields(ignore_fields=ignore_fields)
                        galleryitem.update_in_hdx()
            for old_galleryitem in old_gallery:
                if not old_galleryitem['title'] in galleryitem_titles:
                    old_galleryitem[galleryitem_dataset_id] = self.data['id']
                    old_galleryitem.check_required_fields()
                    old_galleryitem.create_in_hdx()
                    self.gallery.append(old_galleryitem)

    def update_in_hdx(self, update_resources=True, update_gallery=True):
        # type: (Optional[bool], Optional[bool]) -> None
        """Check if dataset exists in HDX and if so, update it

        Args:
            update_resources (Optional[bool]): Whether to update resources. Defaults to True.
            update_gallery (Optional[bool]): Whether to update resources. Defaults to True.

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
        self._dataset_merge_hdx_update(update_resources, update_gallery)

    def create_in_hdx(self):
        # type: () -> None
        """Check if dataset exists in HDX and if so, update it, otherwise create it

        Returns:
            None
        """
        self.check_required_fields()
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
            self._dataset_merge_hdx_update(True, True)
            return

        filestore_resources = list()
        if self.resources:
            ignore_fields = [self.configuration['resource']['dataset_id']]
            for resource in self.resources:
                resource.check_required_fields(ignore_fields=ignore_fields)
                if resource.get_file_to_upload():
                    filestore_resources.append(resource)
            self.data['resources'] = self._convert_hdxobjects(self.resources)
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

        if self.include_gallery:
            self.old_data['gallery'] = self._copy_hdxobjects(self.gallery, GalleryItem)
            galleryitem_dataset_id = self.configuration['galleryitem']['dataset_id']
            for i, galleryitem in enumerate(self.gallery):
                galleryitem[galleryitem_dataset_id] = self.data['id']
                galleryitem.check_required_fields()
                galleryitem.create_in_hdx()

    def delete_from_hdx(self):
        # type: () -> None
        """Deletes a dataset from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('dataset', 'id')

    @staticmethod
    def search_in_hdx(query, include_gallery=True, configuration=None, **kwargs):
        # type: (str, Optional[bool], Optional[Configuration], ...) -> List['Dataset']
        """Searches for datasets in HDX

        Args:
            query (str): Query (in Solr format). Defaults to '*:*'.
            include_gallery (Optional[bool]): Whether to include gallery items in dataset. Defaults to True.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
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
            List[Dataset]: List of datasets resulting from query
        """

        dataset = Dataset(include_gallery=include_gallery, configuration=configuration)
        total_rows = kwargs.get('rows', max_int)
        start = kwargs.get('start', 0)
        all_datasets = None
        attempts = 0
        while attempts < max_attempts and all_datasets is None:  # if the count values vary for multiple calls, then must redo query
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
                            dataset = Dataset(include_gallery=include_gallery, configuration=configuration)
                            dataset.old_data = dict()
                            dataset.data = datasetdict
                            dataset._dataset_create_resources_gallery()
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
        if attempts == max_attempts and all_datasets is None:
            raise HDXError('Maximum attempts reached for searching for datasets!')
        return all_datasets

    @staticmethod
    def get_all_dataset_names(configuration=None, **kwargs):
        # type: (Optional[Configuration], ...) -> List[str]
        """Get all dataset names in HDX

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs: See below
            limit (int): Number of rows to return. Defaults to all dataset names.
            offset (int): Offset in the complete result for where the set of returned dataset names should begin

        Returns:
            List[str]: List of all dataset names in HDX
        """
        dataset = Dataset(configuration=configuration)
        dataset['id'] = 'all dataset names'  # only for error message if produced
        return dataset._write_to_hdx('list', kwargs, 'id')

    @staticmethod
    def get_all_datasets(include_gallery=True, configuration=None, **kwargs):
        # type: (Optional[bool], Optional[Configuration], ...) -> List['Dataset']
        """Get all datasets in HDX

        Args:
            include_gallery (Optional[bool]): Whether to include gallery items in dataset. Defaults to True.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs: See below
            limit (int): Number of rows to return. Defaults to all datasets (sys.maxsize).
            offset (int): Offset in the complete result for where the set of returned datasets should begin

        Returns:
            List[Dataset]: List of all datasets in HDX
        """

        dataset = Dataset(include_gallery=include_gallery, configuration=configuration)
        dataset['id'] = 'all datasets'  # only for error message if produced
        total_rows = kwargs.get('limit', max_int)
        start = kwargs.get('offset', 0)
        all_datasets = None
        attempts = 0
        while attempts < max_attempts and all_datasets is None:  # if the dataset names vary for multiple calls, then must redo query
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
                        dataset = Dataset(include_gallery=include_gallery, configuration=configuration)
                        dataset.old_data = dict()
                        dataset.data = datasetdict
                        dataset._dataset_create_resources_gallery()
                        datasets.append(dataset)
                    all_datasets += datasets
                    if no_results < rows:
                        break
                else:
                    logger.debug(result)
            names_list = [dataset['name'] for dataset in all_datasets]
            names = set(names_list)
            if len(names_list) != len(names):  # check for duplicates (shouldn't happen)
                all_datasets = None
                attempts += 1
            elif total_rows == max_int:
                all_names = set(Dataset.get_all_dataset_names())  # check dataset names match package_list
                if names != all_names:
                    all_datasets = None
                    attempts += 1
        if attempts == max_attempts and all_datasets is None:
            raise HDXError('Maximum attempts reached for getting all datasets!')
        return all_datasets

    @staticmethod
    def get_all_resources(datasets):
        # type: (List['Dataset']) -> List['Resource']
        """Get all resources from a list of datasets (such as returned by search)

        Args:
            datasets (List[Dataset]): List of datasets

        Returns:
            List[Resource]: List of resources within those datasets
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
            dataset_date (datetime.datetime): Dataset date string
            dataset_end_date (Optional[datetime.datetime]): Dataset end date string

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
            List[str]: List of tags or [] if there are none
        """
        tags = self.data.get('tags', None)
        if not tags:
            return list()
        return [x['name'] for x in tags]

    def add_tag(self, tag):
        # type: (str) -> None
        """Add a tag

        Args:
            tag (str): Tag to add

        Returns:
            None
        """
        tags = self.data.get('tags', None)
        if tags:
            if tag in [x['name'] for x in tags]:
                return
        else:
            tags = list()
        tags.append({'name': tag})
        self.data['tags'] = tags

    def add_tags(self, tags):
        # type: (List[str]) -> None
        """Add a list of tag

        Args:
            tags (List[str]): List of tags to add

        Returns:
            None
        """
        for tag in tags:
            self.add_tag(tag)

    def get_location(self):
        # type: () -> List[str]
        """Return the dataset's location

        Returns:
            List[str]: List of locations or [] if there are none
        """
        countries = self.data.get('groups', None)
        if not countries:
            return list()
        return [Location.get_location_from_HDX_code(x['name'], self.configuration) for x in countries]

    def add_country_location(self, country):
        # type: (str) -> None
        """Add a country. If an iso 3 code is not provided, value is parsed and if it is a valid country name,
        converted to an iso 3 code. If the country is already added, it is ignored.

        Args:
            country (str): Country to add

        Returns:
            None
        """
        iso3, match = Location.get_iso3_country_code(country)
        if iso3 is None:
            raise HDXError('Country: %s - cannot find iso3 code!' % country)
        hdx_code, match = Location.get_HDX_code_from_location(iso3, self.configuration)
        if hdx_code is None:
            raise HDXError('Country: %s with iso3: %s could not be found in HDX list!' % (country, iso3))
        groups = self.data.get('groups', None)
        if groups:
            if hdx_code in [x['name'] for x in groups]:
                return
        else:
            groups = list()
        groups.append({'name': hdx_code})
        self.data['groups'] = groups

    def add_country_locations(self, countries):
        # type: (List[str]) -> None
        """Add a list of countries. If iso 3 codes are not provided, values are parsed and where they are valid country
        names, converted to iso 3 codes. If any country is already added, it is ignored.

        Args:
            countries (List[str]): List of countries to add

        Returns:
            None
        """
        for country in countries:
            self.add_country_location(country)

    def add_continent_location(self, continent):
        # type: (str) -> None
        """Add all countries in a  continent. If a 2 letter continent code is not provided, value is parsed and if it
        is a valid continent name, converted to a 2 letter code. If any country is already added, it is ignored.

        Args:
            continent (str): Continent to add

        Returns:
            None
        """
        self.add_country_locations(Location.get_countries_in_continent(continent))

    def add_other_location(self, location):
        # type: (str) -> None
        """Add a location which is not a country or continent. Value is parsed and compared to existing locations in 
        HDX. If the location is already added, it is ignored.

        Args:
            location (str): Location to add

        Returns:
            None
        """
        hdx_code, match = Location.get_HDX_code_from_location(location, self.configuration)
        if hdx_code is None:
            raise HDXError('Location: %s - cannot find in HDX!' % location)
        groups = self.data.get('groups', None)
        if groups:
            if hdx_code in [x['name'] for x in groups]:
                return
        else:
            groups = list()
        groups.append({'name': hdx_code})
        self.data['groups'] = groups
