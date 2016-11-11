#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Dataset class containing all logic for creating, checking, and updating datasets.

It also handles resource and gallery items.
"""
import logging
from os.path import join
from typing import Any, List, Optional

from hdx.configuration import Configuration
from hdx.data.galleryitem import GalleryItem
from hdx.data.hdxobject import HDXObject, HDXError
from hdx.data.resource import Resource
from hdx.utilities.dictionary import merge_two_dictionaries
from hdx.utilities.location import Location

logger = logging.getLogger(__name__)


class Dataset(HDXObject):
    """Dataset class enabling operations on datasets and associated resources and gallery items.

    Args:
        configuration (Configuration): HDX Configuration
        initial_data (Optional[dict]): Initial dataset metadata dictionary. Defaults to None.
        include_gallery (Optional[bool]): Whether to include gallery items in dataset. Defaults to True.
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
        'every year': '365'
    }

    def __init__(self, configuration: Configuration, initial_data: Optional[dict] = None,
                 include_gallery: Optional[bool] = True):
        if not initial_data:
            initial_data = dict()
        super(Dataset, self).__init__(configuration, initial_data)
        self.include_gallery = include_gallery
        self.init_resources()
        self.init_gallery()

    @staticmethod
    def actions() -> dict:
        """Dictionary of actions that can be performed on object

        Returns:
            dict: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'package_show',
            'update': 'package_update',
            'create': 'package_create',
            'delete': 'package_delete',
            'search': 'package_search'
        }

    def __setitem__(self, key: Any, value: Any) -> None:
        """Set dictionary items but do not allow setting of resources or gallery

        Args:
            key (Any): Key in dictionary
            value (Any): Value to put in dictionary

        Returns:
            None
        """
        if key == 'resources':
            raise HDXError('Add resource using add_resource or resources using add_resources!')
        if key == 'gallery':
            raise HDXError('Add gallery item using add_galleryitem or gallery using add_gallery!')
        super(Dataset, self).__setitem__(key, value)

    def separate_resources(self) -> None:
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

    def init_resources(self) -> None:
        """Initialise self.resources list

        Returns:
            None
        """
        self.resources = list()
        """:type : List[Resource]"""

    def add_update_resource(self, resource: Any) -> None:
        """Add new or update existing resource in dataset with new metadata

        Args:
            resource (Any): Resource metadata either from a Resource object or a dictionary

        Returns:
            None
        """
        if isinstance(resource, dict):
            resource = Resource(self.configuration, resource)
        if isinstance(resource, Resource):
            if 'package_id' in resource:
                raise HDXError("Resource %s being added already has a dataset id!" % (resource['name']))
            self._addupdate_hdxobject(self.resources, 'name', self._underlying_object, resource)
            return
        raise HDXError("Type %s cannot be added as a resource!" % type(resource).__name__)

    def add_update_resources(self, resources: List[Any]) -> None:
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

    def delete_resource(self, identifier: str) -> None:
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

    def get_resources(self) -> List[Resource]:
        """Get dataset's resources

        Returns:
            List[Resource]: List of Resource objects
        """
        return self.resources

    def init_gallery(self) -> None:
        """Initialise self.gallery list

        Returns:
            None
        """
        self.gallery = list()

    def add_update_galleryitem(self, galleryitem) -> None:
        """Add new or update existing gallery item in dataset with new metadata

        Args:
            galleryitem (Any): Gallery item metadata either from a GalleryItem object or a dictionary

        Returns:
            None

        """
        if isinstance(galleryitem, dict):
            galleryitem = GalleryItem(self.configuration, galleryitem)
        if isinstance(galleryitem, GalleryItem):
            if 'dataset_id' in galleryitem:
                raise HDXError("Gallery item %s being added already has a dataset id!" % (galleryitem['name']))
            self._addupdate_hdxobject(self.gallery, 'title', self._underlying_object, galleryitem)
            return
        raise HDXError("Type %s cannot be added as a gallery item!" % type(galleryitem).__name__)

    def add_update_gallery(self, gallery: List[Any]):
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

    def delete_galleryitem(self, identifier: str) -> None:
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

    def get_gallery(self) -> List[GalleryItem]:
        """Get dataset's gallery

        Returns:
            List[GalleryItem]: List of GalleryItem objects
        """
        return self.gallery

    def update_from_yaml(self, path: Optional[str] = join('config', 'hdx_dataset_static.yml')) -> None:
        """Update dataset metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_dataset_static.yml.

        Returns:
            None
        """
        super(Dataset, self).update_from_yaml(path)
        self.separate_resources()
        self.separate_gallery()

    def update_from_json(self, path: Optional[str] = join('config', 'hdx_dataset_static.json')) -> None:
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
    def read_from_hdx(configuration: Configuration, identifier: str) -> Optional['Dataset']:
        """Reads the dataset given by identifier from HDX and returns Dataset object

        Args:
            configuration (Configuration): HDX Configuration
            identifier (str): Identifier of dataset

        Returns:
            Optional[Dataset]: Dataset object if successful read, None if not
        """

        dataset = Dataset(configuration)
        result = dataset._dataset_load_from_hdx(identifier)
        if result:
            return dataset
        return None

    def _dataset_create_resources_gallery(self) -> None:
        """Creates resource and gallery item objects in dataset
        """

        if 'resources' in self.data:
            self.old_data['resources'] = self._copy_hdxobjects(self.resources, Resource)
            self.separate_resources()
        if self.include_gallery:
            success, result = self._read_from_hdx('gallery', self.data['id'], 'id', GalleryItem.actions()['list'])
            if success:
                self.data['gallery'] = result
                self.old_data['gallery'] = self._copy_hdxobjects(self.gallery, GalleryItem)
                self.separate_gallery()

    def _dataset_load_from_hdx(self, id_or_name: str) -> bool:
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

    def check_required_fields(self, ignore_fields: List[str] = list()) -> None:
        """Check that metadata for dataset and its resources and gallery is complete

        Args:
            ignore_fields (List[str]): Any fields to ignore in the check. Default is empty list.

        Returns:
            None
        """
        for field in self.configuration['dataset']['required_fields']:
            if field not in ignore_fields and field not in self.data:
                raise HDXError("Field %s is missing in dataset!" % field)

        for resource in self.resources:
            resource.check_required_fields([self.configuration['resource']['dataset_id']])
        for galleryitem in self.gallery:
            galleryitem.check_required_fields([self.configuration['galleryitem']['dataset_id']])

    def _dataset_merge_hdx_update(self, update_resources: bool, update_gallery: bool) -> None:
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
        if update_resources and old_resources:
            resource_dataset_id = [self.configuration['resource']['dataset_id']]
            resource_names = set()
            for resource in self.resources:
                resource_name = resource['name']
                resource_names.add(resource_name)
                for old_resource in old_resources:
                    if resource_name == old_resource['name']:
                        logger.warning('Resource exists. Updating %s' % resource_name)
                        merge_two_dictionaries(resource, old_resource)
                        resource.check_required_fields(resource_dataset_id)
                        break
            for old_resource in old_resources:
                if not old_resource['name'] in resource_names:
                    old_resource.check_required_fields(resource_dataset_id)
                    self.resources.append(old_resource)
        old_gallery = self.old_data.get('gallery', None)
        if self.resources:
            self.data['resources'] = self._convert_hdxobjects(self.resources)
        self._save_to_hdx('update', 'id')
        self.init_resources()
        self.separate_resources()
        if self.include_gallery and update_gallery and old_gallery:
            self.old_data['gallery'] = self._copy_hdxobjects(self.gallery, GalleryItem)
            galleryitem_titles = set()
            galleryitem_dataset_id = self.configuration['galleryitem']['dataset_id']
            for i, galleryitem in enumerate(self.gallery):
                galleryitem_title = galleryitem['title']
                galleryitem_titles.add(galleryitem_title)
                for old_galleryitem in old_gallery:
                    if galleryitem_title == old_galleryitem['title']:
                        logger.warning('Gallery item exists. Updating %s' % galleryitem_title)
                        merge_two_dictionaries(galleryitem, old_galleryitem)
                        galleryitem.check_required_fields([galleryitem_dataset_id])
                        galleryitem.update_in_hdx()
            for old_galleryitem in old_gallery:
                if not old_galleryitem['title'] in galleryitem_titles:
                    old_galleryitem[galleryitem_dataset_id] = self.data['id']
                    old_galleryitem.check_required_fields()
                    old_galleryitem.create_in_hdx()
                    self.gallery.append(old_galleryitem)

    def update_in_hdx(self, update_resources: Optional[bool] = True, update_gallery: Optional[bool] = True) -> None:
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

    def create_in_hdx(self) -> None:
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

        resource_dataset_id = [self.configuration['resource']['dataset_id']]
        if self.resources:
            self.data['resources'] = self._convert_hdxobjects(self.resources)
            for resource in self.resources:
                resource.check_required_fields(resource_dataset_id)
        self._save_to_hdx('create', 'name')
        self.init_resources()
        self.separate_resources()

        if self.include_gallery:
            self.old_data['gallery'] = self._copy_hdxobjects(self.gallery, GalleryItem)
            galleryitem_dataset_id = self.configuration['galleryitem']['dataset_id']
            for i, galleryitem in enumerate(self.gallery):
                galleryitem[galleryitem_dataset_id] = self.data['id']
                galleryitem.check_required_fields()
                galleryitem.create_in_hdx()

    def delete_from_hdx(self) -> None:
        """Deletes a dataset from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('dataset', 'id')

    @staticmethod
    def search_in_hdx(configuration: Configuration, query: str, **kwargs) -> List['Dataset']:
        """Searches for datasets in HDX

        Args:
            configuration (Configuration): HDX Configuration
            query (str): Query (in Solr format). Defaults to '*:*'.
            **kwargs: See below
            fq (string): Any filter queries to apply
            sort (string): Sorting of the search results. Defaults to 'relevance asc, metadata_modified desc'.
            rows (int): Number of matching rows to return
            start (int): Offset in the complete result for where the set of returned datasets should begin
            facet (string): Whether to enable faceted results. Default to True.
            facet.mincount (int): Minimum counts for facet fields should be included in the results
            facet.limit (int): Maximum number of values the facet fields return (- = unlimited). Defaults to 50.
            facet.field (List[str]): Fields to facet upon. Default is empty.
            use_default_schema (bool): Use default package schema instead of custom schema. Defaults to False.

        Returns:
            List[Dataset]: List of datasets resulting from query
        """

        datasets = []
        dataset = Dataset(configuration)
        success, result = dataset._read_from_hdx('dataset', query, 'q', **kwargs)
        if result:
            count = result.get('count', None)
            if count:
                for datasetdict in result['results']:
                    dataset = Dataset(configuration)
                    dataset.old_data = dict()
                    dataset.data = datasetdict
                    dataset._dataset_create_resources_gallery()
                    datasets.append(dataset)
        else:
            logger.debug(result)
        return datasets

    @staticmethod
    def get_all_resources(datasets: List['Dataset']) -> List['Resource']:
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

    @staticmethod
    def transform_update_frequency(frequency: str) -> str:
        """Get numeric update frequency (as string since that is required field format) from textual representation or
        vice versa (eg. 'Every month' = '30', '30' = 'Every month')

        Args:
            frequency (str): Update frequency in one format

        Returns:
            str: Update frequency in alternative format
        """
        return Dataset.update_frequencies[frequency.lower()]

    def get_update_frequency(self) -> Optional[str]:
        """Get textual representation of numeric update frequency as stored in HDX (as string)
        vice versa (eg. '30' = 'Every month')

        Returns:
            Optional[str]: Update frequency in textual form or None if the update frequency doesn't exist or is blank.
        """
        days = self.data.get('data_update_frequency', None)
        if days:
            return Dataset.update_frequencies[days]
        else:
            return None

    def set_update_frequency(self, update_frequency: str) -> None:
        """Set update frequency

        Args:
            update_frequency (str): Update frequency

        Returns:
            None
        """
        try:
            int(update_frequency)
        except ValueError:
            update_frequency = Dataset.update_frequencies.get(update_frequency)
        if not update_frequency:
            raise HDXError('Invalid update frequency supplied!')
        self.data['data_update_frequency'] = update_frequency

    def get_tags(self) -> List[str]:
        """Return the dataset's list of tags

        Returns:
            List[str]: List of tags or [] if there are none
        """
        tags = self.data.get('tags', None)
        if not tags:
            return list()
        return [x['name'] for x in tags]

    def add_tag(self, tag: str) -> None:
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

    def add_tags(self, tags: List[str]) -> None:
        """Add a list of tag

        Args:
            tags (List[str]): List of tags to add

        Returns:
            None
        """
        for tag in tags:
            self.add_tag(tag)

    def get_location(self) -> List[str]:
        """Return the dataset's location

        Returns:
            List[str]: List of locations or [] if there are none
        """
        countries = self.data.get('groups', None)
        if not countries:
            return list()
        return [Location.get_country_name_from_iso3(x['id']) for x in countries]

    def add_country_location(self, country: str) -> None:
        """Add a country. If iso 3 code not provided, tries to parse value and convert to iso 3 code.

        Args:
            country (str): Country to add

        Returns:
            None
        """
        iso3, match = Location.get_iso3_country_code(country)
        if iso3 is None:
            raise HDXError('Country: %s could not be found!')
        countries = self.data.get('groups', None)
        if countries:
            if country in [x['id'] for x in countries]:
                return
        else:
            countries = list()
        countries.append({'id': iso3})
        self.data['groups'] = countries

    def add_country_locations(self, countries: List[str]) -> None:
        """Add a list of countries. If iso 3 codes are not provided, tries to parse values and convert to iso 3 code.

        Args:
            countries (List[str]): List of countries to add

        Returns:
            None
        """
        for country in countries:
            self.add_country_location(country)

    def add_continent_location(self, continent: str) -> None:
        """Add a continent. If a 2 letter continent code is not provided, tries to parse value and convert to 2 letter code.

        Args:
            continent (str): Continent to add

        Returns:
            None
        """
        self.add_country_locations(Location.get_countries_in_continent(continent))
