#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Dataset class containing all logic for creating, checking, and updating datasets.

It also handles resource and gallery items.
"""
import copy
import logging
from os.path import join

from typing import Any, List, Optional

from hdx.configuration import Configuration
from hdx.utilities.dictionary import merge_two_dictionaries
from .galleryitem import GalleryItem
from .hdxobject import HDXObject, HDXError
from .resource import Resource

logger = logging.getLogger(__name__)


class Dataset(HDXObject):
    """Dataset class enabling operations on datasets and associated resources and gallery items.

        Args:
            configuration (Configuration): HDX Configuration
            initial_data (Optional[dict]): Initial dataset metadata dictionary. Defaults to None.
            include_gallery (Optional[bool]): Whether to include gallery items in dataset. Defaults to True.
    """

    _action_url = {
        'show': 'package_show?id=',
        'update': 'package_update',
        'create': 'package_create'
    }

    def __init__(self, configuration: Configuration, initial_data: Optional[dict] = None,
                 include_gallery: Optional[bool] = True):
        if not initial_data:
            initial_data = dict()
        super(Dataset, self).__init__(configuration, self._action_url, initial_data)
        self.include_gallery = include_gallery
        self.init_resources()
        self.init_gallery()

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

    def add_update_resource(self, resource: Any) -> None:
        """Add new or update existing resource in dataset with new metadata

        Args:
            resource (Any): Resource metadata either from a Resource object or a dictionary

        Returns:
            None

        """
        if isinstance(resource, Resource):
            if 'package_id' in resource:
                raise HDXError("Resource %s being added already has a dataset id!" % (resource['name']))
            self._addupdate_hdxobject(self.resources, 'name', self._underlying_object, resource)
            return
        if isinstance(resource, dict):
            self._addupdate_hdxobject(self.resources, 'name', Resource, resource)
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
        if isinstance(galleryitem, GalleryItem):
            if 'dataset_id' in galleryitem:
                raise HDXError("Gallery item %s being added already has a dataset id!" % (galleryitem['name']))
            self._addupdate_hdxobject(self.gallery, 'title', self._underlying_object, galleryitem)
            return
        if isinstance(galleryitem, dict):
            self._addupdate_hdxobject(self.gallery, 'title', GalleryItem, galleryitem)

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

    def update_yaml(self, path: Optional[str] = join('config', 'hdx_dataset_static.yml')) -> None:
        """Update dataset metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_dataset_static.yml.

        Returns:
            None
        """
        super(Dataset, self).update_yaml(path)
        self.separate_resources()
        self.separate_gallery()

    def update_json(self, path: Optional[str] = join('config', 'hdx_dataset_static.json')) -> None:
        """Update dataset metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_dataset_static.json.

        Returns:
            None
        """
        super(Dataset, self).update_json(path)
        self.separate_resources()
        self.separate_gallery()

    @staticmethod
    def read_from_hdx(configuration: Configuration, identifier: str) -> Optional['Dataset']:
        """Loads the dataset given by identifier from HDX

        Args:
            configuration (Configuration): HDX Configuration
            identifier (str): Identifier of dataset

        Returns:
            Optional[Dataset]: Dataset object if loaded, None if not

        """

        dataset = Dataset(configuration)
        result = dataset._dataset_load_from_hdx(identifier)
        if result:
            return dataset
        return None

    def _dataset_load_from_hdx(self, id_or_name: str) -> bool:
        """Loads the dataset given by either id or name from HDX

        Args:
            id_or_name (str): Either id or name of dataset

        Returns:
            bool: True if loaded, False if not

        """

        if not self._load_from_hdx('dataset', id_or_name):
            return False
        if self.data["resources"]:
            self.old_data["resources"] = copy.deepcopy(self.resources)
            self.separate_resources()
        if self.include_gallery:
            success, result = self._get_from_hdx('gallery', self.data['id'], '%s%s' % (self.base_url,
                                                                                       GalleryItem._action_url['list']))
            if success:
                self.data['gallery'] = result
                self.old_data['gallery'] = copy.deepcopy(self.gallery)
                self.separate_gallery()
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
        del self.data['resources']
        del self.data['gallery']
        old_resources = self.old_data.get('resources', None)
        if update_resources and old_resources:
            resource_names = set()
            for resource in self.resources:
                resource_name = resource['name']
                resource_names.add(resource_name)
                for old_resource in old_resources:
                    if resource_name == old_resource['name']:
                        logger.warning('Resource exists. Updating %s' % resource_name)
                        merge_two_dictionaries(resource, old_resource)
                        resource.check_required_fields(['package_id'])
                        break
            for old_resource in old_resources:
                if not old_resource['name'] in resource_names:
                    old_resource.check_required_fields(['package_id'])
                    self.resources.append(old_resource)
        old_gallery = self.old_data.get('gallery', None)
        if self.resources:
            self.data['resources'] = self.resources
        self._save_to_hdx('update', 'id')
        self.init_resources()
        self.separate_resources()
        if self.include_gallery and update_gallery and old_gallery:
            self.old_data['gallery'] = copy.deepcopy(self.gallery)
            galleryitem_titles = set()
            for i, galleryitem in enumerate(self.gallery):
                galleryitem_title = galleryitem['title']
                galleryitem_titles.add(galleryitem_title)
                for old_galleryitem in old_gallery:
                    if galleryitem_title == old_galleryitem['title']:
                        logger.warning('Gallery item exists. Updating %s' % galleryitem_title)
                        merge_two_dictionaries(galleryitem, old_galleryitem)
                        galleryitem.check_required_fields(['dataset_id'])
                        galleryitem.update_in_hdx()
            for old_galleryitem in old_gallery:
                if not old_galleryitem['title'] in galleryitem_titles:
                    old_galleryitem['dataset_id'] = self.data['id']
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
        self._check_existing_object('dataset', 'name')
        if not self._dataset_load_from_hdx(self.data['name']):
            raise HDXError("No existing dataset to update!")
        self._dataset_merge_hdx_update(update_resources, update_gallery)

    def create_in_hdx(self) -> None:
        """Check if dataset exists in HDX and if so, update it, otherwise create it

        Returns:
            None

        """
        self.check_required_fields()
        if self._dataset_load_from_hdx('name'):
            logger.warning('Dataset exists. Updating %s' % self.data['name'])
            self._dataset_merge_hdx_update(True, True)
            return

        if self.resources:
            self.data['resources'] = self.resources
            for resource in self.resources:
                resource.check_required_fields()
        self._save_to_hdx('create', 'name')
        self.init_resources()
        self.separate_resources()

        if self.include_gallery:
            self.old_data['gallery'] = copy.deepcopy(self.gallery)
            for i, galleryitem in enumerate(self.gallery):
                galleryitem['dataset_id'] = self.data['id']
                galleryitem.check_required_fields()
                galleryitem.create_in_hdx()

    def delete_from_hdx(self) -> None:
        """Deletes a dataset from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('dataset', 'id')
