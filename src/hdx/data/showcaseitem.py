# -*- coding: utf-8 -*-
"""ShowcaseItem class containing all logic for creating, checking, and updating showcase items."""
import logging
from os.path import join

from hdx.data.hdxobject import HDXObject, HDXError

logger = logging.getLogger(__name__)


class ShowcaseItem(HDXObject):
    """ShowcaseItem class containing all logic for creating, checking, and updating showcase items.

    Args:
        initial_data (Optional[dict]): Initial showcase item metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """
    dataset_ids_field = 'dataset_ids'

    def __init__(self, initial_data=None, configuration=None):
        # type: (Optional[dict], Optional[Configuration]) -> None
        if not initial_data:
            initial_data = dict()
        super(ShowcaseItem, self).__init__(initial_data, configuration=configuration)

    @staticmethod
    def actions():
        # type: () -> dict
        """Dictionary of actions that can be performed on object

        Returns:
            dict: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'ckanext_showcase_show',
            'update': 'ckanext_showcase_update',
            'create': 'ckanext_showcase_create',
            'delete': 'ckanext_showcase_delete',
            'list': 'ckanext_showcase_list',
            'associate': 'ckanext_showcase_package_association_create',
            'disassociate': 'ckanext_showcase_package_association_delete',
            'list_datasets': 'ckanext_showcase_package_list'
        }

    def update_from_yaml(self, path=join('config', 'hdx_showcaseitem_static.yml')):
        # type: (str) -> None
        """Update showcase item metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_showcaseitem_static.yml.

        Returns:
            None
        """
        super(ShowcaseItem, self).update_from_yaml(path)

    def update_from_json(self, path=join('config', 'hdx_showcaseitem_static.json')):
        # type: (str) -> None
        """Update showcase item metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_showcaseitem_static.json.

        Returns:
            None
        """
        super(ShowcaseItem, self).update_from_json(path)

    @staticmethod
    def read_from_hdx(identifier, configuration=None):
        # type: (str, Optional[Configuration]) -> Optional['ShowcaseItem']
        """Reads the showcase item given by identifier from HDX and returns ShowcaseItem object

        Args:
            identifier (str): Identifier of showcase item
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[ShowcaseItem]: ShowcaseItem object if successful read, None if not
        """

        showcaseitem = ShowcaseItem(configuration=configuration)
        result = showcaseitem._load_from_hdx('showcaseitem', identifier)
        if result:
            assoc_result, datasets = showcaseitem._read_from_hdx('showcaseitem', identifier, fieldname='showcase_id',
                                                              action=showcaseitem.actions()['list_datasets'])
            if assoc_result:
                showcaseitem[ShowcaseItem.dataset_ids_field] = [dataset['id'] for dataset in datasets]
            return showcaseitem
        return None

    def check_required_fields(self, ignore_fields=list()):
        # type: (List[str]) -> None
        """Check that metadata for showcase item is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        self._check_required_fields('showcaseitem', ignore_fields)

    def _create_or_update(self, fn):
        # type: (Callable[[...], None], ...) -> None
        """Helper function for create and update

        Returns:
            None
        """
        if ShowcaseItem.dataset_ids_field not in self.data:
            raise HDXError("Field %s is missing in showcase item!" % ShowcaseItem.dataset_ids_field)
        dataset_ids = self.data[ShowcaseItem.dataset_ids_field]
        fn()
        assoc_result, datasets = self._read_from_hdx('showcaseitem', self.data['id'], fieldname='showcase_id',
                                                     action=self.actions()['list_datasets'])
        if assoc_result:
            already_existing_ids = [dataset['id'] for dataset in datasets]
        else:
            already_existing_ids = list()
        for dataset_id in already_existing_ids:
            if dataset_id in dataset_ids:
                continue
            dataset_showcase = {'package_id': dataset_id, 'showcase_id': self.data['id']}
            self._write_to_hdx('disassociate', dataset_showcase, 'package_id')
        for dataset_id in dataset_ids:
            if dataset_id in already_existing_ids:
                continue
            dataset_showcase = {'package_id': dataset_id, 'showcase_id': self.data['id']}
            self._write_to_hdx('associate', dataset_showcase, 'package_id')
        self.data[ShowcaseItem.dataset_ids_field] = dataset_ids

    def update_in_hdx(self):
        # type: () -> None
        """Check if showcase item exists in HDX and if so, update it

        Returns:
            None
        """
        self._create_or_update(lambda: self._update_in_hdx('showcaseitem', 'id'))

    def create_in_hdx(self):
        # type: () -> None
        """Check if showcase item exists in HDX and if so, update it, otherwise create it

        Returns:
            None
        """
        self._create_or_update(lambda: self._create_in_hdx('showcaseitem', 'id', 'title'))

    def delete_from_hdx(self):
        # type: () -> None
        """Deletes a showcase item from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('showcaseitem', 'id')

    def get_tags(self):
        # type: () -> List[str]
        """Return the dataset's list of tags

        Returns:
            List[str]: List of tags or [] if there are none
        """
        return self._get_tags()

    def add_tag(self, tag):
        # type: (str) -> None
        """Add a tag

        Args:
            tag (str): Tag to add

        Returns:
            None
        """
        self._add_tag(tag)

    def add_tags(self, tags):
        # type: (List[str]) -> None
        """Add a list of tag

        Args:
            tags (List[str]): List of tags to add

        Returns:
            None
        """
        self.add_tags(tags)

