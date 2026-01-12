"""Vocabulary class containing all logic for creating, checking, and updating vocabularies."""

import logging
from collections import OrderedDict
from collections.abc import Sequence
from os.path import join
from typing import Any, Optional

from hdx.utilities.downloader import Download

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXObject

logger = logging.getLogger(__name__)


class ChainRuleError(Exception):
    pass


class Vocabulary(HDXObject):
    """Vocabulary class containing all logic for creating, checking, and updating vocabularies.

    Args:
        initial_data: Initial dataset metadata dictionary. Defaults to None.
        name: Name of vocabulary
        tags: Initial list of tags. Defaults to None.
        configuration: HDX configuration. Defaults to global configuration.
    """

    _approved_vocabulary = None
    _tags_dict = None

    def __init__(
        self,
        initial_data: dict | None = None,
        name: str = None,
        tags: list | None = None,
        configuration: Configuration | None = None,
    ) -> None:
        if not initial_data:
            initial_data = {}
        if name:
            initial_data["name"] = name
        super().__init__(initial_data, configuration=configuration)
        if tags:
            self.add_tags(tags)

    @staticmethod
    def actions() -> dict[str, str]:
        """Dictionary of actions that can be performed on object

        Returns:
            Dictionary of actions that can be performed on object
        """
        return {
            "show": "vocabulary_show",
            "update": "vocabulary_update",
            "create": "vocabulary_create",
            "delete": "vocabulary_delete",
            "list": "vocabulary_list",
            "autocomplete": "tag_autocomplete",
        }

    def update_from_yaml(
        self, path: str = join("config", "hdx_vocabulary_static.yaml")
    ) -> None:
        """Update vocabulary metadata with static metadata from YAML file

        Args:
            path: Path to YAML dataset metadata. Defaults to config/hdx_vocabulary_static.yaml.

        Returns:
            None
        """
        super().update_from_yaml(path)

    def update_from_json(
        self, path: str = join("config", "hdx_vocabulary_static.json")
    ) -> None:
        """Update vocabulary metadata with static metadata from JSON file

        Args:
            path: Path to JSON dataset metadata. Defaults to config/hdx_vocabulary_static.json.

        Returns:
            None
        """
        super().update_from_json(path)

    @classmethod
    def read_from_hdx(
        cls, identifier: str, configuration: Configuration | None = None
    ) -> Optional["Vocabulary"]:
        """Reads the vocabulary given by identifier from HDX and returns Vocabulary object

        Args:
            identifier: Identifier of vocabulary
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            Vocabulary object if successful read, None if not
        """
        return cls._read_from_hdx_class("vocabulary", identifier, configuration)

    @staticmethod
    def get_all_vocabularies(
        configuration: Configuration | None = None,
    ) -> list["Vocabulary"]:
        """Get all vocabulary names in HDX

        Args:
            identifier: Identifier of resource
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            List of Vocabulary objects
        """

        vocabulary = Vocabulary(configuration=configuration)
        vocabulary["id"] = "all vocabulary names"  # only for error message if produced
        vocabularies = []
        for vocabularydict in vocabulary._write_to_hdx("list", {}):
            vocabularies.append(Vocabulary(vocabularydict, configuration=configuration))
        return vocabularies

    def check_required_fields(self, ignore_fields: Sequence[str] = ()) -> None:
        """Check that metadata for vocabulary is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields: Fields to ignore. Default is ().

        Returns:
            None
        """
        self._check_required_fields("vocabulary", ignore_fields)

    def update_in_hdx(self, **kwargs: Any) -> None:
        """Check if vocabulary exists in HDX and if so, update vocabulary

        Returns:
            None
        """
        self._update_in_hdx("vocabulary", "id", force_active=False, **kwargs)

    def create_in_hdx(self, **kwargs: Any) -> None:
        """Check if vocabulary exists in HDX and if so, update it, otherwise create vocabulary

        Returns:
            None
        """
        self._create_in_hdx("vocabulary", "id", "name", force_active=False, **kwargs)

    def delete_from_hdx(self, empty: bool = True) -> None:
        """Deletes a vocabulary from HDX. First tags are removed then vocabulary is deleted.

        Args:
            empty: Remove all tags and update before deleting. Defaults to True.

        Returns:
            None
        """
        if empty and len(self.data["tags"]) != 0:
            self.data["tags"] = []
            self._update_in_hdx(
                "vocabulary", "id", force_active=False, ignore_field="tags"
            )
        self._delete_from_hdx("vocabulary", "id")

    def get_tags(self) -> list[str]:
        """Lists tags in vocabulary

        Returns:
            List of tags in vocabulary
        """
        return self._get_tags()

    def add_tag(self, tag: str) -> bool:
        """Add a tag

        Args:
            tag: Tag to add

        Returns:
            True if tag added or False if tag already present
        """
        return self._add_tag(tag)

    def add_tags(self, tags: Sequence[str]) -> list[str]:
        """Add a list of tags

        Args:
            tags: list of tags to add

        Returns:
            Tags that were successfully added
        """
        return self._add_tags(tags)

    def remove_tag(self, tag: str) -> bool:
        """Remove a tag

        Args:
            tag: Tag to remove

        Returns:
            True if tag removed or False if not
        """
        return self._remove_hdxobject(
            self.data.get("tags"), tag.lower(), matchon="name"
        )

    @classmethod
    def get_approved_vocabulary(
        cls, configuration: Configuration | None = None
    ) -> "Vocabulary":
        """
        Get the HDX approved vocabulary

        Args:
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            HDX Vocabulary object
        """
        if cls._approved_vocabulary is None:
            if configuration is None:
                configuration = Configuration.read()
            vocabulary_name = configuration["approved_tags_vocabulary"]
            cls._approved_vocabulary = Vocabulary.read_from_hdx(
                vocabulary_name, configuration=configuration
            )
        return cls._approved_vocabulary

    @classmethod
    def _read_approved_tags(
        cls,
        url: str | None = None,
        configuration: Configuration | None = None,
    ) -> list[str]:
        """
        Read approved tags

        Args:
            url: Url to read approved tags from. Defaults to None (url in internal configuration).
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            List of approved tags
        """
        with Download(
            full_agent=configuration.get_user_agent(), use_env=False
        ) as downloader:
            if url is None:
                url = configuration["tags_list_url"]
            return list(
                OrderedDict.fromkeys(
                    downloader.download(url).text.replace('"', "").splitlines()
                )
            )

    @classmethod
    def create_approved_vocabulary(
        cls,
        url: str | None = None,
        configuration: Configuration | None = None,
    ) -> "Vocabulary":
        """
        Create the HDX approved vocabulary

        Args:
            url: Tag to check. Defaults to None (url in internal configuration).
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            HDX Vocabulary object
        """
        if configuration is None:
            configuration = Configuration.read()
        vocabulary_name = configuration["approved_tags_vocabulary"]
        tags = cls._read_approved_tags(url=url, configuration=configuration)
        cls._approved_vocabulary = Vocabulary(
            name=vocabulary_name, tags=tags, configuration=configuration
        )
        cls._approved_vocabulary.create_in_hdx()
        return cls._approved_vocabulary

    @classmethod
    def update_approved_vocabulary(
        cls,
        url: str | None = None,
        configuration: Configuration | None = None,
        replace: bool = True,
    ) -> "Vocabulary":
        """
        Update the HDX approved vocabulary

        Args:
            url: Tag to check. Defaults to None (url in internal configuration).
            configuration: HDX configuration. Defaults to global configuration.
            replace: True to replace existing tags, False to append. Defaults to True.

        Returns:
            HDX Vocabulary object
        """
        if configuration is None:
            configuration = Configuration.read()
        vocabulary = cls.get_approved_vocabulary(configuration=configuration)
        if replace:
            vocabulary["tags"] = []
        vocabulary.add_tags(
            cls._read_approved_tags(url=url, configuration=configuration)
        )
        vocabulary.update_in_hdx()
        return vocabulary

    @classmethod
    def delete_approved_vocabulary(
        cls, configuration: Configuration | None = None
    ) -> None:
        """
        Delete the approved vocabulary

        Args:
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            None
        """
        if configuration is None:
            configuration = Configuration.read()
        vocabulary = cls.get_approved_vocabulary(configuration=configuration)
        vocabulary.delete_from_hdx()

    @classmethod
    def approved_tags(cls, configuration: Configuration | None = None) -> list[str]:
        """
        Return list of approved tags

        Args:
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            List of approved tags
        """
        return [
            x["name"]
            for x in cls.get_approved_vocabulary(configuration=configuration)["tags"]
        ]

    @classmethod
    def is_approved(cls, tag: str, configuration: Configuration | None = None) -> bool:
        """
        Return if tag is an approved one or not

        Args:
            tag: Tag to check
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            True if tag is approved, False if not
        """
        if tag.lower() in cls.approved_tags(configuration):
            return True
        return False

    @classmethod
    def read_tags_mappings(
        cls,
        configuration: Configuration | None = None,
        url: str | None = None,
        keycolumn: int = 1,
        failchained: bool = True,
    ) -> dict:
        """
        Read tag mappings and setup tags cleanup dictionaries

        Args:
            configuration: HDX configuration. Defaults to global configuration.
            url: Url of tags cleanup spreadsheet. Defaults to None (internal configuration parameter).
            keycolumn: Column number of tag column in spreadsheet. Defaults to 1.
            failchained: Fail if chained rules found. Defaults to True.

        Returns:
            Returns Tags dictionary
        """
        if not cls._tags_dict:
            if configuration is None:
                configuration = Configuration.read()
            with Download(
                full_agent=configuration.get_user_agent(),
                use_env=False,
            ) as downloader:
                if url is None:
                    url = configuration["tags_mapping_url"]
                cls._tags_dict = downloader.download_tabular_rows_as_dicts(
                    url, keycolumn=keycolumn
                )
                keys = cls._tags_dict.keys()
                chainerror = False
                for i, tag in enumerate(keys):
                    whattodo = cls._tags_dict[tag]
                    final_tags = whattodo["New Tag(s)"]
                    if final_tags is None:
                        continue
                    action = whattodo["Action to Take"]
                    for final_tag in final_tags.split(";"):
                        if final_tag in keys:
                            index = list(keys).index(final_tag)
                            if index != i:
                                whattodo2 = cls._tags_dict[final_tag]
                                action2 = whattodo2["Action to Take"]
                                if action2 != "ok" and action2 != "other":
                                    final_tags2 = whattodo2["New Tag(s)"]
                                    if final_tag not in final_tags2.split(";"):
                                        chainerror = True
                                        if failchained:
                                            logger.error(
                                                f"Chained rules: {action} ({tag} -> {final_tags}) | {action2} ({final_tag} -> {final_tags2})"
                                            )

                if failchained and chainerror:
                    raise ChainRuleError("Chained rules for tags detected!")
        return cls._tags_dict

    @classmethod
    def set_tagsdict(cls, tags_dict: dict) -> None:
        """
        Set tags dictionary

        Args:
            tags_dict: Tags dictionary

        Returns:
            None
        """
        cls._tags_dict = tags_dict

    @classmethod
    def get_mapped_tag(
        cls,
        tag: str,
        log_deleted: bool = True,
        configuration: Configuration | None = None,
    ) -> tuple[list[str], list[str]]:
        """Given a tag, return a list of tag(s) to which it maps and any deleted tags

        Args:
            tag: Tag to map
            log_deleted: Whether to log informational messages about deleted tags. Defaults to True.
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            Tuple containing list of mapped tag(s) and list of deleted tags
        """
        if configuration is None:
            configuration = Configuration.read()
        tag = tag.lower()
        tags_dict = cls.read_tags_mappings(configuration=configuration)
        tags = []
        deleted_tags = []
        whattodo = tags_dict.get(tag)
        if whattodo is None:
            if cls.is_approved(tag):
                tags.append(tag)
            else:
                logger.error(
                    f"Unapproved tag {tag} not in tags mappings! For a list of approved tags see: {configuration['tags_list_url']}"
                )
                deleted_tags.append(tag)
        else:
            action = whattodo["Action to Take"]
            if action == "ok":
                if cls.is_approved(tag):
                    tags.append(tag)
                else:
                    logger.error(
                        f"Tag {tag} is not in CKAN approved tags but is in tags mappings! For a list of approved tags see: {configuration['tags_list_url']}"
                    )
            elif action == "delete":
                if log_deleted:
                    logger.info(
                        f"Tag {tag} is invalid and won't be added! For a list of approved tags see: {configuration['tags_list_url']}"
                    )
                deleted_tags.append(tag)
            elif action == "merge":
                final_tags = whattodo["New Tag(s)"].split(";")
                for final_tag in final_tags:
                    if cls.is_approved(final_tag):
                        tags.append(final_tag)
                    else:
                        logger.error(
                            f"Mapped tag {final_tag} is not in CKAN approved tags but is in tags mappings! For a list of approved tags see: {configuration['tags_list_url']}"
                        )
            else:
                logger.error(f"Invalid action {action}!")
        return tags, deleted_tags

    @classmethod
    def get_mapped_tags(
        cls,
        tags: Sequence[str],
        log_deleted: bool = True,
        configuration: Configuration | None = None,
    ) -> tuple[list[str], list[str]]:
        """Given a list of tags, return a list of tags to which they map and any deleted tags

        Args:
            tags: List of tags to map
            log_deleted: Whether to log informational messages about deleted tags. Defaults to True.
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            Tuple containing list of mapped tags and list of deleted tags
        """
        new_tags = []
        deleted_tags = []
        for tag in tags:
            mapped_tags, del_tags = cls.get_mapped_tag(
                tag, log_deleted=log_deleted, configuration=configuration
            )
            new_tags.extend(mapped_tags)
            deleted_tags.extend(del_tags)
        return list(OrderedDict.fromkeys(new_tags)), list(
            OrderedDict.fromkeys(deleted_tags)
        )

    @classmethod
    def add_mapped_tag(
        cls, hdxobject: HDXObject, tag: str, log_deleted: bool = True
    ) -> tuple[list[str], list[str]]:
        """Add a tag to an HDX object that has tags

        Args:
            hdxobject: HDX object such as dataset
            tag: Tag to add
            log_deleted: Whether to log informational messages about deleted tags. Defaults to True.

        Returns:
            Tuple containing list of added tags and list of deleted tags and tags not added
        """
        return cls.add_mapped_tags(hdxobject, [tag], log_deleted=log_deleted)

    @classmethod
    def add_mapped_tags(
        cls,
        hdxobject: HDXObject,
        tags: Sequence[str],
        log_deleted: bool = True,
    ) -> tuple[list[str], list[str]]:
        """Add a list of tag to an HDX object that has tags

        Args:
            hdxobject: HDX object such as dataset
            tags: List of tags to add
            log_deleted: Whether to log informational messages about deleted tags. Defaults to True.

        Returns:
            Tuple containing list of added tags and list of deleted tags and tags not added
        """
        new_tags, deleted_tags = cls.get_mapped_tags(
            tags,
            log_deleted=log_deleted,
            configuration=hdxobject.configuration,
        )
        added_tags = hdxobject._add_tags(
            new_tags,
            cls.get_approved_vocabulary(configuration=hdxobject.configuration)["id"],
        )
        unadded_tags = [x for x in new_tags if x not in added_tags]
        unadded_tags.extend(deleted_tags)
        return added_tags, unadded_tags

    @classmethod
    def clean_tags(
        cls, hdxobject: HDXObject, log_deleted: bool = True
    ) -> tuple[list[str], list[str]]:
        """Clean tags in an HDX object according to tags cleanup spreadsheet

        Args:
            hdxobject: HDX object such as dataset
            log_deleted: Whether to log informational messages about deleted tags. Defaults to True.

        Returns:
            Tuple containing list of mapped tags and list of deleted tags and tags not added
        """
        tags = hdxobject._get_tags()
        hdxobject["tags"] = []
        return cls.add_mapped_tags(hdxobject, tags, log_deleted=log_deleted)

    @classmethod
    def autocomplete(
        cls,
        name: str,
        limit: int = 20,
        configuration: Configuration | None = None,
        **kwargs: Any,
    ) -> list:
        """Autocomplete a tag name and return matches

        Args:
            name: Name to autocomplete
            limit: Maximum number of matches to return
            configuration: HDX configuration. Defaults to global configuration.
            **kwargs:
            offset: The offset to start returning tags from.

        Returns:
            Autocomplete matches
        """
        return cls._autocomplete(name, limit, configuration, **kwargs)
