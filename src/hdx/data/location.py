"""Location class containing all logic for creating, checking, and updating locations."""

import logging
from collections.abc import Sequence
from typing import Any, Optional

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXObject

logger = logging.getLogger(__name__)


class Location(HDXObject):
    """Location class containing all logic for creating, checking, and updating locations.
    A location is a CKAN group.

    Args:
        initial_data: Initial location metadata dictionary. Defaults to None.
        configuration: HDX configuration. Defaults to global configuration.
    """

    def __init__(
        self,
        initial_data: dict | None = None,
        configuration: Configuration | None = None,
    ) -> None:
        if not initial_data:
            initial_data = {}
        super().__init__(initial_data, configuration=configuration)

    @staticmethod
    def actions() -> dict[str, str]:
        """Dictionary of actions that can be performed on object

        Returns:
            Dictionary of actions that can be performed on object
        """
        return {
            "show": "group_show",
            "update": "group_update",
            "create": "group_create",
            "delete": "group_delete",
            "list": "group_list",
            "autocomplete": "group_autocomplete",
        }

    @classmethod
    def read_from_hdx(
        cls, identifier: str, configuration: Configuration | None = None
    ) -> Optional["Location"]:
        """Reads the location given by identifier from HDX and returns Location object

        Args:
            identifier: Identifier of location
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            Location object if successful read, None if not
        """
        return cls._read_from_hdx_class("group", identifier, configuration)

    def check_required_fields(self, ignore_fields: Sequence[str] = ()) -> None:
        """Check that metadata for location is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields: Fields to ignore. Default is ().

        Returns:
            None
        """
        self._check_required_fields("group", ignore_fields)

    def update_in_hdx(self, **kwargs: Any) -> None:
        """Check if location exists in HDX and if so, update location

        Returns:
            None
        """
        self._update_in_hdx("group", "id", **kwargs)

    def create_in_hdx(self, **kwargs: Any) -> None:
        """Check if location exists in HDX and if so, update it, otherwise create location

        Returns:
            None
        """
        self._create_in_hdx("group", "id", "name", **kwargs)

    def delete_from_hdx(self) -> None:
        """Deletes a location from HDX.

        Returns:
            None
        """
        self._delete_from_hdx("group", "id")

    def get_datasets(self, query: str = "*:*", **kwargs: Any) -> list["Dataset"]:  # noqa: F821
        """Get list of datasets in location

        Args:
            query: Restrict datasets returned to this query (in Solr format). Defaults to '*:*'.
            **kwargs: See below
            sort (string): Sorting of the search results. Defaults to 'relevance asc, metadata_modified desc'.
            rows (int): Number of matching rows to return. Defaults to all datasets (sys.maxsize).
            start (int): Offset in the complete result for where the set of returned datasets should begin
            facet (string): Whether to enable faceted results. Default to True.
            facet.mincount (int): Minimum counts for facet fields should be included in the results
            facet.limit (int): Maximum number of values the facet fields return (- = unlimited). Defaults to 50.
            facet.field (list[str]): Fields to facet upon. Default is empty.
            use_default_schema (bool): Use default package schema instead of custom schema. Defaults to False.

        Returns:
            List of datasets in location
        """
        import hdx.data.dataset  # avoid circular import

        return hdx.data.dataset.Dataset.search_in_hdx(
            query=query,
            configuration=self.configuration,
            fq=f"groups:{self.data['name']}",
            **kwargs,
        )

    @staticmethod
    def get_all_location_names(
        configuration: Configuration | None = None, **kwargs: Any
    ) -> list[str]:
        """Get all location names in HDX

        Args:
            configuration: HDX configuration. Defaults to global configuration.
            **kwargs: See below
            sort (str): Sort the search results according to field name and sort-order. Allowed fields are ‘name’, ‘package_count’ and ‘title’. Defaults to 'name asc'.
            groups (list[str]): List of names of the groups to return.
            all_fields (bool): Return group dictionaries instead of just names. Only core fields are returned - get some more using the include_* options. Defaults to False.
            include_extras (bool): If all_fields, include the group extra fields. Defaults to False.
            include_tags (bool): If all_fields, include the group tags. Defaults to False.
            include_groups (bool): If all_fields, include the groups the groups are in. Defaults to False.

        Returns:
            List of all location names in HDX
        """
        location = Location(configuration=configuration)

        # Early return for the standard, non-paginated case
        if not kwargs.get("all_fields"):
            return location._write_to_hdx("list", kwargs)

        compiled_locations = {}
        # Extract limit and offset to dictate our paging sizes, defaulting to 400 and 0
        page_size = kwargs.pop("limit", 400)
        current_offset = kwargs.pop("offset", 0)

        # 1. Fetch in pages using the provided/default page size
        while True:
            page_kwargs = kwargs.copy()
            page_kwargs["limit"] = page_size
            page_kwargs["offset"] = current_offset

            page_data = location._write_to_hdx("list", page_kwargs)

            if not page_data:
                break

            # Store by name to automatically deduplicate offset shifts
            for loc in page_data:
                compiled_locations[loc["name"]] = loc

            if len(page_data) < page_size:
                break

            current_offset += page_size

        # 2. Establish Ground Truth LAST (without limit/offset constraints)
        truth_kwargs = kwargs.copy()
        truth_kwargs["all_fields"] = False

        ground_truth_names = location._write_to_hdx("list", truth_kwargs)

        # 3 & 4. Check for missing IDs, fetch individually, and implicitly prune deletes
        final_locations = []
        for name in ground_truth_names:
            if name in compiled_locations:
                final_locations.append(compiled_locations[name])
            else:
                try:
                    missing_location = location._write_to_hdx("show", {"id": name})
                    if missing_location:
                        final_locations.append(missing_location)
                except Exception:
                    pass

        return final_locations

    @staticmethod
    def get_data_grid_countries(
        configuration: Configuration | None = None,
    ) -> list[str]:
        """Fetch HDX's active Data Grid countries (3-letter group names).

        Treated as a live/short-TTL lookup rather than a frozen static list, since Data Grid
        membership changes over time.

        Args:
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            Sorted list of active Data Grid country (group) names
        """
        groups = Location.get_all_location_names(
            configuration=configuration, all_fields=True, include_extras=True
        )
        return sorted(
            group["name"]
            for group in groups
            if len(group["name"]) == 3 and group.get("data_completeness") == "active"
        )

    @classmethod
    def autocomplete(
        cls,
        name: str,
        limit: int = 20,
        configuration: Configuration | None = None,
    ) -> list:
        """Autocomplete a location name and return matches

        Args:
            name: Name to autocomplete
            limit: Maximum number of matches to return
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            Autocomplete matches
        """
        return cls._autocomplete(name, limit, configuration)
