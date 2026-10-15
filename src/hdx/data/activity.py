"""Activity class containing all logic for reading activity streams."""

import logging
from collections.abc import Sequence
from typing import Any, Optional

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError, HDXObject

logger = logging.getLogger(__name__)


class Activity(HDXObject):
    """Activity class containing all logic for reading activity streams. Activities
    are read only.

    Args:
        initial_data: Initial activity metadata dictionary. Defaults to None.
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
            "show": "activity_show",
            "data_show": "activity_data_show",
            "diff": "activity_diff",
            "package_list": "package_activity_list",
            "organization_list": "organization_activity_list",
            "group_list": "group_activity_list",
            "user_list": "user_activity_list",
            "recently_changed_packages": "recently_changed_packages_activity_list",
        }

    @classmethod
    def read_from_hdx(
        cls, identifier: str, configuration: Configuration | None = None
    ) -> Optional["Activity"]:
        """Reads the activity given by identifier from HDX and returns Activity object.
        HDX requires permission to edit the object the activity relates to.

        Args:
            identifier: Identifier of activity
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            Activity object if successful read, None if not
        """
        return cls._read_from_hdx_class("activity", identifier, configuration)

    def get_object_type(self) -> str:
        """Get the CKAN object type the activity relates to eg. package,
        organization, group or user. This is the last word of the activity type
        (eg. "changed package") with "dataset" (from "follow dataset") mapped to
        "package".

        Returns:
            Object type
        """
        object_type = self.data["activity_type"].split(" ")[-1]
        if object_type == "dataset":
            return "package"
        return object_type

    def get_data(self) -> dict:
        """Get the data of the activity which includes a snapshot of the object
        at the time of the activity. HDX requires permission to edit the object
        the activity relates to.

        Returns:
            Activity data
        """
        return self._write_to_hdx("data_show", {"id": self.data["id"]}, "id")

    def get_diff(self, diff_type: str = "unified") -> dict:
        """Get a diff of the object compared to its previous version. HDX requires
        permission to edit the object the activity relates to.

        Args:
            diff_type: One of unified, context or html. Defaults to unified.

        Returns:
            Activity diff
        """
        data = {
            "id": self.data["id"],
            "object_type": self.get_object_type(),
            "diff_type": diff_type,
        }
        return self._write_to_hdx("diff", data, "id")

    @classmethod
    def _get_list(
        cls,
        action: str,
        data: dict,
        configuration: Configuration | None = None,
        all_pages: bool = False,
        max_activities: int | None = None,
    ) -> list["Activity"]:
        """Helper method to get a list of activities from HDX

        Args:
            action: Action to perform
            data: Data to pass to CKAN including any offset and limit
            configuration: HDX configuration. Defaults to global configuration.
            all_pages: Whether to get all pages of results. Defaults to False.
            max_activities: Maximum number of activities if all_pages is True. Defaults to None (no maximum).

        Returns:
            List of Activity objects
        """
        activity = cls(configuration=configuration)
        if not all_pages:
            result = activity._write_to_hdx(action, data)
            return [cls(x, configuration=configuration) for x in result]
        data = dict(data)
        # HDX silently caps limit at ckan.activity_list_limit_max
        limit = min(data.get("limit", 100), 100)
        data["limit"] = limit
        offset = data.get("offset", 0)
        activities = []
        while True:
            data["offset"] = offset
            result = activity._write_to_hdx(action, data)
            activities.extend(cls(x, configuration=configuration) for x in result)
            if max_activities is not None and len(activities) >= max_activities:
                return activities[:max_activities]
            if len(result) < limit:
                return activities
            offset += limit

    @classmethod
    def get_activities(
        cls,
        object_type: str,
        identifier: str,
        configuration: Configuration | None = None,
        all_pages: bool = False,
        **kwargs: Any,
    ) -> list["Activity"]:
        """Get the activity stream of a CKAN object. One page of results (whose
        size is given by limit) is returned unless all_pages is True.

        Args:
            object_type: One of package, organization, group or user
            identifier: Id or name of object
            configuration: HDX configuration. Defaults to global configuration.
            all_pages: Whether to get all pages of results. Defaults to False.
            **kwargs: See below
            offset (int): Where to start getting activity items from. Defaults to 0.
            limit (int): Maximum number of activities per page. Defaults to 31 (HDX maximum 100) or 100 if all_pages is True.
            after (int | str): Only activities after this timestamp (package and user only)
            before (int | str): Only activities before this timestamp (package and user only)
            include_hidden_activity (bool): Include hidden activity, sysadmins only (package, organization and group only)
            activity_types (list): Activity types to include (package only)
            exclude_activity_types (list): Activity types to exclude (package only)

        Returns:
            List of Activity objects
        """
        if not identifier:
            raise HDXError(f"No id or name for {object_type} to get activities!")
        data = {"id": identifier}
        data.update(kwargs)
        return cls._get_list(f"{object_type}_list", data, configuration, all_pages)

    @classmethod
    def get_recently_changed_packages(
        cls,
        configuration: Configuration | None = None,
        all_pages: bool = False,
        max_activities: int | None = 1000,
        **kwargs: Any,
    ) -> list["Activity"]:
        """Get the activity stream of all recently added or changed datasets.
        One page of results (whose size is given by limit) is returned unless
        all_pages is True. all_pages is capped by max_activities because the
        stream covers the whole history of HDX.

        Args:
            configuration: HDX configuration. Defaults to global configuration.
            all_pages: Whether to get all pages of results. Defaults to False.
            max_activities: Maximum number of activities if all_pages is True. Defaults to 1000. None means no maximum (very slow).
            **kwargs: See below
            offset (int): Where to start getting activity items from. Defaults to 0.
            limit (int): Maximum number of activities per page. Defaults to 31 (HDX maximum 100) or 100 if all_pages is True.

        Returns:
            List of Activity objects
        """
        return cls._get_list(
            "recently_changed_packages",
            kwargs,
            configuration,
            all_pages,
            max_activities,
        )

    def check_required_fields(self, ignore_fields: Sequence[str] = ()) -> None:
        """Activities are read only so this raises an exception

        Args:
            ignore_fields: Fields to ignore. Default is ().

        Returns:
            None
        """
        raise HDXError("Activities are read only!")

    def update_in_hdx(self, **kwargs: Any) -> None:
        """Activities are read only so this raises an exception

        Returns:
            None
        """
        raise HDXError("Activities are read only!")

    def create_in_hdx(self, **kwargs: Any) -> None:
        """Activities are read only so this raises an exception

        Returns:
            None
        """
        raise HDXError("Activities are read only!")

    def delete_from_hdx(self) -> None:
        """Activities are read only so this raises an exception

        Returns:
            None
        """
        raise HDXError("Activities are read only!")
