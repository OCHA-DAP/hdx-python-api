"""User class containing all logic for creating, checking, and updating users."""
import logging
from os.path import join
from typing import Any, Dict, List, Optional

import hdx.data.organization
from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXObject

logger = logging.getLogger(__name__)


class User(HDXObject):
    """User class containing all logic for creating, checking, and updating users.

    Args:
        initial_data (Optional[Dict]): Initial user metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    def __init__(
        self,
        initial_data: Optional[Dict] = None,
        configuration: Optional[Configuration] = None,
    ) -> None:
        if not initial_data:
            initial_data = dict()
        super().__init__(initial_data, configuration=configuration)

    @staticmethod
    def actions() -> Dict[str, str]:
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            "show": "user_show",
            "update": "user_update",
            "create": "user_create",
            "delete": "user_delete",
            "list": "user_list",
            "listorgs": "organization_list_for_user",
            "autocomplete": "user_autocomplete",
        }

    def update_from_yaml(
        self, path: str = join("config", "hdx_user_static.yml")
    ) -> None:
        """Update user metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_user_static.yml.

        Returns:
            None
        """
        super().update_from_yaml(path)

    def update_from_json(
        self, path: str = join("config", "hdx_user_static.json")
    ) -> None:
        """Update user metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_user_static.json.

        Returns:
            None
        """
        super().update_from_json(path)

    @classmethod
    def read_from_hdx(
        cls, identifier: str, configuration: Optional[Configuration] = None
    ) -> Optional["User"]:
        """Reads the user given by identifier from HDX and returns User object

        Args:
            identifier (str): Identifier of user
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[User]: User object if successful read, None if not
        """
        return cls._read_from_hdx_class("user", identifier, configuration)

    def check_required_fields(self, ignore_fields: List[str] = list()) -> None:
        """Check that metadata for user is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        self._check_required_fields("user", ignore_fields)

    def update_in_hdx(self, **kwargs: Any) -> None:
        """Check if user exists in HDX and if so, update user

        Returns:
            None
        """
        capacity = self.data.get("capacity")
        if capacity is not None:
            del self.data[
                "capacity"
            ]  # remove capacity (which comes from users from Organization)
        self._update_in_hdx("user", "id", **kwargs)
        if capacity is not None:
            self.data["capacity"] = capacity

    def create_in_hdx(self, **kwargs: Any) -> None:
        """Check if user exists in HDX and if so, update it, otherwise create user

        Returns:
            None
        """
        capacity = self.data.get("capacity")
        if capacity is not None:
            del self.data["capacity"]
        self._create_in_hdx("user", "id", "name", **kwargs)
        if capacity is not None:
            self.data["capacity"] = capacity

    def delete_from_hdx(self) -> None:
        """Deletes a user from HDX.

        Returns:
            None
        """
        self._delete_from_hdx("user", "id")

    def email(
        self,
        subject: str,
        text_body: str,
        html_body: Optional[str] = None,
        sender: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Emails a user.

        Args:
            subject (str): Email subject
            text_body (str): Plain text email body
            html_body (str): HTML email body
            sender (Optional[str]): Email sender. Defaults to SMTP username.
            **kwargs: See below
            mail_options (List): Mail options (see smtplib documentation)
            rcpt_options (List): Recipient options (see smtplib documentation)

        Returns:
            None
        """
        self.configuration.emailer().send(
            [self.data["email"]],
            subject,
            text_body,
            html_body=html_body,
            sender=sender,
            **kwargs,
        )

    @staticmethod
    def get_all_users(
        configuration: Optional[Configuration] = None, **kwargs: Any
    ) -> List["User"]:
        """Get all users in HDX

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs: See below
            q (str): Restrict to names containing a string. Defaults to all users.
            order_by (str): Field by which to sort - any user field or edits (number_of_edits). Defaults to 'name'.

        Returns:
            List[User]: List of all users in HDX
        """
        user = User(configuration=configuration)
        result = user._write_to_hdx("list", kwargs)
        users = list()
        if result:
            for userdict in result:
                user = User(userdict, configuration=configuration)
                users.append(user)
        else:
            logger.debug(result)
        return users

    @staticmethod
    def email_users(
        users: List["User"],
        subject: str,
        text_body: str,
        html_body: Optional[str] = None,
        sender: Optional[str] = None,
        cc: Optional[List["User"]] = None,
        bcc: Optional[List["User"]] = None,
        configuration: Optional[Configuration] = None,
        **kwargs: Any,
    ) -> None:
        """Email a list of users

        Args:
            users (List[User]): List of users in To address
            subject (str): Email subject
            text_body (str): Plain text email body
            html_body (str): HTML email body
            sender (Optional[str]): Email sender. Defaults to SMTP username.
            cc (Optional[List[User]]: List of users to cc. Defaults to None.
            bcc (Optional[List[User]]: List of users to bcc. Defaults to None.
            configuration (Optional[Configuration]): HDX configuration. Defaults to configuration of first user in list.
            **kwargs: See below
            mail_options (List): Mail options (see smtplib documentation)
            rcpt_options (List): Recipient options (see smtplib documentation)

        Returns:
            None
        """
        if not users:
            raise ValueError("No users supplied")
        recipients = list()
        for user in users:
            recipients.append(user.data["email"])
        ccemails = list()
        for user in cc:
            ccemails.append(user.data["email"])
        bccemails = list()
        for user in bcc:
            bccemails.append(user.data["email"])
        if configuration is None:
            configuration = users[0].configuration
        configuration.emailer().send(
            recipients,
            subject,
            text_body,
            html_body=html_body,
            sender=sender,
            cc=ccemails,
            bcc=bccemails,
            **kwargs,
        )

    def get_organizations(
        self, permission: str = "read"
    ) -> List["Organization"]:  # noqa: F821
        """Get organizations in HDX that this user is a member of.

        Args:
            permission (str): Permission to check for. Defaults to 'read'.

        Returns:
            List[Organization]: List of organizations in HDX that this user is a member of
        """
        success, result = self._read_from_hdx(
            "user",
            self.data["name"],
            "id",
            self.actions()["listorgs"],
            permission=permission,
        )
        organizations = list()
        if success:
            for organizationdict in result:
                org = hdx.data.organization.Organization.read_from_hdx(
                    organizationdict["id"]
                )
                organizations.append(org)
        return organizations

    @classmethod
    def autocomplete(
        cls,
        name: str,
        limit: int = 20,
        configuration: Optional[Configuration] = None,
    ) -> List:
        """Autocomplete a user name and return matches

        Args:
            name (str): Name to autocomplete
            limit (int): Maximum number of matches to return
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List: Autocomplete matches
        """
        return cls._autocomplete(name, limit, configuration)
