# Set Organization

HDX datasets must be part of an organization.

If you wish to get the current organization, you can do this:

    organization = dataset.get_organization()

If you want to set the organization, you do it like this:

    dataset.set_organization(ORGANIZATION)

ORGANIZATION is either a string id, dictionary or an Organization
object.