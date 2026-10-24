# Install the Library

To include the HDX Python library in your project, you must **pip install** or add to
your **requirements.txt** file the following line:

    hdx-python-api==VERSION

Replace **VERSION** with the latest tag available from
<https://github.com/OCHA-DAP/hdx-python-api/tags>.

If you get dependency errors, it is probably the dependencies of the cryptography
package that are missing eg. for Ubuntu: python-dev, libffi-dev and libssl-dev. See
[cryptography dependencies](https://cryptography.io/en/latest/installation/#building-cryptography-on-linux).

If you get import or other errors, then please either recreate your virtualenv if you
are using one or uninstall hdx-python-api, hdx-python-country and hdx-python-utilities
using **pip uninstall**, then install hdx-python-api (which will pull in the other
dependencies).