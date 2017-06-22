import json


class MockResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text

    def json(self):
        return json.loads(self.text)


organization_data = {
    'name': 'MyOrganization1',
    'title': 'Humanitarian Organization',
    'description': 'We do humanitarian work',
}

user_data = {
    'name': 'MyUser1',
    'email': 'xxx@yyy.com',
    'password': 'xxx',
    'fullname': 'xxx xxx',
    'about': 'Data Scientist',
    'capacity': 'admin'
}
