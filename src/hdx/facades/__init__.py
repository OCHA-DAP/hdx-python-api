import os

logging_kwargs = dict()


def environment_variables(**kwargs):
    hdx_key = os.getenv('HDX_KEY')
    if hdx_key is not None:
        kwargs['hdx_key'] = hdx_key
    user_agent = os.getenv('USER_AGENT')
    if user_agent is not None:
        kwargs['user_agent'] = user_agent
    preprefix = os.getenv('PREPREFIX')
    if preprefix is not None:
        kwargs['preprefix'] = preprefix
    hdx_site = os.getenv('HDX_SITE')
    if hdx_site is not None:
        kwargs['hdx_site'] = hdx_site
    return kwargs
