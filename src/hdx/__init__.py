# -*- coding: utf-8 -*-
configuration = None
remoteckan = None
validlocations = None


def get_validlocations():
    return remoteckan.call_action('group_list', {'all_fields': True},
                                  requests_kwargs={'auth': configuration._get_credentials()})
