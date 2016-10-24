#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Calls a function that generates a dataset and creates it in HDX.

'''
import logging

from hdx.configuration import Configuration
from hdx.facades.scraperwiki import facade
from .my_code import generate_dataset

logger = logging.getLogger(__name__)


def main(configuration: Configuration):
    '''Generate dataset and create it in HDX'''

    dataset = generate_dataset(configuration)
    dataset.create_in_hdx()


if __name__ == '__main__':
    facade(main, hdx_site='test')
