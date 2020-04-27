# -*- coding: utf-8 -*-
'''
Calls a function that generates a dataset and creates it in HDX.

'''
import logging

from hdx.facades.simple import facade
from .my_code import generate_dataset

logger = logging.getLogger(__name__)


def main():
    '''Generate dataset and create it in HDX'''

    dataset = generate_dataset()
    dataset.create_in_hdx()


if __name__ == '__main__':
    facade(main, hdx_site='stage')
