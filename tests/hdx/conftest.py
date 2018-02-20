# -*- coding: UTF-8 -*-
"""Global fixtures"""
import smtplib
from os.path import join

import pytest

from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations


@pytest.fixture(scope='session')
def fixturesfolder():
    return join('tests', 'fixtures')


@pytest.fixture(scope='session')
def configfolder(fixturesfolder):
        return join(fixturesfolder, 'config')


@pytest.fixture(scope='session')
def hdx_key_file():
    return join('tests', 'fixtures', '.hdxkey')


@pytest.fixture(scope='session')
def project_config_yaml():
    return join('tests', 'fixtures', 'config', 'project_configuration.yml')


@pytest.fixture(scope='session')
def locations():
     Locations.set_validlocations([{'display_name': 'Aruba', 'id': 'abw', 'name': 'abw', 'title': 'Aruba'},
                                   {'display_name': 'Afghanistan', 'id': 'afg', 'name': 'afg', 'title': 'Afghanistan'},
                                   {'display_name': 'Angola', 'id': 'ago', 'name': 'ago', 'title': 'Angola'},
                                   {'display_name': 'Anguilla', 'id': 'aia', 'name': 'aia', 'title': 'Anguilla'},
                                   {'display_name': 'Albania', 'id': 'alb', 'name': 'alb', 'title': 'Albania'},
                                   {'display_name': 'Andorra', 'id': 'and', 'name': 'and', 'title': 'Andorra'},
                                   {'display_name': 'Netherlands Antilles', 'id': 'ant', 'name': 'ant', 'title': 'Netherlands Antilles'},
                                   {'display_name': 'United Arab Emirates', 'id': 'are', 'name': 'are', 'title': 'United Arab Emirates'},
                                   {'display_name': 'Argentina', 'id': 'arg', 'name': 'arg', 'title': 'Argentina'},
                                   {'display_name': 'Armenia', 'id': 'arm', 'name': 'arm', 'title': 'Armenia'},
                                   {'display_name': 'American Samoa', 'id': 'asm', 'name': 'asm', 'title': 'American Samoa'},
                                   {'display_name': 'Antartica', 'id': 'ata', 'name': 'ata', 'title': 'Antartica'},
                                   {'display_name': 'French Southern Territories', 'id': 'atf', 'name': 'atf', 'title': 'French Southern Territories'},
                                   {'display_name': 'Antigua And Barbuda', 'id': 'atg', 'name': 'atg', 'title': 'Antigua And Barbuda'},
                                   {'display_name': 'Australia', 'id': 'aus', 'name': 'aus', 'title': 'Australia'},
                                   {'display_name': 'Austria', 'id': 'aut', 'name': 'aut', 'title': 'Austria'},
                                   {'display_name': 'Azerbajan', 'id': 'aze', 'name': 'aze', 'title': 'Azerbajan'},
                                   {'display_name': 'Burundi', 'id': 'bdi', 'name': 'bdi', 'title': 'Burundi'},
                                   {'display_name': 'Belgium', 'id': 'bel', 'name': 'bel', 'title': 'Belgium'},
                                   {'display_name': 'Benin', 'id': 'ben', 'name': 'ben', 'title': 'Benin'},
                                   {'display_name': 'Burkina Faso', 'id': 'bfa', 'name': 'bfa', 'title': 'Burkina Faso'},
                                   {'display_name': 'Bangladesh', 'id': 'bgd', 'name': 'bgd', 'title': 'Bangladesh'},
                                   {'display_name': 'Bulgaria', 'id': 'bgr', 'name': 'bgr', 'title': 'Bulgaria'},
                                   {'display_name': 'Bahrain', 'id': 'bhr', 'name': 'bhr', 'title': 'Bahrain'},
                                   {'display_name': 'Bahamas', 'id': 'bhs', 'name': 'bhs', 'title': 'Bahamas'},
                                   {'display_name': 'Bosnia And Herzegowina', 'id': 'bih', 'name': 'bih', 'title': 'Bosnia And Herzegowina'},
                                   {'display_name': 'Belarus', 'id': 'blr', 'name': 'blr', 'title': 'Belarus'},
                                   {'display_name': 'Belize', 'id': 'blz', 'name': 'blz', 'title': 'Belize'},
                                   {'display_name': 'Bermuda', 'id': 'bmu', 'name': 'bmu', 'title': 'Bermuda'},
                                   {'display_name': 'Bolivia (Plurinational State of)', 'id': 'bol', 'name': 'bol', 'title': 'Bolivia (Plurinational State of)'},
                                   {'display_name': 'Brazil', 'id': 'bra', 'name': 'bra', 'title': 'Brazil'},
                                   {'display_name': 'Barbados', 'id': 'brb', 'name': 'brb', 'title': 'Barbados'},
                                   {'display_name': 'Brunei Darussalam', 'id': 'brn', 'name': 'brn', 'title': 'Brunei Darussalam'},
                                   {'display_name': 'Bhutan', 'id': 'btn', 'name': 'btn', 'title': 'Bhutan'},
                                   {'display_name': 'Bouvet Island', 'id': 'bvt', 'name': 'bvt', 'title': 'Bouvet Island'},
                                   {'display_name': 'Botswana', 'id': 'bwa', 'name': 'bwa', 'title': 'Botswana'},
                                   {'display_name': 'Central African Republic', 'id': 'caf', 'name': 'caf', 'title': 'Central African Republic'},
                                   {'display_name': 'Canada', 'id': 'can', 'name': 'can', 'title': 'Canada'},
                                   {'display_name': 'Cocos (Keeling) Islands', 'id': 'cck', 'name': 'cck', 'title': 'Cocos (Keeling) Islands'},
                                   {'display_name': 'Switzerland', 'id': 'che', 'name': 'che', 'title': 'Switzerland'},
                                   {'display_name': 'Chile', 'id': 'chl', 'name': 'chl', 'title': 'Chile'},
                                   {'display_name': 'China', 'id': 'chn', 'name': 'chn', 'title': 'China'},
                                   {'display_name': "Côte d'Ivoire", 'id': 'civ', 'name': 'civ', 'title': "Côte d'Ivoire"},
                                   {'display_name': 'Cameroon', 'id': 'cmr', 'name': 'cmr', 'title': 'Cameroon'},
                                   {'display_name': 'Democratic Republic of the Congo', 'id': 'cod', 'name': 'cod', 'title': 'Democratic Republic of the Congo'},
                                   {'display_name': 'Congo', 'id': 'cog', 'name': 'cog', 'title': 'Congo'},
                                   {'display_name': 'Cook Islands', 'id': 'cok', 'name': 'cok', 'title': 'Cook Islands'},
                                   {'display_name': 'Colombia', 'id': 'col', 'name': 'col', 'title': 'Colombia'},
                                   {'display_name': 'Comoros', 'id': 'com', 'name': 'com', 'title': 'Comoros'},
                                   {'display_name': 'Cape Verde', 'id': 'cpv', 'name': 'cpv', 'title': 'Cape Verde'},
                                   {'display_name': 'Costa Rica', 'id': 'cri', 'name': 'cri', 'title': 'Costa Rica'},
                                   {'display_name': 'Cuba', 'id': 'cub', 'name': 'cub', 'title': 'Cuba'},
                                   {'display_name': 'Christmas Island', 'id': 'cxr', 'name': 'cxr', 'title': 'Christmas Island'},
                                   {'display_name': 'Cayman Islands', 'id': 'cym', 'name': 'cym', 'title': 'Cayman Islands'},
                                   {'display_name': 'Cyprus', 'id': 'cyp', 'name': 'cyp', 'title': 'Cyprus'},
                                   {'display_name': 'Czech Republic', 'id': 'cze', 'name': 'cze', 'title': 'Czech Republic'},
                                   {'display_name': 'Germany', 'id': 'deu', 'name': 'deu', 'title': 'Germany'},
                                   {'display_name': 'Djibouti', 'id': 'dji', 'name': 'dji', 'title': 'Djibouti'},
                                   {'display_name': 'Dominica', 'id': 'dma', 'name': 'dma', 'title': 'Dominica'},
                                   {'display_name': 'Denmark', 'id': 'dnk', 'name': 'dnk', 'title': 'Denmark'},
                                   {'display_name': 'Dominican Republic', 'id': 'dom', 'name': 'dom', 'title': 'Dominican Republic'},
                                   {'display_name': 'Algeria', 'id': 'dza', 'name': 'dza', 'title': 'Algeria'},
                                   {'display_name': 'Ecuador', 'id': 'ecu', 'name': 'ecu', 'title': 'Ecuador'},
                                   {'display_name': 'Egypt', 'id': 'egy', 'name': 'egy', 'title': 'Egypt'},
                                   {'display_name': 'Eritrea', 'id': 'eri', 'name': 'eri', 'title': 'Eritrea'},
                                   {'display_name': 'Western Sahara', 'id': 'esh', 'name': 'esh', 'title': 'Western Sahara'},
                                   {'display_name': 'Spain', 'id': 'esp', 'name': 'esp', 'title': 'Spain'},
                                   {'display_name': 'Estonia', 'id': 'est', 'name': 'est', 'title': 'Estonia'},
                                   {'display_name': 'Ethiopia', 'id': 'eth', 'name': 'eth', 'title': 'Ethiopia'},
                                   {'display_name': 'Finland', 'id': 'fin', 'name': 'fin', 'title': 'Finland'},
                                   {'display_name': 'Fiji', 'id': 'fji', 'name': 'fji', 'title': 'Fiji'},
                                   {'display_name': 'Falkland Islands (Malvinas)', 'id': 'flk', 'name': 'flk', 'title': 'Falkland Islands (Malvinas)'},
                                   {'display_name': 'France', 'id': 'fra', 'name': 'fra', 'title': 'France'},
                                   {'display_name': 'Faroe Islands', 'id': 'fro', 'name': 'fro', 'title': 'Faroe Islands'},
                                   {'display_name': 'Micronesia, Federated States Of', 'id': 'fsm', 'name': 'fsm', 'title': 'Micronesia, Federated States Of'},
                                   {'display_name': 'France, Metropolitan', 'id': 'fxx', 'name': 'fxx', 'title': 'France, Metropolitan'},
                                   {'display_name': 'Gabon', 'id': 'gab', 'name': 'gab', 'title': 'Gabon'},
                                   {'display_name': 'United Kingdom', 'id': 'gbr', 'name': 'gbr', 'title': 'United Kingdom'},
                                   {'display_name': 'Georgia', 'id': 'geo', 'name': 'geo', 'title': 'Georgia'},
                                   {'display_name': 'Ghana', 'id': 'gha', 'name': 'gha', 'title': 'Ghana'},
                                   {'display_name': 'Gibraltar', 'id': 'gib', 'name': 'gib', 'title': 'Gibraltar'},
                                   {'display_name': 'Guinea', 'id': 'gin', 'name': 'gin', 'title': 'Guinea'},
                                   {'display_name': 'Guadeloupe', 'id': 'glp', 'name': 'glp', 'title': 'Guadeloupe'},
                                   {'display_name': 'Gambia', 'id': 'gmb', 'name': 'gmb', 'title': 'Gambia'},
                                   {'display_name': 'Guinea-Bissau', 'id': 'gnb', 'name': 'gnb', 'title': 'Guinea-Bissau'},
                                   {'display_name': 'Equatorial Guinea', 'id': 'gnq', 'name': 'gnq', 'title': 'Equatorial Guinea'},
                                   {'display_name': 'Greece', 'id': 'grc', 'name': 'grc', 'title': 'Greece'},
                                   {'display_name': 'Grenada', 'id': 'grd', 'name': 'grd', 'title': 'Grenada'},
                                   {'display_name': 'Greenland', 'id': 'grl', 'name': 'grl', 'title': 'Greenland'},
                                   {'display_name': 'Guatemala', 'id': 'gtm', 'name': 'gtm', 'title': 'Guatemala'},
                                   {'display_name': 'French Guiana', 'id': 'guf', 'name': 'guf', 'title': 'French Guiana'},
                                   {'display_name': 'Guam', 'id': 'gum', 'name': 'gum', 'title': 'Guam'},
                                   {'display_name': 'Guyana', 'id': 'guy', 'name': 'guy', 'title': 'Guyana'},
                                   {'display_name': 'Hong Kong', 'id': 'hkg', 'name': 'hkg', 'title': 'Hong Kong'},
                                   {'display_name': 'Heard And McDonald Islands', 'id': 'hmd', 'name': 'hmd', 'title': 'Heard And McDonald Islands'},
                                   {'display_name': 'Honduras', 'id': 'hnd', 'name': 'hnd', 'title': 'Honduras'},
                                   {'display_name': 'Croatia', 'id': 'hrv', 'name': 'hrv', 'title': 'Croatia'},
                                   {'display_name': 'Haiti', 'id': 'hti', 'name': 'hti', 'title': 'Haiti'},
                                   {'display_name': 'Hungary', 'id': 'hun', 'name': 'hun', 'title': 'Hungary'},
                                   {'display_name': 'Indonesia', 'id': 'idn', 'name': 'idn', 'title': 'Indonesia'},
                                   {'display_name': 'India', 'id': 'ind', 'name': 'ind', 'title': 'India'},
                                   {'display_name': 'British Indian Ocean Territory', 'id': 'iot', 'name': 'iot', 'title': 'British Indian Ocean Territory'},
                                   {'display_name': 'Ireland', 'id': 'irl', 'name': 'irl', 'title': 'Ireland'},
                                   {'display_name': 'Iran (Islamic Republic Of)', 'id': 'irn', 'name': 'irn', 'title': 'Iran (Islamic Republic Of)'},
                                   {'display_name': 'Iraq', 'id': 'irq', 'name': 'irq', 'title': 'Iraq'},
                                   {'display_name': 'Iceland', 'id': 'isl', 'name': 'isl', 'title': 'Iceland'},
                                   {'display_name': 'Israel', 'id': 'isr', 'name': 'isr', 'title': 'Israel'},
                                   {'display_name': 'Italy', 'id': 'ita', 'name': 'ita', 'title': 'Italy'},
                                   {'display_name': 'Jamaica', 'id': 'jam', 'name': 'jam', 'title': 'Jamaica'},
                                   {'display_name': 'Jordan', 'id': 'jor', 'name': 'jor', 'title': 'Jordan'},
                                   {'display_name': 'Japan', 'id': 'jpn', 'name': 'jpn', 'title': 'Japan'},
                                   {'display_name': 'Kazakhstan', 'id': 'kaz', 'name': 'kaz', 'title': 'Kazakhstan'},
                                   {'display_name': 'Kenya', 'id': 'ken', 'name': 'ken', 'title': 'Kenya'},
                                   {'display_name': 'Kyrgyzstan', 'id': 'kgz', 'name': 'kgz', 'title': 'Kyrgyzstan'},
                                   {'display_name': 'Cambodia', 'id': 'khm', 'name': 'khm', 'title': 'Cambodia'},
                                   {'display_name': 'Kiribati', 'id': 'kir', 'name': 'kir', 'title': 'Kiribati'},
                                   {'display_name': 'Saint Kitts And Nevis', 'id': 'kna', 'name': 'kna', 'title': 'Saint Kitts And Nevis'},
                                   {'display_name': 'Korea, Republic Of', 'id': 'kor', 'name': 'kor', 'title': 'Korea, Republic Of'},
                                   {'display_name': 'Kuwait', 'id': 'kwt', 'name': 'kwt', 'title': 'Kuwait'},
                                   {'display_name': "Lao People's Democratic Republic", 'id': 'lao', 'name': 'lao', 'title': "Lao People's Democratic Republic"},
                                   {'display_name': 'Lebanon', 'id': 'lbn', 'name': 'lbn', 'title': 'Lebanon'},
                                   {'display_name': 'Liberia', 'id': 'lbr', 'name': 'lbr', 'title': 'Liberia'},
                                   {'display_name': 'Libya', 'id': 'lby', 'name': 'lby', 'title': 'Libya'},
                                   {'display_name': 'Saint Lucia', 'id': 'lca', 'name': 'lca', 'title': 'Saint Lucia'},
                                   {'display_name': 'Liechtenstein', 'id': 'lie', 'name': 'lie', 'title': 'Liechtenstein'},
                                   {'display_name': 'Sri Lanka', 'id': 'lka', 'name': 'lka', 'title': 'Sri Lanka'},
                                   {'display_name': 'Lesotho', 'id': 'lso', 'name': 'lso', 'title': 'Lesotho'},
                                   {'display_name': 'Lithuania', 'id': 'ltu', 'name': 'ltu', 'title': 'Lithuania'},
                                   {'display_name': 'Luxemburg', 'id': 'lux', 'name': 'lux', 'title': 'Luxemburg'},
                                   {'display_name': 'Latvia', 'id': 'lva', 'name': 'lva', 'title': 'Latvia'},
                                   {'display_name': 'Macau', 'id': 'mac', 'name': 'mac', 'title': 'Macau'},
                                   {'display_name': 'Morocco', 'id': 'mar', 'name': 'mar', 'title': 'Morocco'},
                                   {'display_name': 'Monaco', 'id': 'mco', 'name': 'mco', 'title': 'Monaco'},
                                   {'display_name': 'Moldova, Republic Of', 'id': 'mda', 'name': 'mda', 'title': 'Moldova, Republic Of'},
                                   {'display_name': 'Madagascar', 'id': 'mdg', 'name': 'mdg', 'title': 'Madagascar'},
                                   {'display_name': 'Maldives', 'id': 'mdv', 'name': 'mdv', 'title': 'Maldives'},
                                   {'display_name': 'Mexico', 'id': 'mex', 'name': 'mex', 'title': 'Mexico'},
                                   {'display_name': 'Marshall Islands', 'id': 'mhl', 'name': 'mhl', 'title': 'Marshall Islands'},
                                   {'display_name': 'Macedonia', 'id': 'mkd', 'name': 'mkd', 'title': 'Macedonia'},
                                   {'display_name': 'Mali', 'id': 'mli', 'name': 'mli', 'title': 'Mali'},
                                   {'display_name': 'Malta', 'id': 'mlt', 'name': 'mlt', 'title': 'Malta'},
                                   {'display_name': 'Myanmar', 'id': 'mmr', 'name': 'mmr', 'title': 'Myanmar'},
                                   {'display_name': 'Montenegro', 'id': 'mne', 'name': 'mne', 'title': 'Montenegro'},
                                   {'display_name': 'Mongolia', 'id': 'mng', 'name': 'mng', 'title': 'Mongolia'},
                                   {'display_name': 'Northern Mariana Islands', 'id': 'mnp', 'name': 'mnp', 'title': 'Northern Mariana Islands'},
                                   {'display_name': 'Mozambique', 'id': 'moz', 'name': 'moz', 'title': 'Mozambique'},
                                   {'display_name': 'Mauritania', 'id': 'mrt', 'name': 'mrt', 'title': 'Mauritania'},
                                   {'display_name': 'Montserrat', 'id': 'msr', 'name': 'msr', 'title': 'Montserrat'},
                                   {'display_name': 'Martinique', 'id': 'mtq', 'name': 'mtq', 'title': 'Martinique'},
                                   {'display_name': 'Mauritius', 'id': 'mus', 'name': 'mus', 'title': 'Mauritius'},
                                   {'display_name': 'Malawi', 'id': 'mwi', 'name': 'mwi', 'title': 'Malawi'},
                                   {'display_name': 'Malaysia', 'id': 'mys', 'name': 'mys', 'title': 'Malaysia'},
                                   {'display_name': 'Mayotte', 'id': 'myt', 'name': 'myt', 'title': 'Mayotte'},
                                   {'display_name': 'Namibia', 'id': 'nam', 'name': 'nam', 'title': 'Namibia'},
                                   {'display_name': 'New Caledonia', 'id': 'ncl', 'name': 'ncl', 'title': 'New Caledonia'},
                                   {'display_name': 'Nepal Earthquake', 'id': 'fba94eb6-a73b-407d-8acd-c16e61e4f0dc', 'name': 'nepal-earthquake', 'title': 'Nepal Earthquake'},
                                   {'display_name': 'Niger', 'id': 'ner', 'name': 'ner', 'title': 'Niger'},
                                   {'display_name': 'Norfolk Island', 'id': 'nfk', 'name': 'nfk', 'title': 'Norfolk Island'},
                                   {'display_name': 'Nigeria', 'id': 'nga', 'name': 'nga', 'title': 'Nigeria'},
                                   {'display_name': 'Nicaragua', 'id': 'nic', 'name': 'nic', 'title': 'Nicaragua'},
                                   {'display_name': 'Niue', 'id': 'niu', 'name': 'niu', 'title': 'Niue'},
                                   {'display_name': 'Netherlands', 'id': 'nld', 'name': 'nld', 'title': 'Netherlands'},
                                   {'display_name': 'Norway', 'id': 'nor', 'name': 'nor', 'title': 'Norway'},
                                   {'display_name': 'Nepal', 'id': 'npl', 'name': 'npl', 'title': 'Nepal'},
                                   {'display_name': 'Nauru', 'id': 'nru', 'name': 'nru', 'title': 'Nauru'},
                                   {'display_name': 'New Zealand', 'id': 'nzl', 'name': 'nzl', 'title': 'New Zealand'},
                                   {'display_name': 'Oman', 'id': 'omn', 'name': 'omn', 'title': 'Oman'},
                                   {'display_name': 'Pakistan', 'id': 'pak', 'name': 'pak', 'title': 'Pakistan'},
                                   {'display_name': 'Panama', 'id': 'pan', 'name': 'pan', 'title': 'Panama'},
                                   {'display_name': 'Pitcairn', 'id': 'pcn', 'name': 'pcn', 'title': 'Pitcairn'},
                                   {'display_name': 'Peru', 'id': 'per', 'name': 'per', 'title': 'Peru'},
                                   {'display_name': 'Philippines', 'id': 'phl', 'name': 'phl', 'title': 'Philippines'},
                                   {'display_name': 'Palau', 'id': 'plw', 'name': 'plw', 'title': 'Palau'},
                                   {'display_name': 'Papua New Guinea', 'id': 'png', 'name': 'png', 'title': 'Papua New Guinea'},
                                   {'display_name': 'Poland', 'id': 'pol', 'name': 'pol', 'title': 'Poland'},
                                   {'display_name': 'Puerto Rico', 'id': 'pri', 'name': 'pri', 'title': 'Puerto Rico'},
                                   {'display_name': "Democratic People's Republic of Korea", 'id': 'prk', 'name': 'prk', 'title': "Democratic People's Republic of Korea"},
                                   {'display_name': 'Portugal', 'id': 'prt', 'name': 'prt', 'title': 'Portugal'},
                                   {'display_name': 'Paraguay', 'id': 'pry', 'name': 'pry', 'title': 'Paraguay'},
                                   {'display_name': 'State of Palestine', 'id': 'pse', 'name': 'pse', 'title': 'State of Palestine'},
                                   {'display_name': 'French Polynesia', 'id': 'pyf', 'name': 'pyf', 'title': 'French Polynesia'},
                                   {'display_name': 'Qatar', 'id': 'qat', 'name': 'qat', 'title': 'Qatar'},
                                   {'display_name': 'Reunion', 'id': 'reu', 'name': 'reu', 'title': 'Reunion'},
                                   {'display_name': 'Romania', 'id': 'c92bd69f-54c0-4d02-ad30-d33fd1cd1393', 'name': 'rou', 'title': 'Romania'},
                                   {'display_name': 'Russia', 'id': 'rus', 'name': 'rus', 'title': 'Russia'},
                                   {'display_name': 'Rwanda', 'id': 'rwa', 'name': 'rwa', 'title': 'Rwanda'},
                                   {'display_name': 'Saudi Arabia', 'id': 'sau', 'name': 'sau', 'title': 'Saudi Arabia'},
                                   {'display_name': 'Sudan', 'id': 'sdn', 'name': 'sdn', 'title': 'Sudan'},
                                   {'display_name': 'Senegal', 'id': 'sen', 'name': 'sen', 'title': 'Senegal'},
                                   {'display_name': 'Singapore', 'id': 'sgp', 'name': 'sgp', 'title': 'Singapore'},
                                   {'display_name': 'South Georgia And South S.S.', 'id': 'sgs', 'name': 'sgs', 'title': 'South Georgia And South S.S.'},
                                   {'display_name': 'St. Helena', 'id': 'shn', 'name': 'shn', 'title': 'St. Helena'},
                                   {'display_name': 'Svalbard And Jan Mayen Islands', 'id': 'sjm', 'name': 'sjm', 'title': 'Svalbard And Jan Mayen Islands'},
                                   {'display_name': 'Solomon Islands', 'id': 'slb', 'name': 'slb', 'title': 'Solomon Islands'},
                                   {'display_name': 'Sierra Leone', 'id': 'sle', 'name': 'sle', 'title': 'Sierra Leone'},
                                   {'display_name': 'El Salvador', 'id': 'slv', 'name': 'slv', 'title': 'El Salvador'},
                                   {'display_name': 'San Marino', 'id': 'smr', 'name': 'smr', 'title': 'San Marino'},
                                   {'display_name': 'Somalia', 'id': 'som', 'name': 'som', 'title': 'Somalia'},
                                   {'display_name': 'St. Pierre And Miquelon', 'id': 'spm', 'name': 'spm', 'title': 'St. Pierre And Miquelon'},
                                   {'display_name': 'Serbia', 'id': 'srb', 'name': 'srb', 'title': 'Serbia'},
                                   {'display_name': 'South Sudan', 'id': 'ssd', 'name': 'ssd', 'title': 'South Sudan'},
                                   {'display_name': 'Sao Tome And Principe', 'id': 'stp', 'name': 'stp', 'title': 'Sao Tome And Principe'},
                                   {'display_name': 'Suriname', 'id': 'sur', 'name': 'sur', 'title': 'Suriname'},
                                   {'display_name': 'Slovakia (Slovak Republic)', 'id': 'svk', 'name': 'svk', 'title': 'Slovakia (Slovak Republic)'},
                                   {'display_name': 'Slovenia', 'id': 'svn', 'name': 'svn', 'title': 'Slovenia'},
                                   {'display_name': 'Sweden', 'id': 'swe', 'name': 'swe', 'title': 'Sweden'},
                                   {'display_name': 'Swaziland', 'id': 'swz', 'name': 'swz', 'title': 'Swaziland'},
                                   {'display_name': 'Seychelles', 'id': 'syc', 'name': 'syc', 'title': 'Seychelles'},
                                   {'display_name': 'Syrian Arab Republic', 'id': 'syr', 'name': 'syr', 'title': 'Syrian Arab Republic'},
                                   {'display_name': 'Turks And Caicos Islands', 'id': 'tca', 'name': 'tca', 'title': 'Turks And Caicos Islands'},
                                   {'display_name': 'Chad', 'id': 'tcd', 'name': 'tcd', 'title': 'Chad'},
                                   {'display_name': 'Togo', 'id': 'tgo', 'name': 'tgo', 'title': 'Togo'},
                                   {'display_name': 'Thailand', 'id': 'tha', 'name': 'tha', 'title': 'Thailand'},
                                   {'display_name': 'Tajikistan', 'id': 'tjk', 'name': 'tjk', 'title': 'Tajikistan'},
                                   {'display_name': 'Tokelau', 'id': 'tkl', 'name': 'tkl', 'title': 'Tokelau'},
                                   {'display_name': 'Turkmenistan', 'id': 'tkm', 'name': 'tkm', 'title': 'Turkmenistan'},
                                   {'display_name': 'East Timor', 'id': '86b999e4-6981-401e-b57d-e15ad5a9ec86', 'name': 'tls', 'title': 'East Timor'},
                                   {'display_name': 'Tonga', 'id': 'ton', 'name': 'ton', 'title': 'Tonga'},
                                   {'display_name': 'Trinidad And Tobago', 'id': 'tto', 'name': 'tto', 'title': 'Trinidad And Tobago'},
                                   {'display_name': 'Tunisia', 'id': 'tun', 'name': 'tun', 'title': 'Tunisia'},
                                   {'display_name': 'Turkey', 'id': 'tur', 'name': 'tur', 'title': 'Turkey'},
                                   {'display_name': 'Tuvalu', 'id': 'tuv', 'name': 'tuv', 'title': 'Tuvalu'},
                                   {'display_name': 'Taiwan, Province Of China', 'id': 'twn', 'name': 'twn', 'title': 'Taiwan, Province Of China'},
                                   {'display_name': 'United Republic of Tanzania', 'id': 'tza', 'name': 'tza', 'title': 'United Republic of Tanzania'},
                                   {'display_name': 'Uganda', 'id': 'uga', 'name': 'uga', 'title': 'Uganda'},
                                   {'display_name': 'Ukraine', 'id': 'ukr', 'name': 'ukr', 'title': 'Ukraine'},
                                   {'display_name': 'U.S. Minor Islands', 'id': 'umi', 'name': 'umi', 'title': 'U.S. Minor Islands'},
                                   {'display_name': 'Uruguay', 'id': 'ury', 'name': 'ury', 'title': 'Uruguay'},
                                   {'display_name': 'United States', 'id': 'usa', 'name': 'usa', 'title': 'United States'},
                                   {'display_name': 'Uzbekistan', 'id': 'uzb', 'name': 'uzb', 'title': 'Uzbekistan'},
                                   {'display_name': 'Holy See (Vatican City State)', 'id': 'vat', 'name': 'vat', 'title': 'Holy See (Vatican City State)'},
                                   {'display_name': 'Saint Vincent And The Grenadines', 'id': 'vct', 'name': 'vct', 'title': 'Saint Vincent And The Grenadines'},
                                   {'display_name': 'Venezuela', 'id': 'ven', 'name': 'ven', 'title': 'Venezuela'},
                                   {'display_name': 'Virgin Islands (British)', 'id': 'vgb', 'name': 'vgb', 'title': 'Virgin Islands (British)'},
                                   {'display_name': 'Virgin Islands (U.S.)', 'id': 'vir', 'name': 'vir', 'title': 'Virgin Islands (U.S.)'},
                                   {'display_name': 'Viet Nam', 'id': 'vnm', 'name': 'vnm', 'title': 'Viet Nam'},
                                   {'display_name': 'Vanuatu', 'id': 'vut', 'name': 'vut', 'title': 'Vanuatu'},
                                   {'display_name': 'Wallis And Futuna Islands', 'id': 'wlf', 'name': 'wlf', 'title': 'Wallis And Futuna Islands'},
                                   {'display_name': 'World', 'id': 'world', 'name': 'world', 'title': 'World'},
                                   {'display_name': 'Samoa', 'id': 'wsm', 'name': 'wsm', 'title': 'Samoa'},
                                   {'display_name': 'Yemen', 'id': 'yem', 'name': 'yem', 'title': 'Yemen'},
                                   {'display_name': 'South Africa', 'id': 'zaf', 'name': 'zaf', 'title': 'South Africa'},
                                   {'display_name': 'Zambia', 'id': 'zmb', 'name': 'zmb', 'title': 'Zambia'},
                                   {'display_name': 'Zimbabwe', 'id': 'zwe', 'name': 'zwe', 'title': 'Zimbabwe'}])


@pytest.fixture(scope='function')
def configuration(hdx_key_file, project_config_yaml):
    Configuration._create(user_agent='test', hdx_key_file=hdx_key_file,
                          project_config_yaml=project_config_yaml)


@pytest.fixture(scope='function')
def mocksmtp(monkeypatch):
    class MockSMTPBase(object):
        type = None

        def __init__(self, **kwargs):
            self.initargs = kwargs

        def login(self, username, password):
            self.username = username
            self.password = password

        def sendmail(self, sender, recipients, msg, **kwargs):
            self.sender = sender
            self.recipients = recipients
            self.msg = msg
            self.send_args = kwargs

        @staticmethod
        def quit():
            pass

    class MockSMTPSSL(MockSMTPBase):
        type = 'smtpssl'

    class MockLMTP(MockSMTPBase):
        type = 'lmtp'

    class MockSMTP(MockSMTPBase):
        type = 'smtp'
    monkeypatch.setattr(smtplib, 'SMTP_SSL', MockSMTPSSL)
    monkeypatch.setattr(smtplib, 'LMTP', MockLMTP)
    monkeypatch.setattr(smtplib, 'SMTP', MockSMTP)
