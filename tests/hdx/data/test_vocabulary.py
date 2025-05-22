"""Vocabulary Tests"""

import copy
import json
from os.path import join

import pytest
from requests.exceptions import RetryError

from .. import MockResponse
from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.data.vocabulary import ChainRuleError, Vocabulary
from hdx.utilities.dictandlist import merge_two_dictionaries

vocabulary_list = [
    {
        "tags": [],
        "id": "57f71f5f-adb0-48fd-ab2c-6b93b9d30332",
        "name": "Topics",
    },
    {
        "tags": [
            {
                "vocabulary_id": "1731e7fc-ff62-4551-8a70-2a5878e1142b",
                "display_name": "mike",
                "id": "fc7dbc79-6711-4d3e-a5ef-78b3ef5678df",
                "name": "mike",
            },
            {
                "vocabulary_id": "1731e7fc-ff62-4551-8a70-2a5878e1142b",
                "display_name": "mike2",
                "id": "39d6715a-a53f-4eda-97ef-478a8547668d",
                "name": "mike2",
            },
        ],
        "id": "1731e7fc-ff62-4551-8a70-2a5878e1142b",
        "name": "miketest",
    },
    {
        "tags": [
            {
                "id": "6e0600e9-167a-482f-9259-4a17455607c6",
                "name": "administrative boundaries-divisions",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "administrative boundaries-divisions",
            },
            {
                "id": "25d0e6cc-a02c-4c91-b358-7d4bb1a42af0",
                "name": "affected area",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "affected area",
            },
            {
                "id": "9f9d19d4-901f-4b57-b781-e6b2b56e2138",
                "name": "affected population",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "affected population",
            },
            {
                "id": "9b05220b-0f1e-46ba-912a-36a895c04695",
                "name": "africa",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "africa",
            },
            {
                "id": "e6b84407-4b14-4b0a-95c5-314da049f64c",
                "name": "agriculture-livestock",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "agriculture-livestock",
            },
            {
                "id": "4c273295-9f2f-4543-b0bf-2f8429302285",
                "name": "aid effectiveness",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "aid effectiveness",
            },
            {
                "id": "c7b393d2-6eeb-4bb5-9522-f6e4548192c0",
                "name": "aid worker security",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "aid worker security",
            },
            {
                "id": "cffb9ffc-4efe-48c6-a7f0-7f91e99b717f",
                "name": "aid workers",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "aid workers",
            },
            {
                "id": "793640f4-6914-4228-ab4c-911a3b88a123",
                "name": "airports",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "airports",
            },
            {
                "id": "9dfcb72e-d8b5-49c3-98dc-fcabda2be413",
                "name": "americas",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "americas",
            },
            {
                "id": "fea713c8-19db-473e-82ab-cf9d765c9d01",
                "name": "asylum seekers",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "asylum seekers",
            },
            {
                "id": "15611ecd-f4ea-47c2-9037-ff679848170f",
                "name": "aviation",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "aviation",
            },
            {
                "id": "db8205e9-b61c-4df7-a987-1a2658ed8666",
                "name": "baseline population",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "baseline population",
            },
            {
                "id": "3a8971c6-451d-4b2b-bf4d-885113b94af2",
                "name": "births",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "births",
            },
            {
                "id": "0c3a387b-3a97-4b7a-976e-31752145ba21",
                "name": "border crossings",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "border crossings",
            },
            {
                "id": "9b063fb2-3493-4a7b-a5e6-667862f795f8",
                "name": "boys",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "boys",
            },
            {
                "id": "845b021e-ea80-40f3-ab0e-7c87edee378d",
                "name": "buildings",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "buildings",
            },
            {
                "id": "079f6ecd-1738-4f34-bf38-0f3b585ed58d",
                "name": "camp coordination and camp management-cccm",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "camp coordination and camp management-cccm",
            },
            {
                "id": "cf39d778-096e-4dba-a73d-f2db87f177e4",
                "name": "camp facilities",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "camp facilities",
            },
            {
                "id": "3fdf3530-7f85-47c3-877b-5b87390d6e5c",
                "name": "cash based interventions-cbi",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "cash based interventions-cbi",
            },
            {
                "id": "4ca815e8-4949-4ee5-abc9-0b10beffcbf9",
                "name": "cash voucher assistance-cva",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "cash voucher assistance-cva",
            },
            {
                "id": "55dd6b71-e858-40de-ac52-91cd63e94cfa",
                "name": "casualties",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "casualties",
            },
            {
                "id": "37ac0e80-d7cc-4f91-aead-7395c87115a4",
                "name": "cbi",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "cbi",
            },
            {
                "id": "233b3649-7ea7-479e-a4a3-568eabe12f08",
                "name": "census",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "census",
            },
            {
                "id": "e6cd8ab8-d53a-4768-8edc-2c7eba921256",
                "name": "central africa",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "central africa",
            },
            {
                "id": "3a4131ff-a500-4c77-ba6e-02da286daa99",
                "name": "children",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "children",
            },
            {
                "id": "ba8322b6-72e7-4475-b015-72d200bf5957",
                "name": "climate hazards",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "climate hazards",
            },
            {
                "id": "6f0a101d-5c57-408b-aa2a-61c078d32713",
                "name": "climate-weather",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "climate-weather",
            },
            {
                "id": "07bed635-2678-44bc-b132-210bb3cee1d3",
                "name": "cluster system",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "cluster system",
            },
            {
                "id": "4699f937-abf9-4f44-bb69-93967b994208",
                "name": "community engagement",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "community engagement",
            },
            {
                "id": "964a4ed8-0527-4b90-b929-ccea14fc2851",
                "name": "complex emergency-conflict-security",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "complex emergency-conflict-security",
            },
            {
                "id": "d727b3fb-9976-4101-8a77-0fcbee34a954",
                "name": "conflict-violence",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "conflict-violence",
            },
            {
                "id": "4c4c6a4e-00b0-49b0-a1a5-e5082f91f6e6",
                "name": "covid-19",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "covid-19",
            },
            {
                "id": "9dae41e5-eacd-4fa5-91df-8d80cf579e52",
                "state": "active",
                "display_name": "crisis-somewhere",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "name": "crisis-somewhere",
            },
            {
                "id": "326e097b-96f2-46e4-8ef4-0a8d4401a646",
                "name": "cyclones-hurricanes-typhoons",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "cyclones-hurricanes-typhoons",
            },
            {
                "id": "3c5bab40-4c0f-40bc-a2dd-12cd7f945037",
                "name": "damage assessment",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "damage assessment",
            },
            {
                "id": "7aa60d26-5c83-4c50-80d2-0b944fe80122",
                "name": "demographics",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "demographics",
            },
            {
                "id": "3be50db2-69e2-406f-83b8-195598d0c0d5",
                "name": "development",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "development",
            },
            {
                "id": "5b87b92d-3905-4b56-8c52-9bc77ffd28a3",
                "name": "disability",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "disability",
            },
            {
                "id": "4f186471-d358-4f37-b9ab-2743abd61fbd",
                "name": "disaster risk reduction-drr",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "disaster risk reduction-drr",
            },
            {
                "id": "2a4e3877-8487-4a62-b010-8dafdc1ba6d8",
                "name": "disease",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "disease",
            },
            {
                "id": "3e3f28cd-405e-4706-a20b-74ff3e217af2",
                "name": "displacement",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "displacement",
            },
            {
                "id": "34a5c9d1-5554-4f9e-91fa-5983f0c9a721",
                "name": "drought",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "drought",
            },
            {
                "id": "ec3e650b-06d4-410d-915f-213d6156b1b6",
                "name": "earthquake-tsunami",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "earthquake-tsunami",
            },
            {
                "id": "13faf62d-d5bc-46f0-93b6-c8434a99e1a2",
                "name": "earthquakes-tsunami",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "earthquakes-tsunami",
            },
            {
                "id": "7056940c-d78a-45a2-8042-a26adf97d2be",
                "name": "eastern africa",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "eastern africa",
            },
            {
                "id": "f58b7aa5-284f-4e1c-ae35-3ded0f2a4555",
                "name": "economics",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "economics",
            },
            {
                "id": "111f9068-6270-4dbd-a2a9-b4d69ee1735b",
                "name": "education",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "education",
            },
            {
                "id": "b11af0a4-1bf4-45f8-987a-3094d111d439",
                "name": "education facilities - schools",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "education facilities - schools",
            },
            {
                "id": "c29686c1-68c3-4417-a50d-b07de0c47770",
                "name": "education facilities-schools",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "education facilities-schools",
            },
            {
                "id": "5da3914b-3c97-4093-abf9-bfaad4ff4c1c",
                "name": "el nino-el nina",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "el nino-el nina",
            },
            {
                "id": "60c168ce-65d0-4c44-beef-108b65b9b817",
                "name": "elderly",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "elderly",
            },
            {
                "id": "5711a053-4e70-4c14-ae20-9638e36c2666",
                "name": "employment",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "employment",
            },
            {
                "id": "dc9636e1-11fd-4db1-be45-533a3296249e",
                "name": "energy",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "energy",
            },
            {
                "id": "71e54185-e2ea-4dd0-b67d-8316abefe82b",
                "name": "environment",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "environment",
            },
            {
                "id": "2c7a64e9-6c0f-4879-b259-0680987ec1c7",
                "name": "epidemics-outbreaks",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "epidemics-outbreaks",
            },
            {
                "id": "97079843-5cad-4786-867e-7a64e51851b8",
                "name": "facilities and infrastructure",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "facilities and infrastructure",
            },
            {
                "id": "e45272d8-6b73-4b8d-bb08-e880f8900df5",
                "name": "facilities-infrastructure",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "facilities-infrastructure",
            },
            {
                "id": "33b00b7e-db0e-498c-a12b-d5e64605f9f6",
                "name": "fatalities",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "fatalities",
            },
            {
                "id": "3b2f49dc-8f88-46fc-81bb-dead4b8158b8",
                "name": "female",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "female",
            },
            {
                "id": "5f257cb4-e088-4290-9e14-7ec25611062b",
                "name": "financial institutions",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "financial institutions",
            },
            {
                "id": "ce4e7a88-b5e8-4aa5-b953-4077d15decc0",
                "name": "financial services",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "financial services",
            },
            {
                "id": "113b89a7-f22a-41d3-8374-5326e545e198",
                "name": "flooding-storm surge",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "flooding-storm surge",
            },
            {
                "id": "b084d063-149c-4b7d-811b-18e320ca0b8c",
                "name": "food security",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "food security",
            },
            {
                "id": "1f80da28-f2fe-44a9-91ae-3358321d1bf4",
                "name": "forced displacement",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "forced displacement",
            },
            {
                "id": "b4a1a447-c72e-4d84-8b01-1cec4be159f6",
                "name": "forecasting",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "forecasting",
            },
            {
                "id": "f713040c-296d-4cdc-885e-fce07f284466",
                "name": "fuel",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "fuel",
            },
            {
                "id": "2bfc754f-deee-4e10-a85c-61ad8b8983f3",
                "name": "funding",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "funding",
            },
            {
                "id": "853d6f46-3b86-4f54-897f-65ed42a30675",
                "name": "gazetteer",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "gazetteer",
            },
            {
                "id": "ebbcc596-b3a8-4985-a9a2-37ef020d06b8",
                "name": "gender",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "gender",
            },
            {
                "id": "3ec941c4-5f8d-45a5-bbaa-0911f49c11f9",
                "name": "gender-based violence-gbv",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "gender-based violence-gbv",
            },
            {
                "id": "2d27d72f-af37-4b38-b05e-dddc6929bd13",
                "name": "geodata",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "geodata",
            },
            {
                "id": "4e3db92e-488e-41bc-b261-4fc00b64aabe",
                "name": "girls",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "girls",
            },
            {
                "id": "cc6c6b0f-7c21-476c-8aaa-336c4ae19dda",
                "name": "global acute malnutrition-gam",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "global acute malnutrition-gam",
            },
            {
                "id": "08029963-c501-4107-83b6-7011b9f74287",
                "name": "governance and civil society",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "governance and civil society",
            },
            {
                "id": "25039df2-f646-488d-b514-ad658dfa6165",
                "name": "gross domestic product-gdp",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "gross domestic product-gdp",
            },
            {
                "id": "7173a61f-84fd-45ca-856a-3aa2b0372869",
                "name": "hazards and risk",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "hazards and risk",
            },
            {
                "id": "26fe3d20-9de7-436b-b47a-4f7f2e4547d0",
                "name": "health",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "health",
            },
            {
                "id": "056666b8-0c90-46a7-9dda-47d27fa7ebf8",
                "name": "health facilities",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "health facilities",
            },
            {
                "id": "48399747-7922-4a87-b16b-f579908160f0",
                "name": "helicopter landing zone - hlz",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "helicopter landing zone - hlz",
            },
            {
                "id": "fff0918b-4e50-49c6-8a91-a8e59b3e037a",
                "name": "horn of africa",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "horn of africa",
            },
            {
                "id": "60aaf965-1357-4726-bec3-e907f027aae0",
                "name": "human rights",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "human rights",
            },
            {
                "id": "be991471-7303-4923-9c75-69e866fab7ae",
                "name": "humanitarian access",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "humanitarian access",
            },
            {
                "id": "4d810352-78d9-453c-a48f-6a17b8e6761a",
                "name": "humanitarian needs overview-hno",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "humanitarian needs overview-hno",
            },
            {
                "id": "8236f9fe-cdca-43dc-adbf-51c99c4d44aa",
                "name": "humanitarian response plan-hrp",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "humanitarian response plan-hrp",
            },
            {
                "id": "a0fbb23a-6aad-4ccc-8062-e9ef9f20e5d2",
                "name": "hxl",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "hxl",
            },
            {
                "id": "4fc0b5ba-330e-41d4-846a-13415a517f03",
                "name": "hydrology",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "hydrology",
            },
            {
                "id": "08dade96-0bf4-4248-9d9f-421f7b844e53",
                "name": "indicators",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "indicators",
            },
            {
                "id": "73445768-49eb-4d68-b97b-ce7a2a3a4762",
                "name": "integrated food security phase classification-ipc",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "integrated food security phase classification-ipc",
            },
            {
                "id": "4a0f429f-679c-4492-8386-d5cdf2d82ecd",
                "name": "internally displaced persons-idp",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "internally displaced persons-idp",
            },
            {
                "id": "023000bb-5e89-44b8-a7ea-e8008f0492e9",
                "name": "international aid transparency initiative-iati",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "international aid transparency initiative-iati",
            },
            {
                "id": "e268c07d-b583-480d-8f89-f98906c290d1",
                "name": "languages",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "languages",
            },
            {
                "id": "edbbf6c3-ab69-45a5-b428-46eb2cfdbe5e",
                "name": "libya-floods",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "libya-floods",
            },
            {
                "id": "cf89f978-a673-4348-9a0f-2631b7338dbb",
                "name": "literacy",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "literacy",
            },
            {
                "id": "a396d995-bebc-4f57-b0c1-c58317e8f7cf",
                "name": "livelihoods",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "livelihoods",
            },
            {
                "id": "f62348c5-1d96-4b3a-9133-5e8b48b0d1af",
                "name": "logistics",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "logistics",
            },
            {
                "id": "123c9514-2b76-46eb-becc-c987d05869fa",
                "name": "malaria",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "malaria",
            },
            {
                "id": "932d03fe-972f-4c8b-85fd-1fc2048d7e63",
                "name": "malnutrition",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "malnutrition",
            },
            {
                "id": "b62086cd-553f-4464-9cd3-9efb487203fe",
                "name": "markets",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "markets",
            },
            {
                "id": "8dcce29c-c245-42a6-938a-61010562deaf",
                "name": "maternity",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "maternity",
            },
            {
                "id": "2dfa7851-2e12-4c3d-8cb8-e49be4f522f4",
                "name": "men",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "men",
            },
            {
                "id": "6a646884-b858-4b84-86d1-c0b2f3fd8b89",
                "name": "mental health",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "mental health",
            },
            {
                "id": "69f1ff2e-a2ab-4199-9805-ecdd0dde19bd",
                "name": "migration",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "migration",
            },
            {
                "id": "051f22d0-f563-44fc-b7d3-3af64d4ce9fc",
                "name": "mobile money",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "mobile money",
            },
            {
                "id": "ebeb5404-125a-47c4-a6b1-dddaac3d772b",
                "name": "morocco-earthquake",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "morocco-earthquake",
            },
            {
                "id": "87a19f57-52b9-4f70-b23e-138e44bf3a81",
                "name": "mortality",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "mortality",
            },
            {
                "id": "48520851-7df8-418b-aa00-7fa276d7fd88",
                "name": "natural disasters",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "natural disasters",
            },
            {
                "id": "9b4c3273-e3c3-4727-9adf-6760644993d0",
                "name": "needs assessment",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "needs assessment",
            },
            {
                "id": "11e0b9ae-856e-45c4-80a9-27e3f00e61b2",
                "name": "non-food items-nfi",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "non-food items-nfi",
            },
            {
                "id": "5cd44eef-f868-47d8-afb4-7d7d63154533",
                "name": "nutrition",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "nutrition",
            },
            {
                "id": "10aaf0ac-bf2e-4600-9c74-21012436f336",
                "name": "offices",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "offices",
            },
            {
                "id": "9e07a23b-e8ec-42f6-afa6-0a77405a343a",
                "name": "openstreetmap",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "openstreetmap",
            },
            {
                "id": "d8a59526-9f9a-4c71-b38f-5d9f2eb1615a",
                "name": "operational capacity",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "operational capacity",
            },
            {
                "id": "697e5eb8-5a2e-46b9-a777-b4f1e00a8b6a",
                "name": "operational partners",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "operational partners",
            },
            {
                "id": "a25059f9-7e1f-49be-b629-ccccd97a95f8",
                "name": "operational presence",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "operational presence",
            },
            {
                "id": "84a04ea0-89fc-436d-a2b6-c3cdfa82d756",
                "name": "opt-israel-hostilities",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "opt-israel-hostilities",
            },
            {
                "id": "afaf9905-3564-4e92-889d-9fb9c9fbd08b",
                "name": "peacekeeping",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "peacekeeping",
            },
            {
                "id": "3e0df740-714b-476a-ad14-4fc6fc4fc0ba",
                "name": "people in need-pin",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "people in need-pin",
            },
            {
                "id": "7418bcd5-a59a-43c4-972c-29d43d209b1c",
                "name": "places of worship",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "places of worship",
            },
            {
                "id": "68e23b91-dd23-48e1-93c4-0feb5ddb8eed",
                "name": "points of interest - poi",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "points of interest - poi",
            },
            {
                "id": "55dd76c8-6de4-4d7f-bbd4-c7c9240a2c60",
                "name": "points of interest-poi",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "points of interest-poi",
            },
            {
                "id": "7bae4f4f-5832-4d7f-9d8a-441461cdb433",
                "name": "populated places - settlements",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "populated places - settlements",
            },
            {
                "id": "c7d3120a-ca01-4bba-b773-53cd7dc608bc",
                "name": "populated places-settlements",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "populated places-settlements",
            },
            {
                "id": "02e85359-ad5c-4d45-a779-2e7bff747686",
                "name": "population",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "population",
            },
            {
                "id": "482c0e7f-d611-474b-9fe0-bc61f7a44078",
                "name": "ports",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "ports",
            },
            {
                "id": "c3544ec4-753e-4c1b-9cd6-942fb689fab5",
                "name": "poverty",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "poverty",
            },
            {
                "id": "5c655601-e2ef-4b12-933c-3b6abc319b0f",
                "name": "protection",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "protection",
            },
            {
                "id": "5ccaff54-1a2d-45d1-b2db-4282813d5166",
                "name": "railways",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "railways",
            },
            {
                "id": "f32dcfe2-27c4-4fdd-8230-b762c971fa9c",
                "name": "refugee camps",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "refugee camps",
            },
            {
                "id": "8d1398ae-56af-4396-aa94-bef7d99da754",
                "name": "refugee crisis",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "refugee crisis",
            },
            {
                "id": "8af07ef8-0180-4966-9e15-d184b9a2fef1",
                "name": "refugees",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "refugees",
            },
            {
                "id": "f22939f4-c066-47e4-b540-2423baec90cf",
                "name": "returnees",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "returnees",
            },
            {
                "id": "71a7e3ae-f961-433d-9206-b30fd4d62299",
                "name": "rivers",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "rivers",
            },
            {
                "id": "a775d5e5-2168-4b89-b415-87d77e424b0c",
                "name": "roads",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "roads",
            },
            {
                "id": "62d064e7-77bb-4712-bcb7-acb01b7bd057",
                "name": "rural",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "rural",
            },
            {
                "id": "482ab333-40eb-4ed3-b32d-46d2d3f23e63",
                "name": "sahel",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "sahel",
            },
            {
                "id": "4d90e810-32ce-4c5f-bc10-91d7f204e3c8",
                "name": "sanitation",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "sanitation",
            },
            {
                "id": "9f54b0e4-78df-4939-a2a1-892aaf4396ce",
                "name": "services",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "services",
            },
            {
                "id": "4d9682e4-5043-48af-834b-fb628886aabe",
                "name": "severe acute malnutrition-sam",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "severe acute malnutrition-sam",
            },
            {
                "id": "2fdb7b0b-b795-410a-ba03-6055e26aa97c",
                "name": "severity",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "severity",
            },
            {
                "id": "38902364-3f95-4a69-b465-30a9c49bd28c",
                "name": "sex and age disaggregated data-sadd",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "sex and age disaggregated data-sadd",
            },
            {
                "id": "6c8eda16-cb14-4a65-a5de-4a5808da0b12",
                "name": "shelter",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "shelter",
            },
            {
                "id": "7e54319d-dd8d-4cee-b308-5994905c6d9c",
                "name": "shops",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "shops",
            },
            {
                "id": "eb898c38-3c1a-485e-a7de-2176e56f802d",
                "name": "social media data",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "social media data",
            },
            {
                "id": "a64218ff-64ff-4777-a664-6b0a331ab605",
                "name": "socioeconomics",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "socioeconomics",
            },
            {
                "id": "37a0a86e-435c-46fb-a79b-4a5c134669a2",
                "name": "stateless persons",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "stateless persons",
            },
            {
                "id": "c9c5cf06-79f4-4d30-aa75-5e39e703abd9",
                "name": "survey",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "survey",
            },
            {
                "id": "95dbee08-4aea-468c-8e3e-bf2d178164e6",
                "name": "sustainable development",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "sustainable development",
            },
            {
                "id": "570d5718-b98f-46af-ac89-2b6f49fa9ed1",
                "name": "sustainable development goals-sdg",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "sustainable development goals-sdg",
            },
            {
                "id": "bc80e9b0-5bcc-416a-a4c1-e28757465f9c",
                "name": "topography",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "topography",
            },
            {
                "id": "1eb92d87-7c9d-4ce8-bbc1-a49deded7865",
                "name": "trade",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "trade",
            },
            {
                "id": "ee1a5568-91f1-4274-b10a-846a81335975",
                "name": "transportation",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "transportation",
            },
            {
                "id": "213c7bf3-46b3-4436-9471-71348012940b",
                "name": "uganda refugee response",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "uganda refugee response",
            },
            {
                "id": "a859a0b3-7b16-4ee6-8994-d9b1ac38bea8",
                "name": "urban",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "urban",
            },
            {
                "id": "f6b3cf0c-6704-480d-a2ed-b363281def0c",
                "name": "urban search and rescue-usar",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "urban search and rescue-usar",
            },
            {
                "id": "61a7024e-7387-49ca-a9de-344bdceb3698",
                "name": "vaccination-immunization",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "vaccination-immunization",
            },
            {
                "id": "68e37905-de5a-4c5a-8e1e-6aedd9ea9208",
                "name": "villages",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "villages",
            },
            {
                "id": "71476b2a-3d08-4462-8f15-3d19e22eb28a",
                "name": "water bodies - hydrography",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "water bodies - hydrography",
            },
            {
                "id": "f60fd659-e930-4272-8b3c-84781e874999",
                "name": "water points",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "water points",
            },
            {
                "id": "5a4f7135-daaf-4c82-985f-e0bb443fdb94",
                "name": "water sanitation and hygiene-wash",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "water sanitation and hygiene-wash",
            },
            {
                "id": "922005f4-faf1-4e35-ba7f-14c39ab92c2b",
                "name": "waterbodies",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "waterbodies",
            },
            {
                "id": "d52b96b0-0246-4c14-9544-f7f8fc4d94d0",
                "name": "west africa",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "west africa",
            },
            {
                "id": "ec53893c-6dba-4656-978b-4a32289ea2eb",
                "name": "who is doing what and where-3w-4w-5w",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "who is doing what and where-3w-4w-5w",
            },
            {
                "id": "fe10e36f-97a0-4383-ae31-29a8e8265669",
                "name": "women",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "women",
            },
            {
                "id": "74f2383e-bb0c-44a3-b5cf-491a309a6f38",
                "name": "youth",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "youth",
            },
        ],
        "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
        "name": "Topics",
    },
]

resultdict = vocabulary_list[1]

tag_autocomplete = ["health", "healthcare"]


def vocabulary_mockshow(url, datadict):
    if "show" not in url:
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_show"}',
        )
    if (
        datadict["id"] == "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        or datadict["id"] == "MyVocabulary1"
    ):
        result = json.dumps(resultdict)
        return MockResponse(
            200,
            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_show"}'
            % result,
        )
    if (
        datadict["id"] == "Topics"
        or datadict["id"] == "b891512e-9516-4bf5-962a-7a289772a2a1"
    ):
        result = json.dumps(vocabulary_list[2])
        return MockResponse(
            200,
            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_show"}'
            % result,
        )
    if datadict["id"] == "TEST2":
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_show"}',
        )
    if datadict["id"] == "TEST3":
        return MockResponse(
            200,
            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_show"}',
        )
    return MockResponse(
        404,
        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_show"}',
    )


def vocabulary_delete(url, datadict):
    if "show" in url:
        return vocabulary_mockshow(url, datadict)
    if "update" in url:
        resultdictcopy = copy.deepcopy(resultdict)
        resultdictcopy["tags"] = []
        result = json.dumps(resultdictcopy)
        return MockResponse(
            200,
            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_update"}'
            % result,
        )
    if "delete" not in url:
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "TEST ERROR: Not delete", "__type": "TEST ERROR: Not Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_delete"}',
        )
    if datadict["id"] == "1731e7fc-ff62-4551-8a70-2a5878e1142b":
        if len(datadict["tags"]) == 0:
            return MockResponse(
                200,
                '{"success": true, "result": null, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_delete"}',
            )
        else:
            raise RetryError(
                "HTTPSConnectionPool(host='test-data.humdata.org', port=443): Max retries exceeded with url: /api/action/vocabulary_delete (Caused by ResponseError('too many 500 error responses',))"
            )

    return MockResponse(
        404,
        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_delete"}',
    )


def vocabulary_mocklist(url, datadict):
    if "list" not in url:
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "TEST ERROR: Not all", "__type": "TEST ERROR: Not All Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_list"}',
        )
    return MockResponse(
        200,
        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_list"}'
        % json.dumps(vocabulary_list),
    )


class TestVocabulary:
    vocabulary_data = {
        "tags": [{"name": "economy"}, {"name": "health"}],
        "name": "Things",
    }

    @pytest.fixture(scope="class")
    def static_yaml(self):
        return join("tests", "fixtures", "config", "hdx_vocabulary_static.yaml")

    @pytest.fixture(scope="class")
    def static_json(self):
        return join("tests", "fixtures", "config", "hdx_vocabulary_static.json")

    @pytest.fixture(scope="function")
    def read(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                return vocabulary_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_create(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                if "show" in url:
                    return vocabulary_mockshow(url, datadict)
                if "list" in url:
                    return vocabulary_mocklist(url, datadict)
                if "create" not in url:
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_create"}',
                    )

                if datadict["name"] == "A Vocabulary":
                    result = json.dumps(resultdict)
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_create"}'
                        % result,
                    )
                if datadict["name"] == "Topics":
                    result = json.dumps(vocabulary_list[2])
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_show"}'
                        % result,
                    )
                if datadict["name"] == "XXX":
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_create"}',
                    )
                if datadict["name"] == "YYY":
                    return MockResponse(
                        200,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_create"}',
                    )

                return MockResponse(
                    404,
                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_create"}',
                )

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_update(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                if "show" in url:
                    return vocabulary_mockshow(url, datadict)
                if "list" in url:
                    return vocabulary_mocklist(url, datadict)
                if "update" not in url:
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_update"}',
                    )
                if datadict["id"] == "b891512e-9516-4bf5-962a-7a289772a2a1":
                    result = json.dumps(datadict)
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_show"}'
                        % result,
                    )
                resultdictcopy = copy.deepcopy(resultdict)
                merge_two_dictionaries(resultdictcopy, datadict)

                result = json.dumps(resultdictcopy)
                if datadict["tags"] in [
                    [{"name": "peter"}],
                    [{"name": "john"}],
                    [{"name": "wash"}],
                ]:
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_update"}'
                        % result,
                    )
                if datadict["name"] == "XXX":
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_update"}',
                    )
                if datadict["name"] == "YYY":
                    return MockResponse(
                        200,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_update"}',
                    )

                return MockResponse(
                    404,
                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=vocabulary_update"}',
                )

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_delete(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                return vocabulary_delete(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_list(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                return vocabulary_mocklist(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_autocomplete(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode("utf-8")
                datadict = json.loads(decodedata)
                if "autocomplete" not in url or "health" not in datadict["q"]:
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not autocomplete", "__type": "TEST ERROR: Not Autocomplete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=tag_autocomplete"}',
                    )
                result = json.dumps(tag_autocomplete)
                return MockResponse(
                    200,
                    '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=tag_autocomplete"}'
                    % result,
                )

        Configuration.read().remoteckan().session = MockSession()

    def test_init(self, configuration):
        vocabulary = Vocabulary(name="test", tags=["mytesttag1", "mytesttag2"])
        assert vocabulary["name"] == "test"
        assert vocabulary["tags"] == [
            {"name": "mytesttag1"},
            {"name": "mytesttag2"},
        ]

    def test_read_from_hdx(self, configuration, read):
        vocabulary = Vocabulary.read_from_hdx("1731e7fc-ff62-4551-8a70-2a5878e1142b")
        assert vocabulary["id"] == "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        assert vocabulary["name"] == "miketest"
        vocabulary = Vocabulary.read_from_hdx("TEST2")
        assert vocabulary is None
        vocabulary = Vocabulary.read_from_hdx("TEST3")
        assert vocabulary is None

    def test_create_in_hdx(self, configuration, post_create):
        vocabulary = Vocabulary()
        with pytest.raises(HDXError):
            vocabulary.create_in_hdx()
        vocabulary["tags"] = [{"name": "economy"}]
        with pytest.raises(HDXError):
            vocabulary.create_in_hdx()

        data = copy.deepcopy(self.vocabulary_data)
        vocabulary = Vocabulary(data)
        vocabulary["name"] = "A Vocabulary"
        vocabulary.create_in_hdx()
        assert vocabulary["id"] == "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        assert vocabulary["name"] == "miketest"
        assert "state" not in vocabulary

        data["title"] = "XXX"
        vocabulary = Vocabulary(data)
        with pytest.raises(HDXError):
            vocabulary.create_in_hdx()

        data["title"] = "YYY"
        vocabulary = Vocabulary(data)
        with pytest.raises(HDXError):
            vocabulary.create_in_hdx()

    def test_update_in_hdx(self, configuration, post_update):
        vocabulary = Vocabulary()
        vocabulary["id"] = "NOTEXIST"
        with pytest.raises(HDXError):
            vocabulary.update_in_hdx()
        vocabulary["id"] = "LALA"
        with pytest.raises(HDXError):
            vocabulary.update_in_hdx()

        vocabulary = Vocabulary.read_from_hdx("1731e7fc-ff62-4551-8a70-2a5878e1142b")
        assert vocabulary["id"] == "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        assert vocabulary["name"] == "miketest"

        vocabulary["id"] = "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        vocabulary["tags"] = [{"name": "peter"}]
        vocabulary.update_in_hdx()
        assert vocabulary["id"] == "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        vocabulary["tags"] = [{"name": "peter"}]
        assert "state" not in vocabulary

        vocabulary["id"] = "NOTEXIST"
        with pytest.raises(HDXError):
            vocabulary.update_in_hdx()

        vocabulary["id"] = "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        vocabulary["tags"] = [{"name": "john"}]
        vocabulary.update_in_hdx()
        assert vocabulary["tags"] == [{"name": "john"}]

        del vocabulary["id"]
        vocabulary["name"] = "NOTEXIST"
        with pytest.raises(HDXError):
            vocabulary.update_in_hdx()

        data = copy.deepcopy(self.vocabulary_data)
        data["id"] = "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        data["tags"] = [{"name": "wash"}]
        data["description"] = "Custom chart X"
        vocabulary = Vocabulary(data)
        vocabulary.create_in_hdx()
        assert vocabulary["id"] == "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        assert vocabulary["tags"] == [{"name": "wash"}]
        assert "state" not in vocabulary

    def test_delete_from_hdx(self, configuration, post_delete):
        vocabulary = Vocabulary.read_from_hdx("1731e7fc-ff62-4551-8a70-2a5878e1142b")
        with pytest.raises(HDXError):
            vocabulary.delete_from_hdx(empty=False)
        vocabulary.delete_from_hdx()
        vocabulary = Vocabulary.read_from_hdx("1731e7fc-ff62-4551-8a70-2a5878e1142b")
        del vocabulary["id"]
        with pytest.raises(HDXError):
            vocabulary.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        data = copy.deepcopy(self.vocabulary_data)
        vocabulary = Vocabulary(data)
        assert vocabulary["tags"] == [{"name": "economy"}, {"name": "health"}]
        assert vocabulary["name"] == "Things"
        vocabulary.update_from_yaml(static_yaml)
        assert vocabulary["tags"] == [{"name": "haha"}]
        assert vocabulary["name"] == "Another"

    def test_update_json(self, configuration, static_json):
        data = copy.deepcopy(self.vocabulary_data)
        vocabulary = Vocabulary(data)
        assert vocabulary["tags"] == [{"name": "economy"}, {"name": "health"}]
        assert vocabulary["name"] == "Things"
        vocabulary.update_from_json(static_json)
        assert vocabulary["tags"] == [{"name": "papa"}]
        assert vocabulary["name"] == "Stuff"

    def test_get_all_vocabularies(self, configuration, post_list):
        vocabularies = Vocabulary.get_all_vocabularies()
        assert vocabularies[0]["id"] == "57f71f5f-adb0-48fd-ab2c-6b93b9d30332"
        assert vocabularies[1]["id"] == "1731e7fc-ff62-4551-8a70-2a5878e1142b"

    def test_tags(self, configuration):
        vocabulary_data = copy.deepcopy(TestVocabulary.vocabulary_data)
        vocabulary = Vocabulary(vocabulary_data)
        assert vocabulary.get_tags() == ["economy", "health"]
        vocabulary.add_tag("wash")
        assert vocabulary.get_tags() == ["economy", "health", "wash"]
        vocabulary.add_tags(["sanitation"])
        assert vocabulary.get_tags() == [
            "economy",
            "health",
            "wash",
            "sanitation",
        ]
        result = vocabulary.remove_tag("wash")
        assert result is True
        assert vocabulary.get_tags() == ["economy", "health", "sanitation"]
        vocabulary["tags"] = None
        result = vocabulary.remove_tag("wash")
        assert result is False

    def test_delete_approved_vocabulary(self, configuration, post_delete):
        Vocabulary._approved_vocabulary = None
        Vocabulary.get_approved_vocabulary()
        Vocabulary.delete_approved_vocabulary()
        assert Vocabulary._approved_vocabulary.data is None
        Vocabulary._approved_vocabulary = None

    def test_get_approved_vocabulary(self, configuration, read):
        Vocabulary._approved_vocabulary = None
        vocabulary = Vocabulary.get_approved_vocabulary()
        assert vocabulary["name"] == "Topics"
        assert vocabulary["tags"][0]["name"] == "administrative boundaries-divisions"
        Vocabulary._approved_vocabulary = None

    def test_create_approved_vocabulary(self, configuration, post_create):
        vocabulary = Vocabulary.create_approved_vocabulary()
        assert vocabulary["name"] == "Topics"
        assert vocabulary["tags"][0]["name"] == "administrative boundaries-divisions"
        Vocabulary._approved_vocabulary = None

    def test_update_approved_vocabulary(self, configuration, post_update):
        Vocabulary._approved_vocabulary = None
        vocabulary = Vocabulary.update_approved_vocabulary()
        assert vocabulary["name"] == "Topics"
        assert vocabulary["tags"][0]["name"] == "administrative boundaries-divisions"
        vocabulary["tags"] = [
            {
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                "display_name": "blah",
                "id": "d611b4c4-6964-45e4-a682-e6b53cc71f18",
                "name": "blah",
            }
        ]
        vocabulary = Vocabulary.update_approved_vocabulary(replace=False)
        assert vocabulary["tags"][0]["name"] == "blah"
        assert vocabulary["tags"][1]["name"] == "administrative boundaries-divisions"
        Vocabulary._approved_vocabulary = None

    def test_tag_mappings(self, configuration, read):
        Vocabulary._approved_vocabulary = None
        Vocabulary._tags_dict = None
        tags_dict = Vocabulary.read_tags_mappings()
        assert tags_dict["refugee"] == {
            "Action to Take": "merge",
            "New Tag(s)": "refugees",
        }
        assert Vocabulary.get_mapped_tag("refugee") == (["refugees"], list())
        assert Vocabulary.get_mapped_tag("monitoring") == (
            [],
            ["monitoring"],
        )
        tags_dict["refugee"]["Action to Take"] = "ERROR"
        assert Vocabulary.get_mapped_tag("refugee") == ([], list())
        refugeesdict = copy.deepcopy(tags_dict["refugees"])
        del tags_dict["refugees"]
        assert Vocabulary.get_mapped_tag("refugees") == (
            ["refugees"],
            [],
        )  # tag is in CKAN approved list but not tag cleanup spreadsheet
        tags_dict["refugees"] = refugeesdict
        Vocabulary.get_approved_vocabulary().remove_tag("refugees")
        assert Vocabulary.get_mapped_tag("refugees") == (
            [],
            [],
        )  # tag is not in CKAN approved list but is in tag cleanup spreadsheet
        Vocabulary._approved_vocabulary = None
        Vocabulary._tags_dict = None

    def test_chainrule_error(self, configuration, read):
        with pytest.raises(ChainRuleError):
            Vocabulary.set_tagsdict(None)
            url = "https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/main/tests/fixtures/Tag_Mapping_ChainRuleError.csv"
            Vocabulary.read_tags_mappings(url=url, failchained=True)
        Vocabulary._tags_dict = None

    def test_autocomplete(self, configuration, post_autocomplete):
        assert Vocabulary.autocomplete("health") == tag_autocomplete
