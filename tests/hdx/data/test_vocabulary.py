"""Vocabulary Tests"""
import copy
import json
from os.path import join

import pytest
from hdx.utilities.dictandlist import merge_two_dictionaries
from requests.exceptions import RetryError

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.data.vocabulary import ChainRuleError, Vocabulary

from . import MockResponse

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
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "3-word addresses",
                "id": "d611b4c4-6964-45e4-a682-e6b53cc71f18",
                "name": "3-word addresses",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "access to education",
                "id": "0df3df7f-e8b2-4993-a695-1c968b659e9f",
                "name": "access to education",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "access to electricity",
                "id": "53b952bd-ac3f-4e05-af4b-750bab60bfd6",
                "name": "access to electricity",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "access to sanitation",
                "id": "3a443337-e47f-46ec-8f5b-7fe27703e436",
                "name": "access to sanitation",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "access to water",
                "id": "99073d68-baff-45c3-a242-f6178dcb8e04",
                "name": "access to water",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "accident - technical disaster",
                "id": "2b9d9885-a3ae-4f56-afb8-3388e41bca64",
                "name": "accident - technical disaster",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "acronyms",
                "id": "8e35d922-69d8-49f2-bc6d-94d0f796f806",
                "name": "acronyms",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "activities - projects",
                "id": "25c6ab9a-2df7-4009-a76c-6db2dabf9b7d",
                "name": "activities - projects",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "administrative divisions",
                "id": "51cf6af5-6afc-4d7e-9ccf-00ea2c73afce",
                "name": "administrative divisions",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "adolescents",
                "id": "ef700ecf-de33-4958-847a-4edd5aafe2e8",
                "name": "adolescents",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "adults",
                "id": "2c8d30f5-0887-4719-93e1-23447ca32337",
                "name": "adults",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "affected area",
                "id": "f1f7e3cf-3c5a-4d13-a769-b26003805d07",
                "name": "affected area",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "affected population",
                "id": "97e8b6b2-f2a6-4d7c-aef3-74f5c5ad6020",
                "name": "affected population",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "africa",
                "id": "300e32bd-88d4-47db-8835-46a0084c21e4",
                "name": "africa",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "agriculture",
                "id": "b1c3f3bd-96e4-42c0-8ebe-4aabf75b1aa3",
                "name": "agriculture",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "aid funding",
                "id": "408fd5d4-5842-4066-967c-4b1978f8d806",
                "name": "aid funding",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "aid workers",
                "id": "408fd5d4-5842-4066-967c-4b1978f8d806",
                "name": "aid workers",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "aid workers security",
                "id": "c1345998-3852-470b-ad8d-66f17073227d",
                "name": "aid workers security",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "airports",
                "id": "3b2ea17d-2284-4882-bce0-f869eb4cac29",
                "name": "airports",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "albinism",
                "id": "0edb81f2-1806-4c83-8031-82f1199f95ec",
                "name": "albinism",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "americas",
                "id": "546f6ea5-f341-4039-a5e5-7d5e77c72b12",
                "name": "americas",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "armed violence",
                "id": "acc2015e-df7c-4b6d-bc78-cb988f0be4bb",
                "name": "armed violence",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "artemisinin-based combination therapy - act",
                "id": "67111e51-6150-4ce3-85ce-f902611c8ae5",
                "name": "artemisinin-based combination therapy - act",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "assistance targets",
                "id": "e0e2d7d3-7b6b-4046-a813-e66b1157c1a4",
                "name": "assistance targets",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "asylum seekers",
                "id": "2334ad1e-d229-4575-b2af-cb06438f26fa",
                "name": "asylum seekers",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "attacks on civilians",
                "id": "2dc95367-b196-4358-a958-56a47097d324",
                "name": "attacks on civilians",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "avalanche",
                "id": "cf690f76-66b8-48aa-9515-9e24f4cbe8d9",
                "name": "avalanche",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "aviation",
                "id": "ad980330-36cf-4451-9919-6ae6710be1ca",
                "name": "aviation",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "bangladesh - rohingya refugee crisis - 2017-",
                "id": "0d266450-fe2d-4af6-ba4b-d9016a5d75a0",
                "name": "bangladesh - rohingya refugee crisis - 2017-",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "baseline",
                "id": "e24e3401-bdb5-4d1c-a9fd-b68ef5012e13",
                "name": "baseline",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "baseline population",
                "id": "f8d8c26d-e996-4e89-9ec7-1ad032777930",
                "name": "baseline population",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "biomass",
                "id": "02f50138-d169-49e9-9c07-fe1f6c717e93",
                "name": "biomass",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "births",
                "id": "f36f1bc1-f945-44ef-bc70-7a9bcd90fd32",
                "name": "births",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "border crossings",
                "id": "9175b67b-f97e-4d84-9f30-e6370f519486",
                "name": "border crossings",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "boys",
                "id": "db2b25cb-e875-4f3c-b2a8-1cce1266959f",
                "name": "boys",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "bridges",
                "id": "b99ef594-e7cd-4308-aa99-049e529eac55",
                "name": "bridges",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "buildings",
                "id": "ea3ca9c7-a0ed-4ae1-a473-ab79def4e941",
                "name": "buildings",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "c-section - caesarean delivery",
                "id": "76673752-8924-47cc-85ba-f286dc2f9dfa",
                "name": "c-section - caesarean delivery",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "camp coordination and management - ccm",
                "id": "bdcd6a8c-b694-4fb9-a4d5-6c53a51fe4c1",
                "name": "camp coordination and management - ccm",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "camps",
                "id": "010ccfb5-82c9-4547-b974-a878b4316f26",
                "name": "camps",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "caribbean",
                "id": "e9284a90-2b6b-42df-a8e7-690229a9b318",
                "name": "caribbean",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "caseload",
                "id": "b0f43dc4-0e82-482c-b9d9-06dcd2cb80c9",
                "name": "caseload",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "cash assistance",
                "id": "854448cf-775a-44e7-9c26-cca86aeea17d",
                "name": "cash assistance",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "casualties",
                "id": "b217f086-5732-40c6-bb13-e281b0777e3f",
                "name": "casualties",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "census",
                "id": "68d8a726-7970-460c-b321-b0a4c0c39ee1",
                "name": "census",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "central africa",
                "id": "bde0edac-e4ce-4229-971e-0dbdee63f878",
                "name": "central africa",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "chikungunya",
                "id": "0c071765-db64-484e-b24b-6cf25596fa87",
                "name": "chikungunya",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "child marriage",
                "id": "2024aa66-713f-49e5-98ad-18eab649a9db",
                "name": "child marriage",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "child protection",
                "id": "79279bd6-7e1e-4167-8819-11d074eaed98",
                "name": "child protection",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "children",
                "id": "ccc2974e-dffa-4137-ac07-bc8e6986503f",
                "name": "children",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "cholera",
                "id": "967188c1-e00e-4f69-8567-b3e91775b28d",
                "name": "cholera",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "civil society",
                "id": "ef3b1781-66b3-4b75-9a40-da86838a8669",
                "name": "civil society",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "climate",
                "id": "8fb84205-1ca9-4595-990d-fbdace14442b",
                "name": "climate",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "cluster system",
                "id": "86b1b4d0-40c1-42fc-8074-c594641d0aaf",
                "name": "cluster system",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "cold wave",
                "id": "e9208fb4-b80f-44ee-abb0-91cdabc61887",
                "name": "cold wave",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "commodities",
                "id": "bc686ec7-43d2-4f59-aeb5-a3e4754737e2",
                "name": "commodities",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "common operational dataset - cod",
                "id": "4bfa932c-2c6c-4277-aa85-2b9240e45ebf",
                "name": "common operational dataset - cod",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "community perceptions",
                "id": "7dca5226-b86c-443f-8f5a-d2590bc0145a",
                "name": "community perceptions",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "complex emergency",
                "id": "2d7dfc5a-6eed-41ab-8c7c-bdf051382744",
                "name": "complex emergency",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "confinement",
                "id": "e6f94e48-2b8c-4874-878f-8fe1a9c302f6",
                "name": "confinement",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "construction materials",
                "id": "493906f2-5dae-4b4a-94d2-6271221db6ee",
                "name": "construction materials",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "contacts",
                "id": "aaff5849-6bb4-4b99-9226-17be262bb03a",
                "name": "contacts",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "coordination",
                "id": "02e2acaa-90c0-4a1f-84cb-bc2c558b85e3",
                "name": "coordination",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "coping strategies - coping capacity",
                "id": "39b6db02-3bf6-4c89-a6dc-e687289a79d9",
                "name": "coping strategies - coping capacity",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "corruption",
                "id": "01dc91db-bf39-4e3b-b520-a5d64da3cc26",
                "name": "corruption",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "cost of damage",
                "id": "896a5c8e-f81d-4940-a262-4d808cf7a2eb",
                "name": "cost of damage",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "coxs bazar",
                "id": "dac97916-a22e-43b3-ada7-84b64055edca",
                "name": "coxs bazar",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "crime",
                "id": "cdfa4b39-4f3d-4845-9fca-0846e6c38ffd",
                "name": "crime",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "cultural sites",
                "id": "24ee3a3d-cae0-483e-87e5-acd66d3a3f79",
                "name": "cultural sites",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "cyclones - hurricanes - typhoons",
                "id": "ad765c94-fade-49ee-bfd0-1511fada3f59",
                "name": "cyclones - hurricanes - typhoons",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "damage assessment",
                "id": "b9b673aa-4977-4071-bbbb-c4ed1ea44107",
                "name": "damage assessment",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "damaged buildings",
                "id": "c97bc3eb-c54b-4d59-9432-446c48815182",
                "name": "damaged buildings",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "dams",
                "id": "4f815ade-1db7-4e78-b780-6d6fded50545",
                "name": "dams",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "darfur",
                "id": "7e72e47f-91dc-464b-874c-966dcb60e709",
                "name": "darfur",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "demographics",
                "id": "463e6646-dea2-4d04-ac5d-57f6a9b29eca",
                "name": "demographics",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "dengue fever",
                "id": "5158fb50-a369-4056-b47b-3c43fb641366",
                "name": "dengue fever",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "deportees",
                "id": "b24a1072-09f6-4782-910b-195494511ccf",
                "name": "deportees",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "diarrhea",
                "id": "962176fe-c287-4f0b-804e-bcf06814c3e3",
                "name": "diarrhea",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "disabled",
                "id": "7cabe5fc-aeff-4e62-8476-74a23fb1e4b4",
                "name": "disabled",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "disaggregated by sex",
                "id": "636081ca-617d-4ff9-887e-2f19eaff1c47",
                "name": "disaggregated by sex",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "disaster risk reduction - drr",
                "id": "6e5c3e34-0b02-438f-bbc1-4b5bc93cd48e",
                "name": "disaster risk reduction - drr",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "disease",
                "id": "bb2917bd-dc0b-4c74-8538-6db6156c3ca6",
                "name": "disease",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "displaced persons locations - camps - shelters",
                "id": "e6b09719-b6db-479f-ba07-773addd3279f",
                "name": "displaced persons locations - camps - shelters",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "displacement",
                "id": "067a32a1-6db0-4bb1-bc59-812b5ddf388d",
                "name": "displacement",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "distributions",
                "id": "5d91d149-2739-4c53-be13-ee317fc6278d",
                "name": "distributions",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "dr congo - ebola outbreak - aug 2018",
                "id": "a80ddd31-09e9-4c5d-b4cd-4d332f44b740",
                "name": "dr congo - ebola outbreak - aug 2018",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "drought",
                "id": "a841817f-49ca-47b0-b478-35327a87a77f",
                "name": "drought",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "early learning",
                "id": "f580b95a-bb55-4fab-b2fe-3d510ae65454",
                "name": "early learning",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "earthquakes",
                "id": "79c8fc96-9e7d-42f6-b88f-60449508cc43",
                "name": "earthquakes",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "eastern africa",
                "id": "cf650a9a-4cfa-4465-abec-73287523ef7b",
                "name": "eastern africa",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "ebola",
                "id": "af4aa01f-400f-42b2-bc7b-556847987eaa",
                "name": "ebola",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "economics",
                "id": "8a43b269-78cb-4a85-a4ce-7e3e928bc487",
                "name": "economics",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "education",
                "id": "95c5334f-487d-412a-ad93-5217ca924b85",
                "name": "education",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "education cluster",
                "id": "27694218-97da-4759-9067-0c8413d96f79",
                "name": "education cluster",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "education facilities - schools",
                "id": "ad7c57d4-fcbd-4ed0-b373-e887db45359e",
                "name": "education facilities - schools",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "educational attainment",
                "id": "47570a42-9178-4d4e-9074-7c93a672a649",
                "name": "educational attainment",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "educators - teachers",
                "id": "3d53b926-58af-4acd-9c17-49af6b8e0dda",
                "name": "educators - teachers",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "el nino",
                "id": "ba816763-2175-4625-8f01-c4135797891f",
                "name": "el nino",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "elderly",
                "id": "8b7bdc02-f00d-4e6d-ba2a-e69a8ed44585",
                "name": "elderly",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "elections",
                "id": "306d0f13-428d-46f8-85a5-30cd07c75bc9",
                "name": "elections",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "electricity",
                "id": "bb52c41c-f964-42a6-b57d-1478927078d4",
                "name": "electricity",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "elevation - topography - altitude",
                "id": "c2ff0f84-c24e-4f94-a046-db0eb0aed615",
                "name": "elevation - topography - altitude",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "embera",
                "id": "3f2a6e77-0910-453a-8b0e-4e1bc7084b25",
                "name": "embera",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "employment",
                "id": "8580fdda-34f8-4060-a464-a57f7f8f1e11",
                "name": "employment",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "energy",
                "id": "07df0b4f-9148-4ce9-b8a2-5434eaff97e5",
                "name": "energy",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "environment",
                "id": "e8c4c7aa-9b49-49e1-8a6f-22543793e772",
                "name": "environment",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "epidemics and outbreaks",
                "id": "e5cd984c-70d4-48f8-b339-f7504389a6ea",
                "name": "epidemics and outbreaks",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "ethnicity",
                "id": "8f3e789b-86b3-448a-933c-8dd153408a8a",
                "name": "ethnicity",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "europe",
                "id": "d3bdc209-7962-4fdb-be8a-c1caa629cfcf",
                "name": "europe",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "european union - eu",
                "id": "d0372872-fed0-401d-b12c-7a37463e7a20",
                "name": "european union - eu",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "exchange rates - fx",
                "id": "5c48fb25-3981-4274-b06f-58b92f7770b2",
                "name": "exchange rates - fx",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "facilities and infrastructure",
                "id": "72b5b6c1-7244-4467-af90-78a8b328953b",
                "name": "facilities and infrastructure",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "families",
                "id": "528a5920-1ed6-451f-9ded-ff16e038caba",
                "name": "families",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "famine",
                "id": "1fd3236f-b843-4fc5-a31b-d36a1f2b51dc",
                "name": "famine",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "fatalities - deaths",
                "id": "f62ba0cc-0177-4394-a418-e9c176252c60",
                "name": "fatalities - deaths",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "female",
                "id": "ed186131-747e-47c6-ac58-6c6a6476da49",
                "name": "female",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "female genital mutilation and cutting - fgmc",
                "id": "1fbfa1e0-43b1-491a-8cac-1dd40f9c48d1",
                "name": "female genital mutilation and cutting - fgmc",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "ferries",
                "id": "b9294b01-1665-48cc-b95b-5fdcdd843e27",
                "name": "ferries",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "financial behavior",
                "id": "5ddc19a1-08ca-4a91-a37e-3ea37d9bf114",
                "name": "financial behavior",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "financial institutions",
                "id": "3e45af07-c4a9-4915-a1ce-43cac86b9826",
                "name": "financial institutions",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "fisheries",
                "id": "7555dc01-ead1-4663-bc37-89b7f2a3f920",
                "name": "fisheries",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "fistula",
                "id": "740a4288-240b-4018-81cb-9f4bdf3eb19a",
                "name": "fistula",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "flash flood",
                "id": "c68b62af-9cbf-4666-b77c-27f367e6fa14",
                "name": "flash flood",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "flood",
                "id": "5b299af7-0d82-4c2f-84b6-2b38b6cad2d3",
                "name": "flood",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "flood water extent",
                "id": "2124c9b9-1626-4952-9057-2690fa6ead60",
                "name": "flood water extent",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "food assistance",
                "id": "d2dd61cf-a5a9-4e6e-b80f-72b60641ce74",
                "name": "food assistance",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "food consumption",
                "id": "1f2f9dbd-f51b-479c-bc73-7f75f823b3f6",
                "name": "food consumption",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "food production",
                "id": "8113dd42-a84e-4c99-bcb4-688086261862",
                "name": "food production",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "food security",
                "id": "49fcf168-76ac-41e7-a0d0-2cbda8dafae3",
                "name": "food security",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "footpaths",
                "id": "e0393c22-fef1-4ec3-9d53-2e66425ffde6",
                "name": "footpaths",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "gap analysis",
                "id": "40c91fa1-7af4-4ec6-a411-5f4e28188a16",
                "name": "gap analysis",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "gazetteer",
                "id": "16472c9b-c4d4-4918-a8ab-6e1c751a0eeb",
                "name": "gazetteer",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "gender",
                "id": "622aa251-ab33-4613-875c-d5ada832a8e1",
                "name": "gender",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "gender-based violence - gbv",
                "id": "61942811-56a6-4139-8d0f-ac48478a602d",
                "name": "gender-based violence - gbv",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "geodata",
                "id": "ea2fa344-d178-4c8e-93df-d39e5160dc22",
                "name": "geodata",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "girls",
                "id": "40d570ff-c193-4eb3-b9c9-0ad99ba97730",
                "name": "girls",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "global acute malnutrition - gam",
                "id": "bdaa862a-5fb7-4144-813b-1125fa43483b",
                "name": "global acute malnutrition - gam",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "governance and civil society",
                "id": "684bc71a-9c0b-4d90-8951-08c4d89a3755",
                "name": "governance and civil society",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "government expenditure",
                "id": "39997fdd-fa61-4dc6-b6c4-7406faace764",
                "name": "government expenditure",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "green area",
                "id": "a1816fc9-cf87-4b4d-86cb-34af1eaaa539",
                "name": "green area",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "gross domestic product - gdp",
                "id": "5827595a-2d13-4a4c-aec7-5f39f17a2ffc",
                "name": "gross domestic product - gdp",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "gross national income - gni",
                "id": "b74034fc-0014-4da2-bb6a-62715c97faca",
                "name": "gross national income - gni",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "ground motion - shaking intensity",
                "id": "3989df8d-3de9-4ee6-8fbf-eb31506ba7a4",
                "name": "ground motion - shaking intensity",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "hazardous areas",
                "id": "55db5606-aacd-43fe-9497-aaa8c933d566",
                "name": "hazardous areas",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "hazards and risk",
                "id": "92ccd39c-97fd-4d13-91d5-4df411a4a227",
                "name": "hazards and risk",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "health",
                "id": "e1ff4b78-efcf-40e7-888b-7342169068e1",
                "name": "health",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "health districts",
                "id": "7dc09c68-c551-4859-a6cf-8de37a4c0d2c",
                "name": "health districts",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "health facilities",
                "id": "4f1e737b-e6c9-4460-a6cf-ca7e21ad930d",
                "name": "health facilities",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "health services - healthcare",
                "id": "a5335788-2293-4b69-9b50-96266595f233",
                "name": "health services - healthcare",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "health workers",
                "id": "681bf0c9-8ec1-40b2-aa67-d1fdc1570d1a",
                "name": "health workers",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "healthcare",
                "id": "d168b7f4-1912-4ec0-a503-183842dd7b00",
                "name": "healthcare",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "heat waves",
                "id": "ce099aa4-5143-4549-b07a-c0da07910861",
                "name": "heat waves",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "helicopter landing zone - hlz",
                "id": "a100a8eb-8926-48ac-858b-49ecd6b19a52",
                "name": "helicopter landing zone - hlz",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "hiv - aids",
                "id": "01ffbb03-4924-4d24-a285-f96973f562ff",
                "name": "hiv - aids",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "homelessness",
                "id": "a54e5df0-7cf3-41d4-92a2-2ad53f7243e8",
                "name": "homelessness",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "homicides",
                "id": "0c9769a4-0814-46c3-824e-204d8df8e15c",
                "name": "homicides",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "horn of africa",
                "id": "60744c9c-876b-4480-ba18-e68f2265e2a1",
                "name": "horn of africa",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "host communities",
                "id": "15f4f35c-979a-4134-899f-daedca47548f",
                "name": "host communities",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "household budgets",
                "id": "fb04a537-4528-437d-89c6-f82eff46c133",
                "name": "household budgets",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "household energy",
                "id": "d0e7ac88-418d-4b63-b9e2-32d37161f1f2",
                "name": "household energy",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "human rights",
                "id": "58f7c15e-ab77-4fcd-9dbf-bb7bb056249e",
                "name": "human rights",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "humanitarian access",
                "id": "a79bf8e4-7bf9-4b27-831e-73bb35b11776",
                "name": "humanitarian access",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "humanitarian funding",
                "id": "17c9c66b-0bca-4ad7-b118-791926262541",
                "name": "humanitarian funding",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "humanitarian needs overview - hno",
                "id": "0fed8701-ffaa-4798-8eea-298ef287a9ac",
                "name": "humanitarian needs overview - hno",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "humanitarian profile",
                "id": "f607c763-baf2-480c-a821-eb0167ca6638",
                "name": "humanitarian profile",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "humanitarian response plan - hrp",
                "id": "f43636e2-ae9b-452c-a9ff-da8d05567fb3",
                "name": "humanitarian response plan - hrp",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "hurricane irma - sep 2017",
                "id": "95ebd1e6-b937-4a71-b184-75038d846dea",
                "name": "hurricane irma - sep 2017",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "hurricane maria - sep 2017",
                "id": "42609274-caa9-411e-a2ef-105fdd84b610",
                "name": "hurricane maria - sep 2017",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "hurricane matthew - sep 2016",
                "id": "f7fa5c44-e5b8-4f57-8b8d-372b82ff0d17",
                "name": "hurricane matthew - sep 2016",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "hurricane patricia - oct 2015",
                "id": "17e151d2-2107-47ce-ae7a-8fa2f8a38938",
                "name": "hurricane patricia - oct 2015",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "hxl",
                "id": "9874ffb9-4c87-4265-8cd7-44c4a20be46f",
                "name": "hxl",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "imagery",
                "id": "5f89a652-0e33-42a2-8313-fa1fd9e0d419",
                "name": "imagery",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "immunization",
                "id": "b5f7c13a-a11a-4f5d-9d51-19c9326c3e98",
                "name": "immunization",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "improvised explosive devices - ied",
                "id": "abd2292f-c38b-4652-bab0-4474332c35c2",
                "name": "improvised explosive devices - ied",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "incidents of disaster",
                "id": "91962a80-673d-48be-87f5-1e6bd875c76c",
                "name": "incidents of disaster",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "indigenous people",
                "id": "c5d5d002-6a63-4d89-bbf8-b018fdec2de2",
                "name": "indigenous people",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "indonesia - papua - sulawesi  earthquakes - jun 2010",
                "id": "12f4d069-ea08-49b3-bab7-5a5658bbbe72",
                "name": "indonesia - papua - sulawesi  earthquakes - jun 2010",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "inequality",
                "id": "f85f111b-dd00-4bd9-afb9-eda55d0af33d",
                "name": "inequality",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "infant mortality",
                "id": "357eb763-c8fc-4a4b-99c1-fe7531ea8e8d",
                "name": "infant mortality",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "infants",
                "id": "5242e5bd-b8dc-44f6-ba15-581efa251ad1",
                "name": "infants",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "infectious disease",
                "id": "bc9fd429-da22-4a7c-97e6-7918471f0733",
                "name": "infectious disease",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "inflation",
                "id": "428e9914-aa27-4ceb-968d-28b17c65aebc",
                "name": "inflation",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "informal settlements",
                "id": "ab2e7866-9304-4a14-9a00-aef8b1a0560e",
                "name": "informal settlements",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "injured",
                "id": "5c606267-58fc-44ab-85df-77959fdc58f1",
                "name": "injured",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "insect infestation",
                "id": "cd213029-df8c-4a14-90d0-91e31899dc9f",
                "name": "insect infestation",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "insecticide treated nets",
                "id": "61865a12-8765-4284-af77-1d71daef5088",
                "name": "insecticide treated nets",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "integrated phase classification - ipc",
                "id": "d7a26634-7011-48d1-8e0c-c556085aa911",
                "name": "integrated phase classification - ipc",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "internally displaced persons - idp",
                "id": "08061110-6714-4062-8189-41659fa9eb3c",
                "name": "internally displaced persons - idp",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "international aid",
                "id": "cb9a4b5e-158a-417d-a65f-edc036cb96f9",
                "name": "international aid",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "international aid transparency initiative - iati",
                "id": "c3f0205d-ae41-40a0-9d89-b192d734df45",
                "name": "international aid transparency initiative - iati",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "international boundaries",
                "id": "0c8c74be-4b4b-4ddf-b1ae-add86a7b2aca",
                "name": "international boundaries",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "international trade",
                "id": "1abe09b1-49a1-4350-a435-db8689d68bcb",
                "name": "international trade",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "internet",
                "id": "407e8b17-5833-4fca-9a94-06b0ba4d663d",
                "name": "internet",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "intersex",
                "id": "f207706c-21e5-431f-9cae-2ac913eb73ef",
                "name": "intersex",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "kidnappings",
                "id": "609ec6d5-bd30-4c33-b6f9-27ad9714cf9b",
                "name": "kidnappings",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "la nina",
                "id": "f1b2bbbe-4ce5-4a28-80c2-c37cd05ca078",
                "name": "la nina",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "lake chad basin crisis - 2014-2017",
                "id": "05ca8810-49fb-481f-963a-5a29904793df",
                "name": "lake chad basin crisis - 2014-2017",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "land tenure",
                "id": "064c068c-a71d-43b9-8473-b4063b1d91d1",
                "name": "land tenure",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "land use and land cover",
                "id": "975636c9-eaf1-47b3-b004-aa3f5e5dcb0f",
                "name": "land use and land cover",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "landmines and explosive remnants of war - erw",
                "id": "67cc32d5-8ede-4ffa-a2da-ae3bb5f1bdbe",
                "name": "landmines and explosive remnants of war - erw",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "landslides",
                "id": "8f1423d2-69bb-48cf-a5c1-6f98fce10156",
                "name": "landslides",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "latrines",
                "id": "f55389b1-ba57-4f89-a9f3-283a455cb5ec",
                "name": "latrines",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "leptospirosis",
                "id": "958b9582-e045-43a7-a4f4-33ba2ea6c31a",
                "name": "leptospirosis",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "lighting",
                "id": "650a9801-7aab-4e9e-8a88-d145ca564f22",
                "name": "lighting",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "literacy",
                "id": "ec3b70d5-701f-4331-bd01-29c8e93a7483",
                "name": "literacy",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "livelihoods",
                "id": "35669ec9-a0fa-4768-9c6a-3044bc56bbc8",
                "name": "livelihoods",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "livestock",
                "id": "6402f56a-bde2-4feb-991e-01901c515996",
                "name": "livestock",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "logistics",
                "id": "d058725e-6c55-4149-b1dd-fbefb21024ac",
                "name": "logistics",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "malaria",
                "id": "6ec9610b-7bef-4b0b-896f-939761e3327d",
                "name": "malaria",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "malaria first line treatment",
                "id": "8bcf6ec9-9caf-4bde-8bb4-a9ec0a17fe78",
                "name": "malaria first line treatment",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "male",
                "id": "0e0960c2-d5f3-4857-a724-f5faf1f607ff",
                "name": "male",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "malnutrition",
                "id": "4cdcb583-8e1b-43e1-95e7-4f922724b4e8",
                "name": "malnutrition",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "markets",
                "id": "32cd2fd4-5826-476e-9060-2e03a61f5dc9",
                "name": "markets",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "maternal",
                "id": "2a682dc3-f68d-41f4-86a5-f263922feafb",
                "name": "maternal",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "measles",
                "id": "5b67525f-74ef-4dbe-a704-b8751fb89377",
                "name": "measles",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "media",
                "id": "8be68b21-bea6-433d-bdc3-62c2b20ebd85",
                "name": "media",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "mediterranean",
                "id": "5621bc07-2aa6-4c0a-ba9c-d000623a2d4d",
                "name": "mediterranean",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "men",
                "id": "1a1917e5-6fa4-40f3-acf4-a408e13935ca",
                "name": "men",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "meningitis",
                "id": "741dc76d-22c6-43b1-937e-2582c015241d",
                "name": "meningitis",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "mental health",
                "id": "811b2a7e-6b40-443c-b658-d4374101566d",
                "name": "mental health",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "migrants",
                "id": "c5aa3b81-7c14-40af-8c34-216e6caa530e",
                "name": "migrants",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "migration",
                "id": "7b9329e0-9203-4f9d-9e4f-0efb99cf47f0",
                "name": "migration",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "military",
                "id": "ad3590c6-dcc8-4fb1-9164-98a09b5fbc90",
                "name": "military",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "millennium development goals - mdg",
                "id": "5940a861-deee-47dd-becf-fe99b166e055",
                "name": "millennium development goals - mdg",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "mine risk education",
                "id": "dbb85880-66a4-4cc8-891d-e4c7ae424a77",
                "name": "mine risk education",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "mining",
                "id": "87107a6f-1947-4624-970f-77be83b167d7",
                "name": "mining",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "minors",
                "id": "6990f64c-05a4-4b6d-a54e-5aed11cb105e",
                "name": "minors",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "missing persons",
                "id": "462d52ae-5c3a-4155-ab30-f4e825b2eff7",
                "name": "missing persons",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "monitoring and evaluation",
                "id": "3c2a2ebc-e360-42f9-8f54-29e513859f96",
                "name": "monitoring and evaluation",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "mortality",
                "id": "09eb0d2d-7d72-45e6-9e6d-3b91a2ee848f",
                "name": "mortality",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "mosquito nets",
                "id": "0f85380a-0ac1-4cff-8fb6-6bdb74f57e89",
                "name": "mosquito nets",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "mudslide",
                "id": "22514c52-f6f1-4ce6-8f23-d2afee8923f3",
                "name": "mudslide",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "needs assessment",
                "id": "5792b090-82d9-4f37-8c8f-d33f15df756e",
                "name": "needs assessment",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "neighbourhoods",
                "id": "a5c35823-69f8-442c-8849-a422ac9fcd2c",
                "name": "neighbourhoods",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "neonatal",
                "id": "40716366-8d9c-4ca7-891f-ffbd84428242",
                "name": "neonatal",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "nigeria - complex emergency - 2014-",
                "id": "07c13a8a-119a-4e2f-8c30-71caff0ff187",
                "name": "nigeria - complex emergency - 2014-",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "non-food items - nfi",
                "id": "85965e00-9a67-4fe3-b1ac-574f5b38517d",
                "name": "non-food items - nfi",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "nutrition",
                "id": "4d11e987-1529-48af-a536-8b3272326eb6",
                "name": "nutrition",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "office locations",
                "id": "6fb749b0-aa16-41a9-8c16-b26fa585f923",
                "name": "office locations",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "operational capacity",
                "id": "cb026066-da31-4df5-9ac8-7826c76777f1",
                "name": "operational capacity",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "operational presence",
                "id": "9c926e8e-4fc9-41b8-9cf6-07848f4559d9",
                "name": "operational presence",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "oral rehydration treatment - ort",
                "id": "20e87564-c9b9-4b65-9121-55eccd9ad980",
                "name": "oral rehydration treatment - ort",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "out-of-school",
                "id": "2c00aa4d-9420-475f-8264-e99e43c1ce10",
                "name": "out-of-school",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "outbreaks",
                "id": "ed47cbcc-9f07-475a-b248-5e0b48c2db44",
                "name": "outbreaks",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "overcrowding",
                "id": "e8b2af5d-8cb8-4d77-8bce-5e6c554c53e9",
                "name": "overcrowding",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "partners",
                "id": "ba6f2d22-e4d4-4429-a1b3-881c809ca7e4",
                "name": "partners",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "peacekeeping",
                "id": "6c1f5670-dc4a-4e3c-afe1-f013e8a1c6e1",
                "name": "peacekeeping",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "people in need - pin",
                "id": "aa70acf1-13ed-4268-8c7e-b060110de200",
                "name": "people in need - pin",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "people reached with assistance",
                "id": "b16a5080-8df4-4714-be22-e973c2664804",
                "name": "people reached with assistance",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "people targeted for assistance",
                "id": "dadd62b6-3b85-4404-9cf7-d0d88f4419fa",
                "name": "people targeted for assistance",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "pneumonia",
                "id": "457b063f-b994-471e-971e-f1f7f2d69df1",
                "name": "pneumonia",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "points of interest - poi",
                "id": "c79ab089-2d9b-4de9-8d92-04cd7684506a",
                "name": "points of interest - poi",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "police",
                "id": "9734c0ce-81ea-44af-b074-24c0375f5013",
                "name": "police",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "polio",
                "id": "aa8ad5f0-1280-4a32-9dd9-ca8d62fef773",
                "name": "polio",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "polling stations",
                "id": "08328a83-aff1-42e7-920e-8a0bbee25039",
                "name": "polling stations",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "populated places - settlements",
                "id": "cbd3d816-b84d-46a1-a910-e68fb4589995",
                "name": "populated places - settlements",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "population",
                "id": "38b6f7a9-1580-4164-babe-c6fe4a01b43a",
                "name": "population",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "population movement",
                "id": "a4fb685c-7173-4be1-abc4-49fd0d85a493",
                "name": "population movement",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "ports",
                "id": "67a62392-8d29-4ae2-94bf-295ae9f9a016",
                "name": "ports",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "poverty",
                "id": "24fbd06b-9481-4769-8043-725bec1a0d16",
                "name": "poverty",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "practicability",
                "id": "aad76fa3-dfc0-4776-a863-86ba20963eb4",
                "name": "practicability",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "pregnancies",
                "id": "5581ea53-39ce-4082-b995-cdc702617d95",
                "name": "pregnancies",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "prices",
                "id": "af256749-bbff-4a35-8bfe-a6c82d3d74de",
                "name": "prices",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "prioritization",
                "id": "fd5f13da-70dc-4d7d-964f-99682cd54c18",
                "name": "prioritization",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "protected areas",
                "id": "7c03f3f9-a455-49eb-a9b4-9d9678915684",
                "name": "protected areas",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "protection of civilians",
                "id": "62197d2d-3205-454f-9197-7fea48f2c432",
                "name": "protection of civilians",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "protests",
                "id": "07b31733-abe4-4445-8889-1c030bfd5bb0",
                "name": "protests",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "public services",
                "id": "6b059664-112f-4d3c-b03f-d7eaa158f3de",
                "name": "public services",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "rabies",
                "id": "5ba35da2-51e8-4869-a11d-8ed8f85feb6d",
                "name": "rabies",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "radio stations",
                "id": "282b07fd-0cdc-4a81-aea0-82d8d8be3ba1",
                "name": "radio stations",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "railways",
                "id": "4d568ee3-c5ec-42f7-8d88-b99e2b788f9b",
                "name": "railways",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "rainfall - precipitation",
                "id": "e4ceeabf-3349-49de-adc1-8a3e241571d9",
                "name": "rainfall - precipitation",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "refugees",
                "id": "99712e17-9bd3-4453-8326-46eb0a1dd6ea",
                "name": "refugees",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "religion",
                "id": "6cc5c6e0-edea-4e23-a469-303293410c4d",
                "name": "religion",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "religious facilities",
                "id": "2a049bf9-4bd7-4784-aa01-9281fc0f40aa",
                "name": "religious facilities",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "reproductive health and family planning",
                "id": "7f94570e-7f22-42d5-b271-f4861ab019fe",
                "name": "reproductive health and family planning",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "resettlement",
                "id": "64d656de-0744-42fa-9fbb-74af5cff3db7",
                "name": "resettlement",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "resilience",
                "id": "77f8df07-729b-4e97-8d0a-bd8603e91dcf",
                "name": "resilience",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "returnees",
                "id": "441b281c-172f-401b-890a-4203f044b7e9",
                "name": "returnees",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "rivers",
                "id": "ba8b38b8-8cf2-4e46-ad01-a857602c0e7e",
                "name": "rivers",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "roadblocks",
                "id": "1340e6fd-4954-4dda-945c-bb2c61189b76",
                "name": "roadblocks",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "roads",
                "id": "bafafc7e-7fc1-4983-a7c8-30ec45b1dd63",
                "name": "roads",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "rural",
                "id": "e1ee59fa-c20e-41f8-bf4d-f88bf39c9860",
                "name": "rural",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "sahel",
                "id": "a3edb907-da42-4001-9664-1edff3e39b8a",
                "name": "sahel",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "school enrollment",
                "id": "40a12809-5a4c-47e2-b6cf-d7cb6374a367",
                "name": "school enrollment",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "security",
                "id": "febcdbaf-9d8b-4686-aec8-e953fd0c1625",
                "name": "security",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "security incidents",
                "id": "f8da83a5-b281-4352-8c4c-feeef5f3df1b",
                "name": "security incidents",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "services",
                "id": "46a36513-8519-40e4-9284-9af2e090c781",
                "name": "services",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "severe acute malnutrition - sam",
                "id": "9e41ddd2-d4e8-4596-a648-4651204e0117",
                "name": "severe acute malnutrition - sam",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "severe local storms",
                "id": "3036a4e7-b6a8-4438-a8e0-b8fddb68b054",
                "name": "severe local storms",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "severity",
                "id": "ee51816d-37a8-4c4c-9a35-e29a53d76758",
                "name": "severity",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "sex and age disaggregated data - sadd",
                "id": "02307153-987c-46ee-930d-425a5f03357b",
                "name": "sex and age disaggregated data - sadd",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "sexual violence and abuse",
                "id": "9b061039-8ada-4e52-b143-46d71f60dc76",
                "name": "sexual violence and abuse",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "shop locations",
                "id": "cae615b4-1a61-4912-bbf1-ad78f3e159b3",
                "name": "shop locations",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "single parents",
                "id": "276d1143-df2c-47ac-bf34-4f391b0448b0",
                "name": "single parents",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "snowfall",
                "id": "76699ac5-e851-4cc1-869a-908d3e5dd56e",
                "name": "snowfall",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "social development centers",
                "id": "ee92d538-cec2-4e12-8c6b-48238a892595",
                "name": "social development centers",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "social media data",
                "id": "effa87eb-9a0e-4ae3-ad3a-aee63695bea7",
                "name": "social media data",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "socioeconomics",
                "id": "0378ec85-8956-4aa7-9a56-441e3655f716",
                "name": "socioeconomics",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "southern africa",
                "id": "a1df7cda-c29f-4939-93ee-ac722f44574d",
                "name": "southern africa",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "stateless persons",
                "id": "97f56934-0911-446c-ac55-f0ca7e2abd1f",
                "name": "stateless persons",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "stigma",
                "id": "93fd2691-798e-4905-b004-65affbafde5b",
                "name": "stigma",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "storm surge",
                "id": "ac425725-fdf7-4d39-bce4-54130b026342",
                "name": "storm surge",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "students",
                "id": "2d884c0b-c1ad-4f5b-a96e-315fcf3595f5",
                "name": "students",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "survey",
                "id": "6884f580-8038-423d-8846-7faa2e4f58db",
                "name": "survey",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "sustainability",
                "id": "bb408f5d-910f-4ef6-8cc2-df040351a3ff",
                "name": "sustainability",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "sustainable development",
                "id": "c5111013-3021-4e23-9341-a6b1068880c7",
                "name": "sustainable development",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "sustainable development goals - sdg",
                "id": "872b7ada-18f5-4fa2-8a46-e3a3e430e713",
                "name": "sustainable development goals - sdg",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "syria crisis - 2011-",
                "id": "0abd0e98-768f-4fe3-9e37-3a26f1141f38",
                "name": "syria crisis - 2011-",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "taxonomy - vocabulary",
                "id": "4fc7f784-1903-4b4d-a932-3c4a883ab1f0",
                "name": "taxonomy - vocabulary",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "telecommunication",
                "id": "03689cb6-91fd-404a-8218-783387e7f441",
                "name": "telecommunication",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "temperature",
                "id": "0393f280-9b78-4ef7-8be3-345affe97cfb",
                "name": "temperature",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "threats",
                "id": "1515cafb-e704-4904-a0b1-5cb27d255bc2",
                "name": "threats",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "toilets",
                "id": "54e9f918-6d0d-4529-8883-1df9144fa83e",
                "name": "toilets",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "tornado",
                "id": "bbc6b3a5-b3de-45a4-b00a-90dfa30d4053",
                "name": "tornado",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "trails",
                "id": "e8e3d262-4553-4550-87ee-ab5cf05278ad",
                "name": "trails",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "transportation",
                "id": "275ccd84-eb88-4455-b5f1-f42ed5222082",
                "name": "transportation",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "transportation status",
                "id": "be5cefb6-8258-44e5-8298-f13551c276d5",
                "name": "transportation status",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "travel time",
                "id": "12d62252-5214-49ca-93cf-8510ed29d65f",
                "name": "travel time",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "tropical cyclone enawo - mar 2017",
                "id": "db914f15-7dfb-46bb-923b-854eb8539ed4",
                "name": "tropical cyclone enawo - mar 2017",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "tropical cyclone idai - mar 2019",
                "id": "dc1bd565-916f-4d47-92f6-25b13bf2cb15",
                "name": "tropical cyclone idai - mar 2019",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "tropical cyclone kenneth - apr 2019",
                "id": "5c566d8b-0d1d-49f3-a5ff-eb9d2c57ae90",
                "name": "tropical cyclone kenneth - apr 2019",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "tropical cyclone megh - nov 2015",
                "id": "7753dfc5-c416-4747-aa2e-e1aeade11d5f",
                "name": "tropical cyclone megh - nov 2015",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "tropical cyclone pam - mar 2015",
                "id": "da37ae2c-a682-4fa0-b3a5-6831b1883dd7",
                "name": "tropical cyclone pam - mar 2015",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "tropical cyclone winston - feb 2016",
                "id": "320671d8-2176-49e7-904d-c2155879ecc3",
                "name": "tropical cyclone winston - feb 2016",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "tropical storm harvey - aug 2011",
                "id": "d6aca2b0-e832-4f6c-9aee-beb3fd89e85d",
                "name": "tropical storm harvey - aug 2011",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "tsunami",
                "id": "6ba073a2-53a4-45bf-8a3a-97fa55e7825a",
                "name": "tsunami",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "tuberculosis",
                "id": "81139048-e7fb-47c4-b6e0-3e64bbf8cbbf",
                "name": "tuberculosis",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "typhoon hagupit - dec 2014",
                "id": "d418ea5b-d4af-49b6-a3f1-b89605a5c9cd",
                "name": "typhoon hagupit - dec 2014",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "typhoon haima - oct 2016",
                "id": "ab9650d9-3b27-4554-8848-0ba1ec637aad",
                "name": "typhoon haima - oct 2016",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "typhoon haiyan - nov 2013",
                "id": "77713f84-2810-4e99-b433-caf951bdaf19",
                "name": "typhoon haiyan - nov 2013",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "typhoon mangkhut - sep 2018",
                "id": "2665980b-e561-482d-8753-fabae338636b",
                "name": "typhoon mangkhut - sep 2018",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "unaccompanied minors",
                "id": "59b5842a-0a38-4b46-9e89-3d5dfac6a603",
                "name": "unaccompanied minors",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "under 1",
                "id": "18e18c75-1165-49a4-8a4d-36ef8b7be0b7",
                "name": "under 1",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "under 5",
                "id": "8e5c655c-b94e-4db9-97f3-9cb60fb27696",
                "name": "under 5",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "unemployment",
                "id": "549076e4-3295-4588-b61c-e3a55d7743af",
                "name": "unemployment",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "urban",
                "id": "555e026a-bbc6-4218-9ae8-6a7b319337c9",
                "name": "urban",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "urban search and rescue - usar",
                "id": "eb096b9f-fdbb-404a-9977-d1e5d1cc32c4",
                "name": "urban search and rescue - usar",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "utilities",
                "id": "93ab7063-dca2-4d6c-9583-33e1828dacf1",
                "name": "utilities",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "vaccination - immunization",
                "id": "05c3f23c-a6ba-4831-ab4d-99d6737b06f3",
                "name": "vaccination - immunization",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "vector control",
                "id": "2e4c5517-76f2-43f9-b165-2b4e7e5f5392",
                "name": "vector control",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "violence and conflict",
                "id": "1b2a6d16-3b22-4531-883a-29345713a1d8",
                "name": "violence and conflict",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "violent wind",
                "id": "30bfec75-9a60-4683-b98e-bbc5ddf691ef",
                "name": "violent wind",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "vocabulary - taxonomy",
                "id": "e8b0d2c7-0be9-475c-a809-7ffca8e856a6",
                "name": "vocabulary - taxonomy",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "volcano",
                "id": "1a8a684b-e0d1-4bba-b216-575d0ac6e1ed",
                "name": "volcano",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "vulnerable populations",
                "id": "8ff72596-174e-4a85-a3b8-79d63d53cdbd",
                "name": "vulnerable populations",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "water bodies - hydrography",
                "id": "43d84b1d-6a32-4372-a304-a68c98e9d4dc",
                "name": "water bodies - hydrography",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "water points - water sources",
                "id": "d8833126-f414-4660-ab21-00b9eaaee857",
                "name": "water points - water sources",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "water sanitation and hygiene - wash",
                "id": "0281e0e0-f9c8-4c7a-a3bf-12ac57cbbdda",
                "name": "water sanitation and hygiene - wash",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "weapons - arms",
                "id": "37e8cc25-d4bb-4074-b71f-ed1e2dc86b7e",
                "name": "weapons - arms",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "weather and climate",
                "id": "7f9db3b8-e684-43df-a775-ed705d1e5033",
                "name": "weather and climate",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "west africa",
                "id": "d5014c7c-b9fd-4442-a1cc-3b1c6983beb6",
                "name": "west africa",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "who is doing what and where - 3w - 4w - 5w",
                "id": "bb1d620c-6e09-4589-8c2f-fa28d70ff2b4",
                "name": "who is doing what and where - 3w - 4w - 5w",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "wild fire",
                "id": "10f2c577-1ac6-47ac-9b50-305389dae4a9",
                "name": "wild fire",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "wind speed",
                "id": "feb225d2-c8fe-4fbf-93d5-94054d150ca9",
                "name": "wind speed",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "women",
                "id": "f2f10fd9-1876-4bb2-a3a5-6c37be7b3e55",
                "name": "women",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "yellow fever",
                "id": "7f4ff679-074a-4195-af22-90bb33eaf87d",
                "name": "yellow fever",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "youth",
                "id": "9da7fbb7-3c5e-463c-8103-facf5850afa6",
                "name": "youth",
            },
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "zika",
                "id": "aa82ed38-1db1-4315-bd8b-28af680735df",
                "name": "zika",
            },
        ],
        "id": "4381925f-0ae9-44a3-b30d-cae35598757b",
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
        or datadict["id"] == "4381925f-0ae9-44a3-b30d-cae35598757b"
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
        resultdictcopy["tags"] = list()
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
        return join("tests", "fixtures", "config", "hdx_vocabulary_static.yml")

    @pytest.fixture(scope="class")
    def static_json(self):
        return join(
            "tests", "fixtures", "config", "hdx_vocabulary_static.json"
        )

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
                if datadict["id"] == "4381925f-0ae9-44a3-b30d-cae35598757b":
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
        vocabulary = Vocabulary.read_from_hdx(
            "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        )
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

        vocabulary = Vocabulary.read_from_hdx(
            "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        )
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
        vocabulary = Vocabulary.read_from_hdx(
            "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        )
        with pytest.raises(HDXError):
            vocabulary.delete_from_hdx(empty=False)
        vocabulary.delete_from_hdx()
        vocabulary = Vocabulary.read_from_hdx(
            "1731e7fc-ff62-4551-8a70-2a5878e1142b"
        )
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
        assert vocabulary["tags"][0]["name"] == "3-word addresses"

    def test_create_approved_vocabulary(self, configuration, post_create):
        Vocabulary._approved_vocabulary = None
        vocabulary = Vocabulary.create_approved_vocabulary()
        assert vocabulary["name"] == "Topics"
        assert vocabulary["tags"][0]["name"] == "3-word addresses"

    def test_update_approved_vocabulary(self, configuration, post_update):
        Vocabulary._approved_vocabulary = None
        vocabulary = Vocabulary.update_approved_vocabulary()
        assert vocabulary["name"] == "Topics"
        assert vocabulary["tags"][0]["name"] == "3-word addresses"
        vocabulary["tags"] = [
            {
                "vocabulary_id": "4381925f-0ae9-44a3-b30d-cae35598757b",
                "display_name": "blah",
                "id": "d611b4c4-6964-45e4-a682-e6b53cc71f18",
                "name": "blah",
            }
        ]
        vocabulary = Vocabulary.update_approved_vocabulary(replace=False)
        assert vocabulary["tags"][0]["name"] == "blah"
        assert vocabulary["tags"][1]["name"] == "3-word addresses"

    def test_tag_mappings(self, configuration, read):
        tags_dict = Vocabulary.read_tags_mappings()
        assert tags_dict["refugee"] == {
            "Action to Take": "merge",
            "New Tag(s)": "refugees",
            "Number of Public Datasets": "1822",
            "good tag + delete": None,
            "new tag + delete": None,
            "new tag + ok": None,
            "no tag + merge": None,
            "non-accepted tag + ok": None,
            "non-accepted tag + merge": None,
        }
        assert Vocabulary.get_mapped_tag("refugee") == (["refugees"], list())
        assert Vocabulary.get_mapped_tag("monitoring") == (
            list(),
            ["monitoring"],
        )
        tags_dict["refugee"]["Action to Take"] = "ERROR"
        assert Vocabulary.get_mapped_tag("refugee") == (list(), list())
        refugeesdict = copy.deepcopy(tags_dict["refugees"])
        del tags_dict["refugees"]
        assert Vocabulary.get_mapped_tag("refugees") == (
            ["refugees"],
            list(),
        )  # tag is in CKAN approved list but not tag cleanup spreadsheet
        tags_dict["refugees"] = refugeesdict
        Vocabulary.get_approved_vocabulary().remove_tag("refugees")
        assert Vocabulary.get_mapped_tag("refugees") == (
            list(),
            list(),
        )  # tag is not in CKAN approved list but is in tag cleanup spreadsheet
        Vocabulary._approved_vocabulary = None
        Vocabulary.get_approved_vocabulary()

    def test_chainrule_error(self, configuration, read):
        with pytest.raises(ChainRuleError):
            Vocabulary.set_tagsdict(None)
            url = "https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/Tag_Mapping_ChainRuleError.csv"
            Vocabulary.read_tags_mappings(url=url, failchained=True)

    def test_autocomplete(self, configuration, post_autocomplete):
        assert Vocabulary.autocomplete("health") == tag_autocomplete
