# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional
from unittest import TestCase

import freezegun
from airbyte_cdk.sources.source import TState
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_protocol.models import ConfiguredAirbyteCatalog, SyncMode
from source_iusta import SourceIusta

_A_CONFIG = {
    "Authorization": "Authorization"
}
_NOW = datetime.now(timezone.utc)

@freezegun.freeze_time(_NOW.isoformat())
class FullRefreshTest(TestCase):

    @HttpMocker()
    def test_read_a_single_page(self, http_mocker: HttpMocker) -> None:

        http_mocker.get(
            HttpRequest(url="https://api.chevalier.iusta.io/api/Cases"),
            HttpResponse(body="""
            [
    
    {
        "status": 0,
        "number": "3/21",
        "name": "[Test] Mandant ID#3 VerR",
        "private": false,
        "closedAt": "2024-07-15T15:59:21.000Z",
        "invoicingContactId": null,
        "invoicingContactId2": null,
        "invoicingContactId3": null,
        "invoiceUuids": null,
        "comment": "Last automatic cleanup check by WFD#247 performed at 12.08.2024",
        "id": 3,
        "customerId": 3,
        "assignedUserId": 15,
        "closedBy": 22,
        "createdAt": "2022-04-01T00:00:00.000Z",
        "updatedAt": "2024-08-12T21:51:06.000Z",
        "createdBy": 6,
        "updatedBy": 22,
        "permissionSetId": 1,
        "caseGroupId": 2
    },
    {
        "status": 3,
        "number": "20043",
        "name": "20043 - Nadja Meiswinkel",
        "private": false,
        "closedAt": "2023-10-12T12:45:50.000Z",
        "invoicingContactId": null,
        "invoicingContactId2": null,
        "invoicingContactId3": null,
        "invoiceUuids": null,
        "comment": null,
        "id": 10007,
        "customerId": 13656601,
        "assignedUserId": 11,
        "closedBy": 242,
        "createdAt": "1970-01-01T00:00:45.000Z",
        "updatedAt": "2024-06-18T11:36:52.000Z",
        "createdBy": 126,
        "updatedBy": 255,
        "permissionSetId": 1,
        "caseGroupId": 1
    }
]
""", status_code=200)
        )

        output = self._read(_A_CONFIG, _configured_catalog("cases", SyncMode.full_refresh))

        assert len(output.records) == 2

    def _read(self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, expecting_exception: bool = False) -> EntrypointOutput:
        return _read(config, configured_catalog=configured_catalog, expecting_exception=expecting_exception)

def _read(
    config: Mapping[str, Any],
    configured_catalog: ConfiguredAirbyteCatalog,
    state: Optional[Dict[str, Any]] = None,
    expecting_exception: bool = False
) -> EntrypointOutput:
    return read(_source(configured_catalog, config, state), config, configured_catalog, state, expecting_exception)


def _configured_catalog(stream_name: str, sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().with_stream(stream_name, sync_mode).build()


def _source(catalog: ConfiguredAirbyteCatalog, config: Dict[str, Any], state: Optional[TState]) -> SourceIusta:
    return SourceIusta()
