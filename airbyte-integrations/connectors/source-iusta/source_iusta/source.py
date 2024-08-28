#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator

from pathlib import Path
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomAuthenticator(HttpAuthenticator):
    def __init__(self, token):
        self._token = token

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"Authorization": self._token}

# Basic full refresh stream
class IustaStream(HttpStream, ABC):

    url_base = "https://api.chevalier.iusta.io/api/"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        for item in response_json:
            yield item

# class Cases(IustaStream):
#     # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
#     primary_key = "id"

#     def get_json_schema(self):
#         # You can find the schema in source_iusta/schemas/cases.json
#         schema_path = Path(__file__).parent / "schemas/cases.json"
#         schema = json.loads(schema_path.read_text())
#         # Add dynamic properties to the schema
#         schema['properties']['dynamically_determined_property'] = {"type": "string"}
#         return schema     

#     def path(
#         self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
#     ) -> str:
#         """
#         TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
#         should return "customers". Required.
#         """
#         return "cases"

#     def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
#         """
#         :param response: the most recent response from the API
#         :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
#                 If there are no more pages in the result, return None.
#         """
#         data = response.json()
#         if len(data) < 1000:
#             return None
#         # return {"skip": data[-1]["id"]}
#         request_body = json.loads(response.request.body.decode('utf-8'))
#         request_number = request_body.get("filter", {}).get("skip", 0) // 10000
#         return {"request_number": request_number + 1}
        
#     def request_body_json(
#         self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
#     ) -> Optional[Mapping]:
#         step_size = 10000
#         request_number = next_page_token.get("request_number", 0) if next_page_token else 0
#         print(f"Requesting page with skip: {step_size * request_number}")
#         return {
#             "filter": {
#                 "limit": step_size,
#                 "skip": step_size * request_number
#             }
#         }


class Xusers(IustaStream):
    
    primary_key = "id"

    def get_json_schema(self):
        schema_path = Path(__file__).parent / "schemas/xusers.json"
        schema = json.loads(schema_path.read_text())
        # Add dynamic properties to the schema
        schema['properties']['dynamically_determined_property'] = {"type": "string"}
        return schema

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "xusers"
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data = response.json()
        if len(data) < 10000:
            return None
        request_body = json.loads(response.request.body.decode('utf-8'))
        request_number = request_body.get("filter", {}).get("skip", 0) // 10000
        return {"request_number": request_number + 1}
               
    def request_body_json(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Optional[Mapping]:
        step_size = 10000
        request_number = next_page_token.get("request_number", 0) if next_page_token else 0
        print(f"Requesting page with skip: {step_size * request_number}")
        return {
            "filter": {
                "limit": step_size,
                "skip": step_size * request_number
            }
        }


# Basic incremental stream
class IncrementalIustaStream(IustaStream, ABC):
    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = 1000

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.
        :return str: The name of the cursor field.
        """
        return "updatedAt"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        current_state = current_stream_state or {}
        print(f"Initial current_state: {current_state}")
        current_cursor = current_state.get(self.cursor_field)
        print(f"current_cursor: {current_cursor}")
        latest_cursor = latest_record[self.cursor_field]
        print(f"latest_cursor: {latest_cursor}")
        new_state = {self.cursor_field: max(current_cursor, latest_cursor) if current_cursor else latest_cursor}
        logger.info(f"Updated state: {new_state}")
        print(f"Updated state: {new_state}")
        return new_state


class Cases(IncrementalIustaStream):
    primary_key  = "id"
    cursor_field = "updatedAt"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._state = {}

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value
    
    def get_json_schema(self):
        schema_path = Path(__file__).parent / "schemas/cases.json"
        schema = json.loads(schema_path.read_text())
        schema['properties']['dynamically_determined_property'] = {"type": "string"}
        return schema
    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "cases"
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data = response.json()
        if len(data) < 10000:
            return None
        request_body = json.loads(response.request.body.decode('utf-8'))
        request_number = request_body.get("filter", {}).get("skip", 0) // 10000
        return {"request_number": request_number + 1}
    def request_body_json(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Mapping]:
        step_size = 10000
        request_number = next_page_token.get("request_number", 0) if next_page_token else 0
        cursor_value = stream_state.get(self.cursor_field) if stream_state else None
        logger.info(f"Using cursor value: {cursor_value}")
        print(f"Using cursor value: {cursor_value}")
        filter_conditions = {
            "limit": step_size,
            "skip": step_size * request_number,
            "order": "updatedAt desc"
        }
        
        if cursor_value:
            filter_conditions["where"] = {"updatedAt": {"$gte": cursor_value}}
        
        return {
            "filter": filter_conditions
        }
    
    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(*args, **kwargs)
        latest_cursor = None
        for record in records:
            latest_cursor = record[self.cursor_field]
            yield record
        if latest_cursor:
            self.state = self.get_updated_state(self.state, {self.cursor_field: latest_cursor})


class Datasets(IncrementalIustaStream):
    
    primary_key  = "id"
    cursor_field = "updatedAt"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._state = {}

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    def get_json_schema(self):
        schema_path = Path(__file__).parent / "schemas/datasets.json"
        schema = json.loads(schema_path.read_text())
        # Add dynamic properties to the schema
        schema['properties']['dynamically_determined_property'] = {"type": "string"}
        return schema

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "datasets"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data = response.json()
        if len(data) < 10000:
            return None
        request_body = json.loads(response.request.body.decode('utf-8'))
        request_number = request_body.get("filter", {}).get("skip", 0) // 10000
        return {"request_number": request_number + 1}

    def request_body_json(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Mapping]:
        step_size = 10000
        request_number = next_page_token.get("request_number", 0) if next_page_token else 0
        cursor_value = stream_state.get(self.cursor_field) if stream_state else None
        logger.info(f"Using cursor value: {cursor_value}")
        print(f"Using cursor value: {cursor_value}")
        filter_conditions = {
            "limit": step_size,
            "skip": step_size * request_number,
            "order": "updatedAt desc"
        }
        if cursor_value:
            filter_conditions["where"] = {"updatedAt": {"$gte": cursor_value}}
        return {
            "filter": filter_conditions
        }
    
    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(*args, **kwargs)
        latest_cursor = None
        for record in records:
            latest_cursor = record[self.cursor_field]
            yield record
        if latest_cursor:
            self.state = self.get_updated_state(self.state, {self.cursor_field: latest_cursor})


# Source
class SourceIusta(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        api_key = config["Authorization"]
        logger.info(f"Using API key: {api_key}")

        # Prepare the headers for the request
        headers = {
            "User-Agent": "python-requests",
            "Accept-Encoding": "gzip, deflate",
            "Accept": "*/*",
            "Connection": "keep-alive",
            "Authorization": api_key,
            "Content-Type": "application/json"
        }

        try:
            response = requests.get("https://api.chevalier.iusta.io/api/cases", headers=headers)
            logger.info(f"Response status code: {response.status_code}")
            logger.info(f"Response text: {response.text}")

            # Check if the request was successful
            if response.status_code == 200:
                return True, None
            else:
                return False, f"Failed to authenticate: {response.status_code} {response.text}"
        except Exception as e:
            return False, str(e)
            

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = CustomAuthenticator(token=config["Authorization"])
        return [
            Cases(authenticator=auth),
            Datasets(authenticator=auth),
            Xusers(authenticator=auth)
        ]
