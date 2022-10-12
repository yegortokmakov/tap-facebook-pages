"""Stream class for tap-facebook-pages."""
import re
import sys

import pendulum
import backoff
import copy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Callable
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.streams import RESTStream
import urllib.parse
import requests
import logging

logger = logging.getLogger("tap-facebook-pages")
logger_handler = logging.StreamHandler(stream=sys.stderr)
logger.addHandler(logger_handler)
logger.setLevel("INFO")
logger_handler.setFormatter(logging.Formatter('%(levelname)s %(message)s'))

NEXT_FACEBOOK_PAGE = "NEXT_FACEBOOK_PAGE"

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

BASE_URL = "https://graph.facebook.com/v15.0/{page_id}"


class FacebookPagesStream(RESTStream):
    access_tokens = {}
    metrics = []
    # partitions = []
    page_id: str

    def prepare_request(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> requests.PreparedRequest:
        req = super().prepare_request(partition, next_page_token)
        self.logger.info(re.sub("access_token=[a-zA-Z0-9]+&", "access_token=*****&", urllib.parse.unquote(req.url)))
        return req

    @property
    def url_base(self) -> str:
        return BASE_URL

    @property
    def partitions(self) -> Optional[List[dict]]:
        return [{"page_id": x} for x in self.config["page_ids"]]

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures.
        Developers may override this method to provide custom backoff or retry
        handling.
        Args:
            func: Function to decorate.
        Returns:
            A decorated method.
        """
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                FatalAPIError,
                RetriableAPIError,
                requests.exceptions.RequestException,
            ),
            max_tries=5,
            max_time=600,
            # base=30,
            # factor=5,
        )(func)
        return decorator

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.
        If pagination is detected, pages will be recursed automatically.
        Args:
            context: Stream partition or context dictionary.
        Yields:
            An item for every record in the response.
        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        self.page_id = partition["page_id"]
        if next_page_token:
            return urllib.parse.parse_qs(urllib.parse.urlparse(next_page_token).query)

        params = {
            # "access_token": self.config["access_token"],
            "access_token": self.access_tokens[self.page_id],
            "limit": 100,
        }

        starting_datetime = self.get_starting_timestamp(partition)
        if starting_datetime:
            start_date_timestamp = int(starting_datetime.timestamp())
            params.update({"since": start_date_timestamp})

        return params

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any] = None) -> Any:
        resp_json = response.json()
        if "paging" in resp_json and "next" in resp_json["paging"]:
            return resp_json["paging"]["next"]
        return None

    def post_process(self, row: dict, stream_or_partition_state: dict) -> dict:
        if "context" in stream_or_partition_state and "page_id" in stream_or_partition_state["context"]:
            row["page_id"] = stream_or_partition_state["context"]["page_id"]
        return row

    def validate_response(self, response: requests.Response) -> None:
        # occasionally, FB returns a 400 claiming that we aren't using a page API token
        # don't know what's causing this but it appears transient and to resolve w/ a retry
        if response.status_code == 400:
            if response.json().get("error", {}).get("message") == "(#190) This method must be called with a Page " \
                                                                  "Access Token":
                raise RetriableAPIError(response.json())

        if 400 <= response.status_code <= 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}: "
                f"{response.json().get('error', {}).get('message')}"
            )
            raise RetriableAPIError(msg)
        super().validate_response(response)


class Page(FacebookPagesStream):
    name = "page"
    tap_stream_id = "page"
    path = ""
    primary_keys = ["id"]
    replication_key = None
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "page.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = super().get_url_params(partition, next_page_token)
        fields = ','.join(self.config['columns']) if 'columns' in self.config else ','.join(
            self.schema["properties"].keys())
        params.update({"fields": fields})
        return params

    def post_process(self, row: dict, stream_or_partition_state: dict) -> dict:
        return row


class Posts(FacebookPagesStream):
    name = "posts"
    tap_stream_id = "posts"
    path = "/posts"
    primary_keys = ["id"]
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "posts.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:

        params = super().get_url_params(partition, next_page_token)

        # some page requests will throw a 500 error with the msg:
        #   "Please reduce the amount of data you're asking for, then retry your request"
        # this reduces the limit of records to avoid that
        params['limit'] = 30

        if next_page_token:
            return params

        fields = ','.join(self.config['columns']) if 'columns' in self.config else ','.join(
            self.schema["properties"].keys())
        params.update({"fields": fields})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            row["page_id"] = self.page_id
            yield row


class PostTaggedProfile(FacebookPagesStream):
    name = "post_tagged_profile"
    tap_stream_id = "post_tagged_profile"
    path = "/posts"
    primary_keys = ["id"]
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "post_tagged_profile.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = super().get_url_params(partition, next_page_token)
        if next_page_token:
            return params

        params.update({"fields": "id,created_time,to"})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            parent_info = {
                "page_id": self.page_id,
                "post_id": row["id"],
                "post_created_time": row["created_time"]
            }
            if "to" in row:
                for attachment in row["to"]["data"]:
                    attachment.update(parent_info)
                    yield attachment


class PostAttachments(FacebookPagesStream):
    name = "post_attachments"
    tap_stream_id = "post_attachments"
    path = "/posts"
    primary_keys = ["id"]
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "post_attachments.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = super().get_url_params(partition, next_page_token)
        if next_page_token:
            return params

        params.update({"fields": "id,created_time,attachments"})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            parent_info = {
                "page_id": self.page_id,
                "post_id": row["id"],
                "post_created_time": row["created_time"]
            }
            if "attachments" in row:
                for attachment in row["attachments"]["data"]:
                    if "subattachments" in attachment:
                        for sub_attachment in attachment["subattachments"]["data"]:
                            sub_attachment.update(parent_info)
                            yield sub_attachment
                        attachment.pop("subattachments")
                    attachment.update(parent_info)
                    yield attachment


class PageInsights(FacebookPagesStream):
    name = None
    tap_stream_id = None
    path = "/insights"
    primary_keys = ["id"]
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "page_insights.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = super().get_url_params(partition, next_page_token)
        params.update({"metric": ",".join(self.metrics)})
        return params

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any] = None) -> Any:
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            base_item = {
                "name": row["name"],
                "period": row["period"],
                "title": row["title"],
                "id": row["id"],
            }
            if "values" in row:
                for values in row["values"]:
                    if isinstance(values["value"], dict):
                        for key, value in values["value"].items():
                            if isinstance(value, dict):
                                for k, v in value.items():
                                    item = {
                                        "context": f"{key} > {k}",
                                        "value": float(v),
                                        "end_time": values["end_time"]
                                    }
                                    item.update(base_item)
                                    yield item
                            else:
                                item = {
                                    "context": key,
                                    "value": float(value),
                                    "end_time": values["end_time"]
                                }
                                item.update(base_item)
                                yield item
                    else:
                        values.update(base_item)
                        yield values


class PostInsights(FacebookPagesStream):
    name = ""
    tap_stream_id = ""
    # use published_posts instead of feed, as the last one is problematic endpoint
    # path = "/feed"
    path = "/published_posts"
    primary_keys = ["id"]
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "post_insights.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = super().get_url_params(partition, next_page_token)

        # some page requests will throw a 500 error with the msg:
        #   "Please reduce the amount of data you're asking for, then retry your request"
        # this reduces the limit of records to avoid that
        params['limit'] = 10

        if next_page_token:
            return params

        params.update({"fields": "id,created_time,insights.metric(" + ",".join(self.metrics) + ")"})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            for insights in row["insights"]["data"]:
                base_item = {
                    "post_id": row["id"],
                    "page_id": self.page_id,
                    "post_created_time": row["created_time"],
                    "name": insights["name"],
                    "period": insights["period"],
                    "title": insights["title"],
                    "description": insights["description"],
                    "id": insights["id"],
                }
                if "values" in insights:
                    for values in insights["values"]:
                        if isinstance(values["value"], dict):
                            for key, value in values["value"].items():
                                item = {
                                    "context": key,
                                    "value": value,
                                }
                                item.update(base_item)
                                yield item
                        else:
                            values.update(base_item)
                            yield values
