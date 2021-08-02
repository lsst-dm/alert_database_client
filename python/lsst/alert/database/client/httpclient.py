# This file is part of alert_database_client.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import io
import json
import gzip
import struct
import urllib

import fastavro
import requests


class DatabaseClient:
    def __init__(self, url: str):
        parsed_url = urllib.parse.urlparse(url)
        if not parsed_url.scheme:
            url = "http://" + url
        self.url = url

        self._schema_cache = {}

    def _get_alert_url(self, alert_id: int) -> str:
        return urllib.parse.urljoin(self.url, f"/v1/alerts/{alert_id}")

    def get_raw_alert_bytes(self, alert_id: int) -> bytes:
        url = self._get_alert_url(alert_id)
        response = requests.get(url)
        response.raise_for_status()
        decompressed = gzip.decompress(response.content)
        return decompressed

    def _get_schema_url(self, schema_id: int) -> str:
        return urllib.parse.urljoin(self.url, f"/v1/schemas/{schema_id}")

    def get_schema(self, schema_id: int) -> bytes:
        url = self._get_schema_url(schema_id)
        response = requests.get(url)
        response.raise_for_status()
        return response.content

    def get_alert(self, alert_id: int) -> dict:
        raw_bytes = self.get_raw_alert_bytes(alert_id)
        if len(raw_bytes) < 5:
            raise ValueError("corrupted alert data is not in confluent wire format")
        schema_id = self._parse_alert_header(raw_bytes)
        alert_payload = raw_bytes[5:]
        schema = self._get_parsed_schema(schema_id)
        return fastavro.schemaless_reader(io.BytesIO(alert_payload), schema)

    def _get_parsed_schema(self, schema_id: int) -> dict:
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]
        schema_bytes = self.get_schema(schema_id)
        schema = fastavro.parse_schema(json.loads(schema_bytes))
        self._schema_cache[schema_id] = schema
        return schema

    @staticmethod
    def _parse_alert_header(alert_raw_bytes: bytes) -> int:
        if len(alert_raw_bytes) < 5:
            raise ValueError("alert header too short - should be at least 5 bytes")
        magic_byte = alert_raw_bytes[0]
        if magic_byte != 0:
            raise ValueError(
                "alert header has incorrect magic byte, might be corrupted"
            )
        schema_id = struct.unpack(">I", alert_raw_bytes[1:5])[0]
        return schema_id
