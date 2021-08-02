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
import gzip
import io
import json

import fastavro
import pytest
import requests
import responses

from lsst.alert.database.client import DatabaseClient

alert_url_expectations = [
    # input base URL, input alertId, expected output
    ("https://alert-db.lsst.codes", 1111, "https://alert-db.lsst.codes/v1/alerts/1111"),
    ("https://alert-db.lsst.codes/", 1111, "https://alert-db.lsst.codes/v1/alerts/1111"),
    ("https://localhost/", 1111, "https://localhost/v1/alerts/1111"),
    ("localhost/", 1111, "http://localhost/v1/alerts/1111"),
    ("localhost", 1111, "http://localhost/v1/alerts/1111"),
]


@pytest.mark.parametrize("case", alert_url_expectations)
def test_get_alert_url(case):
    url, alert_id, expected = case

    client = DatabaseClient(url)
    have = client._get_alert_url(alert_id)

    assert have == expected


schema_url_expectations = [
    # input base URL, input schema ID, expected output
    ("https://alert-db.lsst.codes", 1111, "https://alert-db.lsst.codes/v1/schemas/1111"),
    ("https://alert-db.lsst.codes/", 1111, "https://alert-db.lsst.codes/v1/schemas/1111"),
    ("https://localhost/", 1111, "https://localhost/v1/schemas/1111"),
    ("localhost/", 1111, "http://localhost/v1/schemas/1111"),
    ("localhost", 1111, "http://localhost/v1/schemas/1111"),
]


@pytest.mark.parametrize("case", schema_url_expectations)
def test_get_schema_url(case):
    url, schema_id, expected = case
    client = DatabaseClient(url)
    have = client._get_schema_url(schema_id)
    assert have == expected


@pytest.fixture
def mock_responses():
    with responses.RequestsMock() as r:
        yield r


def test_get_raw_alert_bytes(mock_responses):
    alert_body = b"zzz"
    mock_responses.add(
        responses.GET, "http://testdb/v1/alerts/1111",
        body=gzip.compress(alert_body), status=200,
        content_type="application/octet-stream",
    )
    client = DatabaseClient("http://testdb/")
    have = client.get_raw_alert_bytes(1111)
    assert have == alert_body


def test_get_raw_alert_bytes_404(mock_responses):
    # An alert that does not exist:
    mock_responses.add(
        responses.GET, "http://testdb/v1/alerts/2",
        body=b"Not Found", status=404,
    )
    client = DatabaseClient("http://testdb/")
    with pytest.raises(requests.HTTPError):
        client.get_raw_alert_bytes(2)


def test_get_schema(mock_responses):
    schema_body = b'{"type": "string"}'
    mock_responses.add(
        responses.GET,
        "http://testdb/v1/schemas/1111",
        body=schema_body,
        status=200,
        content_type="application/vnd.schemaregistry.v1+json",
    )
    client = DatabaseClient("http://testdb")
    have = client.get_schema(1111)
    assert have == schema_body


def test_get_schema_404(mock_responses):
    mock_responses.add(
        responses.GET, "http://testdb/v1/schemas/2",
        body=b"Not Found", status=404,
    )
    client = DatabaseClient("http://testdb")
    with pytest.raises(requests.HTTPError):
        client.get_schema(2)


def test_get_alert(mock_responses):
    schema = {
        "type": "record",
        "name": "test-alert",
        "fields": [
            {"name": "alertId", "type": "long"},
        ],
    }
    encoded_schema = json.dumps(schema)

    alert = {"alertId": 81023}
    encoded_alert_buffer = io.BytesIO()
    encoded_alert_buffer.write(bytearray([0, 0, 0, 0, 1]))
    fastavro.schemaless_writer(encoded_alert_buffer, schema, alert)
    encoded_alert = gzip.compress(encoded_alert_buffer.getvalue())

    mock_responses.add(
        responses.GET,
        "http://testdb/v1/schemas/1",
        body=encoded_schema,
        status=200,
        content_type="application/vnd.schemaregistry.v1+json",
    )

    mock_responses.add(
        responses.GET,
        "http://testdb/v1/alerts/81023",
        body=encoded_alert,
        status=200,
        content_type="application/octet-stream"
    )

    client = DatabaseClient("http://testdb")
    have = client.get_alert(81023)
    assert have == alert

    # Get another alert using the same schema ID. The shcema URl should not get
    # called again, since the client should cache the response.
    assert mock_responses.assert_call_count("http://testdb/v1/schemas/1", 1) is True

    alert2 = {"alertId": 81024}
    encoded_alert_buffer2 = io.BytesIO()
    encoded_alert_buffer2.write(bytearray([0, 0, 0, 0, 1]))
    fastavro.schemaless_writer(encoded_alert_buffer2, schema, alert2)
    encoded_alert2 = gzip.compress(encoded_alert_buffer2.getvalue())

    mock_responses.add(
        responses.GET,
        "http://testdb/v1/alerts/81024",
        body=encoded_alert2,
        status=200,
        content_type="application/octet-stream"
    )

    have = client.get_alert(81024)
    assert have == alert2
    assert mock_responses.assert_call_count("http://testdb/v1/schemas/1", 1) is True


def test_parse_alert_header():
    client = DatabaseClient("http://testdb")
    header = b'\x00\x00\x00\x00\x00'
    parsed_id = client._parse_alert_header(header)
    assert parsed_id == 0

    header = b'\x00\x00\x00\x00\x09'
    parsed_id = client._parse_alert_header(header)
    assert parsed_id == 9

    with pytest.raises(ValueError):
        # Empty string
        header = b""
        client._parse_alert_header(header)

    with pytest.raises(ValueError):
        # Too short
        header = b"\x00\x00\x00"
        client._parse_alert_header(header)

    with pytest.raises(ValueError):
        # Wrong magic byte
        header = b"\x01\x00\x00\x00\x00"
        client._parse_alert_header(header)
