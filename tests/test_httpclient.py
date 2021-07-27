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

import pytest
import requests
import responses

from lsst.alert.database.client import DatabaseClient

alert_url_expectations = [
    # input base URL, input alertId, expected output
    ("https://alert-db.lsst.codes", "1111", "https://alert-db.lsst.codes/v1/alerts/1111"),
    ("https://alert-db.lsst.codes/", "1111", "https://alert-db.lsst.codes/v1/alerts/1111"),
    ("https://localhost/", "1111", "https://localhost/v1/alerts/1111"),
    ("localhost/", "1111", "http://localhost/v1/alerts/1111"),
    ("localhost", "1111", "http://localhost/v1/alerts/1111"),
]


@pytest.mark.parametrize("case", alert_url_expectations)
def test_alert_url_with_scheme(case):
    url, alert_id, expected = case

    client = DatabaseClient(url)
    have = client._get_alert_url(alert_id)

    assert have == expected


@pytest.fixture
def mock_responses():
    with responses.RequestsMock() as r:
        yield r


def test_get_alert(mock_responses):
    # An alert that exists:
    alert_body = b"zzz"
    mock_responses.add(
        responses.GET, "http://testdb/v1/alerts/1111",
        body=gzip.compress(alert_body), status=200,
        content_type="application/octet-stream",
    )
    # An alert that does not exist:
    mock_responses.add(
        responses.GET, "http://testdb/v1/alerts/2",
        body=b"Not Found", status=404,
    )
    client = DatabaseClient("http://testdb/")
    have = client.get_raw_alert_bytes("1111")
    assert have == alert_body

    with pytest.raises(requests.HTTPError):
        client.get_raw_alert_bytes("2")