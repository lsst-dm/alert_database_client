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

# import fastavro
import requests
import urllib
import gzip


class DatabaseClient:
    def __init__(self, url: str):
        parsed_url = urllib.parse.urlparse(url)
        if not parsed_url.scheme:
            url = "http://" + url
        self.url = url

    def _get_alert_url(self, alert_id: str) -> str:
        return urllib.parse.urljoin(self.url, f"/v1/alerts/{alert_id}")

    def get_raw_alert_bytes(self, alert_id: str) -> bytes:
        url = self._get_alert_url(alert_id)
        response = requests.get(url)
        response.raise_for_status()
        decompressed = gzip.decompress(response.content)
        return decompressed
