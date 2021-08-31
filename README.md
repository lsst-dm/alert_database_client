# alert_database_client


This package provides a Python client to access the Rubin alert database, a
long-term archive of all alert packets that have been published.

The alert database is designed for low-volume usage, retrieving single alerts at
a time. Clients request historical alert packets by ID -- that is, by the
`alertId` field of the alert packet.

The client also provides access to the historical schema documents that were used to encode each alert.

## See also

[`DMTN-183`](https://dmtn-183.lsst.io/) describes the alert database system.

[`alert_database_server`](https://github.com/lsst-dm/alert_database_server) is
the corresponding server implementation.

[`alert_database_ingester`](https://github.com/lsst-dm/alert_database_ingester)
is the system that copies data from the alert stream into the archive.
