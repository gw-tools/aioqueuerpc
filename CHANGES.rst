Changes
=======

0.4.1
-----

Date: 2024-05-03

- Added async RpcPeer.run() method that processes messages and runs the default worker loop.

0.4.0
-----

Date: 2024-04-20

- Removed the need to define schemas when registering methods.
- Removed the need to define schemas when registering consumers and producers.
- Removed deprecated methods.

0.3.0
-----

Date: 2024-04-01

- Removed the need to register methods by the caller.

0.2.1
-----

Date: 2024-01-08

- Removed `pytz` dependency (replaced with `datetime.timezone`).

0.2.0
-----

Date: 2023-11-06

- Added CHANGES.rst
- Added optional parameters to the RpcPeer constructor to allow specifying incoming and outgoing `asyncio.Queue` instances at creation.

0.1.0 - 0.1.1
--------------

Date: 2023-08-22

- Initial release.

