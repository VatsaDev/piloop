"""The HTTP API of ``piloop serve``: ``/v1`` on ``$PILOOP_HOME/piloop.sock``, and on the network's address too.

``contract`` is the table of endpoints that the server (``piloop.net.server``) mounts and the client (``client``)
calls, ``views`` the documents they answer. Neither opens the database, so a client imports them cheaply.
"""
