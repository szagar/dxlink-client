"""Token providers that require extra dependencies.

Import the submodule directly, e.g.::

    from dxlink_client.providers.tastytrade import TastytradeTokenProvider

These are NOT re-exported from the top-level package so the core install stays
dependency-light (websockets only). `TastytradeTokenProvider` needs the
``dxlink-client[tastytrade]`` extra (httpx).
"""
