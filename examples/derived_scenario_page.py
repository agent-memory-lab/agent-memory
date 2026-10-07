"""Versioned L2 full rebuild and current readiness; core + SDK required."""

import asyncio

from derived_observation import main

if __name__ == "__main__":
    asyncio.run(main(host_controls=True, pages=True))
