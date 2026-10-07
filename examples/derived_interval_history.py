"""Run: python examples/derived_interval_history.py (core + Python SDK installed).

Query inside the certified unchanged interval following a full publication.
Synthetic protocol example; history before migration is never inferred.
"""

import asyncio

from derived_observation import main

if __name__ == "__main__":
    asyncio.run(main(continuous=True))
