"""Run: python examples/derived_history.py (core + Python SDK installed).

Discover certified published knowledge-time points, then vary valid time.
This deterministic fixture shows protocol behavior, not extraction quality.
"""

import asyncio

from derived_observation import main

if __name__ == "__main__":
    asyncio.run(main(history=True))
