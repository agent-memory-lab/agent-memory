"""Run: python examples/derived_controls.py (core + Python SDK installed).

Reuses the retained source fixture with an explicit versioned query and an
expiring local host authority. Synthetic example, not extraction quality evidence.
"""

import asyncio

from derived_observation import main

if __name__ == "__main__":
    asyncio.run(main(host_controls=True))
