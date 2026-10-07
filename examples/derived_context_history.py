"""Run the source-grounded qualified example with certified dual-time history."""

import asyncio

from derived_contextual_observation import main

if __name__ == "__main__":
    asyncio.run(main(history=True))
