"""Run: python examples/derived_parent_views.py (core + Python SDK installed).

Fixed language Observation → derived view → agent view, preserving complete
blocks and transitive source permissions. These are deterministic views, not
automatic L2 pages or inferred L3 personas. Revocation blocks final delivery.
"""

import asyncio

from derived_observation import main

if __name__ == "__main__":
    asyncio.run(main(host_controls=True, parents=True))
