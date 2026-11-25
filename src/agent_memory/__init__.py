"""Evidence-backed memory with lazy, backwards-compatible public exports.

Use the owning subsystem for new advanced APIs. Importing this facade starts no
work and loads neither storage adapters nor evaluation code until requested.
"""

import sys as _sys
from importlib import import_module as _import_module

from ._compat import LEGACY_MODULES as _LEGACY_MODULES
from ._compat import install_legacy_aliases as _install_aliases
from ._exports import EXPORTS as _EXPORTS

__all__ = list(_EXPORTS)
# Attribute access to e.g. agent_memory.domain also stays lazy.
_MODULES = {owner.lstrip(".").split(".", 1)[0] for owner in _EXPORTS.values()}
_MODULES.update(target.split(".", 1)[0] for target in _LEGACY_MODULES.values())
_install_aliases(_sys.modules[__name__])


def __getattr__(name):
    if name in _EXPORTS:
        value = getattr(_import_module(_EXPORTS[name], __name__), name)
    elif name in _LEGACY_MODULES or name in _MODULES:
        value = _import_module("." + name, __name__)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(_EXPORTS) | set(_LEGACY_MODULES) | _MODULES)
