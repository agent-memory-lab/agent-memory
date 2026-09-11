import asyncio

from agent_memory import MemoryKernel


class Repository:
    def __init__(self) -> None:
        self.initialized = False
        self.closed = False

    async def initialize(self) -> None:
        self.initialized = True

    async def close(self) -> None:
        self.closed = True


class Extractor:
    pass


class Policy:
    pass


class Reranker:
    pass


def test_kernel_delegates_optional_repository_lifecycle() -> None:
    async def scenario() -> None:
        repository = Repository()
        kernel = MemoryKernel(repository, Extractor(), Policy(), Reranker())
        await kernel.initialize()
        await kernel.close()
        assert repository.initialized is True
        assert repository.closed is True

    asyncio.run(scenario())
