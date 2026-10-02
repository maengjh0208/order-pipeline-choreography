import pytest_asyncio
from pipeline_kafka.outbox import Base
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

DATABASE_URL = "postgresql+asyncpg://order:order@localhost:5432/order_test"


@pytest_asyncio.fixture
async def session_factory():
    engine = create_async_engine(DATABASE_URL)

    async with engine.begin() as conn:
        # 테스트때마다 테이블 새로 만듦
        await conn.run_sync(Base.metadata.create_all)

    # AsyncSession 객체를 만들어주는데, AsyncSession 클래스는 __aenter__/__aexit__ 를 구현하고 있어서, session_factory()를 컨텍스트 매니저로 쓸 수 있음.
    yield async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        # 테스트 격리를 위해 지움
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()
