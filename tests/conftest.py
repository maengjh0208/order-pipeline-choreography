import pytest_asyncio
from pipeline_kafka.outbox import Base
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

DATABASE_URL = "postgresql+asyncpg://order:order@localhost:5432/order_test"


@pytest_asyncio.fixture
async def db_session():
    engine = create_async_engine(DATABASE_URL)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)  # 테스트때마다 테이블 새로 만듦

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session  # 여기서 테스트 함수로 세션 넘겨줌

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)  # 테스트 격리를 위해 지움

    await engine.dispose()
