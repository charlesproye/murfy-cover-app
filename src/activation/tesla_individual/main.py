import asyncio

from sqlalchemy.orm import sessionmaker

from activation.tesla_individual.tesla_individual_activator import (
    TeslaIndividualActivator,
)
from core.sql_utils import get_sqlalchemy_engine


async def main():
    engine = get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    db = Session()

    activator = TeslaIndividualActivator()
    await activator.run(db)


if __name__ == "__main__":
    asyncio.run(main())
