import logging

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from core.sql_utils import get_sqlalchemy_engine
from db_models.vehicle import Fleet, Role, User, UserFleet


def ensure_admins_linked_to_fleets(logger: logging.Logger):
    engine = get_sqlalchemy_engine(db_name="rdb")
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        subquery = (
            select(UserFleet.fleet_id)
            .join(Role, UserFleet.role_id == Role.id)
            .where(Role.role_name == "Admin")
        )

        fleets_without_admin = (
            session.execute(select(Fleet).where(Fleet.id.not_in(subquery)))
            .scalars()
            .all()
        )

        logger.info(f"Fleets without admin: {fleets_without_admin}")

        if not fleets_without_admin:
            logger.info("All fleets already have an admin.")
            return

        admins = (
            session.execute(
                select(User)
                .join(Role, User.role_id == Role.id)
                .where(Role.role_name == "Admin")
            )
            .scalars()
            .all()
        )

        if not admins:
            logger.warning(
                f"Found {len(fleets_without_admin)} fleets without admins, but no admin users exist in the database. "
                "Cannot assign admins to fleets."
            )
            return

        to_insert = []
        for fleet in fleets_without_admin:
            admin = admins[0]
            to_insert.append(
                UserFleet(user_id=admin.id, fleet_id=fleet.id, role_id=admin.role_id)
            )

        session.add_all(to_insert)
        session.commit()
        logger.info(f"Inserted {len(to_insert)} user_fleet records.")


if __name__ == "__main__":
    ensure_admins_linked_to_fleets(logging.getLogger(__name__))
