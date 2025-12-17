import logging

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from core.sql_utils import get_sqlalchemy_engine
from db_models import Fleet, Role, User, UserFleet


def ensure_admins_linked_to_fleets(logger: logging.Logger):
    engine = get_sqlalchemy_engine(db_name="rdb")
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        all_fleets = session.execute(select(Fleet)).scalars().all()

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
                "No admin users exist in the database. Cannot assign admins to fleets."
            )
            return

        if not all_fleets:
            logger.info("No fleets exist in the database.")
            return

        # Get existing admin-fleet links
        existing_links = (
            session.execute(
                select(UserFleet)
                .join(Role, UserFleet.role_id == Role.id)
                .where(Role.role_name == "Admin")
            )
            .scalars()
            .all()
        )

        existing_pairs = {(link.user_id, link.fleet_id) for link in existing_links}

        # Find missing admin-fleet combinations
        to_insert = []
        for fleet in all_fleets:
            for admin in admins:
                if (admin.id, fleet.id) not in existing_pairs:
                    to_insert.append(
                        UserFleet(
                            user_id=admin.id, fleet_id=fleet.id, role_id=admin.role_id
                        )
                    )

        if not to_insert:
            logger.info("All admins are already linked to all fleets.")
            return

        session.add_all(to_insert)
        session.commit()
        logger.info(
            f"Inserted {len(to_insert)} user_fleet records to ensure all {len(admins)} admins have access to all {len(all_fleets)} fleets."
        )


if __name__ == "__main__":
    ensure_admins_linked_to_fleets(logging.getLogger(__name__))
