from core.sql_utils import get_connection


def get_unusable_link(source):
    with (
        get_connection(db_name="data-engineering") as conn_data_engineering,
        conn_data_engineering.cursor() as cursor,
    ):
        cursor.execute(
            """
            SELECT link FROM fct_scraped_unusable_links
            WHERE source = %(source)s
            """,
            {"source": source},
        )
        return set(link for (link,) in cursor.fetchall())


def insert_unusable_link(link, source):
    with (
        get_connection(db_name="data-engineering") as conn_data_engineering,
        conn_data_engineering.cursor() as cursor,
    ):
        cursor.execute(
            """
            INSERT INTO fct_scraped_unusable_links (link, source) VALUES (%(link)s, %(source)s)
            """,
            {"link": link, "source": source},
        )
        conn_data_engineering.commit()
