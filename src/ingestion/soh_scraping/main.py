import argparse
import logging
import sys

from ingestion.soh_scraping.websites.scrapping_aramis import main as scrapping_aramis
from ingestion.soh_scraping.websites.scrapping_autospherre import (
    main as scrapping_autospherre,
)
from ingestion.soh_scraping.websites.scrapping_ev_market import (
    main as scrapping_ev_market,
)
from ingestion.soh_scraping.websites.scrapping_spoticar import (
    main as scrapping_spoticar,
)


def main(args: argparse.Namespace, logger: logging.Logger):
    if "aramis" in args.scrapers:
        scrapping_aramis()
    if "autospherre" in args.scrapers:
        scrapping_autospherre()
    if "ev_market" in args.scrapers:
        scrapping_ev_market()
    if "spoticar" in args.scrapers:
        scrapping_spoticar()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
    parser = argparse.ArgumentParser(
        description="Script de compression avec un argument OEM requis."
    )
    parser.add_argument("scrapers", nargs="+", help="Liste de scrapers à démarrer")

    args = parser.parse_args()

    logger = logging.getLogger("ScraperLogger")

    main(args, logger)
