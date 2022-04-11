import os

from bs4 import BeautifulSoup
from sqlalchemy.orm.session import Session

from aws_allowlister.database.raw_scraping_data import RawScrapingData
from aws_allowlister.scrapers.aws_docs import get_aws_html
from aws_allowlister.shared.utils import chomp_keep_single_spaces
from aws_allowlister.scrapers.common import get_table_ids, get_service_name, clean_status_cell_contents

"""Almost the same as the standard table but with extra columns"""


def scrape_gsma_table(db_session: Session, link: str, destination_folder: str, file_name: str, download: bool = True):
    html_file_path = os.path.join(destination_folder, file_name)

    if download:
        if os.path.exists(html_file_path):
            os.remove(html_file_path)
        get_aws_html(link, html_file_path)

    raw_scraping_data = RawScrapingData()

    with open(html_file_path, "r") as f:
        soup = BeautifulSoup(f.read(), "html.parser")
        table_ids = get_table_ids(this_soup=soup)
        for this_table_id in table_ids:
            table = soup.find(id=this_table_id)

            # Get the standard name based on the "tab" name
            tab = table.contents[1]
            standard_name = chomp_keep_single_spaces(str(tab.contents[0]))
            # We are only scraping GSMA here
            if standard_name != "GSMA":
                continue

            print(f"Scraping table for {standard_name}")
            rows = table.find_all("tr")
            if len(rows) == 0:
                continue

            # Scrape it

            for row in rows:
                cells = row.find_all("td")
                # Skip the first row, the rest are the same
                if len(cells) == 0 or len(cells) == 1:
                    continue

                # Cell 0: Service name

                this_service_name = get_service_name(cells)
                # print(f"GSMA service_name: {this_service_name}")

                # Cell 1: GSMA US (East)
                gsma_usstatus, gsma_usstatus_contents = clean_status_cell_contents(cells[1].contents[0])
                if gsma_usstatus:
                    # print(f"gsma_usstatus: {gsma_usstatus}, {gsma_usstatus_contents}")
                    raw_scraping_data.add_entry_to_database(
                        db_session=db_session,
                        compliance_standard_name="GSMA_US",
                        sdk="",
                        service_name=this_service_name,
                    )

                # Cell 2: GSMA EU (Paris)
                gsma_eu_status, gsma_eu_status_contents = clean_status_cell_contents(cells[2].contents[0])
                if gsma_eu_status:
                    # print(f"gsma_eu_status: {gsma_eu_status}, {gsma_eu_status_contents}")
                    raw_scraping_data.add_entry_to_database(
                        db_session=db_session,
                        compliance_standard_name="GSMA_EU",
                        sdk="",
                        service_name=this_service_name,
                    )
