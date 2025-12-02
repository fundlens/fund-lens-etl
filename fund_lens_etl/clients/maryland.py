"""Maryland campaign finance data clients.

Two clients for accessing Maryland campaign finance data:
1. MarylandCRISClient - Session-based client for MDCRIS (contributions, committees)
2. MarylandSBEClient - Direct HTTP client for SBE candidate data
"""

import hashlib
import logging
from datetime import date
from pathlib import Path

import requests
from prefect import get_run_logger
from prefect.exceptions import MissingContextError


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class MarylandCRISClient:
    """
    Session-based client for Maryland Campaign Reporting Information System (MDCRIS).

    Handles contributions and committees data from campaignfinance.maryland.gov.
    Requires session management as filters are stored server-side.
    """

    BASE_URL = "https://campaignfinance.maryland.gov"

    # Contributions endpoints
    CONTRIBUTIONS_SEARCH_URL = f"{BASE_URL}/Public/ViewReceipts"
    CONTRIBUTIONS_EXPORT_URL = f"{BASE_URL}/Public/ExportCsv"

    # Committees endpoints
    COMMITTEES_SEARCH_URL = f"{BASE_URL}/Public/Search"
    COMMITTEES_EXPORT_URL = f"{BASE_URL}/Public/ExporttoCsv"  # Note: double 't'

    def __init__(self, timeout: int = 120):
        """
        Initialize MDCRIS client.

        Args:
            timeout: Request timeout in seconds (default 120 for large exports)
        """
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
        )
        self.timeout = timeout

    def _init_session(self, url: str) -> None:
        """Initialize session by hitting the search page to get cookies."""
        logger = get_logger()
        logger.debug(f"Initializing session at {url}")
        response = self.session.get(url, params={"theme": "vista"}, timeout=self.timeout)
        response.raise_for_status()

    def _export_csv(self, export_url: str, output_path: Path) -> Path:
        """Download CSV from export endpoint."""
        logger = get_logger()
        logger.info(f"Downloading CSV to {output_path}")

        response = self.session.get(
            export_url,
            params={
                "page": "1",
                "orderBy": "~",
                "filter": "~",
                "Grid-size": "15",
                "theme": "vista",
            },
            stream=True,
            timeout=self.timeout,
        )
        response.raise_for_status()

        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Downloaded {output_path.stat().st_size:,} bytes")
        return output_path

    def download_contributions(
        self,
        output_path: Path,
        start_date: date | None = None,
        end_date: date | None = None,
        filing_year: int | None = None,
    ) -> Path:
        """
        Download contributions CSV with optional date range filter.

        Args:
            output_path: Path to save the CSV file
            start_date: Optional contribution start date filter
            end_date: Optional contribution end date filter
            filing_year: Optional filing year filter

        Returns:
            Path to the downloaded CSV file
        """
        logger = get_logger()
        logger.info(
            f"Downloading contributions: filing_year={filing_year}, "
            f"start_date={start_date}, end_date={end_date}"
        )

        # Initialize session
        self._init_session(self.CONTRIBUTIONS_SEARCH_URL)

        # POST search filters to set server-side state
        filter_data = {
            "txtContributorName": "",
            "txtFirstName": "",
            "ContributorType": "",
            "ContributionType": "",
            "ddlEmployerOccupation": "",
            "txtReceivingRegistrant": "",
            "ddlOfficeType": "",
            "ddlOfficeSought": "",
            "ddljurisdiction": "",
            "txtStreet": "",
            "txtTown": "",
            "ddlState": "",
            "txtZipCode": "",
            "txtZipExt": "",
            "ddlCanCountyofResidence": "",
            "MemberId": "",
            "FilingYear": str(filing_year) if filing_year else "",
            "FilingPeriodName": "",
            "ddlFundType": "",
            "dtStartDate": start_date.strftime("%m/%d/%Y") if start_date else "",
            "dtEndDate": end_date.strftime("%m/%d/%Y") if end_date else "",
            "txtAmountRangeFrom": "",
            "txtAmountRangeTo": "",
            "btnSearch": "Search",
        }

        response = self.session.post(
            self.CONTRIBUTIONS_SEARCH_URL,
            params={"theme": "vista"},
            data=filter_data,
            timeout=self.timeout,
        )
        response.raise_for_status()

        # Export the filtered results
        return self._export_csv(self.CONTRIBUTIONS_EXPORT_URL, output_path)

    def download_committees(
        self,
        output_path: Path,
        status: str | None = None,
        committee_type: str | None = None,
    ) -> Path:
        """
        Download committees CSV.

        Args:
            output_path: Path to save the CSV file
            status: Committee status filter ('A' for Active, None for all)
            committee_type: Committee type filter

        Returns:
            Path to the downloaded CSV file
        """
        logger = get_logger()
        logger.info(f"Downloading committees: status={status}, type={committee_type}")

        # Initialize session
        self._init_session(self.COMMITTEES_SEARCH_URL)

        # POST search filters
        filter_data = {
            "MemberId": "",
            "txtCommitteeName": "",
            "hdnAcronymId": "",
            "txtAcronym": "",
            "txtCommitteeID": "",
            "ddlcertificStatus": "",
            "CommitteeType": committee_type or "",
            "CommitteeStatus": status or "",
            "ddlElectionYear": "",
            "ElectionType": "",
            "hdnPersonID": "",
            "txtResOfficer": "",
            "CitationIssued": "3",
            "ddlViolation": "",
            "ddlViolationStatus": "",
            "dtViolationStartDate": "",
            "dtViolationEndDate": "",
            "txtAmountRangeFrom": "",
            "txtAmountRangeTo": "",
            "dtStartDate": "",
            "dtEndDate": "",
            "ddlOfficeSought": "",
            "ddlOfficeType": "",
            "ddljurisdiction": "",
            "btnSearch": "Search",
        }

        response = self.session.post(
            self.COMMITTEES_SEARCH_URL,
            params={"theme": "vista"},
            data=filter_data,
            timeout=self.timeout,
        )
        response.raise_for_status()

        # Export the filtered results
        return self._export_csv(self.COMMITTEES_EXPORT_URL, output_path)


class MarylandSBEClient:
    """
    Client for Maryland State Board of Elections candidate data.

    Direct HTTP GET for CSV files from elections.maryland.gov.
    No session management needed - files are publicly accessible.
    """

    BASE_URL = "https://elections.maryland.gov/elections"

    def __init__(self, timeout: int = 60):
        """
        Initialize SBE client.

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            }
        )

    def _get_candidate_urls(self, year: int) -> list[dict[str, str]]:
        """
        Generate all possible candidate CSV URLs for a year.

        URL patterns vary by year:
        - 2018: suffix _1_, primary only (gubernatorial year)
        - 2020: suffix _3_, primary only (presidential year - no state general)
        - 2022+: suffix _1_, both primary and general

        Args:
            year: Election year

        Returns:
            List of dicts with 'type' and 'url' keys
        """
        urls = []

        # Determine URL suffix (2020 uses _3_, others use _1_)
        suffix = "3" if year == 2020 else "1"

        # Determine which election types are available
        # Presidential years (2020, 2016, etc.) typically only have primary state candidates
        # Gubernatorial years (2018, 2022, 2026) have both primary and general
        is_presidential_year = year % 4 == 0
        election_types = ["Primary"] if is_presidential_year else ["Primary", "General"]

        # Regular elections (even years)
        if year % 2 == 0:
            for election_type in election_types:
                # Handle case inconsistency across years
                for folder_case in [
                    f"{election_type}_candidates",
                    f"{election_type.lower()}_candidates",
                ]:
                    # State candidates
                    urls.append(
                        {
                            "type": f"{election_type}_State",
                            "url": (
                                f"{self.BASE_URL}/{year}/{folder_case}/"
                                f"gen_cand_lists_{year}_{suffix}_ALL.csv"
                            ),
                        }
                    )
                    # Local candidates - try both URL patterns
                    # Older years use _by_county, some use double underscore
                    for local_suffix in ["_by_county_ALL.csv", "__by_county_ALL.csv"]:
                        urls.append(
                            {
                                "type": f"{election_type}_Local",
                                "url": (
                                    f"{self.BASE_URL}/{year}/{folder_case}/"
                                    f"gen_cand_lists_{year}_{suffix}{local_suffix}"
                                ),
                            }
                        )

        # Special elections (any year)
        for sp_type in ["SP_Primary_Candidate", "SP_General_Candidate"]:
            urls.append(
                {
                    "type": f"Special_{sp_type}",
                    "url": (
                        f"{self.BASE_URL}/{year}/{sp_type}/"
                        f"gen_cand_lists_{year}_{suffix}_by_county_ALL.csv"
                    ),
                }
            )

        return urls

    def download_candidates(
        self,
        year: int,
        output_dir: Path,
    ) -> list[Path]:
        """
        Download all available candidate files for a year.

        Tries all possible URL patterns and downloads those that exist.
        Deduplicates based on content hash to avoid saving the same file twice.

        Args:
            year: Election year
            output_dir: Directory to save CSV files

        Returns:
            List of paths to downloaded CSV files
        """
        logger = get_logger()
        logger.info(f"Downloading candidate files for {year}")

        output_dir.mkdir(parents=True, exist_ok=True)
        downloaded = []
        seen_content_hashes = set()  # Track content to avoid duplicates

        for url_info in self._get_candidate_urls(year):
            try:
                response = self.session.get(
                    url_info["url"],
                    timeout=self.timeout,
                )

                if response.status_code == 200:
                    # Check if it's actually a CSV (not an error page)
                    content_type = response.headers.get("Content-Type", "")
                    if "text/csv" in content_type or "application/csv" in content_type:
                        is_csv = True
                    else:
                        # Check content - CSV should have commas and election-related content
                        # 2022+ files have header "Office Name", older files have data like "Governor"
                        first_line = response.text.split("\n")[0] if response.text else ""
                        # Known office types that appear in Maryland election data
                        election_keywords = [
                            "Office",  # 2022+ header format
                            "Governor",
                            "President",
                            "Senator",
                            "Representative",
                            "Delegate",
                            "Commissioner",  # County Commissioner
                            "Board of Education",  # School Board
                            "Sheriff",
                            "Council",  # County Council
                            "Attorney",  # State's Attorney
                            "Judge",
                            "Clerk",
                        ]
                        is_csv = "," in first_line and any(
                            kw in first_line for kw in election_keywords
                        )

                    if is_csv:
                        # Check if we've already downloaded this exact content
                        content_hash = hashlib.sha256(response.content).hexdigest()
                        if content_hash in seen_content_hashes:
                            logger.debug(f"Skipping duplicate content from {url_info['url']}")
                            continue

                        seen_content_hashes.add(content_hash)
                        # Use content hash in filename to ensure uniqueness
                        filename = f"candidates_{year}_{url_info['type']}_{content_hash[:8]}.csv"
                        output_path = output_dir / filename
                        output_path.write_bytes(response.content)
                        logger.info(
                            f"Downloaded {url_info['type']}: {output_path.stat().st_size:,} bytes"
                        )
                        downloaded.append(output_path)
                    else:
                        logger.debug(f"URL returned non-CSV content: {url_info['url']}")
                elif response.status_code == 404:
                    logger.debug(f"Not found (expected for some patterns): {url_info['url']}")
                else:
                    logger.warning(
                        f"Unexpected status {response.status_code} for {url_info['url']}"
                    )

            except requests.RequestException as e:
                logger.warning(f"Failed to fetch {url_info['url']}: {e}")
                continue

        logger.info(f"Downloaded {len(downloaded)} candidate files for {year}")
        return downloaded


def generate_contribution_hash(
    receiving_committee: str,
    contribution_date: str,
    contributor_name: str,
    contributor_address: str,
    contribution_amount: str,
    contribution_type: str,
) -> str:
    """
    Generate SHA-256 hash for contribution deduplication.

    Maryland contributions don't have a unique ID, so we generate
    a hash from key fields to detect duplicates.

    Args:
        receiving_committee: Committee name
        contribution_date: Date string
        contributor_name: Contributor name
        contributor_address: Address string
        contribution_amount: Amount string
        contribution_type: Type of contribution

    Returns:
        64-character hex hash string
    """
    content = "|".join(
        [
            receiving_committee or "",
            contribution_date or "",
            contributor_name or "",
            contributor_address or "",
            contribution_amount or "",
            contribution_type or "",
        ]
    )
    return hashlib.sha256(content.encode()).hexdigest()
