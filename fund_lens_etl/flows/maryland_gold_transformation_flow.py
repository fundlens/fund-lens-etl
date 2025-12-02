"""
Maryland Gold Transformation Flow

Transforms Maryland Silver layer data into Gold layer (analytics-ready dimensional model).
This integrates Maryland data into the unified gold layer alongside FEC data.

- Contributors: Deduplicate and merge into gold_contributor
- Committees: Merge into gold_committee (using state_committee_id for MD CCF ID)
- Candidates: Merge into gold_candidate (using state_candidate_id)
- Contributions: Create fact records in gold_contribution with FK resolution
"""

import re
from typing import Any

import pandas as pd
from fund_lens_models.gold import (
    GoldCandidate,
    GoldCommittee,
    GoldContribution,
    GoldContributor,
)
from fund_lens_models.silver import (
    SilverMarylandCandidate,
    SilverMarylandCommittee,
    SilverMarylandContribution,
)
from prefect import flow, get_run_logger, task
from prefect.exceptions import MissingContextError
from sqlalchemy import select

from fund_lens_etl.database import get_session

# Retry configuration for transformation tasks
MD_GOLD_RETRY_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": 30,
}
MD_GOLD_TASK_TIMEOUT = 1800  # 30 minutes


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        import logging

        return logging.getLogger(__name__)


def _extract_candidate_name_from_committee(committee_name: str) -> str | None:
    """
    Extract a potential candidate name from a committee name.

    Common patterns:
    - "Friends of John Smith"
    - "John Smith for Governor"
    - "Committee to Elect John Smith"
    - "Citizens for John Smith"
    - "Smith (John) for Delegate"
    - "Smith  John Friends of" (MD format: Last  First suffix)
    - "Zubairi  Daniel - Personal Treasurer Account"
    - "Guthrie  Dion F. Citizens to Elect"

    Args:
        committee_name: The committee name

    Returns:
        Normalized potential candidate name or None
    """
    if not committee_name:
        return None

    name = committee_name.strip()

    # Check for MD format: "Last  First... suffix" (has double space between last and first)
    # Pattern: one or more words (last name), then 2+ spaces, then more content
    # Note: Last name can be multi-word (e.g., "Arango Millan  Juan")
    md_pattern = re.match(r"^([\w\s\-\']+?)\s{2,}(.+)$", name)
    if md_pattern:
        last_name = md_pattern.group(1).strip()
        rest = md_pattern.group(2).strip()

        # List of suffix patterns that indicate where the name ends
        # These can appear anywhere in 'rest'
        # Order matters - more specific patterns first
        suffix_patterns = [
            r"\bCitizens\s+to\s+Elect\b",
            r"\bCommittee\s+to\s+Elect\b",
            r"\bFriends\s+of\b",
            r"\bFriends\s+for\b",
            r"\bCitizens\s+for\b",
            r"\bCommittee\s+for\b",
            r"\bfor\s+Governor\b",
            r"\bfor\s+Congress\b",
            r"\bfor\s+Senate\b",
            r"\bfor\s+Delegate\b",
            r"\bfor\s+County\b",
            r"\bfor\s+State\b",
            r"\bfor\s+School\b",
            r"\bfor\s+Commissioner\b",
            r"\bfor\s+Judge\b",
            r"\bfor\s+Sheriff\b",
            r"\bfor\s+Maryland\b",
            r"\bfor\s+Baltimore\b",
            # Maryland counties and cities
            r"\bfor\s+Allegany\b",
            r"\bfor\s+Anne\s+Arundel\b",
            r"\bfor\s+Calvert\b",
            r"\bfor\s+Caroline\b",
            r"\bfor\s+Carroll\b",
            r"\bfor\s+Cecil\b",
            r"\bfor\s+Charles\b",
            r"\bfor\s+Dorchester\b",
            r"\bfor\s+Frederick\b",
            r"\bfor\s+Garrett\b",
            r"\bfor\s+Harford\b",
            r"\bfor\s+Howard\b",
            r"\bfor\s+Kent\b",
            r"\bfor\s+Montgomery\b",
            r"\bfor\s+Prince\s+George\b",
            r"\bfor\s+Queen\s+Anne\b",
            r"\bfor\s+Somerset\b",
            r"\bfor\s+Talbot\b",
            r"\bfor\s+Washington\b",
            r"\bfor\s+Wicomico\b",
            r"\bfor\s+Worcester\b",
            r"\bfor\s+Hagerstown\b",
            r"\bfor\s+Cumberland\b",
            r"\bfor\s+Our\b",
            r"\bfor\s+Republican\b",
            r"\bfor\s+Democrat\b",
            r"\bfor\s+Citizens\b",  # "Jamar for Citizens"
            r"\bfor\s+Progress\b",
            r"\bfor\s+Change\b",
            r"\bfor\s+Mayor\b",
            r"\bfor\s+Council\b",
            r"\bfor\s+District\b",
            r"\bMarylanders\s+For\b",
            r"\bCampaign\s+for\b",
            r"\bCampaign\b",
            r"\bCommittee\b",
            r"\bCommitte\b",  # typo variant
            r"\s*-\s*Personal\s+Treasurer",
            r"\bPersonal\s+Treasurer\b",
            r"\bThe\s+Friends\s+Of\b",
        ]

        first_name = rest
        for pattern in suffix_patterns:
            match = re.search(pattern, first_name, re.IGNORECASE)
            if match:
                first_name = first_name[: match.start()].strip()
                break

        # Handle "(Nick)" style nicknames
        # If the first name is ONLY a parenthetical, extract the content
        paren_only = re.match(r"^\(([^)]+)\)$", first_name.strip())
        if paren_only:
            first_name = paren_only.group(1)
        else:
            # Otherwise remove parentheticals (they're nicknames alongside real name)
            first_name = re.sub(r"\s*\([^)]+\)\s*", " ", first_name).strip()

        # Clean up any trailing punctuation
        first_name = first_name.rstrip(".,;:-")

        name = f"{first_name} {last_name}" if first_name else last_name
    else:
        # Standard format processing (no double space)
        name_normalized = re.sub(r"\s+", " ", name)

        # Remove common prefixes
        prefixes = [
            "Friends of",
            "Friends for",
            "Committee to Elect",
            "Committee for",
            "Citizens for",
            "Citizens to Elect",
            "Elect",
            "Re-Elect",
            "Re Elect",
            "Team of",
            "Vote for",
            "Campaign for",
            "The Friends Of",
            "Marylanders for",
        ]

        name_lower = name_normalized.lower()
        for prefix in prefixes:
            if name_lower.startswith(prefix.lower()):
                name_normalized = name_normalized[len(prefix) :].strip()
                break

        # Remove common suffixes
        suffixes = [
            "for Governor",
            "for Congress",
            "for Senate",
            "for Delegate",
            "for County Council",
            "for State Senate",
            "for School Board",
            "for Commissioner",
            "for County Executive",
            "for Judge",
            "for Sheriff",
            "for Maryland",
            "for Our County",
            "Campaign",
            "Committee",
            "- Personal Treasurer Account",
            "- Personal Treasurer",
            "Personal Treasurer Account",
        ]

        name_lower = name_normalized.lower()
        for suffix in suffixes:
            if name_lower.endswith(suffix.lower()):
                name_normalized = name_normalized[: -len(suffix)].strip()
                break

        name = name_normalized

    # Handle "(First) Last" pattern -> "First Last"
    paren_match = re.match(r"(\w+)\s*\((\w+)\)", name)
    if paren_match:
        name = f"{paren_match.group(2)} {paren_match.group(1)}"

    # Handle "Last, First" pattern
    if "," in name:
        parts = name.split(",", 1)
        if len(parts) == 2:
            name = f"{parts[1].strip()} {parts[0].strip()}"

    # Clean up extra whitespace
    name = re.sub(r"\s+", " ", name).strip()

    return name.upper() if name else None


def _normalize_candidate_name(name: str) -> str:
    """Normalize a candidate name for matching."""
    if not name:
        return ""
    # Remove punctuation and extra spaces
    name = re.sub(r'[,.\-\'"()]', " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name.upper()


def _normalize_party(party: str | None) -> str | None:
    """
    Normalize party designation to consistent abbreviations.

    Converts various party name formats to standard 3-letter codes:
    - Democratic/Democrat/DEM -> DEM
    - Republican/REP -> REP
    - Green/GRE -> GRN
    - Libertarian/LIB -> LIB
    - Independent/IND/Unaffiliated -> IND
    - Non-Partisan/Judicial -> NPP (No Party Preference)

    Args:
        party: Raw party string from source

    Returns:
        Normalized party code or None
    """
    if not party:
        return None

    party_upper = party.strip().upper()

    # Map to standard abbreviations
    party_mapping = {
        # Democratic variants
        "DEMOCRATIC": "DEM",
        "DEMOCRAT": "DEM",
        "DEM": "DEM",
        "D": "DEM",
        # Republican variants
        "REPUBLICAN": "REP",
        "REP": "REP",
        "R": "REP",
        # Green variants
        "GREEN": "GRN",
        "GRE": "GRN",
        "GRN": "GRN",
        # Libertarian variants
        "LIBERTARIAN": "LIB",
        "LIB": "LIB",
        # Independent/Unaffiliated
        "INDEPENDENT": "IND",
        "IND": "IND",
        "UNAFFILIATED": "IND",
        "UNA": "IND",
        # Non-partisan
        "NON-PARTISAN": "NPP",
        "NONPARTISAN": "NPP",
        "NON PARTISAN": "NPP",
        "JUDICIAL": "NPP",
        "NPP": "NPP",
        # Other minor parties (preserve as-is but standardize)
        "CONSTITUTION": "CON",
        "WORKING CLASS": "WCP",
        "BREAD AND ROSES": "BNR",
        "OTHER CANDIDATES": "OTH",
        "OTHER": "OTH",
        # FEC codes (already abbreviated)
        "AIP": "AIP",
        "PPY": "PPY",
        "W": "WRI",  # Write-in
        "PRO": "PRO",
    }

    return party_mapping.get(party_upper, party_upper[:3] if len(party_upper) > 3 else party_upper)


def _normalize_office(office: str | None) -> str:
    """
    Normalize office designation to consistent format.

    Converts Maryland office names to standardized codes that match
    the gold schema conventions.

    Args:
        office: Raw office string from source

    Returns:
        Normalized office code
    """
    if not office:
        return "OTHER"

    office_clean = office.strip()

    # Comprehensive office mapping
    office_mapping = {
        # Federal offices
        "U.S. Senator": "US_SENATE",
        "United States Senator": "US_SENATE",
        "Representative in Congress": "US_HOUSE",
        "U.S. Representative": "US_HOUSE",
        # Statewide offices
        "Governor / Lt. Governor": "GOVERNOR",
        "Governor": "GOVERNOR",
        "Lieutenant Governor": "LT_GOVERNOR",
        "Attorney General": "ATTORNEY_GENERAL",
        "Comptroller": "COMPTROLLER",
        "Treasurer": "TREASURER",
        # State legislature
        "State Senator": "STATE_SENATE",
        "House of Delegates": "STATE_HOUSE",
        "Delegate": "STATE_HOUSE",
        # County executive offices
        "County Executive": "COUNTY_EXEC",
        "County Council": "COUNTY_COUNCIL",
        "County Council At Large": "COUNTY_COUNCIL",
        "President of the County Council": "COUNTY_COUNCIL",
        "County Commissioner": "COUNTY_COMM",
        "County Commissioner President": "COUNTY_COMM",
        "County Commissioner At Large": "COUNTY_COMM",
        # City councils (Baltimore City, Cumberland, Hagerstown)
        "Member of the City Council": "CITY_COUNCIL",
        "President of the City Council": "CITY_COUNCIL",
        "Council - City of Cumberland": "CITY_COUNCIL",
        "Council - City of Hagerstown": "CITY_COUNCIL",
        # County elected officials
        "Sheriff": "SHERIFF",
        "State's Attorney": "STATES_ATTORNEY",
        "Register of Wills": "REGISTER_WILLS",
        "Clerk of the Circuit Court": "CIRCUIT_CLERK",
        # Judicial
        "Judge of the Circuit Court": "CIRCUIT_JUDGE",
        "Judge of the Orphans' Court": "ORPHANS_JUDGE",
        # Education
        "Board of Education": "SCHOOL_BOARD",
        "Board of Education At Large": "SCHOOL_BOARD",
        # Party committees (not really candidates but in the data)
        "Democratic Central Committee": "PARTY_COMM",
        "Republican Central Committee": "PARTY_COMM",
    }

    mapped = office_mapping.get(office_clean)
    if mapped:
        return mapped

    # Fallback: extract key words and create reasonable code
    office_lower = office_clean.lower()

    if "congress" in office_lower or "representative" in office_lower:
        return "US_HOUSE"
    if "senator" in office_lower and "state" in office_lower:
        return "STATE_SENATE"
    if "senator" in office_lower:
        return "US_SENATE"
    # National convention delegates (presidential primary)
    if "national convention" in office_lower or "alternate delegate" in office_lower:
        return "NATL_DELEGATE"
    # State house delegates (not national convention)
    if "delegate" in office_lower and "house" in office_lower:
        return "STATE_HOUSE"
    if office_clean == "Delegate":
        return "STATE_HOUSE"
    if "governor" in office_lower:
        return "GOVERNOR"
    # Distinguish city council from county council
    if "city council" in office_lower or "council - city" in office_lower:
        return "CITY_COUNCIL"
    if "county council" in office_lower:
        return "COUNTY_COUNCIL"
    if "council" in office_lower:
        # Generic council - default to county
        return "COUNTY_COUNCIL"
    if "commissioner" in office_lower:
        return "COUNTY_COMM"
    if "sheriff" in office_lower:
        return "SHERIFF"
    if "judge" in office_lower:
        return "JUDGE"
    if "board" in office_lower and "education" in office_lower:
        return "SCHOOL_BOARD"
    if "clerk" in office_lower:
        return "CLERK"
    if "register" in office_lower:
        return "REGISTER_WILLS"
    if "attorney" in office_lower:
        return "STATES_ATTORNEY"
    if "central committee" in office_lower:
        return "PARTY_COMM"
    if "treasurer" in office_lower:
        return "TREASURER"

    # Last resort: use first word, uppercased
    return office_clean.split()[0].upper()[:20] if office_clean else "OTHER"


def _get_jurisdiction_level(office: str) -> str:
    """
    Determine the jurisdiction level based on office type.

    Args:
        office: Normalized office code

    Returns:
        FEDERAL, STATE, COUNTY, or CITY
    """
    federal_offices = {"US_HOUSE", "US_SENATE", "PRESIDENT"}
    state_offices = {
        "GOVERNOR",
        "LT_GOVERNOR",
        "ATTORNEY_GENERAL",
        "COMPTROLLER",
        "TREASURER",
        "STATE_SENATE",
        "STATE_HOUSE",
        "NATL_DELEGATE",
    }
    # County offices - most local MD offices are county-level
    county_offices = {
        "COUNTY_EXEC",
        "COUNTY_COUNCIL",
        "COUNTY_COMM",
        "SHERIFF",
        "STATES_ATTORNEY",
        "REGISTER_WILLS",
        "CIRCUIT_CLERK",
        "CIRCUIT_JUDGE",
        "ORPHANS_JUDGE",
        "SCHOOL_BOARD",
        "PARTY_COMM",
    }
    # City offices - Baltimore City and other municipalities
    city_offices = {"CITY_COUNCIL", "MAYOR"}

    if office in federal_offices:
        return "FEDERAL"
    elif office in state_offices:
        return "STATE"
    elif office in city_offices:
        return "CITY"
    elif office in county_offices:
        return "COUNTY"
    else:
        # Default to COUNTY for unknown local offices
        return "COUNTY"


def _extract_county_city(jurisdiction: str | None) -> tuple[str | None, str | None]:
    """
    Extract county and city from jurisdiction string.

    Baltimore City is the only independent city in Maryland.
    All other jurisdictions are counties.

    Args:
        jurisdiction: Raw jurisdiction string from source (e.g., "Montgomery County", "Baltimore City")

    Returns:
        Tuple of (county, city) - one will be None
    """
    if not jurisdiction:
        return None, None

    jurisdiction_clean = jurisdiction.strip()

    # Baltimore City is a special case - it's an independent city, not a county
    if jurisdiction_clean.lower() in ("baltimore city", "city of baltimore"):
        return None, "Baltimore"

    # Clean up county name
    # Remove " County" suffix if present
    county_name = jurisdiction_clean
    if county_name.lower().endswith(" county"):
        county_name = county_name[:-7].strip()

    # Normalize common variations
    county_mapping = {
        "PRINCE GEORGE'S": "Prince George's",
        "PRINCE GEORGE`S": "Prince George's",
        "SAINT MARY'S": "St. Mary's",
        "ST. MARY'S": "St. Mary's",
        "QUEEN ANNE'S": "Queen Anne's",
    }

    county_upper = county_name.upper()
    if county_upper in county_mapping:
        county_name = county_mapping[county_upper]
    else:
        # Title case the county name
        county_name = county_name.title()

    return county_name, None


def _normalize_district(district: str | None, office: str | None) -> str | None:
    """
    Normalize district designation for consistency.

    Standardizes district formats:
    - Legislative districts: "District 31" or "31"
    - Congressional districts: "MD-06" format
    - County-based: County name only
    - Councilmanic/Commissioner: "District X" format

    Args:
        district: Raw district string
        office: Office type for context

    Returns:
        Normalized district string or None
    """
    if not district:
        return None

    district_clean = district.strip()

    # Extract numeric district if present
    district_num_match = re.search(r"(\d+)", district_clean)
    district_num = district_num_match.group(1) if district_num_match else None

    office_norm = _normalize_office(office) if office else ""

    # Congressional districts -> "MD-XX" format
    if office_norm == "US_HOUSE" or "congressional" in district_clean.lower():
        if district_num:
            return f"MD-{int(district_num):02d}"
        return district_clean

    # Legislative districts -> just the number
    if office_norm in ("STATE_SENATE", "STATE_HOUSE") or "legislative" in district_clean.lower():
        if district_num:
            return district_num
        return district_clean

    # Councilmanic/Commissioner districts -> "District X"
    if (
        "councilmanic" in district_clean.lower()
        or "commissioner district" in district_clean.lower()
    ):
        if district_num:
            return f"District {district_num}"
        return district_clean

    # Judicial circuits -> "Circuit X"
    if "circuit" in district_clean.lower() or "judicial" in district_clean.lower():
        if district_num:
            return f"Circuit {district_num}"
        return district_clean

    # County names - normalize common variations
    county_mapping = {
        "PRINCE GEORGE`S": "Prince George's",
        "PRINCE GEORGE'S": "Prince George's",
        "SAINT MARY'S": "St. Mary's",
        "ST. MARY'S": "St. Mary's",
        "QUEEN ANNE'S": "Queen Anne's",
    }

    district_upper = district_clean.upper()
    if district_upper in county_mapping:
        return county_mapping[district_upper]

    # Return as title case for county names
    if not district_num and len(district_clean.split()) <= 2:
        return district_clean.title()

    return district_clean


def _normalize_md_contributor_key(
    name: str | None,
    city: str | None,
    state: str | None,
    employer: str | None,
    contributor_type: str | None = None,
) -> str:
    """
    Create a normalized key for Maryland contributor matching.

    For organizations (Business, Political Committee, etc.):
    - Name is normalized (punctuation removed)
    - Location is IGNORED

    For individuals:
    - Full matching on name + city + state + employer

    Args:
        name: Contributor name
        city: Contributor city
        state: Contributor state
        employer: Contributor employer
        contributor_type: Maryland contributor type

    Returns:
        Normalized key string for matching
    """
    # Maryland organization types
    org_types = {
        "Business",
        "Political Committee",
        "Labor Organization",
        "Association",
        "Other",
        "Candidate Loan",
    }

    is_org = contributor_type in org_types if contributor_type else False

    if is_org:
        # Normalize org name
        name_norm = re.sub(r'[,.\-\'"()]', "", name or "")
        name_norm = re.sub(r"\s+", " ", name_norm).upper().strip()
        return f"ORG|{name_norm}"

    # For individuals
    name_norm = (name or "").upper().strip()
    city_norm = (city or "").upper().strip()
    state_norm = (state or "").upper().strip()
    employer_norm = (employer or "").upper().strip()

    return f"{name_norm}|{city_norm}|{state_norm}|{employer_norm}"


def _calculate_md_match_confidence(row: pd.Series) -> float:
    """
    Calculate confidence score for Maryland contributor match.

    Args:
        row: Pandas Series with contributor data

    Returns:
        Confidence score between 0.0 and 1.0
    """
    confidence = 0.0

    if pd.notna(row.get("name")) and row.get("name"):
        confidence += 0.4

    if pd.notna(row.get("city")) and row.get("city"):
        confidence += 0.2
    if pd.notna(row.get("state")) and row.get("state"):
        confidence += 0.2

    if pd.notna(row.get("employer")) and row.get("employer"):
        confidence += 0.1
    if pd.notna(row.get("occupation")) and row.get("occupation"):
        confidence += 0.1

    return min(confidence, 1.0)


@task(
    name="transform_md_contributors_to_gold",
    description="Deduplicate and transform Maryland contributors to gold",
    retries=MD_GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_GOLD_TASK_TIMEOUT,
)
def transform_md_contributors_task() -> dict[str, Any]:
    """
    Transform Maryland silver contributors to gold dimension with deduplication.

    Extracts unique contributors from silver_md_contribution and merges
    into the unified gold_contributor table.

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting Maryland contributor transformation to gold layer")

    with get_session() as session:
        # Get unique contributors from Maryland contributions
        stmt = select(
            SilverMarylandContribution.contributor_name,
            SilverMarylandContribution.contributor_city,
            SilverMarylandContribution.contributor_state,
            SilverMarylandContribution.contributor_zip,
            SilverMarylandContribution.employer_name,
            SilverMarylandContribution.employer_occupation,
            SilverMarylandContribution.contributor_type,
        ).group_by(
            SilverMarylandContribution.contributor_name,
            SilverMarylandContribution.contributor_city,
            SilverMarylandContribution.contributor_state,
            SilverMarylandContribution.contributor_zip,
            SilverMarylandContribution.employer_name,
            SilverMarylandContribution.employer_occupation,
            SilverMarylandContribution.contributor_type,
        )

        df = pd.read_sql(stmt, session.connection())
        logger.info(f"Loaded {len(df)} unique contributor records from MD silver")

        if df.empty:
            return {
                "total_silver_contributors": 0,
                "total_gold_contributors": 0,
                "new_contributors": 0,
                "deduplication_rate": 0.0,
            }

        # Rename columns for gold schema
        df = df.rename(
            columns={
                "contributor_name": "name",
                "contributor_city": "city",
                "contributor_state": "state",
                "contributor_zip": "zip",
                "employer_name": "employer",
                "employer_occupation": "occupation",
            }
        )

        # Map Maryland contributor_type to FEC-style entity_type
        type_mapping = {
            "Individual": "IND",
            "Business": "ORG",
            "Political Committee": "COM",
            "Labor Organization": "ORG",
            "Association": "ORG",
            "Candidate Loan": "IND",
            "Other": "ORG",
        }
        df["entity_type"] = df["contributor_type"].map(type_mapping).fillna("IND")

        # Create normalized key for deduplication
        df["match_key"] = df.apply(
            lambda r: _normalize_md_contributor_key(
                r["name"], r["city"], r["state"], r["employer"], r["contributor_type"]
            ),
            axis=1,
        )

        # Deduplicate
        df_deduped = df.drop_duplicates(subset=["match_key"], keep="first").copy()
        df_deduped["match_confidence"] = df_deduped.apply(_calculate_md_match_confidence, axis=1)
        df_deduped = df_deduped.drop(columns=["match_key", "contributor_type"])

        dedup_count = len(df) - len(df_deduped)
        logger.info(f"Deduplicated {dedup_count} contributor records")

        # Build lookup of existing contributors
        existing_lookup = set()
        for contributor in session.execute(
            select(
                GoldContributor.name,
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.employer,
                GoldContributor.entity_type,
            )
        ):
            key = _normalize_md_contributor_key(
                contributor.name,
                contributor.city,
                contributor.state,
                contributor.employer,
                None,  # entity_type not used for key in gold
            )
            existing_lookup.add(key)
        logger.info(f"Found {len(existing_lookup)} existing contributors in gold")

        # Create lookup key for new contributors
        df_deduped["lookup_key"] = df_deduped.apply(
            lambda r: _normalize_md_contributor_key(
                r["name"], r["city"], r["state"], r["employer"], None
            ),
            axis=1,
        )

        # Filter to new contributors only
        new_contributors = df_deduped[~df_deduped["lookup_key"].isin(existing_lookup)].copy()
        new_contributors = new_contributors.drop(columns=["lookup_key"])

        logger.info(f"Identified {len(new_contributors)} new contributors to insert")

        # Bulk insert new contributors
        loaded_count = 0
        if not new_contributors.empty:
            contributors = [
                GoldContributor(
                    name=row["name"],
                    city=row["city"],
                    state=row["state"],
                    zip=row["zip"],
                    employer=row["employer"],
                    occupation=row["occupation"],
                    entity_type=row["entity_type"],
                    match_confidence=row["match_confidence"],
                )
                for _, row in new_contributors.iterrows()
            ]

            session.add_all(contributors)
            session.commit()
            loaded_count = len(contributors)
            logger.info(f"Loaded {loaded_count} new contributors to gold")

        return {
            "total_silver_contributors": len(df),
            "total_gold_contributors": len(df_deduped),
            "new_contributors": loaded_count,
            "deduplication_rate": dedup_count / len(df) if len(df) > 0 else 0.0,
        }


@task(
    name="transform_md_committees_to_gold",
    description="Transform Maryland committees to gold dimension",
    retries=MD_GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_GOLD_TASK_TIMEOUT,
)
def transform_md_committees_task() -> dict[str, Any]:
    """
    Transform Maryland silver committees to gold dimension.

    Uses state_committee_id to store the Maryland CCF ID.

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting Maryland committee transformation to gold layer")

    with get_session() as session:
        # Get committees not yet in gold (by state_committee_id)
        subquery = (
            select(GoldCommittee.state_committee_id)
            .where(GoldCommittee.state_committee_id == SilverMarylandCommittee.source_ccf_id)
            .exists()
        )
        stmt = select(SilverMarylandCommittee).where(~subquery)

        df = pd.read_sql(stmt, session.connection())
        logger.info(f"Loaded {len(df)} new committee records from MD silver")

        if df.empty:
            return {
                "total_committees": 0,
                "loaded_count": 0,
            }

        # Map committee types to gold schema
        type_mapping = {
            "Candidate Committee": "CANDIDATE",
            "Political Action Committee": "PAC",
            "Party Central Committee": "PARTY",
            "Ballot Issue Committee": "PAC",
            "Independent Expenditure": "SUPER_PAC",
            "Super Political Action Committee": "SUPER_PAC",
            "Inaugural Committee": "OTHER",
            "Slate Committee": "PAC",
            "Out-of-State Committee": "PAC",
            "Participating Organization Committee": "PAC",
            "Public Financing Committee": "OTHER",
            "Gubernatorial Ticket Committee": "CANDIDATE",
            "Legislative Party Caucus Committee": "PARTY",
        }

        committees = [
            GoldCommittee(
                name=row["name"],
                committee_type=type_mapping.get(row["committee_type"], "OTHER"),
                state="MD",
                city=None,  # MD data doesn't include city
                candidate_id=None,  # Will be linked later if needed
                state_committee_id=row["source_ccf_id"],
                fec_committee_id=None,
                is_active=row.get("is_active", True),
            )
            for _, row in df.iterrows()
        ]

        session.add_all(committees)
        session.commit()
        loaded_count = len(committees)

        logger.info(f"Loaded {loaded_count} new committees to gold")

        return {
            "total_committees": len(df),
            "loaded_count": loaded_count,
        }


@task(
    name="transform_md_candidates_to_gold",
    description="Transform Maryland candidates to gold dimension",
    retries=MD_GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_GOLD_TASK_TIMEOUT,
)
def transform_md_candidates_task(min_election_year: int = 2026) -> dict[str, Any]:
    """
    Transform Maryland silver candidates to gold dimension.

    Only candidates who have filed for elections in min_election_year or later
    are included. This ensures gold layer only contains "active" candidates
    who are actually running in current/upcoming cycles.

    Uses state_candidate_id to store the Maryland content hash.

    Args:
        min_election_year: Minimum election year to include (default: 2026).
            Only candidates with election_year >= this value are transformed.

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info(
        f"Starting Maryland candidate transformation to gold layer (min_election_year={min_election_year})"
    )

    with get_session() as session:
        # Get candidates not yet in gold (by state_candidate_id)
        # AND only those filing for elections >= min_election_year
        subquery = (
            select(GoldCandidate.state_candidate_id)
            .where(GoldCandidate.state_candidate_id == SilverMarylandCandidate.source_content_hash)
            .exists()
        )
        stmt = select(SilverMarylandCandidate).where(
            ~subquery,
            SilverMarylandCandidate.election_year >= min_election_year,
        )

        df = pd.read_sql(stmt, session.connection())
        logger.info(f"Loaded {len(df)} new candidate records from MD silver")

        if df.empty:
            return {
                "total_candidates": 0,
                "loaded_count": 0,
            }

        # Transform candidates with normalized office, party, district, and derived fields
        candidates = []
        for _, row in df.iterrows():
            raw_office = row["office"]
            raw_district = row.get("district")
            raw_party = row.get("party")
            raw_jurisdiction = row.get("jurisdiction")

            # Normalize all fields
            normalized_office = _normalize_office(raw_office)
            normalized_party = _normalize_party(raw_party)
            normalized_district = _normalize_district(raw_district, raw_office)

            # Derive jurisdiction level from office type
            jurisdiction_level = _get_jurisdiction_level(normalized_office)

            # Extract county/city from jurisdiction field
            county, city = _extract_county_city(raw_jurisdiction)

            # For Baltimore City with city-specific offices, use CITY jurisdiction
            # (e.g., Baltimore City School Board, not state-level offices like delegates)
            if city == "Baltimore" and jurisdiction_level == "COUNTY":
                jurisdiction_level = "CITY"

            # Only set office_county for COUNTY-level candidates
            # Only set office_city for CITY-level candidates
            office_county = county if jurisdiction_level == "COUNTY" else None
            office_city = city if jurisdiction_level == "CITY" else None

            # Get election year from silver record
            election_year = row.get("election_year")

            candidates.append(
                GoldCandidate(
                    name=row["name"],
                    office=normalized_office[:20],  # Truncate to fit schema
                    office_raw=raw_office,  # Preserve original office name
                    state="MD",
                    district=normalized_district,
                    jurisdiction_level=jurisdiction_level,
                    office_county=office_county,
                    office_city=office_city,
                    party=normalized_party,
                    first_election_year=election_year,
                    last_election_year=election_year,
                    is_active=row.get("is_active", True),
                    state_candidate_id=row["source_content_hash"],
                    fec_candidate_id=None,
                )
            )

        session.add_all(candidates)
        session.commit()
        loaded_count = len(candidates)

        logger.info(f"Loaded {loaded_count} new candidates to gold")

        return {
            "total_candidates": len(df),
            "loaded_count": loaded_count,
        }


@task(
    name="link_md_committees_to_candidates",
    description="Link Maryland committees to their candidates via CCF ID and name matching",
    retries=MD_GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_GOLD_TASK_TIMEOUT,
)
def link_md_committees_to_candidates_task() -> dict[str, Any]:
    """
    Link Maryland gold committees to their candidates via CCF ID and name matching.

    Linkage is done in two passes:
    1. CCF ID matching: Uses committee_ccf_id from silver_md_candidate to match
       state_committee_id in gold_committee
    2. Committee name matching: Uses committee_name from silver_md_candidate to match
       gold_committee.name directly

    Returns:
        Dictionary with linkage statistics
    """
    logger = get_logger()
    logger.info("Linking Maryland committees to candidates")

    with get_session() as session:
        from fund_lens_models.silver import SilverMarylandCandidate

        # Build mapping from state_candidate_id to gold_candidate.id
        candidate_id_map = {}
        for row in session.execute(
            select(GoldCandidate.id, GoldCandidate.state_candidate_id).where(
                GoldCandidate.state_candidate_id.isnot(None)
            )
        ):
            candidate_id_map[row.state_candidate_id] = row.id
        logger.info(f"Found {len(candidate_id_map)} gold candidates with state_candidate_id")

        # Pass 1: Link by CCF ID
        stmt = select(
            SilverMarylandCandidate.source_content_hash,
            SilverMarylandCandidate.committee_ccf_id,
        ).where(
            SilverMarylandCandidate.committee_ccf_id.isnot(None),
            SilverMarylandCandidate.committee_ccf_id != "",
            SilverMarylandCandidate.committee_ccf_id != "NaN",
        )

        ccf_to_candidate = {}
        for row in session.execute(stmt):
            if row.committee_ccf_id and row.committee_ccf_id not in ("NaN", "nan", ""):
                ccf_to_candidate[row.committee_ccf_id] = row.source_content_hash

        logger.info(f"Pass 1: Found {len(ccf_to_candidate)} candidates with committee CCF IDs")

        linked_by_ccf = 0
        for row in session.execute(
            select(GoldCommittee).where(
                GoldCommittee.state_committee_id.isnot(None),
                GoldCommittee.candidate_id.is_(None),
            )
        ):
            committee = row[0]
            ccf_id = committee.state_committee_id

            if ccf_id in ccf_to_candidate:
                state_candidate_id = ccf_to_candidate[ccf_id]
                if state_candidate_id in candidate_id_map:
                    committee.candidate_id = candidate_id_map[state_candidate_id]
                    linked_by_ccf += 1

        session.commit()
        logger.info(f"Pass 1: Linked {linked_by_ccf} committees by CCF ID")

        # Pass 2: Link by committee name (for remaining unlinked committees)
        stmt = select(
            SilverMarylandCandidate.source_content_hash,
            SilverMarylandCandidate.committee_name,
        ).where(
            SilverMarylandCandidate.committee_name.isnot(None),
            SilverMarylandCandidate.committee_name != "",
        )

        name_to_candidate = {}
        for row in session.execute(stmt):
            if row.committee_name:
                # Normalize name for matching
                name_norm = row.committee_name.strip().upper()
                name_to_candidate[name_norm] = row.source_content_hash

        logger.info(f"Pass 2: Found {len(name_to_candidate)} candidates with committee names")

        linked_by_name = 0
        for row in session.execute(
            select(GoldCommittee).where(
                GoldCommittee.state_committee_id.isnot(None),
                GoldCommittee.candidate_id.is_(None),
                GoldCommittee.committee_type == "CANDIDATE",
            )
        ):
            committee = row[0]
            name_norm = committee.name.strip().upper() if committee.name else ""

            if name_norm in name_to_candidate:
                state_candidate_id = name_to_candidate[name_norm]
                if state_candidate_id in candidate_id_map:
                    committee.candidate_id = candidate_id_map[state_candidate_id]
                    linked_by_name += 1

        session.commit()
        logger.info(f"Pass 2: Linked {linked_by_name} committees by name")

        # Pass 3: Fuzzy name matching - extract candidate name from committee name
        # Build candidate name lookup from gold_candidate
        candidate_name_lookup = {}
        for row in session.execute(
            select(GoldCandidate.id, GoldCandidate.name).where(
                GoldCandidate.state_candidate_id.isnot(None)
            )
        ):
            name_norm = _normalize_candidate_name(row.name)
            if name_norm:
                candidate_name_lookup[name_norm] = row.id
                # Also add last name only for matching
                parts = name_norm.split()
                if len(parts) >= 2:
                    # Last name might be at end
                    candidate_name_lookup[parts[-1]] = row.id
                    # First + Last name (skip middle initial/name)
                    # e.g., "CONNOR P ROCHE" -> "CONNOR ROCHE"
                    if len(parts) >= 3:
                        first_last = f"{parts[0]} {parts[-1]}"
                        candidate_name_lookup[first_last] = row.id
                        # Also handle two-word last names
                        candidate_name_lookup[f"{parts[-2]} {parts[-1]}"] = row.id

        logger.info(
            f"Pass 3: Built lookup with {len(candidate_name_lookup)} candidate name variants"
        )

        linked_by_fuzzy = 0
        for row in session.execute(
            select(GoldCommittee).where(
                GoldCommittee.state_committee_id.isnot(None),
                GoldCommittee.candidate_id.is_(None),
                GoldCommittee.committee_type == "CANDIDATE",
            )
        ):
            committee = row[0]
            extracted_name = _extract_candidate_name_from_committee(committee.name)

            if extracted_name and extracted_name in candidate_name_lookup:
                committee.candidate_id = candidate_name_lookup[extracted_name]
                linked_by_fuzzy += 1

        session.commit()
        logger.info(f"Pass 3: Linked {linked_by_fuzzy} committees by fuzzy name matching")

        # Pass 4: Reverse lookup - use committee_name from silver_md_candidate
        # to find the committee and link it to the candidate
        # This catches cases where the committee name in silver doesn't match
        # the gold committee name format
        committee_name_to_id = {}
        for row in session.execute(
            select(GoldCommittee.id, GoldCommittee.name).where(
                GoldCommittee.state_committee_id.isnot(None),
                GoldCommittee.candidate_id.is_(None),
            )
        ):
            if row.name:
                # Store both the original and extracted name for matching
                name_norm = row.name.strip().upper()
                committee_name_to_id[name_norm] = row.id
                # Also extract candidate name and use it as a key
                extracted = _extract_candidate_name_from_committee(row.name)
                if extracted:
                    committee_name_to_id[extracted] = row.id

        logger.info(f"Pass 4: Built committee lookup with {len(committee_name_to_id)} entries")

        linked_by_reverse = 0
        for row in session.execute(
            select(
                SilverMarylandCandidate.source_content_hash,
                SilverMarylandCandidate.committee_name,
                SilverMarylandCandidate.name,
            ).where(
                SilverMarylandCandidate.committee_name.isnot(None),
                SilverMarylandCandidate.committee_name != "",
            )
        ):
            state_candidate_id = row.source_content_hash
            committee_name = row.committee_name
            candidate_name = row.name

            if state_candidate_id not in candidate_id_map:
                continue

            gold_candidate_id = candidate_id_map[state_candidate_id]

            # Try to find the committee by multiple methods
            committee_id = None

            # Method 1: Direct name match (normalized)
            name_norm = committee_name.strip().upper() if committee_name else ""
            if name_norm in committee_name_to_id:
                committee_id = committee_name_to_id[name_norm]

            # Method 2: Try candidate name as extracted from committee
            if not committee_id and candidate_name:
                candidate_norm = _normalize_candidate_name(candidate_name)
                if candidate_norm in committee_name_to_id:
                    committee_id = committee_name_to_id[candidate_norm]

            if committee_id:
                # Update the committee with the candidate_id
                session.execute(
                    GoldCommittee.__table__.update()
                    .where(GoldCommittee.id == committee_id)
                    .values(candidate_id=gold_candidate_id)
                )
                # Remove from lookup so we don't link it again
                if name_norm in committee_name_to_id:
                    del committee_name_to_id[name_norm]
                linked_by_reverse += 1

        session.commit()
        logger.info(f"Pass 4: Linked {linked_by_reverse} committees by reverse lookup")

        total_linked = linked_by_ccf + linked_by_name + linked_by_fuzzy + linked_by_reverse
        logger.info(f"Total linked: {total_linked} committees to candidates")

        return {
            "total_candidates_with_committees": len(ccf_to_candidate) + len(name_to_candidate),
            "linked_by_ccf": linked_by_ccf,
            "linked_by_name": linked_by_name,
            "linked_by_fuzzy": linked_by_fuzzy,
            "linked_by_reverse": linked_by_reverse,
            "committees_linked": total_linked,
        }


@task(
    name="transform_md_contributions_to_gold",
    description="Transform Maryland contributions to gold fact table",
    retries=MD_GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_GOLD_TASK_TIMEOUT * 2,  # 1 hour for large datasets
)
def transform_md_contributions_task(
    chunksize: int = 10_000,
) -> dict[str, Any]:
    """
    Transform Maryland silver contributions to gold fact table.

    Creates contribution records with FK resolution to:
    - GoldContributor (resolved by normalized key)
    - GoldCommittee (resolved by state_committee_id = CCF ID)

    Args:
        chunksize: Number of records to process per chunk

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting Maryland contribution transformation to gold layer")

    # Build FK lookup caches
    with get_session() as cache_session:
        logger.info("Building FK lookup caches...")

        # Cache contributors
        contributor_cache = {}
        for contributor in cache_session.execute(
            select(
                GoldContributor.id,
                GoldContributor.name,
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.employer,
            )
        ):
            key = _normalize_md_contributor_key(
                contributor.name,
                contributor.city,
                contributor.state,
                contributor.employer,
                None,
            )
            contributor_cache[key] = contributor.id
        logger.info(f"Cached {len(contributor_cache)} contributors")

        # Cache committees (by state_committee_id for MD)
        committee_cache = {}
        for committee in cache_session.execute(
            select(GoldCommittee.id, GoldCommittee.state_committee_id)
        ).all():
            if committee.state_committee_id:
                committee_cache[committee.state_committee_id] = committee.id
        logger.info(f"Cached {len(committee_cache)} committees with state_committee_id")

    # Track contributions inserted this run
    contributions_inserted: set[str] = set()

    # Process in chunks
    total_processed = 0
    loaded_count = 0
    unresolved_contributors = 0
    unresolved_committees = 0
    chunks_processed = 0
    last_id = 0

    while True:
        with get_session() as session:
            # Get contributions not yet in gold
            subquery = (
                select(GoldContribution.source_sub_id)
                .where(
                    GoldContribution.source_system == "MARYLAND",
                    GoldContribution.source_sub_id
                    == SilverMarylandContribution.source_content_hash,
                )
                .exists()
            )
            stmt = select(SilverMarylandContribution).where(~subquery)

            # Cursor-based pagination
            stmt = stmt.where(SilverMarylandContribution.id > last_id)
            stmt = stmt.order_by(SilverMarylandContribution.id).limit(chunksize)

            chunk_df = pd.read_sql(stmt, session.connection())

            if chunk_df.empty:
                break

            chunk_size = len(chunk_df)
            total_processed += chunk_size
            last_id = int(chunk_df["id"].iloc[-1])

            # Resolve contributor FK
            chunk_df["contributor_key"] = chunk_df.apply(
                lambda r: _normalize_md_contributor_key(
                    r["contributor_name"],
                    r["contributor_city"],
                    r["contributor_state"],
                    r["employer_name"],
                    r["contributor_type"],
                ),
                axis=1,
            )
            chunk_df["contributor_id_gold"] = chunk_df["contributor_key"].map(contributor_cache)

            # Resolve committee FK (using CCF ID)
            chunk_df["committee_id_gold"] = chunk_df["committee_ccf_id"].map(committee_cache)

            # Track unresolved
            unresolved_contributors += chunk_df["contributor_id_gold"].isna().sum()
            unresolved_committees += chunk_df["committee_id_gold"].isna().sum()

            # Filter to valid records
            valid_chunk = chunk_df[
                chunk_df["contributor_id_gold"].notna() & chunk_df["committee_id_gold"].notna()
            ].copy()

            # Filter out already inserted
            valid_chunk = valid_chunk[
                ~valid_chunk["source_content_hash"].isin(contributions_inserted)
            ]

            # Insert contributions
            if not valid_chunk.empty:
                contributions = []
                for _, row in valid_chunk.iterrows():
                    # Extract year from filing_period (e.g., "2025 Annual" -> 2025)
                    filing_period = row.get("filing_period", "")
                    election_year = 2024  # Default
                    if filing_period:
                        year_match = re.search(r"(\d{4})", str(filing_period))
                        if year_match:
                            election_year = int(year_match.group(1))

                    contribution = GoldContribution(
                        contributor_id=int(row["contributor_id_gold"]),
                        recipient_committee_id=int(row["committee_id_gold"]),
                        recipient_candidate_id=None,  # MD doesn't have direct candidate link
                        contribution_date=row["contribution_date"],
                        amount=row["contribution_amount"],
                        contribution_type=row.get("contribution_type") or "DIRECT",
                        election_type=None,  # MD doesn't have election type in contribution data
                        source_system="MARYLAND",
                        source_transaction_id=None,  # MD doesn't have transaction IDs
                        source_sub_id=row["source_content_hash"],
                        election_year=election_year,
                        election_cycle=election_year,
                        memo_text=None,
                    )
                    contributions.append(contribution)

                session.add_all(contributions)
                session.commit()
                loaded_count += len(contributions)

                contributions_inserted.update(valid_chunk["source_content_hash"].values)

            chunks_processed += 1

            if chunks_processed % 10 == 0:
                logger.info(
                    f"Progress: {chunks_processed} chunks, "
                    f"{total_processed:,} processed, "
                    f"{loaded_count:,} inserted"
                )

    logger.info(f"Completed: {chunks_processed} chunks, {total_processed:,} contributions")
    logger.info(f"Loaded {loaded_count} new contributions to gold")

    if unresolved_contributors > 0:
        logger.warning(f"Unresolved contributors: {unresolved_contributors}")
    if unresolved_committees > 0:
        logger.warning(f"Unresolved committees: {unresolved_committees}")

    return {
        "total_contributions": total_processed,
        "loaded_count": loaded_count,
        "unresolved_contributors": int(unresolved_contributors),
        "unresolved_committees": int(unresolved_committees),
        "chunks_processed": chunks_processed,
    }


@task(
    name="validate_md_gold_transformation",
    description="Validate Maryland gold layer transformation results",
)
def validate_md_gold_transformation_task(
    contributor_stats: dict[str, Any],
    committee_stats: dict[str, Any],
    candidate_stats: dict[str, Any],
    contribution_stats: dict[str, Any],
) -> dict[str, Any]:
    """
    Validate Maryland gold layer transformation results.

    Args:
        contributor_stats: Statistics from contributor transformation
        committee_stats: Statistics from committee transformation
        candidate_stats: Statistics from candidate transformation
        contribution_stats: Statistics from contribution transformation

    Returns:
        Dictionary with validation results
    """
    logger = get_logger()
    logger.info("Validating Maryland gold layer transformation")

    warnings = []
    errors = []

    # Validate contributors
    if (
        contributor_stats["new_contributors"] == 0
        and contributor_stats["total_silver_contributors"] > 0
    ):
        warnings.append("No new contributors added (may all exist already)")

    # Validate committees
    if committee_stats["loaded_count"] == 0 and committee_stats["total_committees"] > 0:
        warnings.append("No new committees added (may all exist already)")

    # Validate contributions
    total = contribution_stats["total_contributions"]
    if total > 0:
        unresolved_rate = (
            contribution_stats["unresolved_contributors"]
            + contribution_stats["unresolved_committees"]
        ) / total

        if unresolved_rate > 0.1:
            errors.append(f"High FK resolution failure rate: {unresolved_rate:.1%}")
        elif unresolved_rate > 0:
            warnings.append(
                f"Some contributions skipped due to unresolved FKs: {unresolved_rate:.1%}"
            )

    status = "error" if errors else "warning" if warnings else "success"

    if errors:
        for error in errors:
            logger.error(f"  - {error}")
    if warnings:
        for warning in warnings:
            logger.warning(f"  - {warning}")
    if not errors and not warnings:
        logger.info("✓ Validation passed")

    return {
        "validation_passed": len(errors) == 0,
        "status": status,
        "warnings": warnings,
        "errors": errors,
    }


@flow(
    name="maryland_gold_transformation",
    description="Transform Maryland silver layer data to gold layer",
    log_prints=True,
)
def maryland_gold_transformation_flow(
    chunksize: int = 10_000,
) -> dict[str, Any]:
    """
    Main flow to transform Maryland silver layer data to gold layer.

    Execution order:
    1. Transform contributors (with deduplication)
    2. Transform committees
    3. Transform candidates
    4. Transform contributions (with FK resolution)
    5. Validate results

    Args:
        chunksize: Records per chunk for contributions

    Returns:
        Dictionary with comprehensive transformation statistics
    """
    logger = get_logger()
    logger.info("=" * 60)
    logger.info("MARYLAND GOLD TRANSFORMATION FLOW")
    logger.info("=" * 60)

    # Step 1: Transform contributors
    logger.info("\nStep 1: Transforming contributors...")
    contributor_stats = transform_md_contributors_task()
    logger.info(
        f"✓ Contributors: {contributor_stats['new_contributors']} new "
        f"(from {contributor_stats['total_silver_contributors']} silver)"
    )

    # Step 2: Transform committees
    logger.info("\nStep 2: Transforming committees...")
    committee_stats = transform_md_committees_task()
    logger.info(f"✓ Committees: {committee_stats['loaded_count']} loaded")

    # Step 3: Transform candidates
    logger.info("\nStep 3: Transforming candidates...")
    candidate_stats = transform_md_candidates_task()
    logger.info(f"✓ Candidates: {candidate_stats['loaded_count']} loaded")

    # Step 4: Link committees to candidates
    logger.info("\nStep 4: Linking committees to candidates...")
    linkage_stats = link_md_committees_to_candidates_task()
    logger.info(f"✓ Linked: {linkage_stats['committees_linked']} committees to candidates")

    # Step 5: Transform contributions
    logger.info("\nStep 5: Transforming contributions...")
    contribution_stats = transform_md_contributions_task(chunksize=chunksize)
    logger.info(f"✓ Contributions: {contribution_stats['loaded_count']} loaded")

    if contribution_stats["unresolved_contributors"] > 0:
        logger.warning(
            f"⚠ Skipped {contribution_stats['unresolved_contributors']} "
            "due to unresolved contributors"
        )
    if contribution_stats["unresolved_committees"] > 0:
        logger.warning(
            f"⚠ Skipped {contribution_stats['unresolved_committees']} "
            "due to unresolved committees"
        )

    # Step 6: Validate
    logger.info("\nStep 6: Validating transformation...")
    validation_stats = validate_md_gold_transformation_task(
        contributor_stats=contributor_stats,
        committee_stats=committee_stats,
        candidate_stats=candidate_stats,
        contribution_stats=contribution_stats,
    )

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("MARYLAND GOLD TRANSFORMATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Contributors:  {contributor_stats['new_contributors']:,} new")
    logger.info(f"Committees:    {committee_stats['loaded_count']:,}")
    logger.info(f"Candidates:    {candidate_stats['loaded_count']:,}")
    logger.info(f"Linkages:      {linkage_stats['committees_linked']:,} committees linked")
    logger.info(f"Contributions: {contribution_stats['loaded_count']:,}")
    logger.info("=" * 60)

    if validation_stats["validation_passed"]:
        logger.info("✓ Validation: PASSED")
    else:
        logger.error("✗ Validation: FAILED")

    return {
        "contributor_stats": contributor_stats,
        "committee_stats": committee_stats,
        "candidate_stats": candidate_stats,
        "linkage_stats": linkage_stats,
        "contribution_stats": contribution_stats,
        "validation_stats": validation_stats,
        "success": validation_stats["validation_passed"],
    }
