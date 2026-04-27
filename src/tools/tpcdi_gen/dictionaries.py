"""Dictionary data loading for TPC-DI data generation.

Loads name, address, and lookup dictionaries from the datagen dict files
bundled in the repository or available on the volume.

Dictionary Loading
-------------------
Dictionaries are flat text files (one entry per line) used to populate realistic
values for names, addresses, employers, industries, etc. They originate from the
TPC-E data generator (PDGF) and are stored in the tpc-e/ subdirectory.

Each dictionary file may have one of two formats:
  1. Plain: one value per line (e.g. "Smith")
  2. Indexed: tab-separated "index\\tvalue" per line (e.g. "1\\tSmith")
The _load_dict_file() function auto-detects and strips index prefixes.

Loaded dictionaries are returned as a Python dict mapping logical names
(e.g. "last_names", "address_lines") to Python lists of strings. These
lists are then registered as Spark temp views by register_dict_views()
in utils.py, enabling broadcast joins for Photon-compatible, UDF-free
dictionary lookups.

Fallback Strategy
------------------
If dictionary files cannot be found at any search path, _fallback_dicts()
returns minimal embedded dictionaries hardcoded in this module. These
fallbacks produce valid output but with less variety than the full TPC-E
dictionaries (e.g. 40 last names instead of 13,483). The fallback is
primarily useful for local development and testing outside Databricks.

Search Path Resolution
-----------------------
load_all() searches for dictionary files in this order:
  1. Explicit dict_dir parameter (if provided by caller)
  2. Relative to this source file: ./dicts/tpc-e/
  3. Relative to this source file: ../datagen/pdgf/dicts/tpc-e/ (repo layout)
  4. UC Volume path: /Volumes/.../spark_datagen/_module/tpcdi_gen/dicts/tpc-e/
  5. Workspace Repos paths (may fail on DBR 18+ CLASSIC_PREVIEW)
The first path containing a valid last_names.dict file wins.

HR-Specific Dictionaries
--------------------------
The HR module uses larger name dictionaries than CustomerMgmt:
  - Family-Names.dict (13,483 entries) loaded as "hr_family_names"
  - Given-Names.dict (8,608 entries) loaded as "hr_given_names"
These reside in the parent directory of tpc-e/ (i.e. pdgf/dicts/) and are
loaded by _load_hr_dict() which checks both the parent and tpc-e/ directories.
"""

import os


def _load_dict_file(path: str) -> list:
    """Load a dictionary file, stripping index prefixes if present.

    Handles two formats:
      - Plain: "Smith\\n" -> ["Smith"]
      - Indexed: "1\\tSmith\\n" -> ["Smith"] (tab-separated index prefix stripped)

    Args:
        path: Absolute path to the dictionary file.

    Returns:
        List of string values, one per non-empty line.
    """
    with open(path, "r") as f:
        lines = [l.strip() for l in f if l.strip()]
    # Strip "1\tValue" format if first line looks like "digit\\tvalue"
    if lines and "\t" in lines[0] and lines[0].split("\t")[0].isdigit():
        lines = [l.split("\t", 1)[1] for l in lines if "\t" in l]
    return lines


def load_all(dict_dir: str = None) -> dict:
    """Load all dictionaries. Returns a dict of name -> list.

    Tries multiple locations for the dict files:
    1. Explicit dict_dir parameter
    2. Workspace Repos path
    3. Relative to this file (local development)

    The returned dict maps logical names to lists of strings:
      - "first_names": customer first names (from first_names.dict or Given-Names.dict)
      - "last_names": customer last names
      - "address_lines": street addresses
      - "cities", "provinces", "zip_codes": geographic data
      - "employers": company names for Prospect employer field
      - "mail_providers": email domain names
      - "industry_ids/names/sc_ids": industry classification codes
      - "taxrate_ids/names/rates": tax rate reference data
      - "countries": fixed list ["United States of America", "Canada"]
      - "hr_family_names": larger name dict for HR (Family-Names.dict, 13483 entries)
      - "hr_given_names": larger name dict for HR (Given-Names.dict, 8608 entries)

    Args:
        dict_dir: Optional explicit path to the tpc-e/ dictionary directory.

    Returns:
        dict mapping logical dictionary names to lists of string values.
    """
    # Build ordered list of candidate paths to search
    search_paths = []
    if dict_dir:
        search_paths.append(dict_dir)

    # Relative to this source file (works when module is in UC Volume or local)
    this_dir = os.path.dirname(os.path.abspath(__file__))
    search_paths.append(os.path.join(this_dir, "dicts", "tpc-e"))
    search_paths.append(os.path.join(this_dir, "..", "datagen", "pdgf", "dicts", "tpc-e"))

    # UC Volume path (reliable across all DBR versions)
    search_paths.append("/Volumes/main/tpcdi_raw_data/tpcdi_volume/spark_datagen/_module/tpcdi_gen/dicts/tpc-e")

    # Workspace paths (may fail on DBR 18+ CLASSIC_PREVIEW due to /Workspace restrictions)
    search_paths.extend([
        "/Workspace/Repos/shannon.barrow@databricks.com/databricks-tpc-di/src/tools/datagen/pdgf/dicts/tpc-e",
        "/Workspace/Users/shannon.barrow@databricks.com/tpc-di/tpcdi_gen/dicts/tpc-e",
    ])

    # Find first valid path: must be a directory containing last_names.dict.
    # OSError is caught because /Workspace paths raise OSError on newer DBR versions.
    base = None
    for p in search_paths:
        try:
            if os.path.isdir(p) and os.path.exists(os.path.join(p, "last_names.dict")):
                base = p
                break
        except OSError:
            continue

    # Fall back to minimal embedded dictionaries if no files found
    if base is None:
        print("WARNING: Could not find dictionary files. Using embedded fallbacks.")
        return _fallback_dicts()

    print(f"Loading dictionaries from: {base}")

    def ld(fn):
        """Shorthand to load a dict file from the resolved base path."""
        return _load_dict_file(os.path.join(base, fn))

    return {
        "first_names": ld("first_names.dict") if os.path.exists(os.path.join(base, "first_names.dict")) else ld("Given-Names.dict"),
        "last_names": ld("last_names.dict"),
        "address_lines": ld("address_line1.dict"),
        "cities": ld("cities.dict"),
        "provinces": ld("provinces.dict"),
        "employers": ld("employers.dict"),
        "mail_providers": ld("mail_provider.dict"),
        "zip_codes": ld("zip_code_code.dict"),
        "industry_ids": ld("industry_id.dict"),
        "industry_names": ld("industry_name.dict"),
        "industry_sc_ids": ld("industry_sc_id.dict"),
        "taxrate_ids": ld("taxrate_id.dict"),
        "taxrate_names": ld("taxrate_name.dict"),
        "taxrate_rates": ld("taxrate_rate.dict"),
        "countries": ["United States of America", "Canada"],
        # HR uses larger name dicts from pdgf/dicts/ (parent of tpc-e/)
        "hr_family_names": _load_hr_dict(base, "Family-Names.dict"),
        "hr_given_names": _load_hr_dict(base, "Given-Names.dict"),
    }


def _load_hr_dict(tpc_e_base: str, filename: str) -> list:
    """Load HR-specific dict from pdgf/dicts/ (parent of tpc-e/).

    HR name dictionaries (Family-Names.dict, Given-Names.dict) live in the
    parent directory of the tpc-e/ dict folder. This function checks both
    the parent directory and the tpc-e/ directory itself as a fallback.

    Args:
        tpc_e_base: Path to the tpc-e/ dictionary directory.
        filename: Name of the dictionary file (e.g. "Family-Names.dict").

    Returns:
        List of name strings, or empty list if file not found.
    """
    # Primary: check parent directory (pdgf/dicts/)
    parent = os.path.dirname(tpc_e_base)
    path = os.path.join(parent, filename)
    if os.path.exists(path):
        return _load_dict_file(path)
    # Fallback: check within tpc-e/ itself
    path2 = os.path.join(tpc_e_base, filename)
    if os.path.exists(path2):
        return _load_dict_file(path2)
    print(f"  WARNING: {filename} not found")
    return []


def _fallback_dicts() -> dict:
    """Minimal embedded dictionaries for when files aren't found.

    Returns hardcoded dictionary values sufficient to produce valid TPC-DI
    output, but with significantly less variety than the full TPC-E dictionaries.
    Primarily used for local development and testing outside Databricks.

    Coverage: 40 first/last names, 10K addresses, 50 cities, 60 provinces,
    20 employers, 19 mail providers, ~90K zip codes, 20 industry codes, ~20 tax rates.
    """
    return {
        "first_names": ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda","David","Elizabeth","William","Barbara","Richard","Susan","Joseph","Jessica","Thomas","Sarah","Charles","Karen","Christopher","Lisa","Daniel","Nancy","Matthew","Betty","Anthony","Margaret","Mark","Sandra","Donald","Ashley","Steven","Kimberly","Paul","Emily","Andrew","Donna","Joshua","Michelle"],
        "last_names": ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker","Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores"],
        "address_lines": [f"{n} {s}" for n in range(100, 30000, 3) for s in ["Main St","Oak Ave","Pine Rd","Elm Dr","Cedar Ln","Maple Ct","Birch Way","Spruce Cir","Willow Pl","Ash Blvd"]][:10000],
        "cities": ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia","San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville","Fort Worth","Columbus","Charlotte","Indianapolis","San Francisco","Seattle","Denver","Washington","Nashville","Oklahoma City","El Paso","Boston","Portland","Las Vegas","Memphis","Louisville","Baltimore","Milwaukee","Albuquerque","Tucson","Fresno","Mesa","Sacramento","Atlanta","Kansas City","Colorado Springs","Omaha","Raleigh","Long Beach","Virginia Beach","Miami","Oakland","Minneapolis","Tampa","Tulsa","Arlington","New Orleans","St. Paul"],
        "provinces": ["AK","AL","AR","AZ","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID","IL","IN","KS","KY","LA","MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY","Ontario","Quebec","Nova Scotia","New Brunswick","Manitoba","British Columbia","Prince Edward Island","Saskatchewan","Alberta","Newfoundland"],
        "employers": ["Exxon Mobil","Wal-Mart Stores","Chevron","General Electric","Microsoft","Google","Amazon.com","Apple","IBM","Intel","Cisco Systems","Oracle","Hewlett-Packard","AT&T","Verizon","Bank of America","JPMorgan Chase","Wells Fargo","Citigroup","Goldman Sachs"],
        "mail_providers": ["gmail.com","yahoo.com","hotmail.com","aol.com","outlook.com","icloud.com","mail.com","gmx.com","protonmail.com","zoho.com","hush.ai","wildmail.com","moose-mail.com","snail-mail.net","deadaddress.com","gmx.ph","mac.hush.com","gawab.com","ip6.li"],
        "zip_codes": [str(z) for z in range(10001, 99999)],
        "industry_ids": ["AA","AC","AD","AE","AM","AP","AR","AT","AV","BA","BC","BD","BN","BS","CA","CC","CD","CE","CF","CG"],
        "industry_names": ["Apparel/Accessories","Air Courier","Aerospace & Defense","Auto/Truck Parts","Airlines","Auto Parts","Automotive","Agriculture","Audio/Video","Banks","Biotechnology","Broadcasting","Business Services","Building Materials","Casinos","Chemicals","Containers","Comp Electronics","Closed-end Funds","Conglomerates"],
        "industry_sc_ids": ["CC","TR","CG","SV","CN","BM","TC","FN","CO","UT","TC","CO","SV","BM","SV","BM","CG","TC","FN","CG"],
        "taxrate_ids": [f"US{i}" for i in range(1, 6)] + [f"CN{i}" for i in range(1, 5)] + [f"CA{i}" for i in range(1, 7)] + [f"NY{i}" for i in range(1, 6)],
        "taxrate_names": [f"Tax Rate {i}" for i in range(1, 20)],
        "taxrate_rates": ["0.10000","0.15000","0.25000","0.28000","0.33000","0.15000","0.22000","0.26000","0.29000","0.01000","0.02000","0.04000","0.06000","0.08000","0.09300","0.01000","0.04000","0.06500","0.08500"],
        "countries": ["United States of America", "Canada"],
    }
