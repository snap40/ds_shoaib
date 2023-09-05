from adsquery.ADSQuery import ADSQuery
import pandas as pd
from pipelines.config.config import pipeline_config 
from pipelines.utils.data_fetch import get_valid_orgs
import os
from pathlib import Path


if aws_region := os.getenv("AWS_REGION"):
        region_code = aws_region.split("-")[0].upper()
else:
    raise ValueError("AWS_REGION expected as environment variable")

config = pipeline_config()

test_patients = pd.read_csv(config.test_patient_s3_uri)["patient_id"]


org_ids = get_valid_orgs(
        config.license_s3_uri,
        region_code=region_code,
        sheetname=config.license_sheetname,
    )

# org_ids=set(list(org_ids)[:2])
org_ids_sql = "'" + "','".join(org_id for org_id in org_ids) + "'"
print(org_ids_sql)

def admissions_query(adsq: ADSQuery, org_ids: str) -> pd.DataFrame:
    """Get admissions data.

    Args:
      adsq: ADSQuery:
      org_ids: str: A list of organisation ids that are active
      or present in the license count file.

    Returns:
        pd.DataFrame: return pandas dataframe with admissions data

    """

    query = """--sql
    SELECT
        ad.organisation_id,
        ad.patient_id,
        prof.name AS profile_name,
        loc.name as location_name
    FROM
        reporting.device_management_federated.reporting_admission ad
        LEFT JOIN reporting.organisation_management_federated.reporting_location loc ON loc.location_id = ad.location_id
        LEFT JOIN organisation_management_federated.patient_profile prof ON prof.id = ad.profile_id
    WHERE ad.organisation_id IN ({org_ids})
    """

    query = query.format(org_ids=org_ids)
    return adsq.query_redshift_and_fetch_results(query)

adsq = ADSQuery(region_name=aws_region)


profile_pathway = admissions_query(adsq,org_ids_sql)

# data_path = Path("data/")
# for file in data_path.glob("*"):
#      file.unlink()
     

profile_pathway_excl_test = profile_pathway[~profile_pathway.patient_id.isin(test_patients)]
profile_pathway_excl_test.to_csv(f"data/profile_pathway_{aws_region}.csv")
profile_pathway_grp = profile_pathway_excl_test.groupby("profile_name").agg({"profile_name":"count"})
print(profile_pathway_grp)

profile_pathway_grp.to_csv(f"data/unique_profiles_{aws_region}.csv")
