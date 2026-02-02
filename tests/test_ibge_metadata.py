import json
import requests
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_ibge_metadata_urls(config_path="config/ibge_metadata.json"):
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    base_url = config['api_base_url']
    results = []

    # Test probe: Requesting only 1 municipality to save time/bandwidth
    # Code 1100015 = Alta Floresta D'Oeste - RO
    geo_probe = "n6/1100015"

    print(f"{'Dataset Name':<30} | {'Status':<10} | {'Reason'}")
    print("-" * 60)

    for ds in config['datasets']:
        name = ds['name']
        table = ds['table_id']
        var = ds.get('variable', 'allxp')
        period = str(ds['period']).replace(" ", "%20")
        classif = ds.get('classifications', '')

        # Constructing the probe URL
        url = f"{base_url}/t/{table}/{geo_probe}/v/{var}/p/{period}"
        if classif:
            url += f"/{classif}"
        url += "?formato=json"

        try:
            # Short timeout since we are only fetching 1 row
            response = requests.get(url, timeout=15)

            if response.status_code == 200:
                data = response.json()
                # Sidra returns a list; index 0 is header, index 1 is the data
                if isinstance(data, list) and len(data) > 1:
                    status = "PASS"
                    reason = "Valid Data Found"
                else:
                    status = "WARN"
                    reason = "Empty Result (Check Period/Variable)"
            else:
                status = "FAIL"
                reason = f"HTTP {response.status_code}"

        except Exception as e:
            status = "FAIL"
            reason = str(e)

        print(f"{name:<30} | {status:<10} | {reason}")
        results.append((name, status == "PASS"))

    return results

if __name__ == "__main__":
    # Ensure this points to your actual JSON file
    test_ibge_metadata_urls("config/ibge_metadata.json")