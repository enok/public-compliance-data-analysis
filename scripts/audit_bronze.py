import json
import re
from pathlib import Path

# Parse log for SUCCESS entries
log_path = Path('docs/data_sources.log')
text = log_path.read_text(encoding='utf-8', errors='ignore')
blocks = [b.strip() for b in text.split('-'*50) if b.strip()]

success_months = set()
for block in blocks:
    if 'federal_transfers' not in block.lower():
        continue
    params_match = re.search(r'Params: (\{.*?\})', block)
    if not params_match:
        continue
    try:
        params = json.loads(params_match.group(1))
    except:
        continue
    ma = params.get('mesAnoInicio')
    if not ma:
        continue
    mm, yy = ma.split('/')
    month_key = f'{yy}_{mm}'
    if 'Status: SUCCESS' in block:
        success_months.add(month_key)

# S3 data files (from aws s3 ls output)
s3_data = {
    '2013_12', '2014_01', '2014_02', '2014_03', '2014_04', '2014_05', '2014_06',
    '2014_07', '2014_08', '2014_09', '2014_10', '2014_11', '2014_12', '2015_01',
    '2015_02', '2015_05', '2015_06', '2015_07', '2015_08', '2015_09', '2015_10',
    '2015_11', '2015_12', '2016_01', '2016_02', '2016_03', '2016_04', '2016_05',
    '2016_06', '2016_08', '2016_09', '2016_10', '2016_11', '2016_12', '2017_01',
    '2017_02', '2017_03', '2017_04', '2017_05', '2017_06', '2017_07', '2017_08',
    '2017_09', '2017_10', '2017_11'
}

# Cross-check
print('=' * 60)
print('BRONZE LAYER AUDIT REPORT - Federal Transfers')
print('=' * 60)
print()
print(f'Log SUCCESS entries: {len(success_months)}')
print(f'S3 data files:       {len(s3_data)}')
print()

# Perfect match check
if success_months == s3_data:
    print('[OK] PERFECT MATCH: Log and S3 are in sync')
else:
    in_log_not_s3 = success_months - s3_data
    in_s3_not_log = s3_data - success_months
    
    if in_log_not_s3:
        print(f'[WARNING] In LOG but NOT in S3 ({len(in_log_not_s3)}):')
        print('  ', ' '.join(sorted(in_log_not_s3)))
    
    if in_s3_not_log:
        print(f'[WARNING] In S3 but NOT in LOG ({len(in_s3_not_log)}):')
        print('  ', ' '.join(sorted(in_s3_not_log)))

print()
print('=' * 60)
print('COVERAGE BY YEAR')
print('=' * 60)
by_year = {}
for m in sorted(s3_data):
    year = m.split('_')[0]
    month = m.split('_')[1]
    by_year.setdefault(year, []).append(month)

for year in sorted(by_year.keys()):
    months = sorted(by_year[year])
    print(f'{year}: {len(months):2d} months - {" ".join(months)}')

print()
print('=' * 60)
print('GAPS ANALYSIS')
print('=' * 60)
for year in sorted(by_year.keys()):
    months = sorted([int(m) for m in by_year[year]])
    gaps = []
    for i in range(1, 13):
        if i not in months:
            gaps.append(f'{i:02d}')
    if gaps:
        print(f'{year}: Missing months {" ".join(gaps)}')
    else:
        print(f'{year}: Complete (all 12 months)')

print()
print('=' * 60)
print('SUMMARY FOR THESIS')
print('=' * 60)
print(f'Total months available: {len(s3_data)}')
print(f'Year range target: 2010-2022')
print(f'Expected complete years: 2010 through 2022')
print(f'Any partial year indicates missing intercensal monthly coverage')
