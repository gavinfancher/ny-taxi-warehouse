'''Download 2020 NYC TLC trip data (Yellow Taxi + FHVHV).'''

import httpx
import pandas as pd
from pathlib import Path

BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
LOOKUP_URL = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
DATASETS = {
    'yellow': 'yellow_tripdata', 
    'fhvhv': 'fhvhv_tripdata'
}
YEAR = 2020


def main():
    for label, prefix in DATASETS.items():
        monthly_files = []
        for month in range(1, 13):
            filename = f'{prefix}_{YEAR}-{month:02d}.parquet'
            dest = Path(filename)
            if not dest.exists():
                print(f'Downloading {filename}...')
                resp = httpx.get(
                    f'{BASE_URL}/{filename}',
                    follow_redirects=True,
                    timeout=60
                )
                resp.raise_for_status()
                dest.write_bytes(resp.content)
            monthly_files.append(dest)

        df = pd.concat([pd.read_parquet(f) for f in monthly_files])
        output = f'{label}_trips_{YEAR}.parquet'
        df.to_parquet(output, compression='snappy')
        print(f'Wrote {output} ({len(df):,} rows)')

        for f in monthly_files:
            f.unlink()

    lookup = Path('taxi_zone_lookup.csv')
    if not lookup.exists():
        resp = httpx.get(LOOKUP_URL, follow_redirects=True, timeout=60)
        resp.raise_for_status()
        lookup.write_bytes(resp.content)
    print('Done.')


if __name__ == '__main__':
    main()
