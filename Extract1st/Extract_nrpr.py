#Extracting the folder of compressed file(.gz)
import json
import ijson
import gzip
from decimal import Decimal
from pathlib import Path

def merge(folder):
    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)
    output_dir = Path('output_nrpr')
    output_dir.mkdir(exist_ok=True)

    nrpr_path = output_dir / 'combined_nrpr.json'
    gz_folder = Path(folder)  

    with open(nrpr_path, 'w') as nrpr_file:
        for file in gz_folder.glob('*.json.gz'):
            with gzip.open(file, 'rt') as f:
                for item in ijson.items(f, ''):
                    nrpr_file.write(json.dumps(item, default=con_decimal) + '\n')
    return nrpr_path