import ijson
import zipfile
import json
from decimal import Decimal
from pathlib import Path
from io import BytesIO
import gzip

def merge(zip_file,pdetail_file):
    output_dir = Path('output_nrpr')
    output_dir.mkdir(exist_ok=True)

    nrpr_path = output_dir / 'combined_nrpr.json'
    pro_path= output_dir/'add_provider'

    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)

    with open(nrpr_path,'w') as nrpr_file:
            with zipfile.ZipFile(zip_file, 'r') as outer_zip:
                for name in outer_zip.namelist():
                    if name.endswith('.json.gz'):
                        with outer_zip.open(name) as zipped_file:
                            data = zipped_file.read()
                            with gzip.open(BytesIO(data), 'rt', encoding='utf-8') as f:
                                for item in ijson.items(f, 'in_network.item'):
                                    item['source'] = 'in_network'
                                    nrpr_file.write(json.dumps(item, default=con_decimal) + '\n')

    with zipfile.ZipFile(pdetail_file, 'r') as z:
        for name in z.namelist():
            if name.endswith('.parquet'):
                with z.open(name, 'r') as f:
                    with open(pro_path, 'wb') as f1:
                        f1.write(f.read())


    return nrpr_path,pro_path
