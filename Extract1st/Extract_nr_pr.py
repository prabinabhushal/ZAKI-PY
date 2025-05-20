import ijson
import zipfile
import json
from decimal import Decimal
from pathlib import Path

def extract(zip_file):
    script_dir = Path(__file__).resolve().parent

    output_dir = script_dir.parent / 'output'

    output_dir.mkdir(exist_ok=True)      
    net_path = output_dir / 'in_network.json'
    pro_path = output_dir / 'provider_table.json'

    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)


    with zipfile.ZipFile(zip_file, 'r') as z:
        for name in z.namelist():
            if name.endswith('.json'):
                with z.open(name, 'r') as f:
                    with open(pro_path, 'w') as f1:
                        for item in ijson.items(f, 'provider_references.item'):
                            f1.write(json.dumps(item,default=con_decimal) + '\n')
                
                    f.seek(0)

                    with open(net_path, 'w') as f2:
                        for item in ijson.items(f, 'in_network.item'):
                            f2.write(json.dumps(item,default=con_decimal) + '\n')

    return net_path, pro_path
