import ijson
import zipfile
import os
import json
from decimal import Decimal
from pathlib import Path


def extract(zip_file):

    zip_path = Path(zip_file)
    output_path = zip_file
    network = []
    provider = []
    
    
    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)
    

    
    with zipfile.ZipFile(zip_file, 'r') as z:
                for name in z.namelist():
                    if name.endswith('.json'):
                        with z.open(name) as f:
                            for item in ijson.items(f, 'provider_references.item'):
                                provider.append(item)
    
                        with z.open(name) as f:
                            for item in ijson.items(f, 'in_network.item'):
                                network.append(item)
 

    net_path = output_path/ 'in_network.json'
    pro_path = output_path/ 'provider_table.json'

   
    with open(net_path, 'w') as f1:
        json.dump(network, f1, indent=4, default=con_decimal)
    
    with open(pro_path, 'w') as f2:
        json.dump(provider, f2, indent=4, default=con_decimal)

    return net_path,pro_path
    