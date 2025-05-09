import ijson
import zipfile
import os
import json
from decimal import Decimal


def extract(zip_file):

    network = []
    provider = []
    
    folder_path = '/home/prabina-bhushal/Desktop/PrabinaZaki/3ETL-prabinaBhushal/IgnoreFolder/MagnaCarePPO_In-Network11'
    
    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)
    
    for file in os.listdir(folder_path):
        if file.endswith('.zip'):
            zip_path = os.path.join(folder_path, file)
    
            with zipfile.ZipFile(zip_path, 'r') as z:
                for name in z.namelist():
                    if name.endswith('.json'):
                        with z.open(name) as f:
                            for item in ijson.items(f, 'provider_references.item'):
                                provider.append(item)
    
                        with z.open(name) as f:
                            for item in ijson.items(f, 'in_network.item'):
                                network.append(item)
    
    net_path = '/home/prabina-bhushal/Desktop/PrabinaZaki/3ETL-prabinaBhushal/IgnoreFolder/in_network.json'
    pro_path = '/home/prabina-bhushal/Desktop/PrabinaZaki/3ETL-prabinaBhushal/IgnoreFolder/provider_table.json'
    with open(net_path, 'w') as f1:
        json.dump(network, f1, indent=4, default=con_decimal)
    
    with open(pro_path, 'w') as f2:
        json.dump(provider, f2, indent=4, default=con_decimal)

    return net_path,pro_path
    