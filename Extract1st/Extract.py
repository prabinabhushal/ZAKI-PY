
# Extract

import ijson
import json
import zipfile
from decimal import Decimal 


def extract(zip_file):
    data=[]
    rate=[]

    def con_decimal(obj):
     if isinstance(obj, Decimal):
            return float(obj)

    with zipfile.ZipFile(zip_file, 'r') as z:
        for name in z.namelist():
            with z.open(name,'r') as f:
                    for item in ijson.items(f, 'in_network.item'):
                        data.append(item)
            with z.open(name,'r') as f1:
                        for item1 in ijson.items(f1, 'provider_references.item'):
                             rate.append(item1)

    with open('/home/prabina-bhushal/Desktop/PrabinaZaki/3ETL-prabinaBhushal/IgnoreFolder/in_network_data.json', 'w') as out_file1:
        json.dump(data, out_file1, default=con_decimal)
        f.write('\n')

    with open('/home/prabina-bhushal/Desktop/PrabinaZaki/3ETL-prabinaBhushal/IgnoreFolder/provider_references_data.json', 'w') as out_file2:
        json.dump(rate, out_file2)
        f1.write('\n')
