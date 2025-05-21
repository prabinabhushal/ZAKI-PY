#Extracting the folder of compressed file(.gz)
import json
import gzip
from pathlib import Path

def merge(folder):
    output_dir = Path('output_nrpr')
    output_dir.mkdir(exist_ok=True)

    nrpr_path = output_dir / 'combined_nrpr.json'
    gz_folder = Path(folder)  

    with open(nrpr_path, 'w', encoding='utf-8') as nrpr_file:
        for file in gz_folder.glob('*.json.gz'):
            with gzip.open(file, 'rt', encoding='utf-8') as f:
                data = json.load(f)
                json.dump(data, nrpr_file)
                nrpr_file.write('\n')  
    return nrpr_path
