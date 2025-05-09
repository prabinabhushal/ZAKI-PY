import sys
from Extract1st import Extractzip
from ScrubExplode2nd import Scrubzip
from LoadSQL import Loadzip

def main ():
    zip_file = sys.argv[1]

    net_path,pro_path= Extractzip.extract(zip_file)
    net_parquet,pro_parquet=Scrubzip.scrub(net_path,pro_path)
    Loadzip.load(net_parquet,pro_parquet)


if __name__ == "__main__":
    main()