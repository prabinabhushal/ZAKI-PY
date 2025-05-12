# import sys
import argparse
from Extract1st import Extractzip
from ScrubExplode2nd import Scrubzip
from LoadSQL import Loadzip

def main ():
    #  zip_file = sys.argv[1]
 
    parser = argparse.ArgumentParser(description="ETL for ZIP file with network and provider data.") #argumentParse object
    parser.add_argument("--zip_file",required=True, help="Full path to the ZIP file to process")  #optional argument
    args = parser.parse_args()
   

    net_path,pro_path= Extractzip.extract(args.zip_file)
    net_parquet,pro_parquet=Scrubzip.scrub(net_path,pro_path)
    Loadzip.load(net_parquet,pro_parquet)


if __name__ == "__main__":
    main()