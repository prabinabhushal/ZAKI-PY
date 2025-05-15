# import sys
import logging
import argparse
from ETL import ExtractTransferLoad

def main ():
    #  zip_file = sys.argv[1]
    logging.basicConfig(filename='etl.log',
    level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("getLogger")

    parser = argparse.ArgumentParser(description="ETL for ZIP file or api for planet sort")
    parser.add_argument("--zip_file",help="Full path to the ZIP file to process")

    args = parser.parse_args()
    etl = ExtractTransferLoad()
    etl.run(args,logger)
 
if __name__ == "__main__":
    main()
