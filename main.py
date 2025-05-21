# import sys
import logging
import argparse
from ETL_nrpr import ExtractTransferLoad

def main ():
    #  zip_file = sys.argv[1]
    logging.basicConfig(filename='etl.log',
    level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("getLogger")

    parser = argparse.ArgumentParser(description="ETL for ZIP file or api for planet sort")
    # parser.add_argument("--zip",help="Full path to the ZIP file to process")
    parser.add_argument("--folder",help="Full path to the folder to process")
    parser.add_argument("--pd",help="New path of provider detail")
    parser.add_argument("--bc",help="New path of billing code")

    args = parser.parse_args()
    etl = ExtractTransferLoad()
    etl.run(args,logger)
 
if __name__ == "__main__":
    main()
