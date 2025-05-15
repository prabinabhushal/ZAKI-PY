# import sys
import logging
import argparse
from ETL import ExtractTransferLoad
from api import show_ascending,show_descending

def main ():
    #  zip_file = sys.argv[1]
    logging.basicConfig(
    level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("getLogger")

    parser = argparse.ArgumentParser(description="ETL for ZIP file or api for planet sort")
    parser.add_argument("--zip_file",help="Full path to the ZIP file to process")
    parser.add_argument("--apiA",action='store_true', help="'apiA'for planet sort")
    parser.add_argument("--apiD", action='store_true', help="'apiD' for planet sort")

    args = parser.parse_args()

    if args.zip_file:
        etl = ExtractTransferLoad()
        etl.run(args,logger)

    elif args.apiA:
        show_ascending(logger)

    elif args.apiD:
        show_descending(logger)

    else:
        logger.info("Type --zip_file for ETL or --sort for planet sort")
 
if __name__ == "__main__":
    main()
