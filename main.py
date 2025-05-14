# import sys
from ETL import ExtractTransferLoad
def main ():
    #  zip_file = sys.argv[1]
 
    etl = ExtractTransferLoad()
    etl.run()

if __name__ == "__main__":
    main()
