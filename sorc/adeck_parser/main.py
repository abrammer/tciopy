import adeck_parser
import pandas as pd

def main():
    import time
    stime = time.time()
    df = adeck_parser.parse_adeck\
("/home/abrammer/repos/tciopy/data/aal032\
023.dat")
    print(time.time() - stime)
    # print(df)


if __name__ == "__main__":
    main()
