import pandas as pd
import time

def main():
    start = time.perf_counter()
    df = pd.read_csv('./customers-100.csv')
    end = time.perf_counter()
    print(round(end-start, 6))

if __name__ == '__main__':
    main()