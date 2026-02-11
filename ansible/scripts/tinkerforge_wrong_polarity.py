import pandas as pd
import os


SOURCE_PATH='/storage/full/5GHAT/.flip-pol/'

def flip_polarity(input_csv: str, safe=True):

    assert(os.path.isfile(input_csv))
    df = pd.read_csv(input_csv)
    if safe:
        current_mean = df.loc[ df['TYPE']=='current', 'VAL'].mean()
        power_mean = df.loc[ df['TYPE']=='power', 'VAL'].mean()
        if current_mean > 0 or power_mean > 0:
            print('SKIP')
            return

    df.loc[ df['TYPE']=='current', 'VAL'] = df.loc[ df['TYPE']=='current', 'VAL'] * -1
    df.loc[ df['TYPE']=='power', 'VAL'] = df.loc[ df['TYPE']=='power', 'VAL'] * -1
    df.to_csv(input_csv, index=False)


if __name__ == "__main__":
    for e in os.listdir(SOURCE_PATH):
        if (os.path.isdir(f'{SOURCE_PATH}/{e}')
            and os.path.isfile(f'{SOURCE_PATH}/{e}/power_ue.csv')
            and os.path.getsize(f'{SOURCE_PATH}/{e}/power_ue.csv') > 1000
            and not os.path.isfile(f'{SOURCE_PATH}/{e}/FAILED')
            ):
            # print(e)
            file = f'{SOURCE_PATH}/{e}/power_ue.csv'
            # print(f'{e}  \t\t{file}')
            print(file)
            flip_polarity(f'{SOURCE_PATH}/{e}/power_ue.csv')
