import pandas as pd

ce0 = pd.read_csv('../data/first24chartevents.csv')
ce1 = pd.read_csv('../data/chartevents.csv')
le0 = pd.read_csv('../data/first24labevents.csv')
le1 = pd.read_csv('../data/labevents.csv')

ce0_grouped = ce0.drop(['icustay_id', 'charthour'], axis=1).groupby(ce0['icustay_id'])
ce0_max = ce0_grouped.max().rename(columns=lambda x: x + '_max')
ce0_min = ce0_grouped.min().rename(columns=lambda x: x + '_min')
result = pd.concat([ce0, ce0_max, ce0_min], axis=1)
print(ce0_hr_max)
