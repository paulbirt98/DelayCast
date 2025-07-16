import pandas as pd

df = pd.read_csv('eus_liv_joined.csv')

df['Scheduled Time'] = pd.to_datetime(df['Scheduled Time'])

#define end dates for training and vlaidation sets
training_end_date = '2021-05-31'
validation_end_date = '2023-05-31'

training_df = df[df['Scheduled Time'] <= training_end_date]
validation_df = df[(df['Scheduled Time'] > training_end_date ) & (df['Scheduled Time'] <= validation_end_date)]
test_df = df[df['Scheduled Time'] > validation_end_date]

print("Train:", training_df.shape)
print("Validation:", validation_df.shape)
print("Test:", test_df.shape)


training_df.to_csv("train_data.csv", index=False)
validation_df.to_csv("validation_data.csv", index=False)
test_df.to_csv("test_data.csv", index=False)


