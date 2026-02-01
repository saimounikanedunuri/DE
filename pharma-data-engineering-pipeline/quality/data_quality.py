def check_dataset_not_empty(df):
    if len(df.head(1)) == 0:
        raise ValueError("Landing quality failed | dataset is empty")

    print("Landing quality check passed | dataset is not empty")
