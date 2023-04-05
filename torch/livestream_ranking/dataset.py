import os
import pandas as pd

def read_df(data_directory):
    df_list = []

    for i in os.listdir(data_directory):
        dfs = pd.read_csv(data_directory + "/" + i)
        df_list.append(dfs)

    main_df = pd.concat(df_list).reset_index(drop=True)
    main_df.rename(columns={'hostId': 'creatorId', 'memberId': 'userId'}, inplace=True)
    return main_df

def create_dataset(data_directory):
    main_df = read_df(data_directory)
    # 2023-04-03
    main_df["val"] = "0"
    main_df.loc[main_df["interaction_date"] == "2023-04-03", "val"] = "1"
    main_df.loc[main_df["interaction_date"] == "2023-04-02", "val"] = "2"
    print("train df shape: ", main_df[main_df["val"] == "0"].shape)
    print("val df shape: ", main_df[main_df["val"] == "1"].shape)
    print("gap df shape: ", main_df[main_df["val"] == "2"].shape)

    main_df['userIndex'] = main_df.groupby('userId').ngroup()
    main_df = main_df[main_df['userIndex'] <= 10000000]
    main_df['creatorIndex'] = main_df.groupby('creatorId').ngroup()
    main_df['label'] = main_df['total_timespent'].apply(lambda x: 1 if x*60>=60 else 0)
    print(f"n_user: {max(main_df['userIndex'])}, n_creator: {max(main_df['creatorIndex'])}")
    return main_df