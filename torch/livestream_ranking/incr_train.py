import torch.nn as nn
import torch.optim as optim

from torch.utils.data import DataLoader
import torch
from model import catDataset, rankerV0

import pandas as pd
import numpy as np
import os

import datetime
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

## reading pretrained user embeddings
uemb_dir = "livestream_ranker_pretrained_uemb/"
uemb_df = []
for i in os.listdir(uemb_dir):
    uemb_df.append(pd.read_csv(uemb_dir+i))
uemb_df = pd.concat(uemb_df).reset_index(drop=True)
uemb_df['userIndex'] = uemb_df['userIndex'] -1

## reading pretrained creator embeddings
cemb_dir = "livestream_ranker_pretrained_cemb/"
cemb_df = []
for i in os.listdir(cemb_dir):
    cemb_df.append(pd.read_csv(cemb_dir+i))
cemb_df = pd.concat(cemb_df).reset_index(drop=True)
cemb_df['creatorIndex'] = cemb_df['creatorIndex'] - 1

## reading training data
data_directory = "livestream_ranker_train_data/"
model_update = "model_out/"
os.listdir(data_directory)

df_list = []

for i in os.listdir(data_directory):
    dfs = pd.read_csv(data_directory + i)
    df_list.append(dfs)

main_df = pd.concat(df_list).reset_index(drop=True)
main_df.rename(columns={'hostId': 'creatorId', 'memberId': 'userId'}, inplace=True)

## assiging indexes to ids (for training purpose)
main_df = main_df.merge(uemb_df[['userId','userIndex']],how='inner',on='userId')
main_df = main_df.merge(cemb_df[['creatorId','creatorIndex']],how='inner',on='creatorId')
print(f"n_user: {len(main_df['userIndex'].unique())}, n_creator: {len(main_df['creatorIndex'].unique())}")

def predict(model, dataloader, feature_x=False, is_sample=False):
    pred_lis = []
    label_lis = []
    loss_lis = []
    step = 0
    for batch in dataloader:
        if feature_x:
            u_ind, c_ind, features, labels = batch[0].to(device), batch[1].to(device), batch[2].to(device), batch[3].to(
                device)
            preds = model(u_ind, c_ind, features)
            loss = CELoss(preds, labels)
            #             print(preds.shape, labels.shape)
            preds = torch.nn.functional.softmax(preds, dim=1)[:, 1]
        else:
            u_ind, c_ind, labels = batch[0].to(device), batch[1].to(device), batch[2].to(device)
            preds = model(u_ind, c_ind)
            loss = CELoss(preds, labels)

        # print(loss.item())
        preds = preds.detach().cpu().numpy()

        labels = labels.detach().cpu().numpy()
        # pred_cls = np.argmax(preds, axis=1)
        pred_lis.extend(preds)
        label_lis.extend(labels)
        loss_lis.append(loss.cpu().item())
        step += 1
        if is_sample and step == 10:
            break

    return np.array(pred_lis), np.array(label_lis), np.mean(loss_lis)

torch.set_num_threads(8)
main_df['label'] = main_df['total_timespent'].apply(lambda x: 1 if x*60>=60 else 0)

# train_data, val_data = train_test_split(main_df, test_size=.2, stratify=main_df['label'])
train_dataset = catDataset(main_df[main_df['val']==0])
train_dataloader = DataLoader(train_dataset, batch_size= 8192, shuffle=True)
val_dataset = catDataset(main_df[main_df['val']==1])
val_dataloader = DataLoader(val_dataset, batch_size= 8192, shuffle=True)


uemb_df = uemb_df.sort_values('userIndex').reset_index(drop=True)
cemb_df = cemb_df.sort_values('creatorIndex').reset_index(drop=True)
u_cols = ["ue"+str(i) for i in range(64)]
c_cols = ["ce"+str(i) for i in range(64)]
model = rankerV0(torch.tensor(uemb_df[u_cols].to_numpy()),torch.tensor(cemb_df[c_cols].to_numpy()), ispretrained=True).float()

device = torch.device("cpu")
learning_rate =.01
CELoss = nn.BCELoss()
model.to(device)
optimizer = optim.Adam(model.parameters(),lr = learning_rate)
print("model will run on: {}".format(device))


is_early = True
feat_scores = []
train_loss = []
val_loss = []
loss_lis_train = []
val_roc=[]
min_loss = 9999999


model_name = "model_incr"
loss_list_name = "loss_list_incr"
for epoch in range(2):
    print(epoch)
    total_loss = 0
    step = 0
    model.train()
    c = 0
    for batch in train_dataloader:
        model.zero_grad()

        u_ind, c_ind, labels = batch[0].to(device), batch[1].to(device), batch[2].to(device).float()
        preds = model(u_ind, c_ind)

        loss = CELoss(preds, labels)

        # backward pass to calculate the gradients
        loss.backward()

        # update parameters
        optimizer.step()

        # torch.cuda.empty_cache()
        # add on to the total loss
        loss_item = loss.item()
        loss_lis_train.append(loss_item)
        total_loss += loss_item

        # progress update after every 100 batches.
        if step % 50 == 0 and not step == 0:
            print("loss: ", total_loss / step)
            torch.cuda.empty_cache()
            prediction, true, _ = predict(model, val_dataloader, is_sample=True)
            val_loss.append(_)
            met = roc_auc_score(true, prediction)
            print("roc_auc: ", met)
            val_roc.append(met)
            if _ < min_loss:
                min_loss = _
                torch.save(model.state_dict(), f"{model_update}{model_name}.pt")
            # if epoch >= 3 and is_early :
            #     if val_loss[-1] > val_loss[-2] and val_loss[-2] > val_loss[-3] and val_loss[-3] > val_loss[-4]:
            #         print("stopping training")
            #         break

        step += 1
    f = open(f"{loss_list_name}.txt", "w")
    f.write(str(loss_lis_train))
    f.close()
    train_loss.append(total_loss / step)
    print("loss: ", total_loss / step)
    model.eval()
    prediction, true, _ = predict(model, val_dataloader)

    if _ < min_loss:
        min_loss = _
        torch.save(model.state_dict(), model_name)

    met = roc_auc_score(true, prediction)
    print("roc_auc: ", met)

dt_time = datetime.datetime.now()
u_dict = main_df[['userIndex','userId']].drop_duplicates('userId').set_index('userIndex', drop=True)
u_dict.sort_index(inplace=True)
u_dict['time'] = dt_time
u_dict['model_name'] = "rankerV0"
u_dict['uemb'] = model.uemb.weight.data.numpy().tolist()
u_dict.to_csv(f"{model_update}user_emb_{model_name}.csv", index=False)

c_dict = main_df[['creatorIndex','creatorId']].drop_duplicates('creatorId').set_index('creatorIndex', drop=True)
c_dict.sort_index(inplace=True)
c_dict['time'] = dt_time
c_dict['model_name'] = "rankerV0"
c_dict['cemb'] = model.cemb.weight.data.numpy().tolist()
c_dict.to_csv(f"{model_update}creator_emb_{model_name}.csv", index=False)

#torch.save(model.state_dict(), f"{model_update}{model_name}_last_step.pt")
