#!/usr/bin/env python
import argparse
import torch.nn as nn
import torch.optim as optim

from torch.utils.data import DataLoader
import torch
from model import catDataset, rankerV0, rankerV1, rankerV00, rankerV2, rankerOld

import pandas as pd
import numpy as np
import os

import datetime
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

from dataset import create_dataset

data_directory = "livestream_ranker_train_data/"
model_update = "model_out/"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-type", type=str, required=True, choices=["rankerOld", "rankerV0", "rankerV00", "rankerV1", "rankerV2"])
    parser.add_argument("--train-input-path", default="livestream_ranker_train_data", help="path to local directory with .csv files")
    # add another int option "epochs"
    parser.add_argument("--epochs", type=int, default=5, help="number of epochs")
    parser.add_argument("--embedding-dim", type=int, help="Embedding lookup dimension")
    args = parser.parse_args()

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

    # train_data, val_data = train_test_split(main_df, test_size=.2, stratify=main_df['label'])
    main_df = create_dataset(args.train_input_path)
    train_dataset = catDataset(main_df[main_df["val"] == "0"])
    train_dataloader = DataLoader(train_dataset, batch_size= 32000, shuffle=True)
    val_dataset = catDataset(main_df[main_df["val"]=="1"])
    val_dataloader = DataLoader(val_dataset, batch_size= 524280, shuffle=True)
    torch.set_num_threads(32)

    device = torch.device("cpu")
    learning_rate =.01
    print("args.model_type: ", args.model_type)
    if args.model_type == "rankerOld":
        model = rankerOld(max(main_df['userIndex'])+1,max(main_df['creatorIndex'])+1, args.embedding_dim).float()
    elif args.model_type == "rankerV0":
        model = rankerV0(max(main_df['userIndex'])+1,max(main_df['creatorIndex'])+1, args.embedding_dim).float()
    elif args.model_type == "rankerV00":
        model = rankerV00(max(main_df['userIndex'])+1,max(main_df['creatorIndex'])+1).float()
    elif args.model_type == "rankerV1":
        model = rankerV1(max(main_df['userIndex'])+1,max(main_df['creatorIndex'])+1, args.embedding_dim).float()
    elif args.model_type == "rankerV2":
        model = rankerV2(max(main_df['userIndex'])+1,max(main_df['creatorIndex'])+1, args.embedding_dim).float()
    else:
        assert False
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


    # def weights_init(m):
    #     if isinstance(m, nn.Linear):
    #         torch.nn.init.xavier_uniform_(m.weight.data)
    # model.apply(weights_init)

    model_name = "model_scratch"
    loss_list_name = "loss_list_scratch"
    train_losses = []
    eval_aucs = []
    for epoch in range(args.epochs):
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
            if step % 100 == 0 and not step == 0:
                print("train loss: ", total_loss / step)
                train_losses.append(total_loss / step)
                torch.cuda.empty_cache()
                prediction, true, _ = predict(model, val_dataloader, is_sample=True)
                val_loss.append(_)
                met = roc_auc_score(true, prediction)
                print("val roc_auc: ", met)
                eval_aucs.append(met)
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
        print("train loss: ", total_loss / step)
        train_losses.append(total_loss / step)
        model.eval()
        prediction, true, _ = predict(model, val_dataloader)

        if _ < min_loss:
            min_loss = _
            torch.save(model.state_dict(), model_name)

        met = roc_auc_score(true, prediction)
        print("val roc_auc: ", met)
        eval_aucs.append(met)
    
    print("Train losses history: ", ",".join(list(map(lambda x: "{:.3f}".format(x), train_losses))))
    print("Eval aucs history: ", ",".join(list(map(lambda x: "{:.3f}".format(x), eval_aucs))))

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

if __name__ == "__main__":
    main()
