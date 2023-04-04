import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from torch.utils.data import DataLoader, Dataset
import torch


class catDataset(Dataset):
    def __init__(self, df, feature_x=False, feature_x_lis=None):
        self.df = df.reset_index(drop=True)
        self.feature_x = feature_x
        self.flis = feature_x_lis

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        if self.feature_x:
            return (torch.tensor(self.df.loc[idx, 'userIndex']), torch.tensor(self.df.loc[idx, 'creatorIndex']),
                    torch.tensor(self.df.loc[idx, self.flis]), torch.tensor(self.df.loc[idx, 'label']))

        return (torch.tensor(self.df['userIndex'][idx]), torch.tensor(self.df['creatorIndex'][idx]),
                torch.tensor(self.df['label'][idx]).float())


class rankerV0(nn.Module):
    def __init__(self, n_uemb, n_cemb, embedding_dim=64, ispretrained=False):
        super(rankerV0, self).__init__()
        if ispretrained:
            self.uemb = nn.Embedding.from_pretrained(n_uemb)
            self.cemb = nn.Embedding.from_pretrained(n_cemb)
        else:
            self.uemb = nn.Embedding(n_uemb, embedding_dim=embedding_dim)
            self.cemb = nn.Embedding(n_cemb, embedding_dim=embedding_dim)
            self.user_bias = nn.Embedding(n_uemb, embedding_dim=1)
            self.creator_bias = nn.Embedding(n_cemb, embedding_dim=1)
            torch.nn.init.xavier_uniform_(self.uemb.weight)
            torch.nn.init.xavier_uniform_(self.cemb.weight)
            torch.nn.init.zeros_(self.user_bias.weight)
            torch.nn.init.zeros_(self.creator_bias.weight)
        self.uemb.weight.requires_grad = True
        self.cemb.weight.requires_grad = True
        self.user_bias.weight.requires_grad = True
        self.creator_bias.weight.requires_grad = True

    def forward(self, x1, x2):
        user_embedding = self.uemb(x1)
        creator_embedding = self.cemb(x2)
        user_bias = self.user_bias(x1)
        creator_bias = self.creator_bias(x2)

        dot = torch.sum(torch.mul(user_embedding, creator_embedding) + user_bias + creator_bias, dim=1)

        return torch.sigmoid(dot)

class rankerV00(nn.Module):
    def __init__(self, n_uemb, n_cemb, ispretrained=False):
        super(rankerV0, self).__init__()
        if ispretrained:
            assert False
        else:
            self.user_bias = nn.Embedding(n_uemb, embedding_dim=1)
            self.creator_bias = nn.Embedding(n_cemb, embedding_dim=1)
            torch.nn.init.zeros_(self.user_bias.weight)
            torch.nn.init.zeros_(self.creator_bias.weight)
        self.user_bias.weight.requires_grad = True
        self.creator_bias.weight.requires_grad = True

    def forward(self, x1, x2):
        user_bias = self.user_bias(x1)
        creator_bias = self.creator_bias(x2)

        dot = torch.sum(user_bias + creator_bias, dim=1)

        return torch.sigmoid(dot)

class rankerV1(nn.Module):
    def __init__(self, n_uemb, n_cemb, embedding_dim=64, ispretrained=False):
        super(rankerV1, self).__init__()
        if ispretrained:
            assert False
        else:
            self.user_embedding = nn.Embedding(n_uemb, embedding_dim=embedding_dim)
            self.creator_embedding = nn.Embedding(n_cemb, embedding_dim=embedding_dim)
            self.mlp = nn.Sequential(
                nn.Linear(2 * embedding_dim, 1),
            )
            torch.nn.init.xavier_uniform_(self.user_embedding.weight)
            torch.nn.init.xavier_uniform_(self.creator_embedding.weight)
            def init_weights(m):
                if type(m) == nn.Linear:
                    torch.nn.init.xavier_uniform_(m.weight)
            self.mlp.apply(init_weights)
        self.user_embedding.weight.requires_grad = True
        self.creator_embedding.weight.requires_grad = True


    def forward(self, x1, x2):
        user_embedding = self.user_embedding(x1)
        creator_embedding = self.creator_embedding(x2)
        out = self.mlp(torch.cat((user_embedding, creator_embedding), dim=1))

        dot = torch.sum(torch.mul(user_embedding, creator_embedding), dim=1)

        return torch.sigmoid(dot)

class rankerV2(nn.Module):
    def __init__(self, n_uemb, n_cemb, embedding_dim=64, ispretrained=False):
        super(rankerV2, self).__init__()
        if ispretrained:
            assert False
        else:
            self.user_embedding = nn.Embedding(n_uemb, embedding_dim=embedding_dim)
            self.creator_embedding = nn.Embedding(n_cemb, embedding_dim=embedding_dim)
            # with batch_first=True, expects batch_size, seq_len, emb_dim input shape
            self.multihead_attention = nn.MultiheadAttention(embedding_dim, num_heads=8, batch_first=True)
            self.mlp = nn.Sequential(
                nn.Linear(2 * embedding_dim, 1),
            )
            torch.nn.init.xavier_uniform_(self.user_embedding.weight)
            torch.nn.init.xavier_uniform_(self.creator_embedding.weight)
            def init_weights(m):
                if type(m) == nn.Linear:
                    torch.nn.init.xavier_uniform_(m.weight)
            self.mlp.apply(init_weights)
            self.multihead_attention.apply(init_weights)
        self.user_embedding.weight.requires_grad = True
        self.creator_embedding.weight.requires_grad = True


    def forward(self, x1, x2):
        user_embedding = self.user_embedding(x1) # batch_size, emb_dim
        creator_embedding = self.creator_embedding(x2) # batch_size, emb_dim
        common_embedding = torch.stack((user_embedding, creator_embedding), dim=1) # batch_size, 2, emb_dim, 2 is seq_length
        attention_outputs, _ = self.multihead_attention(common_embedding, common_embedding, common_embedding) # batch_size, 2, emb_dim, 2 is seq_length
        attention_outputs = torch.flatten(attention_outputs, start_dim = 1) # batch_size, 2 * emb_dim
        out = self.mlp(attention_outputs) # batch_size, 1
        out = torch.flatten(out, start_dim=0) # batch_size,
        return torch.sigmoid(out)