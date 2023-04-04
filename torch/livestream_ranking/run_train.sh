#!/bin/sh
set -e

gcs_bucket=$1  #"deep-ctr"
folder=$2
runtime=$3 #"20210131" #runtime
dataset=$4 #moj_recall_production

echo "Arguments:" $@
gcloud config list account
if test -f "/root/.gcp/ds-moj-composer-sa.json"; then
    gcloud auth activate-service-account --key-file=/root/.gcp/ds-moj-composer-sa.json --project=moj-prod
fi
gcloud config list account



#bq extract maximal-furnace-783:moj_livestream_ranking.livestream_ranker_train_data gs://$gcs_bucket/$folder/$runtime/data/livestream_ranker_train_data/*.csv
#bq extract maximal-furnace-783:moj_livestream_ranking.livestream_ranker_pretrained_uemb gs://$gcs_bucket/$folder/$runtime/data/livestream_ranker_pretrained_uemb/*.csv
#bq extract maximal-furnace-783:moj_livestream_ranking.livestream_ranker_pretrained_cemb gs://$gcs_bucket/$folder/$runtime/data/livestream_ranker_pretrained_cemb/*.csv

#gsutil -m cp -r gs://$gcs_bucket/$folder/$runtime/data/livestream_ranker_train_data ./
#gsutil -m cp -r gs://$gcs_bucket/$folder/$runtime/data/livestream_ranker_pretrained_uemb ./
#gsutil -m cp -r gs://$gcs_bucket/$folder/$runtime/data/livestream_ranker_pretrained_cemb ./
mkdir -p model_out
python3 train.py


#gsutil -m cp -r model_out/* gs://$gcs_bucket/$folder/$runtime/models/
#bq load --autodetect  --replace=false --time_partitioning_type=DAY --time_partitioning_field=time --source_format=CSV maximal-furnace-783:moj_livestream_ranking.user_emb_live ./model_out/user_emb*.csv
#bq load --autodetect  --replace=false --time_partitioning_type=DAY --time_partitioning_field=time --source_format=CSV maximal-furnace-783:moj_livestream_ranking.creator_emb_live ./model_out/creator_emb*.csv





