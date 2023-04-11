import argparse
import datetime
import os
import random
import string
import tensorflow as tf
print("tf.__version__:", tf.__version__)
assert tf.__version__ == "2.11.0"
import sys
import pandas as pd

from lib.config import Config
from lib.dataset import create_dataset, load_vocabs
from lib.model import create_model
from lib.utils import save_string_gcs
from lib.strategy import get_strategy
from lib import utils

def gen_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def get_tmp_gcs_name(bucket):
    return f"gs://{bucket}/tmp/{gen_random_string(12)}/part"

def upload_to_bq(df, tmp_bucket, bq_path):
    tmp_gcs_name = get_tmp_gcs_name(tmp_bucket)
    print("Upload to tmp gcs location: ", tmp_gcs_name)
    utils.write_csv_parallel(df, tmp_gcs_name)
    print("Uploading gcs to bq")
    cmd = f"bq load --autodetect --time_partitioning_type=DAY --time_partitioning_expiration=-1 --time_partitioning_field=timestamp --source_format=CSV {bq_path} '{tmp_gcs_name}*'"
    print("Running cmd: ", cmd)
    os.system(cmd)
    print("Cleaning up gcs data")
    cmd = f"gsutil -m rm -r '{tmp_gcs_name}*'"
    print("Executing cmd: ", cmd)
    os.system(cmd)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--load-model-path", type=str, required=True)
    parser.add_argument("--user-gcs-pattern", type=str, required=True, help="Example: gs://mgaiduk-us-central1/recall_simulator_15_day_v2/base_user_cat_map/csv_gzip/part*")
    parser.add_argument("--post-gcs-pattern", type=str, required=True, help="Example: gs://mgaiduk-us-central1/recall_simulator_15_day_v2/base_post_cat_map/csv_gzip/part*")
    parser.add_argument("--tmp-gcs-bucket", type=str, required=True, help="Example: mgaiduk-us-central1, bucket used to store temp data before writing to bq")
    parser.add_argument("--output-bq-prefix", type=str, required=True, help="Example: maximal-furnace-783:recall_simulator_15_day_v2.model35. Path to write extracted embeddings. _user_emb, _post_emb added by default")
    parser.add_argument("--tpu-name", type=str, required=False)
    parser.add_argument("--tpu-zone", type=str, required=False)
    parser.add_argument("--config", type=str, required=True)
    parser.add_argument("--steps-per-execution", type=int, default=10)
    parser.add_argument("--timestamp", type=str, required=True, help="Example: 2023-03-10T13:15:50")
    args = parser.parse_args()
    ts = datetime.datetime.strptime(args.timestamp,"%Y-%m-%dT%H:%M:%S" )
    
    
    config = Config(args.config)
    strategy = get_strategy(args.tpu_name, args.tpu_zone)
    model = create_model(strategy, config, args.steps_per_execution)
    model_path = args.load_model_path + "/weights/"
    print("model.load_weights(", model_path, ")")
    model.load_weights(model_path)
    vocabs = load_vocabs(config)
    # instead of normal dataset code, we just use Pandas to load the entire dataset into memory
    for entity in ["user", "post"]:
        if entity == "user":
            path = args.user_gcs_pattern
        else:
            path = args.post_gcs_pattern
        df = utils.read_csv_parallel(path)
        idStr = f"{entity}Id"
        assert idStr in df
        features = {}
        for feature in config.model.features:
            if feature.belongs_to == entity:
                assert feature.name in df
                col = tf.convert_to_tensor(df[feature.name])
                if feature.name in vocabs:
                    col = vocabs[feature.name][col]
                features[feature.name] = col
        if entity == "user":
            embedding, bias, _ = model.get_user_embedding(features, training=False)
        else:
            embedding, bias, _ = model.get_post_embedding(features, training=False)
        result = {}
        result[idStr] = df[idStr]
        for i in range(embedding.shape[1]):
            ith_embedding = embedding[:,i].numpy()
            result[f"{entity}_emb_{i}"] = ith_embedding
        result[f"{entity}_bias"] = tf.squeeze(bias)
        result["timestamp"] = ts
        result_df = pd.DataFrame.from_dict(result)
        upload_to_bq(result_df, args.tmp_gcs_bucket, f"{args.output_bq_prefix}_{entity}_emb")

if __name__ == "__main__":
    main()