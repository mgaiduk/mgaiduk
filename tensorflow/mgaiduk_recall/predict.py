import argparse
import tensorflow as tf
print("tf.__version__:", tf.__version__)
assert tf.__version__ == "2.11.0"
import pandas as pd

from lib.config import Config
from lib.dataset import create_dataset, load_vocabs
from lib.model import create_model
from lib.utils import save_string_gcs
from lib.strategy import get_strategy

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eval-gcs-pattern", type=str, required=True)
    parser.add_argument("--eval-rows", type=int)
    parser.add_argument("--tpu-name", type=str, required=False)
    parser.add_argument("--tpu-zone", type=str, required=False)
    parser.add_argument("--config", type=str, required=True)
    parser.add_argument("--load-model-path", type=str, required=True)
    parser.add_argument("--prediction-store-path", type=str)
    args = parser.parse_args()
    
    config = Config(args.config)
    print(config)
    
    strategy = get_strategy(args.tpu_name, args.tpu_zone)
    # predict code fails with steps_per_execution > 1 because of uneven batch sizes (necessary to predict on 100% of the dataset)
    model = create_model(strategy, config, 1)
    
    if args.load_model_path:
        model_path = args.load_model_path + "/weights/"
        print("model.load_weights(", model_path, ")")
        model.load_weights(model_path)

    vocabs = load_vocabs(config)
    eval_dataset = create_dataset(strategy, args.eval_gcs_pattern, config, vocabs)

    assert not config.drop_remainder
    assert args.prediction_store_path
    assert args.load_model_path
    for i, (features, labels) in enumerate(eval_dataset):
        print("Running prediction. Batch #", i)
        preds = model(features, training=False)[0]["label"]
        preds = tf.squeeze(preds)
        features["preds"] = preds
        df = pd.DataFrame(features)
        df.to_csv(f"{args.prediction_store_path}_{i}", compression={"method": "gzip"})

if __name__ == "__main__":
    main()