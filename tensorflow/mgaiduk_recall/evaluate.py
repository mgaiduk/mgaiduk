import argparse
import tensorflow as tf
print("tf.__version__:", tf.__version__)
assert tf.__version__ == "2.11.0"

from lib.config import Config
from lib.dataset import create_dataset, load_vocabs
from lib.utils import save_string_gcs
from lib.model import create_model
from lib.strategy import get_strategy

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcs-pattern", type=str, required=True)
    parser.add_argument("--eval-rows", type=int, required=True)
    parser.add_argument("--tpu-name", type=str, required=False)
    parser.add_argument("--tpu-zone", type=str, required=False)
    parser.add_argument("--config", type=str, required=True)
    parser.add_argument("--load-model-path", type=str, required=True)
    parser.add_argument("--save-metrics-path", type=str, help="Gcs path to save metrics to")
    parser.add_argument("--steps-per-execution", type=int, default=10)
    args = parser.parse_args()
    
    config = Config(args.config)
    print(config)
    
    strategy = get_strategy(args.tpu_name, args.tpu_zone)
    model = create_model(strategy, config, args.steps_per_execution)
    
    model_path = args.load_model_path + "/weights/"
    print("model.load_weights(", model_path, ")")
    model.load_weights(model_path)

    vocabs = load_vocabs(config)
    eval_dataset = create_dataset(strategy, args.gcs_pattern, config, vocabs)
    eval_steps_per_epoch = args.eval_rows // config.global_batch_size
    eval_scores = model.evaluate(eval_dataset, return_dict=True, steps=eval_steps_per_epoch)
    if args.save_metrics_path:
        metrics = {}
        metrics["eval"] = eval_scores
        save_string_gcs(metrics, args.save_metrics_path, f"metrics_pretrain.json")

if __name__ == "__main__":
    main()