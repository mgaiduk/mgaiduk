import argparse
from datetime import datetime
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-gcs-pattern", type=str, required=True)
    parser.add_argument("--eval-gcs-pattern", type=str, required=True)
    parser.add_argument("--train-rows", type=int, help="How many rows to read from the dataset for training. Must not be greater then dataset size", required=True)
    parser.add_argument("--eval-rows", type=int, required=True)
    parser.add_argument("--trainval-rows", type=int)
    parser.add_argument("--tpu-name", type=str, required=False)
    parser.add_argument("--tpu-zone", type=str, required=False)
    parser.add_argument("--config", type=str, required=True)
    parser.add_argument("--load-model-path", type=str)
    parser.add_argument("--save-model-path", type=str, required=True)
    parser.add_argument("--steps-per-execution", type=int, default=10)
    parser.add_argument("--tb", action="store_true", help="Enable tensorboard")
    args = parser.parse_args()
    
    if not args.trainval_rows:
        args.trainval_rows = args.eval_rows

    config = Config(args.config)
    print(config)
    
    strategy = get_strategy(args.tpu_name, args.tpu_zone)
    model = create_model(strategy, config, args.steps_per_execution)
    
    if args.load_model_path:
        model_path = args.load_model_path + "/weights/"
        print("model.load_weights(", model_path, ")")
        model.load_weights(model_path)

    vocabs = load_vocabs(config)
    eval_dataset = create_dataset(strategy, args.eval_gcs_pattern, config, vocabs)
    train_dataset = create_dataset(strategy, args.train_gcs_pattern, config, vocabs)
    if args.trainval_rows:
        trainval_dataset = create_dataset(strategy, args.eval_gcs_pattern, config, vocabs)
    eval_steps_per_epoch = args.eval_rows // config.global_batch_size
    train_steps_per_epoch = args.train_rows // config.global_batch_size
    trainval_steps_per_epoch = args.trainval_rows // config.global_batch_size
    
    with strategy.scope():
        checkpoints_cb = tf.keras.callbacks.ModelCheckpoint(args.save_model_path  + '/checkpoints/',  save_freq = train_steps_per_epoch//3)
        # checkpoints don't work in tf 2.11 - pass none shape tensor for some reason
        # callbacks=[checkpoints_cb]
        callbacks = []
        if args.tb:
            tb_path = args.save_model_path + '/tb/'
            print("Tensorboard enabled. Saving to ", tb_path)
            tb = tf.keras.callbacks.TensorBoard(tb_path, update_freq=200)
            callbacks.append(tb)
        if args.trainval_rows:
            history = model.fit(train_dataset, epochs=config.epochs, callbacks=callbacks, steps_per_epoch=train_steps_per_epoch,
            validation_data=trainval_dataset, validation_steps=trainval_steps_per_epoch)
        else:
            history = model.fit(train_dataset, epochs=config.epochs, callbacks=callbacks, steps_per_epoch=train_steps_per_epoch,)
        print("train end")
        i = 0
        while True:
            try:
                print(f"save_weights attempt #{i}, time: {datetime.now().strftime('%H:%M:%S')}")
                model.save_weights(args.save_model_path  + '/weights/')
                i += 1
                break
            except Exception:
                continue
        print("save_weights end ", datetime.now().strftime("%H:%M:%S"))
        print("evaluate")
        eval_scores = model.evaluate(eval_dataset, return_dict=True, steps=eval_steps_per_epoch)
        eval_steps = args.eval_rows // config.global_batch_size
        print("evaluate end")
        metrics = {}
        metrics["eval"] = eval_scores
        metrics["history"] = history.history
        metrics["args"] = sys.argv
        metrics["config"] = repr(config)
        save_string_gcs(metrics, args.save_model_path, f"metrics_pretrain.json")

if __name__ == "__main__":
    main()