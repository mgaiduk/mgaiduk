import json
from multiprocessing import Pool
import numpy as np
import os
import pandas as pd
import tensorflow as tf
print("tf.__version__:", tf.__version__)
assert tf.__version__ == "2.11.0"
from tensorflow.python.lib.io import file_io

def save_string_gcs(string_object, gcs_dir, filename):
    string_string = json.dumps(string_object)
    with open(filename, "w") as f:
        f.write(string_string)
    os.system(f"gsutil -m cp {filename} {gcs_dir}/{filename}")
    os.system(f"rm {filename}")

def read_csv_file(filename):
    with file_io.FileIO(filename, 'rb') as f:
        df = pd.read_csv(f, compression={"method": "gzip"})
    return df

def read_csv_parallel(directory):
    files = tf.io.gfile.glob(directory)
    with Pool() as p:
        dfs = p.map(read_csv_file, files)
    return pd.concat(dfs)

def write_csv_file(df, output_prefix, idx):
    filename = f"{output_prefix}_{idx}.csv"
    df.to_csv(filename)

def write_csv_task(task):
    write_csv_file(task[0], task[1], task[2])

def write_csv_parallel(df, output_prefix):
    parallel_threads = 128
    dfs = np.array_split(df, parallel_threads)
    tasks = []
    for i, df in enumerate(dfs):
        tasks.append([df, output_prefix, i])
    with Pool() as p:
        p.map(write_csv_task, tasks)