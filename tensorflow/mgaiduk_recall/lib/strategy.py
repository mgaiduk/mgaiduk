import tensorflow as tf
print("tf.__version__:", tf.__version__)
assert tf.__version__ == "2.11.0"

def get_strategy(tpu_name = "", tpu_zone = ""):
    if tpu_name:
        assert tpu_zone
        print("Initializing tpu")
        handle = tf.distribute.cluster_resolver.TPUClusterResolver(tpu=tpu_name, zone=tpu_zone)
        tpu=handle.connect(tpu_name, tpu_zone)
        strategy = tf.distribute.TPUStrategy(tpu)
        tf.tpu.experimental.initialize_tpu_system(handle)
        print("num_replicas_in_sync: ", strategy.num_replicas_in_sync)
    else:
        print("Initializing default strategy")
        strategy = tf.distribute.get_strategy()
    return strategy