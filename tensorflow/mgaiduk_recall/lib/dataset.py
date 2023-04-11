import tensorflow as tf
import tensorflow_io as tfio
from tensorflow.python.framework import dtypes
from tensorflow_io.bigquery import BigQueryClient
from tensorflow_io.bigquery import BigQueryReadSession

def load_vocabs(config):
    result = {}
    for feature in config.model.features:
        if feature.vocab_path:
            print(f"Loading vocabs for {feature.name}")
            filenames = tf.data.Dataset.list_files(feature.vocab_path, shuffle=False)
            vocab_dataset = tf.data.TextLineDataset(filenames, compression_type="GZIP")
            defaults = [tf.constant(0, dtype=tf.int64), tf.constant(0, dtype=tf.int64)] # expect vocabs to be in id, index CSV format
            def vocab_decode_fn(record_bytes):
                csv_row = tf.io.decode_csv(record_bytes, defaults)
                result = {}
                result["token"] = csv_row[0]
                result["index"] = csv_row[1]
                return result
            vocab_dataset = vocab_dataset.batch(feature.vocab_size).map(vocab_decode_fn)
            vocab_tensor = vocab_dataset.get_single_element()
            print(f"Loaded vocab tensor for {feature.name} with shapes (token, index): {vocab_tensor['token'].shape, vocab_tensor['index'].shape}")
            vocab_initializer = tf.lookup.KeyValueTensorInitializer(
                keys=vocab_tensor["token"],
                values=vocab_tensor["index"])
            vocab_table = tf.lookup.StaticVocabularyTable(
                vocab_initializer,
                num_oov_buckets=feature.num_oov_buckets)
            result[feature.name] = vocab_table
    return result

class DatasetReader:
    def __init__(self, input_path, config, vocabs):
        self.vocabs = vocabs
        self.input_path = input_path
        self.config = config
        if self.config.format == "csv":
            defaults = []
            for feature in self.config.dataset_features:
                if feature.type  == "int":
                    defaults.append(tf.constant(0, dtype=tf.int64))
                elif feature.type == "float":
                    defaults.append(tf.constant(0.0, dtype=tf.float32))
                elif feature.type == "str":
                    defaults.append(tf.constant("", dtype=tf.string))
            self.defaults = defaults
        elif self.config.format == "parquet":
            self.column_spec = {}
            for feature in self.config.dataset_features:
                if feature.type == "int":
                    self.column_spec[feature.name] = tf.TensorSpec(shape=(),dtype=tf.dtypes.int64)
                elif feature.type == "float":
                    self.column_spec[feature.name] = tf.TensorSpec(shape=(),dtype=tf.dtypes.float32)
                elif feature.type == "str":
                    self.column_spec[feature.name] = tf.TensorSpec(shape=(),dtype=tf.dtypes.string)
        elif self.config.format == "bq":
            self.bq_colnames = []
            self.bq_coltypes = []
            self.sampling_column = None
            for feature in self.config.dataset_features:
                self.bq_colnames.append(feature.name)
                if feature.use_for_sampling:
                    assert self.sampling_column is None
                    self.sampling_column = feature.name
                if feature.type == "int":
                    self.bq_coltypes.append(dtypes.int64)
                elif feature.type == "float":
                    self.bq_coltypes.append(dtypes.float32)
                elif feature.type == "str":
                    self.bq_coltypes.append(dtypes.string)
            assert self.sampling_column
        else:
            assert False
                

    def __call__(self, ctx: tf.distribute.InputContext):
        batch_size = ctx.get_per_replica_batch_size(
            self.config.global_batch_size) if ctx else self.config.global_batch_size
        num_shards = 1
        shard_idx = 0
        if ctx:
            num_shards = ctx.num_input_pipelines
            shard_idx = ctx.input_pipeline_id
        if not self.config.format == "bq":
            filenames = tf.data.Dataset.list_files(self.input_path, shuffle=True, seed=42)
            if ctx and ctx.num_input_pipelines > 1:
                filenames = filenames.shard(ctx.num_input_pipelines, ctx.input_pipeline_id)
        @tf.function
        def decode_fn(record_bytes):
            if self.config.format == "tfrecord":
                schema = {}
                for feature in self.config.dataset_features:
                    if feature.type == "int":
                        schema[feature.name] = tf.io.FixedLenFeature([], dtype=tf.int64)
                    elif feature.type == "string":
                        schema[feature.name] = tf.io.FixedLenFeature([], dtype=tf.string)
                    else:
                        assert False
                parsed_example = tf.io.parse_example(
                    record_bytes,
                    schema,
                )
            elif self.config.format == "csv":
                csv_row = tf.io.decode_csv(record_bytes, self.defaults)
                parsed_example = {}
                for i, feature in enumerate(self.config.dataset_features):
                    parsed_example[feature.name] = csv_row[i]
            return parsed_example

        def make_dataset_fn(idx):
            filenames_for_shard = filenames.shard(self.config.cycle_length, idx)
            if self.config.format == "tfrecord":
                dataset = tf.data.TFRecordDataset(filenames_for_shard)
            else:
                dataset = tf.data.TextLineDataset(filenames_for_shard, compression_type=self.config.compression)
            if self.config.shuffle_buffer_size:
                dataset = dataset.shuffle(self.config.shuffle_buffer_size)
            dataset = dataset\
                .batch(batch_size, drop_remainder=self.config.drop_remainder)\
                .repeat(self.config.epochs).map(decode_fn, num_parallel_calls=tf.data.experimental.AUTOTUNE)
            return dataset

        def make_parquet_dataset_fn(filename):
            dataset = tfio.IODataset.from_parquet(filename, columns=self.column_spec)
            dataset = dataset.batch(batch_size, drop_remainder=True)
            #if self.config.shuffle_buffer_size:
            #    dataset = dataset.shuffle(self.config.shuffle_buffer_size)
            return dataset

        @tf.function
        def transform_fn(parsed_example):
            features = {}
            for feature in self.config.model.features:
                t = parsed_example[feature.name]
                if feature.convert_to_string:
                    t = tf.strings.as_string(t)
                if feature.split_by_space:
                    t = tf.strings.split(
                        t, sep=" "
                    )
                    # fill in missing values
                    t = tf.where(tf.equal(t, ""), tf.constant("0"), t)
                    # convert ragged tensor to tensor with padding and truncation
                    t = t.to_tensor(default_value="0", shape=[batch_size, feature.seq_len])
                    t = tf.reshape(t, [batch_size, feature.seq_len, 1])
                if feature.convert_to_int_after_split:
                    t = tf.strings.to_number(t, out_type=tf.dtypes.int64)
                if feature.hash:
                    t = tf.strings.to_hash_bucket(t, feature.vocab_size)
                vocab_key = feature.name
                if feature.reuse_vocab:
                    vocab_key = feature.reuse_vocab
                if vocab_key in self.vocabs:
                    t = self.vocabs[vocab_key][t] # transform tokens to index using preloaded vocab
                features[feature.name] = t
            for feature in self.config.dataset_features:
                if feature.keep and feature.name not in features:
                    features[feature.name] = parsed_example[feature.name]
            labels = {
                "label": parsed_example[self.config.label]
            }
            return (features, labels)

        if self.config.format == "parquet":
            dataset = filenames.interleave(
                map_func=make_parquet_dataset_fn,
                cycle_length=self.config.cycle_length,
                num_parallel_calls=tf.data.experimental.AUTOTUNE)
            dataset = dataset.repeat(self.config.epochs)
            #.batch(batch_size, drop_remainder=self.config.drop_remainder)\
        elif self.config.format == "bq":
            client = BigQueryClient()
            GCP_PROJECT_ID='maximal-furnace-783'
            DATASET_GCP_PROJECT_ID, DATASET_ID, TABLE_ID,  = self.input_path.split('.')
            # data sharding for data parallel training
            row_restriction = f"MOD(abs(FARM_FINGERPRINT(cast({self.sampling_column} as string))) + {shard_idx}, {num_shards}) <= 0"
            bqsession = client.read_session(
                "projects/" + GCP_PROJECT_ID,
                DATASET_GCP_PROJECT_ID, TABLE_ID, DATASET_ID,
                self.bq_colnames, self.bq_coltypes,
                requested_streams=self.config.cycle_length,
                row_restriction=row_restriction)
            dataset = bqsession.parallel_read_rows(sloppy=True, num_parallel_calls=tf.data.experimental.AUTOTUNE)
            if self.config.shuffle_buffer_size:
                dataset = dataset.shuffle(self.config.shuffle_buffer_size)
            dataset = dataset.repeat(self.config.epochs)
            # batching is done after repeat, so remainder batches should not happen!
            dataset = dataset.batch(batch_size)
        else:
            indices = tf.data.Dataset.range(self.config.cycle_length)
            dataset = indices.interleave(
                map_func=make_dataset_fn,
                cycle_length=self.config.cycle_length,
                num_parallel_calls=tf.data.experimental.AUTOTUNE)
        dataset = dataset.map(transform_fn, num_parallel_calls=tf.data.experimental.AUTOTUNE)
        dataset = dataset.prefetch(tf.data.experimental.AUTOTUNE)
        return dataset

try:
    tf_opts = tf.distribute.InputOptions(experimental_prefetch_to_device=False)
except:
    tf_opts = tf.distribute.InputOptions(experimental_fetch_to_device=False)

def create_dataset(strategy, gcs_pattern, config, vocabs):
    dataset_callable = DatasetReader(
        input_path=gcs_pattern,
        config=config,
        vocabs=vocabs
    )
    dataset = strategy.distribute_datasets_from_function(
        dataset_fn=dataset_callable,
        options=tf_opts,
    )
    return dataset