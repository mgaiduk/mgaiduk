import math
import tensorflow as tf
import tensorflow_addons as tfa
import tensorflow_recommenders as tfrs

class BaseModel(tfrs.models.Model):
    def __init__(self, loss):
        super().__init__()
        self.loss = loss
        assert loss in ["bce", "mse"]
        if loss == "bce":
            self.task = tfrs.tasks.Ranking(
                loss=tf.keras.losses.BinaryCrossentropy(
                    reduction=tf.keras.losses.Reduction.SUM
                ),
                metrics=[
                    tf.keras.metrics.BinaryCrossentropy(name="label-crossentropy"),
                    tf.keras.metrics.AUC(name="auc"),
                    tf.keras.metrics.AUC(curve="PR", name="pr-auc"),
                    tf.keras.metrics.BinaryAccuracy(name="accuracy"),
                ],
                prediction_metrics=[
                    tf.keras.metrics.Mean("prediction_mean"),
                ],
                label_metrics=[
                    tf.keras.metrics.Mean("label_mean")
                ]
            )
        elif loss in ["mse"]:
            self.task = tfrs.tasks.Ranking(
                loss=tf.keras.losses.MeanSquaredError(
                ),
                metrics=[
                    tf.keras.metrics.MeanSquaredError(name="mse"),
                ],
                prediction_metrics=[
                    tf.keras.metrics.Mean("prediction_mean"),
                ],
                label_metrics=[
                    tf.keras.metrics.Mean("label_mean")
                ]
            )
        else:
            assert False
    
    def call(self, inputs):
        raise NotImplementedError

    def compute_loss(self, inputs, training=False):
        features, labels = inputs
        outputs, regloss = self(features, training=training)
        labels = labels["label"]
        loss = self.task(labels=labels, predictions=outputs["label"])
        loss = tf.reduce_mean(loss)
        return loss + regloss

class BiasOnlyModel(BaseModel):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.optimizer = tfa.optimizers.LazyAdam(learning_rate = config.model.learning_rate)
        embedding_layer_feature_config = {}
        for feature in self.config.model.features:
            initializer = tf.initializers.TruncatedNormal(
                mean=0.0, stddev=1 / math.sqrt(feature.embedding_dim), seed=42,
            )
            embedding_layer_feature_config[feature.name] = tf.tpu.experimental.embedding.FeatureConfig(
                table=tf.tpu.experimental.embedding.TableConfig(
                vocabulary_size=feature.vocab_size,
                initializer=initializer,
                dim=1))
        self.embedding_layer = tfrs.layers.embedding.TPUEmbedding(
            feature_config=embedding_layer_feature_config,
            optimizer=self.optimizer)
        if self.config.model.loss != "mse":
            self.final_activation = tf.keras.layers.Activation('sigmoid')
        

    def call(self, inputs):
        embeddings = self.embedding_layer(inputs)
        embs = []
        for feature in self.config.model.features:
            embs.append(embeddings[feature.name])
        embs = tf.concat(embs, axis=1)
        out = tf.reduce_sum(embs, axis=1)
        if self.config.model.loss != "mse":
            prediction = self.final_activation(out) 
        return {
            "label": prediction
        }

class Model(BaseModel):
    def __init__(self, config):
        super().__init__(loss=config.model.loss)
        self.config = config
        self.optimizer = tfa.optimizers.LazyAdam(learning_rate = config.model.learning_rate)
        if self.config.model.embedding_type == "tpu":
            self.tpu_embedding_optimizer = tf.tpu.experimental.embedding.Adam(learning_rate = config.model.learning_rate)
        self.global_bias_initializer = tf.keras.initializers.Zeros()(shape=())
        self.global_bias = tf.Variable(self.global_bias_initializer, name="global_bias")
        self.embedding_layers = {}
        self.bias_layers = {}
        table_configs = {}
        feature_configs = {}
        self.attention_layers = {}
        self.attention_dense_layers = {}
        self.flatten_layer = tf.keras.layers.Flatten()
        self.batchnorm_layer = tf.keras.layers.BatchNormalization()
        for feature in self.config.model.features:
            if feature.type == "embedding_lookup":
                if not feature.reuse_embedding:
                    initializer = tf.initializers.TruncatedNormal(
                        mean=0.0, stddev=1 / math.sqrt(feature.embedding_dim), seed=42,
                    )
                embedding_dim = feature.embedding_dim
                if feature.num_oov_buckets:
                    embedding_dim += feature.num_oov_buckets
                if self.config.model.embedding_type == "cpu":
                    if feature.reuse_embedding:
                        continue
                    emb_layer = tf.keras.layers.Embedding(feature.vocab_size, feature.embedding_dim, embeddings_initializer=initializer, name="embedding_" + feature.name)
                    self.embedding_layers[feature.name] = emb_layer
                    bias_layer = tf.keras.layers.Embedding(feature.vocab_size, 1, embeddings_initializer="zeros", name="bias_" + feature.name)
                    self.bias_layers[feature.name] = bias_layer
                else:
                    if feature.reuse_embedding:
                        table_config = table_configs[feature.reuse_embedding]
                    else:
                        table_config = tf.tpu.experimental.embedding.TableConfig(
                            vocabulary_size=feature.vocab_size,
                            dim=feature.embedding_dim + 1, # +1 for bias
                            combiner="mean",
                            initializer=initializer,
                            name=feature.name)
                        table_configs[feature.name] = table_config
                    feature_config = tf.tpu.experimental.embedding.FeatureConfig(table=table_config)
                    feature_configs[feature.name] = feature_config
            if feature.combine_mode == "attention":
                self.attention_layers[feature.name] = tf.keras.layers.MultiHeadAttention(num_heads=2, key_dim=32, dropout=0.1)
                self.attention_dense_layers[feature.name] = tf.keras.layers.Dense(units=32)
        if self.config.model.embedding_type == "tpu":
            self.tpu_embedding_layer = tfrs.layers.embedding.TPUEmbedding(feature_configs, self.tpu_embedding_optimizer)
        if self.config.model.dropout:
            self.dropout_layer = tf.keras.layers.Dropout(rate=self.config.model.dropout, name="dropout")
        if len(self.config.model.user_linear_units) > 0:
            self.user_dense = tfrs.layers.blocks.MLP(units=self.config.model.user_linear_units, name="user_mlp")
        if len(self.config.model.post_linear_units) > 0:
            self.post_dense = tfrs.layers.blocks.MLP(units = self.config.model.post_linear_units, name="post_mlp")
        if self.config.model.loss != "mse":
            self.final_activation = tf.keras.layers.Activation('sigmoid', name="final_activation")
        self.regularizer = tf.keras.regularizers.L1L2(l1=self.config.model.l1_regularization, l2=self.config.model.l2_regularization)
    
    # inputs - dict with all fields for which to calculate embeddings
    def get_embeddings_and_biases(self, inputs, training=False):
        if self.config.model.embedding_type == "cpu":
            embeddings = {}
            biases = {}
            for feature in self.config.model.features:
                if feature.type != "embedding_lookup":
                    continue
                embedding_key = feature.name
                if feature.reuse_embedding:
                    embedding_key = feature.reuse_embedding
                embedding = self.embedding_layers[embedding_key](inputs[feature.name])
                bias = self.bias_layers[embedding_key](inputs[feature.name])
                if feature.seq_len:
                    # change history input (batch_size * seq_len * emb_size) to mean history input (batch_size * seq_len)
                    embedding = tf.squeeze(embedding, axis=[2])
                    embedding = tf.reduce_sum(embedding, axis=1, )
                    bias = tf.reduce_sum(bias, axis=1, )
                    bias = tf.squeeze(bias, axis=[2])
                embeddings[feature.name] = embedding
                biases[feature.name] = bias
            return embeddings, biases
        else:
            embs = self.tpu_embedding_layer(inputs)
            embeddings = {}
            biases = {}
            for feature in self.config.model.features:
                if feature.type != "embedding_lookup":
                    continue
                emb = embs[feature.name]
                if feature.seq_len:
                    emb = tf.squeeze(emb)
                    if feature.combine_mode == "sum":
                        emb = tf.reduce_sum(emb, axis=1)
                    elif feature.combine_mode == "attention":
                        emb = self.attention_layers[feature.name](emb, emb)
                        emb = self.batchnorm_layer(emb)
                        emb = self.flatten_layer(emb)
                        emb = self.attention_dense_layers[feature.name](emb)
                    else:
                        assert False
                embeddings[feature.name] = emb[:,:-1]
                biases[feature.name] = emb[:,-1:]
            return embeddings, biases

    def get_final_embedding(self, inputs, embeddings, biases, belongs_to, training=False):
        embeddings_list = []
        dense_features = []
        biases_list = []
        reglosses = []
        for feature in self.config.model.features:
            if feature.belongs_to != belongs_to:
                continue
            if feature.type == "embedding_lookup":
                embedding = embeddings[feature.name]
                #if training: # checkpoints callback passes zero tensor shape for some reason
                #    regloss = self.regularizer(embedding) / embedding.shape[0] # we need mean, because loss is computed for mean for some reason
                #    reglosses.append(regloss)
                bias = biases[feature.name]
                embeddings_list.append(embedding)
                biases_list.append(bias)
            elif feature.type == "dense":
                dense_feature = inputs[feature.name]
                dense_feature = tf.cast(dense_feature, tf.float32)
                dense_features.append(dense_feature)
            else:
                assert False
        embeddings = tf.concat(embeddings_list, axis = 1)
        if training and self.config.model.dropout:
            embeddings = self.dropout_layer(embeddings)
        if len(dense_features) > 0:
            dense_features = tf.stack(dense_features, axis=1)
            embeddings = tf.concat([embeddings, dense_features], axis=1)
        if belongs_to == "user":
            if len(self.config.model.user_linear_units) > 0:
                embeddings = self.user_dense(embeddings)
        elif belongs_to == "post":
            if len(self.config.model.post_linear_units) > 0:
                embeddings = self.post_dense(embeddings)
        else:
            assert False
        final_bias = tf.add_n(biases_list)
        return embeddings, final_bias, reglosses

    def get_sparse_inputs(self, inputs, training=False):
        sparse_inputs = {}
        for feature in self.config.model.features:
            if feature.type == "embedding_lookup":
                t = inputs[feature.name]
                sparse_inputs[feature.name] = t
        return sparse_inputs

    def get_user_embedding(self, inputs, training=False):
        sparse_inputs = self.get_sparse_inputs(inputs, training)
        embeddings, biases = self.get_embeddings_and_biases(sparse_inputs, training)
        return self.get_final_embedding(inputs, embeddings, biases, belongs_to="user", training=training)
    
    def get_post_embedding(self, inputs, training=False):
        sparse_inputs = self.get_sparse_inputs(inputs, training)
        embeddings, biases = self.get_embeddings_and_biases(sparse_inputs, training)
        return self.get_final_embedding(inputs, embeddings, biases, belongs_to="post", training=training)
                

    def call(self, inputs, training=False):
        sparse_inputs = self.get_sparse_inputs(inputs, training)
        embeddings, biases = self.get_embeddings_and_biases(sparse_inputs, training)
        user_embedding, user_bias, user_regloss = self.get_final_embedding(inputs, embeddings, biases, belongs_to="user", training=training)
        post_embedding, post_bias, post_regloss = self.get_final_embedding(inputs, embeddings, biases, belongs_to="post", training=training)
        prediction = tf.keras.backend.batch_dot(user_embedding, post_embedding) + self.global_bias + user_bias + post_bias
        if self.config.model.loss != "mse":
            prediction = self.final_activation(prediction)
        reglosses = user_regloss + post_regloss
        total_regloss = tf.reduce_sum(tf.stack(reglosses, axis=0))
        return {
            "label": prediction
        }, total_regloss

def create_model(strategy, config, steps_per_execution):
    with strategy.scope():
        if config.model.model_type == "Model":
            model = Model(config)
        elif config.model.model_type == "BiasOnlyModel":
            model = BiasOnlyModel(config)
        else:
            assert False
    model.compile(model.optimizer, steps_per_execution=steps_per_execution)
    return model