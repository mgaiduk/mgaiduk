import yaml

class DatasetFeature:
    def __init__(self, feature_name, dic):
        self.name = feature_name
        self.type = dic["type"]
        assert self.type in ["str", "int", "float"]
        self.use_for_sampling = False
        if "use_for_sampling" in dic:
            self.use_for_sampling = dic["use_for_sampling"]
        self.keep = False
        if "keep" in dic:
            self.keep = dic["keep"]
    
    def __repr__(self):
        return "DatasetFeature: " + str(self.__dict__)

class Feature:
    def __init__(self, feature_name, dic):
        self.name = feature_name
        self.reuse_vocab = None
        if "reuse_vocab" in dic:
            self.reuse_vocab = dic["reuse_vocab"]
        self.reuse_embedding = None
        if "reuse_embedding" in dic:
            self.reuse_embedding = dic["reuse_embedding"]
        self.hash = False
        self.type = "embedding_lookup"
        if "type" in dic:
            self.type = dic["type"]
        assert self.type in ["embedding_lookup", "dense"]
        self.embedding_dim = None
        if "embedding_dim" in dic:
            self.embedding_dim = dic["embedding_dim"]
        if self.type == "embedding_lookup" and self.reuse_embedding is None:
            assert self.embedding_dim, f"Feature: {self.name} reuse_embedding: {self.reuse_embedding}, dic: {dic}"
        if "hash" in dic:
            self.hash = dic["hash"]
            if self.hash:
                assert "vocab_size" in dic
        if "vocab_size" in dic:
            self.vocab_size = dic["vocab_size"]
        self.vocab_path = None
        self.num_oov_buckets = None
        if "vocab_path" in dic:
            self.vocab_path = dic["vocab_path"]
            assert "num_oov_buckets" in dic
            self.num_oov_buckets = dic["num_oov_buckets"]
        self.split_by_space = False
        self.seq_len = None
        if "split_by_space" in dic:
            self.split_by_space = dic["split_by_space"]
            assert "seq_len" in dic
            self.seq_len = dic["seq_len"]
        self.convert_to_string = False
        self.belongs_to = dic["belongs_to"]
        if "convert_to_string" in dic:
            self.convert_to_string = dic["convert_to_string"]
        self.convert_to_int_after_split = False
        if "convert_to_int_after_split" in dic:
            self.convert_to_int_after_split = dic["convert_to_int_after_split"]
        self.combine_mode = "sum"
        if "combine_mode" in dic:
            self.combine_mode = dic["combine_mode"]


    def __repr__(self):
        return "Feature: " + str(self.__dict__)
        
class Model:
    def __init__(self, dic):
        self.embedding_type = "cpu"
        if "embedding_type" in dic:
            self.embedding_type = dic["embedding_type"]
        self.learning_rate = dic["learning_rate"]
        self.features = []
        self.user_linear_units = dic["user_linear_units"]
        self.post_linear_units = dic["post_linear_units"]
        self.model_type = dic["model_type"]
        self.dropout = None
        self.loss = dic["loss"]
        self.l1_regularization = dic["l1_regularization"]
        self.l2_regularization = dic["l2_regularization"]
        if "dropout" in dic:
            self.dropout = dic["dropout"]
        for feature_name, feature_dic in dic["features"].items():
            self.features.append(Feature(feature_name, feature_dic))

    def __repr__(self):
        return "Model: " + str(self.__dict__)

class Config:
    def __init__(self, path):
        dic = yaml.safe_load(open(path, 'r'))
        self.epochs = dic["epochs"]
        self.drop_remainder = dic["drop_remainder"]
        self.global_batch_size = dic["global_batch_size"]
        self.label = dic["label"]
        self.model = Model(dic["model"])
        self.compression = None
        if "compression" in dic:
            self.compression = dic["compression"]
        self.shuffle_buffer_size = None
        if "shuffle_buffer_size" in dic:
            self.shuffle_buffer_size = dic["shuffle_buffer_size"]
        self.dataset_features = []
        self.format = dic["format"]
        self.cycle_length = dic["cycle_length"]
        allowed_formats = ["csv", "tfrecord", "parquet", "bq"]
        assert self.format in allowed_formats, f"Expected format to be one of {allowed_formats}, got {self.format}"
        for feature_name, feature_dic in dic["dataset_features"].items():
            self.dataset_features.append(DatasetFeature(feature_name, feature_dic))
       
    def __repr__(self):
        return "Config: " + str(self.__dict__)