import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

enum class InputType {
    DATASET,
    FEATURES
}

const val InputTypeName = "£input_type"
abstract class TMapper : Mapper<LongWritable, Text, Text, Text>() {
    abstract fun getInputType(): InputType
    public override fun map(key: LongWritable, value: Text, context: Context) {
        val conf: Configuration = context.configuration
        val reduceBy = conf.get("reduceBy").split(",")
        // parse value into json
        val json = Json.parseToJsonElement(value.toString()).jsonObject.toMutableMap()
        val groupingKey = StringBuilder()
        for (reduceKeyName in reduceBy) {
            val reduceKey = json[reduceKeyName]!!.jsonPrimitive.content
            groupingKey.append(reduceKey)
            // sentinel to make sure we don't have a key that is a prefix of another key
            groupingKey.append("£")
        }
        // sentinel to make sure we don't override a field that could already be in the input
        json[InputTypeName] = JsonPrimitive(getInputType().toString())
        context.write(Text(groupingKey.toString()), Text(Json.encodeToString(JsonObject(json))))
    }
}

class TDatasetMapper : TMapper() {
    override fun getInputType(): InputType {
        return InputType.DATASET
    }
}
class TFeaturesMapper: TMapper() {
    override fun getInputType(): InputType {
        return InputType.FEATURES
    }
}