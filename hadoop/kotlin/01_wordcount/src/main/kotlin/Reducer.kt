import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class TReducer : Reducer<Text, IntWritable, Text, IntWritable>() {
    override fun reduce(key: Text, values: Iterable<IntWritable>, context: Context) {
        val sum = values.sumOf(IntWritable::get)
        context.write(key, IntWritable(sum))
    }
}