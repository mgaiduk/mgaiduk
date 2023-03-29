import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import java.util.regex.Pattern

class TMapper : Mapper<LongWritable, Text, Text, IntWritable>() {
    public override fun map(key: LongWritable, value: Text, context: Context) {
        val words = value.toString().split(Pattern.compile("\\W+"), limit = 0)
        words.map(String::lowercase)
            .forEach { context.write(Text(it), IntWritable(1)) }
    }
}