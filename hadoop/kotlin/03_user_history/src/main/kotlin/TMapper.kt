import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class TMapper : Mapper<LongWritable, Text, Text, Text>() {
    public override fun map(key: LongWritable, value: Text, context: Context) {
        // parse value as csv
        val row = Row(value.toString())
        context.write(Text(row.memberId), value)
    }
}