
import kotlinx.datetime.Instant
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import kotlin.time.Duration.Companion.hours


class HistoryAggregator(){
    var history: List<String> = mutableListOf()
    private val maxSize = 32
    fun update(row: Row) {
        history = history + row.hostId
        if (history.size > 2 * maxSize) {
            history = history.slice(maxSize until history.size)
        }
    }

    fun getHistory(): String {
        return history.joinToString(" ")
    }
}

val TRAIN_START_TIME = Instant.fromEpochSeconds(1680652800)
class TReducer : Reducer<Text, Text, Text, NullWritable>() {
    private val gap = 1.hours
    override fun reduce(key: Text, values: Iterable<Text>, context: Context) {
        val elements: MutableList<Row> = mutableListOf()
        for (value in values) {
            elements.add(Row(value.toString(), isDataset = true))
            val extraRow = Row(value.toString(), isDataset = false)
            extraRow.livestreamExitTime += gap
            elements.add(extraRow)
        }
        elements.sortBy { it.livestreamExitTime }
        val positivesHistoryAggregator = HistoryAggregator()
        val negativesHistoryAggregator = HistoryAggregator()
        for (element in elements) {
            if (!element.isDataset) {
                if (element.label > 0) {
                    positivesHistoryAggregator.update(element)
                } else {
                    negativesHistoryAggregator.update(element)
                }
            }
            if (element.isDataset && element.livestreamExitTime >= TRAIN_START_TIME) {
                element.positivesHistory = positivesHistoryAggregator.getHistory()
                element.negativesHistory = negativesHistoryAggregator.getHistory()
                context.write(Text(element.toCsvRow()), NullWritable.get())
            }
        }
    }
}