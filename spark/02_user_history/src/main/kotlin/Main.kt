/* SimpleApp.kt */
@file:JvmName("WordCount")
import org.jetbrains.kotlinx.spark.api.*
import org.jetbrains.kotlinx.spark.api.tuples.*
import kotlinx.cli.*
import kotlin.time.Duration.Companion.hours
import kotlinx.datetime.Instant

class HistoryAggregator(){
    private var history: List<String> = mutableListOf()
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

fun groupFn(value: String): String {
    val row = Row(value)
    return row.memberId
}

val TRAIN_START_TIME = Instant.fromEpochSeconds(1680652800)
fun reduceFn(key: String, values: Iterator<String>): List<String> {
    val result: MutableList<String> = mutableListOf()
    val gap = 1.hours
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
            result.add(element.toCsvRow())
        }
    }
    return result
}

fun main(args: Array<String>) {
    val parser = ArgParser("wordcount")
    val input by
        parser.option(ArgType.String, fullName = "input", shortName = "i", description = "Input file").required()
    val output by
        parser.option(ArgType.String, fullName = "output", shortName = "o", description = "spark out").required()
    parser.parse(args)
    withSpark {
        spark.read().textFile(input)
            .groupByKey(::groupFn)
            .mapGroups(::reduceFn)
            .flatten()
            .write()
            .option("compression", "gzip")
            .format("text")
            .save(output)
    }
}