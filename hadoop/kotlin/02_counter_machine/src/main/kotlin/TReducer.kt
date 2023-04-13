import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.jsonPrimitive
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration
import kotlinx.datetime.Instant
import kotlinx.serialization.encodeToString
import org.apache.hadoop.conf.Configuration
import kotlin.math.exp
import kotlin.time.DurationUnit


class Counter(private val decayInterval: Duration,
              private val eventName: String,
              val name: String,
              val consumeEvent: (MutableMap<String, JsonElement>) -> Double,){
    var value = 0.0
    private var lastUpdate = Instant.fromEpochSeconds(0)
    fun update(event: MutableMap<String, JsonElement>) {
        if (event.getEventName() != this.eventName) {
            return
        }
        val eventTime = event.getEventTime()
        val timeDiff = eventTime - lastUpdate
        val decay = exp(-timeDiff.toDouble(DurationUnit.SECONDS) / decayInterval.toDouble(DurationUnit.SECONDS))
        value = decay * value + consumeEvent(event)
        lastUpdate = eventTime
    }
}

fun makeCounters() : List<Counter> {
    val result = mutableListOf<Counter>()
    result.add(Counter(decayInterval = 30.days,
        eventName = "view_end",
        name = "timespent_decay_decay_30d") {
        it["duration_ms"]!!.jsonPrimitive.content.toDouble()
    })
    result.add(Counter(decayInterval = 30.days,
        eventName = "view_end",
        name = "lives_count_decay_30d") {
        val durationMs = it["duration_ms"]!!.jsonPrimitive.content.toDouble()
        if (durationMs > 1500*60) {
            1.0
        } else {
            0.0
        }
    })
    result.add(Counter(decayInterval = 30.days,
        eventName = "like",
        name = "like_count_decay_30d") {
        it["like_counter"]!!.jsonPrimitive.content.toDouble()
    })
    result.add(Counter(decayInterval = 30.days,
        eventName = "gift",
        name = "gift_count_decay_30d") {
        it["gift_quantity"]!!.jsonPrimitive.content.toDouble()
    })
    result.add(Counter(decayInterval = 30.days,
        eventName = "gift",
        name = "gift_value_decay_30d") {
        it["gift_quantity"]!!.jsonPrimitive.content.toDouble() * it["gift_cheers_value"]!!.jsonPrimitive.content.toDouble()
    })
    result.add(Counter(decayInterval = 30.days,
        eventName = "share",
        name = "shares_count_decay_30d") {
        1.0
    })
    result.add(Counter(decayInterval = 30.days,
        eventName = "comment",
        name = "comments_count_decay_30d") {
        1.0
    })
    return result
}
class TReducer : Reducer<Text, Text, Text, NullWritable>() {
    override fun reduce(key: Text, values: Iterable<Text>, context: Context) {
        val conf: Configuration = context.configuration
        val featurePrefix = StringBuilder("feature_")
        val reduceBy = conf.get("reduceBy").split(",")
        for (k in reduceBy) {
            featurePrefix.append(k)
            featurePrefix.append("_")
        }
        var elements = values.map {
            Json.parseToJsonElement(it.toString()).jsonObject.toMutableMap()
        }

        for (gap in listOf(1.minutes, 1.hours, 1.days)) {
            elements = elements.map {
                if (it.getInputType() != InputType.DATASET) {
                    it.addGap(gap)
                }
                it
            }.sortedBy { it.getEventTime() }
            val counters = makeCounters()
            for (element in elements) {
                for (counter in counters) {
                    counter.update(element)
                }
                if (element.getInputType() == InputType.DATASET) {
                    for (counter in counters) {
                        val featureName = featurePrefix.toString() + "gap{$gap}_" + counter.name
                        element[featureName] = JsonPrimitive(counter.value)
                    }
                } else {
                    element.addGap(-gap)
                }
            }
        }
        for (element in elements) {
            if (element.getInputType() == InputType.DATASET) {
                context.write(Text(Json.encodeToString(element)), NullWritable.get())
            }
        }
    }
}