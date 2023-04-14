import kotlinx.datetime.Instant
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import kotlin.time.Duration

fun MutableMap<String, JsonElement>.getEventName() : String {
    return this["event_name"]!!.jsonPrimitive.content
}

fun MutableMap<String, JsonElement>.addGap(gap: Duration) {
    val eventTimeSeconds = this["event_time"]!!.jsonPrimitive.content.toLong()
    var eventTime = Instant.fromEpochSeconds(eventTimeSeconds)
    eventTime += gap
    this["event_time"] = JsonPrimitive(eventTime.epochSeconds.toString())
}

fun MutableMap<String, JsonElement>.getInputType() : InputType {
    return InputType.valueOf(this[InputTypeName]!!.jsonPrimitive.content)
}

fun MutableMap<String, JsonElement>.getEventTime() : Instant {
    return Instant.fromEpochSeconds(this["event_time"]!!.jsonPrimitive.content.toLong())
}