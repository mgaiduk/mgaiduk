import kotlinx.datetime.Instant

class Row(csvRow: String, val isDataset: Boolean = true) {
    private val livestreamId: String
    val hostId: String
    val memberId: String
    private val interactionDate: String
    private val totalTimespentMins: String
    var livestreamExitTime: Instant
    val label: Int
    private val evaluationFlag: String
    var positivesHistory: String = ""
    var negativesHistory: String = ""

    init {
        val values = csvRow.split(',', limit = 8)
        this.livestreamId = values[0]
        this.hostId = values[1]
        this.memberId = values[2]
        this.interactionDate = values[3]
        this.totalTimespentMins = values[4]
        this.livestreamExitTime = Instant.fromEpochSeconds(values[5].toLong())
        this.label = values[6].toInt()
        this.evaluationFlag = values[7]
    }

    fun toCsvRow(): String {
        return "$livestreamId,$hostId,$memberId,$interactionDate,$totalTimespentMins," +
                "${livestreamExitTime.epochSeconds},$label,$evaluationFlag,$positivesHistory,$negativesHistory"
    }
}