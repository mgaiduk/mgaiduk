import kotlinx.cli.*

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat


// MR job has to be instantiated in a class
// because Hadoop runtime expects a class entrypoint
object WordCount {
    fun run(inputDataset: String, inputFeatures: String, output: String, reduceBy: String) : Boolean {
        val inputDatasetPath = Path(inputDataset)
        val inputFeaturesPath = Path(inputFeatures)
        val outputPath = Path(output)
        val conf = Configuration(true)
        conf.set("reduceBy", reduceBy)
        val job = Job.getInstance(conf)
        job.setJarByClass(WordCount::class.java)
        job.reducerClass = TReducer::class.java

        job.mapOutputKeyClass = Text::class.java
        job.mapOutputValueClass = Text::class.java
        job.outputKeyClass = Text::class.java
        job.outputValueClass = NullWritable::class.java

        MultipleInputs.addInputPath(job, inputDatasetPath, TextInputFormat::class.java, TDatasetMapper::class.java)
        MultipleInputs.addInputPath(job, inputFeaturesPath, TextInputFormat::class.java, TFeaturesMapper::class.java)

        FileOutputFormat.setOutputPath(job, outputPath)
        job.outputFormatClass = TextOutputFormat::class.java

        return job.waitForCompletion(true)
    }
}

fun main(args: Array<String>) {
    val jobUuid = java.util.UUID.randomUUID().toString()
    println("Starting job with id: $jobUuid")
    val parser = ArgParser("wordcount")
    val tmpPrefix by
    parser.option(ArgType.String, fullName = "tmp-prefix", description =
    "Filesystem prefix for intermediate results. Can be a local folder (/tmp/hadoop_jobs) or gs:// path").required()
    val inputFeatures by
    parser.option(ArgType.String, fullName = "input-features", description = "Input file").required()
    val inputDataset by
    parser.option(ArgType.String, fullName = "input-dataset", description = "Input file").required()
    val output by parser.option(ArgType.String, fullName = "output", shortName = "o", description = "output directory").required()
    parser.parse(args)
    val keyGroups = listOf("hostId", "memberId", "livestreamId", "hostId,memberId")
    var currentInputDataset = inputDataset
    for (i in keyGroups.indices) {
        val keyGroup = keyGroups[i]
        // create unique uuid
        val uuid = java.util.UUID.randomUUID().toString()
        val currentOutput = if (i == keyGroups.size - 1) {
            output
        } else {
            "$tmpPrefix/job_$jobUuid/$uuid"
        }
        println("Running job with keyGroup: $keyGroup, saving output to $currentOutput")
        val code = if (WordCount.run(inputFeatures = inputFeatures,
                inputDataset = currentInputDataset,
                output = currentOutput, reduceBy = keyGroup)) 0 else 1
        if (code != 0) {
            kotlin.system.exitProcess(code)
        }
        currentInputDataset = currentOutput
    }
}