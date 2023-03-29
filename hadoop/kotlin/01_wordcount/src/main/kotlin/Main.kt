import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

// MR job has to be instantiated in a class
// because Hadoop runtime expects a class entrypoint
object WordCount {
    fun run(input: String, output: String) : Boolean {
        val inputPath = Path(input)
        val outputPath = Path(output)
        val conf = Configuration(true)
        val job = Job.getInstance(conf)
        job.setJarByClass(WordCount::class.java)
        job.setMapperClass(TMapper::class.java)
        job.setReducerClass(TReducer::class.java)

        job.mapOutputKeyClass = Text::class.java
        job.mapOutputValueClass = IntWritable::class.java
        job.outputKeyClass = Text::class.java
        job.outputValueClass = IntWritable::class.java

        FileInputFormat.addInputPath(job, inputPath)
        job.setInputFormatClass(TextInputFormat::class.java)

        FileOutputFormat.setOutputPath(job, outputPath)
        job.setOutputFormatClass(TextOutputFormat::class.java)

        return job.waitForCompletion(true)
    }
}

fun main(args: Array<String>) {
    println("Hello World!")

    // Try adding program arguments via Run/Debug configuration.
    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
    println("Program arguments: ${args.joinToString()}")

     if (args.size != 2) {
        error("Expected <input_path> <output_path>, but received ${args.size} elements.")
    }

    val code = if (WordCount.run(args[0], args[1])) 0 else 1
    System.exit(code)
}