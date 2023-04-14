/* SimpleApp.kt */
@file:JvmName("WordCount")
import org.jetbrains.kotlinx.spark.api.*
import org.jetbrains.kotlinx.spark.api.tuples.*
import kotlinx.cli.*

fun main(args: Array<String>) {
    val parser = ArgParser("wordcount")
    val input by
        parser.option(ArgType.String, fullName = "input", shortName = "i", description = "Input file").required()
    val output by
        parser.option(ArgType.String, fullName = "output", shortName = "o", description = "spark out").required()
    parser.parse(args)
    withSpark {
        spark.read().textFile(input)
            .map { it.split(Regex("\\s"))}
            .flatten()
            .groupByKey { it }
            .mapGroups { k, iter -> k X iter.asSequence().count() }
            .write()
            .format("csv")
            .save(output)
    }
}