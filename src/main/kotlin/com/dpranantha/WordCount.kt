package com.dpranantha

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.SimpleCollector
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.Validation
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.slf4j.LoggerFactory

internal object WordCount {

    private val logger = LoggerFactory.getLogger(WordCount::class.java)

    // Tokenizes line of text into individual words, this is passed into ParDo in the pipeline
    class ExtractWordsFn: DoFn<String, String>() {
        private val emptyLines = Metrics.counter(ExtractWordsFn::class.java, "emptyLines")
        private val lineLenDist = Metrics.distribution(ExtractWordsFn::class.java, "lineLenDistro")
        private val tokenizerPattern = "[^\\p{L}]+".toPattern()

        @ProcessElement
        fun processElement(@Element element: String, receiver: OutputReceiver<String>) {
            lineLenDist.update(element.length.toLong())
            if (element.trim().isEmpty()) emptyLines.inc()

            //Split the line into words
            val words = element.split(tokenizerPattern)

            //Output each word encountered into the output PCollection
            for (word in words) {
                if (word.isNotEmpty()) receiver.output(word)
            }
        }
    }

    // Converts K-V word counts into printable string
    class FormatAsTextFn: SimpleFunction<KV<String, Long>, String>() {
        override fun apply(input: KV<String, Long>): String {
            val wordCountString = "${input.key}: ${input.value}"
            logger.debug(wordCountString)
            return wordCountString
        }
    }

    // Converts a PCollection of lines of text into PCollection of formatted word counts
    class CountWords: PTransform<PCollection<String>, PCollection<KV<String, Long>>>() {
        override fun expand(input: PCollection<String>): PCollection<KV<String, Long>> {
            //Convert lines of text into individual words then count the occurrences of each word
            return input.apply(ParDo.of(ExtractWordsFn()))
                .apply(Count.perElement())
        }
    }

    interface WordCountOptions: PipelineOptions {

        //Refer to a public dataset (bucket) containing the test of King Lear
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        fun getInputFile(): String

        fun setInputFile(value: String)

        @Description("Path of the file to write to")
        @Validation.Required
        fun getOutput(): String

        fun setOutput(value: String)
    }

    @JvmStatic
    fun runWordCount(options: WordCountOptions): PipelineResult {
        val pipeline = Pipeline.create(options)

        pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
            .apply(CountWords())
            .apply(MapElements.via(FormatAsTextFn()))
            .apply("WriteCounts", TextIO.write().to(options.getOutput()))

        val results = pipeline.run()
        results.waitUntilFinish()

        return results
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args)
            .withValidation()
            .`as`(WordCountOptions::class.java)

        val results = runWordCount(options)

        //this is here just to check the metrics, in real pro env, you can add any metrics as part of simplefunction, processfunction or DoFn
        results.metrics().allMetrics().counters
            .forEach { counter -> counter.transform {
                val name = counter.name.namespace.split(".").last().replace("$","") + "_" + counter.name.name
                if (CollectorRegistry.defaultRegistry.getSampleValue(name) == null) {
                    Counter.build()
                        .name(name)
                        .help("metrics for $name")
                        .register()
                        .inc(it.toDouble())
                }
            }
        }
    }
}
