package com.dpranantha.integration

import com.dpranantha.WordCount
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.io.File
import java.nio.file.Paths
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@RunWith(JUnit4::class)
internal class WordCountIntegrationTest {

    @Test
    fun `test WordCount should create files and metrics`() {

        // This is a simple way to create option, there are more advanced way to do so
        val options = PipelineOptionsFactory.create().`as`(WordCount.WordCountOptions::class.java)
        options.runner = DirectRunner::class.java
        // Clean and set output directory
        val url = WordCountIntegrationTest::class.java.getResource("")
        val outputPath = "${url.path}/results/"
        val outputDir = File(outputPath)
        if (outputDir.exists() && outputDir.isDirectory) {
            for (file in outputDir.listFiles().orEmpty()) {
                file.delete()
            }
        }
        options.setOutput("${outputPath}result")
        // Set input directory
        val inputPath = Paths.get("src", "test", "resources", "merchantofvenice.txt")
        options.setInputFile(inputPath.toFile().absolutePath)

        // Run the job
        val results = WordCount.runWordCount(options)

        // Collect metrics
        val metrics = HashMap<String, Long>()
        results.metrics().allMetrics().counters.forEach { counter -> counter
            .transform {
                metrics.putIfAbsent(counter.name.namespace.split(".").last().replace("$", "") + "_" + counter.name.name, it)
            }
        }

        // Check output files are there, the number of the files are dependent of number of workers
        assertTrue(outputDir.listFiles().orEmpty().isNotEmpty())

        // Check metrics are there with correct value
        assertEquals(1, metrics.size)
        assertEquals(1002, metrics.getOrDefault("WordCountExtractWordsFn_emptyLines", 0))

        // Check value of the output - later, but you get the idea
    }


}
