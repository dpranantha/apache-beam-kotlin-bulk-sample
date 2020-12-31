package com.dpranantha

import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ParDo
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
internal class WordCountTest {

    @Rule
    @JvmField
    var pipeline: TestPipeline = TestPipeline.create()

    @Test
    fun `test ExtractWordsFn`() {
        val words = listOf(" some input words ", " ", " cool ", " foo", "bar ")
        val output = pipeline.apply(Create.of(words).withCoder(StringUtf8Coder.of()))
            .apply(ParDo.of(WordCount.ExtractWordsFn()))
        PAssert.that(output).containsInAnyOrder("some", "input", "words", "cool", "foo", "bar")
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `test CountWords`() {
        val input = pipeline.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()))

        val output = input.apply(WordCount.CountWords())
            .apply(MapElements.via(WordCount.FormatAsTextFn()))

        PAssert.that(output).containsInAnyOrder(*COUNT_ARRAY)
        pipeline.run().waitUntilFinish()
    }

    companion object {
        @JvmStatic
        val WORDS_ARRAY = arrayOf("hi there", "hi", "hi sue bob", "hi sue", "bob hi")
        @JvmStatic
        val WORDS = WORDS_ARRAY.asList()
        @JvmStatic
        val COUNT_ARRAY = arrayOf("hi: 5", "there: 1", "sue: 2", "bob: 2")
    }
}
