package io.spring.kafkaRetryBug

import org.junit.jupiter.api.Test
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import java.time.Duration

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
	topics = ["foo", "foo-dlt"],
	bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class ReproduceBugTest(
	@Autowired private val embeddedKafka: EmbeddedKafkaBroker,
	@Autowired private val kafkaTemplate: KafkaTemplate<String, String>,
) {

	@MockBean
	private lateinit var recordProcessor: RecordProcessor

	@Test
	fun `this fails, because FatalException is not recognized after RetryException`() {
		val payload = "does not matter"
		`when`(recordProcessor.processFooRecord()).thenThrow(RetryException(), FatalException())

		kafkaTemplate.send("foo", payload).get()

		Thread.sleep(5_000)

		// expect: record is processed twice: 1. RetryException -> 2. FatalException
		verify(recordProcessor, times(2)).processFooRecord()
		embeddedKafka.assertThatRecordValueIsOnTopic(payload, "foo-dlt", timeout = Duration.ofSeconds(10))
	}
}
