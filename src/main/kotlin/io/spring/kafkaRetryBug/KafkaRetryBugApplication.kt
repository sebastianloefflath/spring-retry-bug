package io.spring.kafkaRetryBug

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import org.springframework.stereotype.Component
import org.springframework.util.backoff.FixedBackOff

@SpringBootApplication
class KafkaRetryBugApplication(
	private val recordProcessor: RecordProcessor
) {

	private val log: Logger = LoggerFactory.getLogger(this::class.java)

	@KafkaListener(topics = ["foo"], groupId = "bar")
	fun fooListener(record: ConsumerRecord<String, String>): Unit = try {
		recordProcessor.processFooRecord()
	} catch (ex: Exception) {
		log.error("processing record failed, cause ${ex::class.simpleName}")
		throw ex
	}
}

@Component
class RecordProcessor {
	fun processFooRecord(): Unit = TODO("will be mocked in the test")
}

@Configuration
class KafkaConfig : RetryTopicConfigurationSupport() {
	@Bean
	fun defaultRetryConfig(
		kafkaTemplate: KafkaTemplate<*, *>
	) =
		RetryTopicConfigurationBuilder
			.newInstance()
			.maxAttempts(1)
			.dltSuffix("-dlt")
			.notRetryOn(FatalException::class.java) // send directly to DLT
			.traversingCauses(true)
			.autoStartDltHandler(false) // do not consume DLT topic
			.create(kafkaTemplate)

	override fun configureBlockingRetries(blockingRetries: BlockingRetriesConfigurer) {
		blockingRetries
			.retryOn(RetryException::class.java)
			.backOff(FixedBackOff(1_000, FixedBackOff.UNLIMITED_ATTEMPTS))
	}

	override fun manageNonBlockingFatalExceptions(nonBlockingRetriesExceptions: MutableList<Class<out Throwable>>) {
		nonBlockingRetriesExceptions.add(FatalException::class.java)
	}

	@Bean
	fun taskScheduler() = ThreadPoolTaskScheduler()
}

class RetryException : RuntimeException()
class FatalException : RuntimeException()


fun main(args: Array<String>) {
	runApplication<KafkaRetryBugApplication>(*args)
}
