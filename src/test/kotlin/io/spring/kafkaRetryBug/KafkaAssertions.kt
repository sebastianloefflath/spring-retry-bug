package io.spring.kafkaRetryBug

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hamcrest.MatcherAssert
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.MessageListener
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.hamcrest.KafkaMatchers
import org.springframework.kafka.test.utils.ContainerTestUtils
import org.springframework.kafka.test.utils.KafkaTestUtils
import java.time.Duration
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

inline fun <reified T> EmbeddedKafkaBroker.assertThatRecordValueIsOnTopic(
    payload: T,
    topic: String,
    timeout: Duration
) {
    val cf = DefaultKafkaConsumerFactory<String, T>(
        KafkaTestUtils.consumerProps(
            "test-${UUID.randomUUID()}",
            "false",
            this
        )
    )
    val containerProperties = ContainerProperties(topic)
    val container = KafkaMessageListenerContainer(cf, containerProperties)
    val records: BlockingQueue<ConsumerRecord<String, T>> = LinkedBlockingQueue()
    container.setupMessageListener(
        MessageListener {
            println(it)
            records.add(it)
        }
    )
    container.start()
    ContainerTestUtils.waitForAssignment(container, this.partitionsPerTopic)
    MatcherAssert.assertThat(records.poll(timeout.toMillis(), TimeUnit.MILLISECONDS), KafkaMatchers.hasValue(payload))
    container.stop()
}
