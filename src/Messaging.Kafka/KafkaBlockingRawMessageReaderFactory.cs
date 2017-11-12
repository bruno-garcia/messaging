using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Messaging.Kafka
{
    /// <summary>
    /// Creates instances of <see cref="T:Messaging.Kafka.KafkaBlockingRawMessageReader" /> which are already subscribed to the specified topic
    /// </summary>
    /// <inheritdoc />
    public class KafkaBlockingRawMessageReaderFactory : IBlockingMessageReaderFactory<KafkaOptions>
    {
        private static readonly ByteArrayDeserializer Deserializer = new ByteArrayDeserializer();

        /// <summary>
        /// Creates a new <see cref="KafkaBlockingRawMessageReader"/> subscribed already to the specified <param name="topic"/>
        /// </summary>
        /// <param name="topic">The topic to subscribe the reader</param>
        /// <param name="options">Kafka Options</param>
        /// <returns><see cref="KafkaBlockingRawMessageReader"/> subscribed to <param name="topic"/></returns>
        public IBlockingRawMessageReader<KafkaOptions> Create(string topic, KafkaOptions options)
        {
            var consumer = new Consumer<Null, byte[]>(options.Properties, null, Deserializer);
            options.Subscriber.ConsumerCreatedCallback?.Invoke(consumer);
            consumer.Subscribe(topic);
            return new KafkaBlockingRawMessageReader(consumer);
        }

        private class ByteArrayDeserializer : IDeserializer<byte[]>
        {
            public byte[] Deserialize(byte[] data) => data;
        }
    }
}
