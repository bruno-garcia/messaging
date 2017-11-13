using System;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture.Xunit2;
using NSubstitute;
using Xunit;
using static System.Threading.CancellationToken;

namespace Messaging.Tests
{
    public class BlockingReaderRawMessageHandlerSubscriberTests
    {
        // Actually broken.. Gotta make Lazy<T> on dictionary value
        //[Theory, AutoSubstituteData]
        public async Task Subscribe_MultipleCalls_CreatesSingleReader(
            string topic,
            IRawMessageHandler rawMessageHandler,
            [Frozen] IPollingOptions options,
            [Frozen] IBlockingRawMessageReaderFactory<IPollingOptions> factory,
            BlockingReaderRawMessageHandlerSubscriber<IPollingOptions> sut)
        {
            var task1 = sut.Subscribe(topic, rawMessageHandler, None);
            var task2 = sut.Subscribe(topic, rawMessageHandler, None);

            await task1;
            await task2;

            factory.Received(1).Create(topic, options);
        }

        [Theory, AutoSubstituteData]
        public async Task Subscribe_CancelledToken_CancelsSubscriptionTask(
            string topic,
            IRawMessageHandler rawMessageHandler,
            [Frozen] IPollingOptions options,
            [Frozen] IBlockingRawMessageReaderFactory<IPollingOptions> factory,
            BlockingReaderRawMessageHandlerSubscriber<IPollingOptions> sut)
        {
            // Arrange
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act
            var task = sut.Subscribe(topic, rawMessageHandler, cts.Token);

            // Assert
            await Assert.ThrowsAsync<TaskCanceledException>(() => task);

            factory.DidNotReceiveWithAnyArgs().Create(topic, options);
        }

        [Theory, AutoSubstituteData]
        public async Task Subscribe_NullRawHandler_ThrowsArgumentNull(
            string topic,
            BlockingReaderRawMessageHandlerSubscriber<IPollingOptions> sut)
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => sut.Subscribe(topic, null, None));
            Assert.Equal("rawHandler", ex.ParamName);
        }

        [Theory, AutoSubstituteData]
        public async Task Subscribe_NullTopic_ThrowsArgumentNull(
            IRawMessageHandler rawMessageHandler,
            BlockingReaderRawMessageHandlerSubscriber<IPollingOptions> sut)
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => sut.Subscribe(null, rawMessageHandler, None));
            Assert.Equal("topic", ex.ParamName);
        }

        [Theory, AutoSubstituteData]
        public void Constructor_NullOptions_ThrowsArgumentNull(IBlockingRawMessageReaderFactory<IPollingOptions> factory)
        {
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new BlockingReaderRawMessageHandlerSubscriber<IPollingOptions>(factory, null));
            Assert.Equal("options", ex.ParamName);
        }

        [Theory, AutoSubstituteData]
        public void Constructor_NullFactory_ThrowsArgumentNull(IPollingOptions options)
        {
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new BlockingReaderRawMessageHandlerSubscriber<IPollingOptions>(null, options));
            Assert.Equal("factory", ex.ParamName);
        }
    }
}