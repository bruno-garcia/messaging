using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Messaging
{
    /// <summary>
    /// An adapter to a blocking/polling Raw Message Handler Subscriber
    /// </summary>
    /// <remarks>
    /// Creates a task per subscription to call a blocking <see cref="IBlockingRawMessageReader{TOptions}"/> for messages
    /// A call to subscribe will return a task which completes when the subcription is done.
    /// A subscription is considered to be done when a <see cref="IBlockingRawMessageReader{TOptions}"/> is created
    /// using the provided <see cref="IBlockingRawMessageReaderFactory{TOptions}"/> factory
    /// </remarks>
    /// <inheritdoc cref="IRawMessageHandlerSubscriber" />
    /// <inheritdoc cref="IDisposable" />
    public class BlockingReaderRawMessageHandlerSubscriber<TOptions> : IRawMessageHandlerSubscriber, IDisposable
        where TOptions : IPollingOptions
    {
        private readonly TOptions _options;
        private readonly IBlockingRawMessageReaderFactory<TOptions> _factory;

        private readonly ConcurrentDictionary<string,
            (Task task, CancellationTokenSource tokenSource)> _readers
            = new ConcurrentDictionary<string,
                (Task task, CancellationTokenSource tokenSource)>();

        public BlockingReaderRawMessageHandlerSubscriber(
            IBlockingRawMessageReaderFactory<TOptions> factory,
            TOptions options)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _options = options != null ? options : throw new ArgumentNullException(nameof(options));
        }

        /// <summary>
        /// Subscribes to the specified topic with the provided blocking/polling-based raw message handler
        /// </summary>
        /// <remarks>
        /// A Task is created to call the blocking <see cref="IBlockingRawMessageReader{TOptions}"/>"/>.
        /// </remarks>
        /// <param name="topic">The topic to subscribe to</param>
        /// <param name="rawHandler">The raw handler to invoke with the bytes received</param>
        /// <param name="subscriptionCancellation">A token to cancel the topic subscription</param>
        /// <returns>Task that completes when the subscription process is finished</returns>
        /// <inheritdoc />
        public Task Subscribe(string topic, IRawMessageHandler rawHandler, CancellationToken subscriptionCancellation)
        {
            if (topic == null) throw new ArgumentNullException(nameof(topic));
            if (rawHandler == null) throw new ArgumentNullException(nameof(rawHandler));

            var subscriptionTask = new TaskCompletionSource<bool>();

            _readers.AddOrUpdate(
                topic,
                CreateReader(topic, rawHandler, subscriptionTask, subscriptionCancellation),
                (_, tuple) =>
                {
                    if (tuple.task.IsFaulted || tuple.task.IsCompleted)
                    {
                        return CreateReader(topic, rawHandler, subscriptionTask, subscriptionCancellation);
                    }

                    subscriptionTask.SetResult(true);
                    return tuple;
                });

            return subscriptionTask.Task;
        }

        private (Task task, CancellationTokenSource token) CreateReader(
            string topic,
            IRawMessageHandler rawHandler,
            TaskCompletionSource<bool> subscriptionTask,
            CancellationToken subscriptionCancellation)
        {
            // Reader cancellation will let us stop this task on Dispose/Unsubscribe
            var readerCancellation = new CancellationTokenSource();

            var consumerTask = Task.Run(async () => await ReaderTaskCreation(),
                // The Reader will handle the cancellation by gracefully shutting down.
                CancellationToken.None);

            return (consumerTask, readerCancellation);

            async Task ReaderTaskCreation()
            {
                if (subscriptionCancellation.IsCancellationRequested)
                {
                    subscriptionTask.SetCanceled();
                    return;
                }

                IBlockingRawMessageReader<TOptions> reader;
                try
                {
                    reader = _factory.Create(topic, _options);
                }
                catch (Exception e)
                {
                    subscriptionTask.SetException(e);
                    return;
                }

                subscriptionTask.SetResult(true);

                await ReadMessageLoop(topic, rawHandler, reader, _options, readerCancellation.Token);
            }
        }

        // Internal for testability
        internal static async Task ReadMessageLoop(
            string topic,
            IRawMessageHandler rawHandler,
            IBlockingRawMessageReader<TOptions> reader,
            TOptions options,
            CancellationToken consumerCancellation)
        {
            try
            {
                do
                {
                    // Blocking call to the reader to retrieve message
                    if (reader.TryGetMessage(out var msg, options))
                    {
                        await rawHandler.Handle(topic, msg, consumerCancellation);
                    }
                    else if (options.SleepBetweenPolling != default(TimeSpan))
                    {
                        // Implementations where TryGetMessage will block wait for a message
                        // there's no need to sleep here..
                        await Task.Delay(options.SleepBetweenPolling, consumerCancellation);
                    }
                } while (!consumerCancellation.IsCancellationRequested);
            }
            finally
            {
                // ReSharper disable once SuspiciousTypeConversion.Global
                ((IDisposable)reader)?.Dispose();
            }
        }

        public Task Unsubscribe(string topic, IRawMessageHandler _, CancellationToken __)
        {
            if (_readers.TryRemove(topic, out var tuple))
            {
                tuple.tokenSource.Cancel();
                tuple.tokenSource.Dispose();
            }

            return tuple.task;
        }

        public void Dispose()
        {
            foreach (var topic in _readers.Keys)
            {
                try
                {
                    Unsubscribe(topic, null, CancellationToken.None)
                        .GetAwaiter()
                        .GetResult();
                }
                catch // CA1065
                {
                    // ignored
                }
            }
        }
    }
}