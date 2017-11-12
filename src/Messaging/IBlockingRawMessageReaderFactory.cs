namespace Messaging
{
    public interface IBlockingMessageReaderFactory<in TOptions>
        where TOptions : IPollingOptions
    {
        IBlockingRawMessageReader<TOptions> Create(string topic, TOptions options);
    }
}