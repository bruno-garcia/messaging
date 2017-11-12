namespace Messaging
{
    public interface IBlockingMessageReaderFactory<in TOptions>
        where TOptions : IPollingOptions
    {
        IBlockingMessageReader<TOptions> Create(string topic, TOptions options);
    }
}