namespace Messaging
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TOptions"></typeparam>
    public interface IBlockingRawMessageReaderFactory<in TOptions>
        where TOptions : IPollingOptions
    {
        IBlockingRawMessageReader<TOptions> Create(string topic, TOptions options);
    }
}