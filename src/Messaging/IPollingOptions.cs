using System;

namespace Messaging
{
    public interface IPollingOptions
    {
        TimeSpan SleepBetweenPolling { get; }
    }
}