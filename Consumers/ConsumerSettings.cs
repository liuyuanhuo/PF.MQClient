using System.Net;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ConsumerSetting
    {
        public string BrokerConsumerIPEndPoint { get; set; }
        public int ConsumeThreadMaxCount { get; set; }
        //public int DefaultTimeoutMilliseconds { get; set; }
        //public int RebalanceInterval { get; set; }
        public int UpdateTopicQueueCountInterval { get; set; }
        public ushort HeartbeatBrokerIntervalSecond { get; set; }
        //public int PersistConsumerOffsetInterval { get; set; }
        //public int PullThresholdForQueue { get; set; }
        //public int PullTimeDelayMillsWhenFlowControl { get; set; }
        //public int SuspendPullRequestMilliseconds { get; set; }
        public int PullRequestTimeoutMilliseconds { get; set; }
        public int RetryMessageInterval { get; set; }
        //public int PullMessageBatchSize { get; set; }
        public MessageHandleMode MessageHandleMode { get; set; }
        //public ConsumeFromWhere ConsumeFromWhere { get; set; }

        public ConsumerSetting()
        {
            BrokerConsumerIPEndPoint = "amqp://ActorRD:ActorRD@192.168.0.79:5672/Test";
            ConsumeThreadMaxCount = 64;
            //DefaultTimeoutMilliseconds = 60 * 1000;
            //RebalanceInterval = 1000 * 5;
            HeartbeatBrokerIntervalSecond = 5;
            UpdateTopicQueueCountInterval = 1 * 1000;
            //PersistConsumerOffsetInterval = 1000 * 5;
            //PullThresholdForQueue = 10000;
            //PullTimeDelayMillsWhenFlowControl = 3000;
            //SuspendPullRequestMilliseconds = 60 * 1000;
            PullRequestTimeoutMilliseconds = 1 * 1000;
            RetryMessageInterval = 1 * 000;
            //PullMessageBatchSize = 50;
            MessageHandleMode = MessageHandleMode.Parallel;
            //ConsumeFromWhere = ConsumeFromWhere.LastOffset;
        }
    }
}
