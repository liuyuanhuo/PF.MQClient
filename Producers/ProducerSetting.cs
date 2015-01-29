using System.Net;

namespace EQueue.Clients.Producers
{
    public class ProducerSetting
    {
        public string BrokerProducerIPEndPoint { get; set; }
        public ushort HeartbeatBrokerIntervalSecond { get; set; }
        //public int SendMessageTimeoutMilliseconds { get; set; }
        //public int UpdateTopicQueueCountInterval { get; set; }

        public ProducerSetting()
        {
            BrokerProducerIPEndPoint = "amqp://ActorRD:ActorRD@192.168.0.79:5672/Test";
            HeartbeatBrokerIntervalSecond = 5;
            //SendMessageTimeoutMilliseconds = 1000 * 60;
            //UpdateTopicQueueCountInterval = 1000 * 60;
        }
    }
}
