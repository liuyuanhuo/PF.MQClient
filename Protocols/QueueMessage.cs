using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueueMessage : Message
    {
        //public long MessageOffset { get; private set; }
        public int QueueId { get; set; }
        //public long QueueOffset { get; private set; }
        public DateTime StoredTime { get; set; }
        public string RoutingKey { get; set; }
        public ulong DeliveryTag { get; set; }
        public string RoutingKeyRabbitMQ { get; set; }

        public QueueMessage(string topic, int code, byte[] body
            //, long messageOffset
            , int queueId
           // ,long queueOffset
            , DateTime storedTime
            , string routingKey
            , ulong deliveryTag)
            : base(topic, code, body)
        {
            //MessageOffset = messageOffset;
            QueueId = queueId;
            //QueueOffset = queueOffset;
            StoredTime = storedTime;
            RoutingKey = routingKey;
            DeliveryTag = deliveryTag;
        }
    }
}
