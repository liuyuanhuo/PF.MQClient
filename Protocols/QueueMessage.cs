﻿using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueueMessage : Message
    {
        //public string MessageId { get; private set; }
        //public long MessageOffset { get; private set; }
        public int QueueId { get; private set; }
        //public long QueueOffset { get; private set; }
        public DateTime ArrivedTime { get; private set; }
        public DateTime StoredTime { get; internal set; }
        public string RoutingKey { get; private set; }
        public ulong DeliveryTag { get; set; }
        public string RoutingKeyRabbitMQ { get; set; }

        public QueueMessage(
            //string messageId, 
            string topic, int code, byte[] body,
            //long messageOffset, 
            int queueId, 
            //long queueOffset, 
            DateTime createdTime, DateTime arrivedTime, DateTime storedTime,
            string routingKey, 
            ulong deliveryTag)
            : base(topic, code, body, createdTime)
        {
            //MessageId = messageId;
            //MessageOffset = messageOffset;
            QueueId = queueId;
            //QueueOffset = queueOffset;
            ArrivedTime = arrivedTime;
            StoredTime = storedTime;
            RoutingKey = routingKey;
            DeliveryTag = deliveryTag;
        }
    }
}