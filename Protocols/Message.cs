using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class Message
    {
        public string Topic { get; set; }
        public int Code { get; set; }
        public byte[] Body { get; set; }

        public Message(string topic, int code, byte[] body)
        {
            Topic = topic;
            Code = code;
            Body = body;
        }
    }
}
