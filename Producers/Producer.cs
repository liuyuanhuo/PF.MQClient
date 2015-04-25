using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EQueue.Protocols;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Logging;
using ECommon.Utilities;
using EQueue.Clients;
using RabbitMQ.Client;
using ECommon.Components;

namespace EQueue.Clients.Producers
{
    public class Producer
    {
        private readonly object _lockObject;
        private readonly ConcurrentDictionary<string, IList<int>> _topicQueueIdsDict;
        private readonly IScheduleService _scheduleService;

        private IConnection _connection;
        private IModel _channel;
        private IBasicProperties _basicProp;

        private readonly IBinarySerializer _binarySerializer;
        private readonly IQueueSelector _queueSelector;
        private readonly ILogger _logger;
        private readonly List<int> _taskIds;

        public string Id { get; private set; }
        public ProducerSetting Setting { get; private set; }

        public Producer(string id) : this(id, null) { }
        public Producer(string id, ProducerSetting setting)
        {
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }
            Id = id;
            Setting = setting ?? new ProducerSetting();

            _lockObject = new object();
            _taskIds = new List<int>();
            _topicQueueIdsDict = new ConcurrentDictionary<string, IList<int>>();
            
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueSelector = ObjectContainer.Resolve<IQueueSelector>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName); 
        }

        private void EnsureProduceMQClient()
        {
            if (this._connection == null || !this._connection.IsOpen)
            {
                //if (this._connection != null)
                //{
                //    this._connection.Dispose();
                //}
                var connectionFactory = new ConnectionFactory
                {
                    Uri = Setting.BrokerProducerIPEndPoint,
                    RequestedHeartbeat = Setting.HeartbeatBrokerIntervalSecond
                };
                _connection = connectionFactory.CreateConnection();
            }
            if (this._channel == null || !this._channel.IsOpen)
            {
                //if (_channel != null)
                //{
                //    _channel.Dispose();
                //}
                _channel = _connection.CreateModel();
            }
            _basicProp = _channel.CreateBasicProperties();
            _basicProp.DeliveryMode = 2;
        }
        private void CloseProduceMQClient()
        {
            if (_channel != null && _channel.IsOpen)
            {
                if (_channel.IsOpen)
                {
                    _channel.Close();
                }
                //_channel.Dispose();
            }
            if (this._connection != null)
            {
                if (_connection.IsOpen)
                {
                    _connection.Close();
                }
                //this._connection.Dispose();
            }
        }

        public Producer Start()
        {
            //EnsureProduceMQClient();

            _logger.Info(String.Format("Started, producerId:{0}", Id));
            return this;
        }

        public Producer Shutdown()
        {
            CloseProduceMQClient();

            _logger.Info(String.Format("Shutdown, producerId:{0}", Id));
            return this;
        }

        public SendResult Send(Message message, object routingKey)
        {
            Ensure.NotNull(message, "message");

            EnsureProduceMQClient();

            var currentRoutingKey = GetStringRoutingKey(routingKey);
            var queueIds = GetTopicQueueIds(message.Topic);
            var queueId = _queueSelector.SelectQueueId(queueIds, message, currentRoutingKey);
            if (queueId < 0)
            {
                throw new Exception(string.Format("No available routing queue for topic [{0}].", message.Topic));
            }

            string strExchange = message.Topic + "." + queueId;
            string strRoutingKey = message.Topic + "." + queueId;

            var queueMessage = new QueueMessage(message.Topic, message.Code, message.Body, queueId, DateTime.Now, DateTime.Now, DateTime.Now, currentRoutingKey, 0);
            var data = _binarySerializer.Serialize(queueMessage);
            //var request = new SendMessageRequest { Message = message, QueueId = queueId, RoutingKey = routingKey.ToString() };
            //var data = MessageUtils.EncodeSendMessageRequest(request);
            _channel.BasicPublish(strExchange, strRoutingKey, _basicProp, data);

            return new SendResult(SendStatus.Success);
        }
        public async Task<SendResult> SendAsync(Message message, object routingKey)
        {
            return Send(message, routingKey);
        }

        private string GetStringRoutingKey(object routingKey)
        {
            Ensure.NotNull(routingKey, "routingKey");
            var ret = routingKey.ToString();
            Ensure.NotNullOrEmpty(ret, "routingKey");
            return ret;
        }
        private IList<int> GetTopicQueueIds(string topic)
        {
            IList<int> queueIds;
            if (!_topicQueueIdsDict.TryGetValue(topic, out queueIds))
            {
                var queueIdsFromServer = GetTopicQueueIdsAndInitServer(topic).ToList();
                _topicQueueIdsDict[topic] = queueIdsFromServer;
                queueIds = queueIdsFromServer;
            }

            return queueIds;
        }

        private IEnumerable<int> GetTopicQueueIdsAndInitServer(string topic)
        {
            var queueIds = "1,2,3,4";
            IEnumerable<int> strQueueIds = queueIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(x => int.Parse(x));
            
            foreach (int strQueueID in strQueueIds)
            {
                string strExchange = topic + "." + strQueueID;
                string strQueue = topic + "." + strQueueID;
                string strRoutingKey = topic + "." + strQueueID;
                _channel.ExchangeDeclare(strExchange, "topic", true, false, null);
                _channel.QueueDeclare(strQueue, true, false, false, null);
                _channel.QueueBind(strQueue, strExchange, strRoutingKey);
            }

            return strQueueIds;
        }
    }
}
