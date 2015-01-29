using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using ECommon.Extensions;
using System.Threading.Tasks;
using EQueue.Protocols;
using ECommon.Scheduling;
using ECommon.Logging;
using ECommon.Serializing;
using EQueue.Clients;
using RabbitMQ.Client;
using EQueue.Clients.Consumers;
using RabbitMQ.Client.Events;
using ECommon.Components;

namespace EQueue.Clients.Consumers
{
    public class Consumer
    {
        #region Private Members

        private readonly object _lockObject;

        private IConnection _connection;
        private IModel _channelConsumer; 
        private  QueueingBasicConsumer _consumer;

        private readonly IBinarySerializer _binarySerializer;
        private readonly ConcurrentBag<string> _subscriptionTopics;
        private readonly List<int> _taskIds;
        private readonly TaskFactory _taskFactory;
        private readonly ConcurrentDictionary<string, IList<MessageQueue>> _topicQueuesDict;
        private readonly ConcurrentDictionary<string, ConsumingMessage> _handlingMessageDict;
        private ConcurrentQueue<ulong> _finishedMessageTag;
        private readonly BlockingCollection<ConsumingMessage> _consumingMessageQueue;
        private readonly BlockingCollection<ConsumingMessage> _messageRetryQueue;
        private readonly IScheduleService _scheduleService;
        //private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStragegy;
        private Worker _executePullRequestWorker;
        private Worker _handleMessageWorker;
        private readonly ILogger _logger;
        //private readonly AutoResetEvent _waitSocketConnectHandle;
        private IMessageHandler _messageHandler;
        private bool _stoped;
        private bool _isBrokerServerConnected;
        private bool _bNeedUpdateConsumer = true;

        #endregion

        #region Public Properties

        public string Id { get; private set; }
        public ConsumerSetting Setting { get; private set; }
        public string GroupName { get; private set; }
        public IEnumerable<string> SubscriptionTopics
        {
            get { return _subscriptionTopics; }
        }

        #endregion

        #region Constructors

        public Consumer(string id, string groupName) : this(id, groupName, new ConsumerSetting()) { }
        public Consumer(string id, string groupName, ConsumerSetting setting)
        {
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }
            if (groupName == null)
            {
                throw new ArgumentNullException("groupName");
            }
            Id = id;
            GroupName = groupName;
            Setting = setting ?? new ConsumerSetting();

            _lockObject = new object();
            _subscriptionTopics = new ConcurrentBag<string>();
            _topicQueuesDict = new ConcurrentDictionary<string, IList<MessageQueue>>();
            _consumingMessageQueue = new BlockingCollection<ConsumingMessage>(new ConcurrentQueue<ConsumingMessage>());
            _messageRetryQueue = new BlockingCollection<ConsumingMessage>(new ConcurrentQueue<ConsumingMessage>());
            _handlingMessageDict = new ConcurrentDictionary<string, ConsumingMessage>();
            _finishedMessageTag = new ConcurrentQueue<ulong>();
            _taskIds = new List<int>();
            _taskFactory = new TaskFactory(new LimitedConcurrencyLevelTaskScheduler(Setting.ConsumeThreadMaxCount));


            _binarySerializer = ObjectContainer.Resolve < IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            //_allocateMessageQueueStragegy = Part.Global.Interfaces.IOCContainer.Resolve<IAllocateMessageQueueStrategy>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            //_waitSocketConnectHandle = new AutoResetEvent(false);
        }

        #endregion

        #region Public Methods

        public Consumer SetMessageHandler(IMessageHandler messageHandler)
        {
            if (messageHandler == null)
            {
                throw new ArgumentNullException("messageHandler");
            }
            _messageHandler = messageHandler;
            return this;
        }
        public Consumer Start()
        {
            //EnsureMQClient();

            _isBrokerServerConnected = true;

            _stoped = false;
            //_waitSocketConnectHandle.WaitOne();

            RefreshTopicQueues();
            _executePullRequestWorker = new Worker("Consumer.ExecutePullRequest", ExecutePullRequest);
            _handleMessageWorker = new Worker("Consumer.HandleMessage", HandleMessage);
            StartBackgroundJobs();

            _logger.Info(String.Format("Started, consumerId:{0}, group:{1}.", Id, GroupName));
            return this;
        }

        private void EnsureMQConnection()
        {
            if (this._connection == null || !this._connection.IsOpen)
            {
                //CloseMQConnection();

                var connectionFactory = new ConnectionFactory
                {
                    Uri = Setting.BrokerConsumerIPEndPoint,
                    RequestedHeartbeat = Setting.HeartbeatBrokerIntervalSecond
                };
                _connection = connectionFactory.CreateConnection();
                _bNeedUpdateConsumer = true;
            }
        }
        private void EnsureMQChannel()
        {
            if (this._channelConsumer == null || !this._channelConsumer.IsOpen)
            {
                //CloseMQChannel();

                _channelConsumer = _connection.CreateModel();
                _bNeedUpdateConsumer = true;
            }
        }
        private void CloseMQConnection()
        {
            if (this._connection != null)
            {
                if (_connection.IsOpen)
                {
                    _connection.Close();
                }
               // this._connection.Dispose();
            }
        }
        private void CloseMQChannel()
        {
            if (this._channelConsumer != null)
            {
                if (_channelConsumer.IsOpen)
                {
                    _channelConsumer.Close();
                }
               // this._channelConsumer.Dispose();
            }
        }
        public Consumer Shutdown()
        {
            StopBackgroundJobs();

            _stoped = true;

            CloseMQChannel();
            CloseMQConnection();

            _logger.Info(String.Format("Shutdown, consumerId:{0}, group:{1}.", Id, GroupName));
            return this;
        }
        public Consumer Subscribe(string topic)
        {
            if (!_subscriptionTopics.Contains(topic))
            {
                _subscriptionTopics.Add(topic);

                if (_isBrokerServerConnected)
                {
                    UpdateTopicQueues(topic);
                }
            }
            return this;
        }

        #endregion

        #region Private Methods

        private void ExecutePullRequest()
        {
            if (_stoped) return;
            try
            {
                if (_channelConsumer != null && this._channelConsumer.IsOpen)
                {
                    ulong deliveryTag;
                    while (this._finishedMessageTag.TryDequeue(out deliveryTag))
                    {
                        this._channelConsumer.BasicAck(deliveryTag, false);
                    }
                }

                EnsureMQConnection();
                EnsureMQChannel();
                if (_bNeedUpdateConsumer)
                {
                    _bNeedUpdateConsumer = false;

                    this._finishedMessageTag = new ConcurrentQueue<ulong>();

                    _consumer = new QueueingBasicConsumer(_channelConsumer);

                    foreach (var topic in SubscriptionTopics)
                    {
                        if (_topicQueuesDict.ContainsKey(topic))
                        {
                            IEnumerable<MessageQueue> messageQueues = _topicQueuesDict[topic];
                            foreach (var queue in messageQueues)
                            {
                                string strQueue = queue.Topic + "." + queue.QueueId;
                                //string consumerTag = strQueue;
                                //_channelConsumer.BasicCancel(consumerTag);
                                _channelConsumer.BasicConsume(strQueue, noAck: false, consumerTag: strQueue, consumer: _consumer);
                            }
                        }
                    }
                }

                bool bWaitToAck = false;
                if (this._finishedMessageTag.Count > 0 || _handlingMessageDict.Count > 0 || _consumingMessageQueue.Count > 0)
                {
                    bWaitToAck = true;
                }
                BasicDeliverEventArgs basicDeliverEventArgs;
                if (bWaitToAck)
                {
                    basicDeliverEventArgs = this._consumer.Queue.DequeueNoWait(null);
                }
                else
                {
                    this._consumer.Queue.Dequeue(Setting.PullRequestTimeoutMilliseconds, out basicDeliverEventArgs);
                }
                if (basicDeliverEventArgs != null)
                {
                    QueueMessage message = _binarySerializer.Deserialize<QueueMessage>(basicDeliverEventArgs.Body);
                    message.DeliveryTag = basicDeliverEventArgs.DeliveryTag;
                    message.RoutingKeyRabbitMQ = basicDeliverEventArgs.RoutingKey;

                    _consumingMessageQueue.Add(new ConsumingMessage(message));
                    bWaitToAck = true;
                }
            }
            catch (Exception ex)
            {
                _bNeedUpdateConsumer = true;
                if (_stoped) return;
                if (_isBrokerServerConnected)
                {
                    _logger.Error(string.Format("PullMessage has exception, ConsumerTag:{0}", this._consumer==null? " ":this._consumer.ConsumerTag ), ex);
                }
            }
        }
        private void HandleMessage()
        {
            var consumingMessage = _consumingMessageQueue.Take();

            if (_stoped) return;
            if (consumingMessage == null) return;
            //if (consumingMessage.ProcessQueue.IsDropped) return;

            var handleAction = new Action(() =>
            {
                if (!_handlingMessageDict.TryAdd(consumingMessage.Message.RoutingKey, consumingMessage))
                {
                    _logger.Warn(String.Format("Ignore to handle message [RoutingKey={0}, topic={1}, queueId={2}, queueOffset={3}, consumerId={4}, group={5}], as it is being handling.",
                        consumingMessage.Message.RoutingKey,
                        consumingMessage.Message.Topic,
                        consumingMessage.Message.QueueId,
                        -999,//consumingMessage.Message.QueueOffset,
                        Id,
                        GroupName));
                    return;
                }
                HandleMessage(consumingMessage);
            });

            if (Setting.MessageHandleMode == MessageHandleMode.Sequential)
            {
                handleAction();
            }
            else if (Setting.MessageHandleMode == MessageHandleMode.Parallel)
            {
                _taskFactory.StartNew(handleAction);
            }
        }
        private void RetryMessage()
        {
            HandleMessage(_messageRetryQueue.Take());
        }
        private void HandleMessage(ConsumingMessage consumingMessage)
        {
            if (_stoped) return;
            if (consumingMessage == null) return;
            //if (consumingMessage.ProcessQueue.IsDropped) return;

            try
            {
                _messageHandler.Handle(consumingMessage.Message, new MessageContext(currentQueueMessage => RemoveHandledMessage(consumingMessage)));
            }
            catch (Exception ex)
            {
                //TODO，目前，对于消费失败（遇到异常）的消息，我们先记录错误日志，然后将该消息放入本地内存的重试队列；
                //放入重试队列后，会定期对该消息进行重试，重试队列中的消息会每隔1s被取出一个来重试。
                //通过这样的设计，可以确保消费有异常的消息不会被认为消费已成功，也就是说不会从ProcessQueue中移除；
                //但不影响该消息的后续消息的消费，该消息的后续消息仍然能够被消费，但是ProcessQueue的消费位置，即滑动门不会向前移动了；
                //因为只要该消息一直消费遇到异常，那就意味着该消息所对应的queueOffset不能被认为已消费；
                //而我们发送到broker的是当前最小的已被成功消费的queueOffset，所以broker上记录的当前queue的消费位置（消费进度）不会往前移动，
                //直到当前失败的消息消费成功为止。所以，如果我们重启了消费者服务器，那下一次开始消费的消费位置还是从当前失败的位置开始，
                //即便当前失败的消息的后续消息之前已经被消费过了；所以应用需要对每个消息的消费都要支持幂等，不过enode对所有的command和event的处理都支持幂等；
                //以后，我们会在broker上支持重试队列，然后我们可以将消费失败的消息发回到broker上的重试队列，发回到broker上的重试队列成功后，
                //就可以让当前queue的消费位置往前移动了。
                LogMessageHandlingException(consumingMessage, ex);
                _messageRetryQueue.Add(consumingMessage);
            }
        }
        private void RemoveHandledMessage(ConsumingMessage consumingMessage)
        {
            ConsumingMessage consumedMessage;
            if (_handlingMessageDict.TryRemove(consumingMessage.Message.RoutingKey, out consumedMessage))
            {
                _finishedMessageTag.Enqueue(consumingMessage.Message.DeliveryTag);
            }
        }
        private void LogMessageHandlingException(ConsumingMessage consumingMessage, Exception exception)
        {
            _logger.Error(string.Format(
                "Message handling has exception, message info:[RoutingKey={0}, topic={1}, queueId={2}, queueOffset={3}, storedTime={4}, consumerId={5}, group={6}]",
                consumingMessage.Message.RoutingKey,
                consumingMessage.Message.Topic,
                consumingMessage.Message.QueueId,
                -999,//consumingMessage.Message.QueueOffset,
                consumingMessage.Message.StoredTime,
                Id,
                GroupName), exception);
        }

        private void RefreshTopicQueues()
        {
            foreach (var topic in SubscriptionTopics)
            {
                UpdateTopicQueues(topic);
            }
        }
        private void UpdateTopicQueues(string topic)
        {
            try
            {
                if (!_topicQueuesDict.Keys.Contains(topic))
                {
                    _topicQueuesDict[topic] = GetTopicQueueIdsAndInitServer(topic);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("UpdateTopicQueues failed, consumerId:{0}, group:{1}, topic:{2}", Id, GroupName, topic), ex);
            }
        }

        private List<MessageQueue> GetTopicQueueIdsAndInitServer(string topic)
        {
            var queueIds = "1,2,3,4";
            IEnumerable<int> strQueueIds = queueIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(x => int.Parse(x));

            var messageQueues = new List<MessageQueue>();

            EnsureMQConnection();
            using (IModel channelDeclare = _connection.CreateModel())
            {
                foreach (int queueId in strQueueIds)
                {
                    string strExchange = topic + "." + queueId;
                    string strQueue = topic + "." + queueId;
                    string strRoutingKey = topic + "." + queueId;
                    channelDeclare.ExchangeDeclare(strExchange, "topic", true, false, null);
                    channelDeclare.QueueDeclare(strQueue, true, false, false, null);
                    channelDeclare.QueueBind(strQueue, strExchange, strRoutingKey);

                    messageQueues.Add(new MessageQueue(topic, queueId));
                }
                _bNeedUpdateConsumer = true;
                _topicQueuesDict[topic] = messageQueues;
                channelDeclare.Close();
            }

            return messageQueues;
        }
        private void StartBackgroundJobs()
        {
            lock (_lockObject)
            {
                StopBackgroundJobsInternal();
                StartBackgroundJobsInternal();
            }
        }
        private void StopBackgroundJobs()
        {
            lock (_lockObject)
            {
                StopBackgroundJobsInternal();
            }
        }
        private void StartBackgroundJobsInternal()
        {
            _taskIds.Add(_scheduleService.ScheduleTask("Consumer.RefreshTopicQueues", RefreshTopicQueues, Setting.UpdateTopicQueueCountInterval, Setting.UpdateTopicQueueCountInterval));
            //_taskIds.Add(_scheduleService.ScheduleTask("Consumer.SendHeartbeat", SendHeartbeat, Setting.HeartbeatBrokerInterval, Setting.HeartbeatBrokerInterval));
            //_taskIds.Add(_scheduleService.ScheduleTask("Consumer.Rebalance", Rebalance, Setting.RebalanceInterval, Setting.RebalanceInterval));
            //_taskIds.Add(_scheduleService.ScheduleTask("Consumer.PersistOffset", PersistOffset, Setting.PersistConsumerOffsetInterval, Setting.PersistConsumerOffsetInterval));
            _taskIds.Add(_scheduleService.ScheduleTask("Consumer.RetryMessage", RetryMessage, Setting.RetryMessageInterval, Setting.RetryMessageInterval));

            _executePullRequestWorker.Start();
            _handleMessageWorker.Start();
        }
        private void StopBackgroundJobsInternal()
        {
            foreach (var taskId in _taskIds)
            {
                _scheduleService.ShutdownTask(taskId);
            }

            _executePullRequestWorker.Stop();
            _handleMessageWorker.Stop();

            if (_consumingMessageQueue.Count == 0)
            {
                _consumingMessageQueue.Add(null);
            }
            if (_messageRetryQueue.Count == 0)
            {
                _messageRetryQueue.Add(null);
            }

            Clear();
        }
        private void Clear()
        {
            _taskIds.Clear();
            _topicQueuesDict.Clear();
        }
        private bool IsIntCollectionChanged(IList<int> first, IList<int> second)
        {
            if (first.Count != second.Count)
            {
                return true;
            }
            for (var index = 0; index < first.Count; index++)
            {
                if (first[index] != second[index])
                {
                    return true;
                }
            }
            return false;
        }

        #endregion

        //void ISocketClientEventListener.OnConnectionClosed(ITcpConnectionInfo connectionInfo, SocketError socketError)
        //{
        //    _isBrokerServerConnected = false;
        //    StopBackgroundJobs();
        //}
        //void ISocketClientEventListener.OnConnectionEstablished(ITcpConnectionInfo connectionInfo)
        //{
        //    _isBrokerServerConnected = true;
        //    _waitSocketConnectHandle.Set();
        //    StartBackgroundJobs();
        //}
        //void ISocketClientEventListener.OnConnectionFailed(ITcpConnectionInfo connectionInfo, SocketError socketError)
        //{
        //}
    }
}
