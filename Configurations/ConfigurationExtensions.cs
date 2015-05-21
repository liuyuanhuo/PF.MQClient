
using EQueue.Clients.Consumers;
using EQueue.Clients.Producers;
using ECommon.Configurations;

namespace EQueue.Configurations
{
    public static class ConfigurationExtensions
    {
        public static Configuration RegisterEQueueComponents(this Configuration appBuilder)
        {
            //PF.Global.Interfaces.IOCContainer.Register<AverageAllocateMessageQueueStrategy, IAllocateMessageQueueStrategy>();
            appBuilder.SetDefault<IQueueSelector, QueueHashSelector>();
            //appBuilder.SetDefault<IMessageStore, InMemoryMessageStore>(new InMemoryMessageStore(new InMemoryMessageStoreSetting()));
            //appBuilder.SetDefault<IMessageService, MessageService>();
            //appBuilder.SetDefault<IOffsetManager, InMemoryOffsetManager>();
            return appBuilder;
        }

        //public static IAppBuilder UseInMemoryMessageStore(this IAppBuilder appBuilder, InMemoryMessageStoreSetting setting)
        //{
        //    appBuilder.SetDefault<IMessageStore, InMemoryMessageStore>(new InMemoryMessageStore(setting));
        //    return appBuilder;
        //}
        //public static IAppBuilder UseSqlServerMessageStore(this IAppBuilder appBuilder, SqlServerMessageStoreSetting setting)
        //{
        //    appBuilder.SetDefault<IMessageStore, SqlServerMessageStore>(new SqlServerMessageStore(setting));
        //    return appBuilder;
        //}
        //public static IAppBuilder UseSqlServerOffsetManager(this IAppBuilder appBuilder, SqlServerOffsetManagerSetting setting)
        //{
        //    appBuilder.SetDefault<IOffsetManager, SqlServerOffsetManager>(new SqlServerOffsetManager(setting));
        //    return appBuilder;
        //}
    }
}
