using ALE.ETLBox.Logging;
using ETLBoxDemo.Handler;
using Rebus.Activation;
using Rebus.Config;
using Rebus.NLog.Config;
using Rebus.Retry.Simple;
using System;
using System.Collections;
using System.Collections.Generic;

namespace ETLBoxDemo
{
    class eInsightETLService
    {
        private string ConnString = System.Configuration.ConfigurationManager.AppSettings["eInsightETLQueueAddress"];
        private string Queue = System.Configuration.ConfigurationManager.AppSettings["eInsightETLQueueName"];

        private BuiltinHandlerActivator adapter;

        public void Start()
        {
            NLog.Config.ConfigurationItemFactory.Default.LayoutRenderers.RegisterDefinition("etllog", typeof(ETLLogLayoutRenderer));
            adapter = new BuiltinHandlerActivator();
            adapter.Register(x => new eInsightETLHandler(adapter.Bus));
            adapter.Register(x => new eInsightETLErrorHandler(adapter.Bus));

            var endpoints = new List<Rebus.RabbitMq.ConnectionEndpoint>();
            foreach (var connStr in ConnString.Split(';'))
                endpoints.Add(new Rebus.RabbitMq.ConnectionEndpoint
                {
                    ConnectionString = connStr
                });

            Configure.With(adapter)
                .Logging(l => l.NLog())
                .Transport(x => x.UseRabbitMq(endpoints, Queue).StrictPriorityQueue(10).Prefetch(30))
                .Options(o => {
                    o.SetNumberOfWorkers(3);
                    o.SetMaxParallelism(3);
                    o.SimpleRetryStrategy(maxDeliveryAttempts: 1, secondLevelRetriesEnabled: true);
                })
                //.Options(x => x.SimpleRetryStrategy(maxDeliveryAttempts: 1, secondLevelRetriesEnabled: true))
                //.Routing(r => r.TypeBased().MapAssemblyOf<SendTransactionalEmail>("TransactionalEmailQueue"))
                .Start();
        }

        public void Stop()
        {

        }
    }
}
