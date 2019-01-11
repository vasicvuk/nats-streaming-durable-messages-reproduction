using STAN.Client;
using System;
using System.Text;
using System.Threading;

namespace NATSDurableSubscriptionIssueReplication
{
    class Program
    {
        static void Main(string[] args)
        {
            new Program(args);
            Console.ReadLine();
        }

        public Program(string[] args)
        {
            var clientId = "";
            var topic = "sample";
            if (args.Length > 1)
            {
                topic = args[1];
            }
            if (args.Length > 2)
            {
                clientId = args[2];
            }
            var cf = new StanConnectionFactory();
            var options = StanOptions.GetDefaultOptions();
            options.ConnectTimeout = 1000;
            options.NatsURL = "nats://ruser:T0pS3cr3t@localhost:4222";
            IStanConnection connection = cf.CreateConnection("test-cluster", clientId, options);
            if (args.Length > 0 && args[0] == "send")
            {
                while (true)
                {
                    connection.Publish(topic, Encoding.UTF8.GetBytes("Hello NATS " + Guid.NewGuid().ToString()));
                    Console.WriteLine("Message sent to topic: " + topic);
                    Thread.Sleep(500);
                }
            }
            else
            {
                var subName = "subscription-1";
                if (args.Length > 0)
                {
                    subName = args[0];
                }
                EventHandler<StanMsgHandlerArgs> eh = (sender, argsMsg) =>
                {
                    var body = Encoding.UTF8.GetString(argsMsg.Message.Data);
                    // TODO: Handle headers in right way
                    Console.WriteLine(body);
                    Thread.Sleep(1000);
                    argsMsg.Message.Ack();
                };

                var opts = StanSubscriptionOptions.GetDefaultOptions();
                opts.DurableName = subName;
                opts.ManualAcks = true;
                opts.AckWait = 60000;
                opts.MaxInflight = 1;
                IStanSubscription subscription = subscription = connection.Subscribe(topic, subName, opts, eh);

            }
        }
    }
}
