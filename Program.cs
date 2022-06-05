using NetMQ;
using NetMQ.Sockets;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            TestNetMq();
        }

        static void TestNetMq()
        {
            var tasks = new List<Task>();
            tasks.Add( Task.Factory.StartNew(() => { RunPublisher(); }) );
            for (int i = 1; i < 8; ++i)
            {
                int j = i;
                tasks.Add(Task.Factory.StartNew(() => { RunSubscriber(j); }));
            }

            Task.WaitAll(tasks.ToArray());
        }

        static void RunPublisher()
        {
            Random rand = new Random(50);
            using (var pubSocket = new PublisherSocket())
            {
                Console.WriteLine("Publisher socket binding...");
                pubSocket.Options.SendHighWatermark = 1000;
                pubSocket.Bind("tcp://*:12345");
                for (var i = 0; i < 10; i++)
                {
                    var randomizedTopic = rand.Next(3);
                    var str = $"Topic {randomizedTopic} - {i}";
                    switch(randomizedTopic)
                    {
                        case 0:
                            Console.WriteLine($"Pub Topic1 {str}");
                            pubSocket.SendMoreFrame("Topic1").SendFrame(str);
                            break;

                        case 1:
                            Console.WriteLine($"Pub Topic2 {str}");
                            pubSocket.SendMoreFrame("Topic2").SendFrame(str);
                            break;

                        case 2:
                            Console.WriteLine($"Pub Topic3 {str}");
                            pubSocket.SendMoreFrame("Topic3").SendFrame(str);
                            break;
                    }

                    Thread.Sleep(1000);
                }
                pubSocket.SendMoreFrame("Topic1").SendFrame("End");
                pubSocket.SendMoreFrame("Topic2").SendFrame("End");
                pubSocket.SendMoreFrame("Topic3").SendFrame("End");
            }
        }

        static void RunSubscriber(int t)
        {

            using (var subSocket = new SubscriberSocket())
            {
                subSocket.Options.ReceiveHighWatermark = 1000;
                subSocket.Connect("tcp://localhost:12345");
                string topics = "";
                if ((t & 1) != 0)
                {
                    subSocket.Subscribe($"Topic1");
                    topics += "Topic1";
                }
                if ((t & 2) != 0)
                {
                    subSocket.Subscribe($"Topic2");
                    topics += "Topic2";
                }
                if ((t & 4) != 0)
                {
                    subSocket.Subscribe($"Topic3");
                    topics += "Topic3";
                }
                Console.WriteLine($"Subscriber {t} topics: {topics}");
                while (true)
                {
                    string messageTopicReceived = subSocket.ReceiveFrameString();
                    string messageReceived = subSocket.ReceiveFrameString();
                    Console.WriteLine($"Sub[{t}] topic={messageTopicReceived} msg={messageReceived}");
                    if (messageReceived == "End")
                        break;
                }
            }
        }
    }
}
