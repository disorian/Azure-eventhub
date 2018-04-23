using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using System;
using System.Text;
using System.Threading.Tasks;

namespace EventHubTest
{
    class Program
    {
        private const string EhConnectionString = "Endpoint=sb://nkr-eventhub-test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=sTxWEvedilvaT/lpor4EcFpKsfY/zF/rMDBiIUoaGrA=";
        private const string EhEntityPath = "eventone";
        private const string StorageContainerName = "nkreventdatastorage";
        private const string StorageAccountName = "nkrstorage";
        private const string StorageAccountKey = "OfYUdE1gvb34lORLqDTjzB4trUFi3NXhrpoRh417xNAUfuzibWqH0HkYoawsGJHN63Myf7Afs4HtoXTwm09cAw==";

        private static readonly string StorageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", StorageAccountName, StorageAccountKey);
        private static EventHubClient eventHubClient;

        static void Main(string[] args)
        {
            Console.WriteLine("Sending...");
            SendMainAsync(args).GetAwaiter().GetResult();
            Console.WriteLine("\n\nFinished sending\n\n");

            Console.WriteLine("\n\n>> Reading...");
            ReadMainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task SendMainAsync(string[] args)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString)
            {
                EntityPath = EhEntityPath
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            await SendMessagesToEventHubAsync(100);

            await eventHubClient.CloseAsync();

            Console.WriteLine("Press ENTER to exit.");
            //Console.ReadLine();
        }

        private static async Task ReadMainAsync(string[] args)
        {
            Console.WriteLine("Registering EventProcessor...");

            var eventProcessorHost = new EventProcessorHost(
                EhEntityPath, PartitionReceiver.DefaultConsumerGroupName, EhConnectionString, StorageConnectionString, StorageContainerName);

            // Registers the Event Processor Host and starts receiving messages
            await eventProcessorHost.RegisterEventProcessorAsync<SimpleEventProcessor>();

            Console.WriteLine("Receiving. Press ENTER to stop worker.");
            Console.ReadLine();

            // Disposes of the Event Processor Host
            await eventProcessorHost.UnregisterEventProcessorAsync();
        }

        private static async Task SendMessagesToEventHubAsync(int numberOfMessages)
        {
            for (int i = 0; i < numberOfMessages; i++)
            {
                try
                {
                    var message = $"Message {i}";
                    Console.WriteLine($"Sending message { message }");

                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{ DateTime.Now } > Exception: { exception.Message }");
                }

                await Task.Delay(10);
            }

            Console.WriteLine($"{ numberOfMessages} messages sent");
        }
    }
}
