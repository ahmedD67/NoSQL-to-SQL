using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using MongoDB.Driver;

namespace DataGenerator
{
    static public class Kafkaesque
    {
        public static async Task Produce(string topicName, ClientConfig config, IMongoDatabase db)
        {
            IProducer<string, string> producer = null;
            try
            {
                producer = new ProducerBuilder<string, string>(config).Build();
                using (var cursor = db.Watch())
                {
                    foreach (var change in cursor.ToEnumerable())
                    {
                        Console.WriteLine($"Change made: {change.OperationType.ToString()}");
                        Console.WriteLine($"Change ID: {change.DocumentKey.ToString()}");
                        producer.Produce(topicName, new Message<string, string>
                        { Key = change.OperationType.ToString(), Value = change.DocumentKey.ToString() });
                    }
                    var queueSize = producer.Flush(TimeSpan.FromSeconds(5));
                    if (queueSize > 0)
                    {
                        Console.WriteLine("WARNING: Producer event queue has " + queueSize + " pending events on exit.");
                    }
                }
            }
            finally 
            {
                var queueSize = producer.Flush(TimeSpan.FromSeconds(5));
                if (queueSize > 0)
                {
                    Console.WriteLine("WARNING: Producer event queue has " + queueSize + " pending events on exit.");
                }
                producer.Dispose();
            }
        }
    }
}
