using Bogus;
using DataGenerator;
using MongoDB.Driver;
using Microsoft.Extensions.Configuration;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;

/* 1. Populate the database with many fake json Person objects
 * 2. Start a while(true) loop that will randomly choose one CRUD operation
 * 3. Perform the chosen CRUD op on 10 randomly selected objects. 
 * To do: 
 * 1. Create a config file that stores the db connection settings. 
 * 2. Figure out how to perform CRUD operations using MongoDB.Driver 
 * 3. Instantiate the Faker objects. */



var fruit = new[] { "apple", "banana", "orange", "strawberry", "kiwi" };

var orderIds = 0;
var testOrders = new Faker<Order>()
   //Ensure all properties have rules. By default, StrictMode is false
   //Set a global policy by using Faker.DefaultStrictMode if you prefer.
   .RuleFor(o => o.OrderId, f => Guid.NewGuid())
   //Pick some fruit from a basket
   .RuleFor(o => o.Item, f => f.PickRandom(fruit))
   //A random quantity from 1 to 10
   .RuleFor(o => o.Quantity, f => f.Random.Number(1, 10))
   //A nullable int? with 80% probability of being null.
   //The .OrNull extension is in the Bogus.Extensions namespace.
   .RuleFor(o => o.LotNumber, f => f.Random.Int(0, 100).OrNull(f, .8f));

var userIds = 0;
var testUsers = new Faker<User>()
   //Optional: Call for objects that have complex initialization
   .RuleFor(u => u.FirstName, f => f.Name.FirstName())
   .RuleFor(u => u.LastName, f => f.Name.LastName())
   .RuleFor(u => u.Avatar, f => f.Internet.Avatar())
   .RuleFor(u => u.UserName, (f, u) => f.Internet.UserName(u.FirstName, u.LastName))
   .RuleFor(u => u.Email, (f, u) => f.Internet.Email(u.FirstName, u.LastName))
   .RuleFor(u => u.SomethingUnique, f => $"Value {f.UniqueIndex}")
   .RuleFor(u => u.SomeGuid, f => Guid.NewGuid())

   //Use an enum outside scope.
   .RuleFor(u => u.Gender, f => f.PickRandom<Gender>())
   //Use a method outside scope.
   .RuleFor(u => u.CartId, f => Guid.NewGuid())
   //Compound property with context, use the first/last name properties
   .RuleFor(u => u.FullName, (f, u) => u.FirstName + " " + u.LastName)
   //And composability of a complex collection.
   .RuleFor(u => u.Orders, f => testOrders.Generate(3))
   //After all rules are applied finish with the following action
   .FinishWith((f, u) =>
   {
       Console.WriteLine("User Created! Name={0}", u.FullName);
   });

/* IConfigurationBuilder configBuilder = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json");
var config = configBuilder.Build(); */

MongoCRUD db = new MongoCRUD("ShoppingBook");
/* db.InsertRecord("UsersOrders", testUsers.Generate(15).ToList());

var recs = db.LoadRecords<User>("UsersOrders");
recs.ForEach(x => Console.WriteLine(JsonSerializer.Serialize(x)));


/* var specialRec = db.LoadRecordById<User>("UsersOrders", new Guid("35f1a29a-8f48-4fad-b419-7781822bcf4e"));
Console.WriteLine("\n" + JsonSerializer.Serialize(specialRec)); */



Console.WriteLine("Now watching the stream");

var clientConfig = new ClientConfig();
clientConfig.BootstrapServers = "pkc-6ojrj.swedencentral.azure.confluent.cloud:9092";
clientConfig.SecurityProtocol = Confluent.Kafka.SecurityProtocol.SaslSsl;
clientConfig.SaslMechanism = Confluent.Kafka.SaslMechanism.Plain;
clientConfig.SaslUsername = "JOJUPDWBHUSJAXS4";
clientConfig.SaslPassword = "ynI9tacbVNwmQtDitAFLAogQVmFweClD3LLUYiO6S8hl7uwcTkx35F0Wnb+E5hAG";
clientConfig.SslCaLocation = "probe";

await Kafkaesque.Produce("MongoDBChanges", clientConfig, db.GetDb());