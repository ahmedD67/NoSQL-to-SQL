using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataGenerator
{
    internal class MongoCRUD
    {
        private IMongoDatabase db;
        public MongoCRUD(string database)
        {
            var client = new MongoClient("mongodb+srv://Ahmed:Pa$$w0rd123@testcluster.gbmvmuu.mongodb.net/?retryWrites=true&connect=replicaSet");
            this.db = client.GetDatabase(database);
        }
        public IMongoDatabase GetDb()
        {
            return db;
        }

        public void InsertRecord<T>(string collectionName, List<T> record)
        {
            var collection = db.GetCollection<T>(collectionName);
            collection.InsertMany(record);
        }

        public List<T> LoadRecords<T>(string collectionName)
        {
            var collection = db.GetCollection<T>(collectionName);
            return collection.Find(new BsonDocument()).ToList();
        }

        public T LoadRecordById<T>(string collectionName, Guid id)
        {
            var collection = db.GetCollection<T>(collectionName);
            var filter = Builders<T>.Filter.Eq("Id", id);

            return collection.Find(filter).First();
        }

        public void UpsertRecord<T>(string collectionName, Guid id, T record)
        {
            var collection = db.GetCollection<T>(collectionName);
            var result = collection.ReplaceOne(
                new BsonDocument("_id", id), record, new UpdateOptions { IsUpsert = true });
        }

        public void DeleteRecord<T>(string collectionName, Guid id)
        {
            var collection = db.GetCollection<T>(collectionName);
            var filter = Builders<T>.Filter.Eq("Id", id);
            collection.DeleteOne(filter);
        }

        public void WatchStream()
        {
            using (var cursor = db.Watch())
            {
                foreach (var change in cursor.ToEnumerable())
                {
                    Console.WriteLine(change.OperationType);
                    Console.WriteLine(change.DocumentKey);
                    Console.WriteLine(change.FullDocument);
                    Console.WriteLine(change.UpdateDescription);
                }
            }
        }
    }
}
