//using Microsoft.Azure.CosmosDB.BulkExecutor;
//using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
//using Microsoft.Azure.Documents;
//using Microsoft.Azure.Documents.Client;
//using Newtonsoft.Json;
//using Newtonsoft.Json.Serialization;
//using System;
//using System.Collections.Generic;
//using System.Collections.ObjectModel;
//using System.Diagnostics;
//using System.IO;
//using System.Linq;
//using System.Text;
//using System.Threading;
//using System.Threading.Tasks;

//namespace Cosom_Db_POC
//{
//    class Program
//    {
//        static void Main(string[] args)
//        {
//            try
//            {
//                new CosmoDB().Create().Wait();
//            }
//            catch(AggregateException ex)
//            {
//                Console.WriteLine(ex.ToString());
//                Console.ReadLine();
//            }
//        }
//    }


//    public class Employee
//    {
//        public string Id { get; set; }
//        public string DepartmentId { get; set; }
//        public string Name { get; set; }
//        public string Address { get; set; }
//        public string DOB { get; set; }


//        public List<string> GenerateDummyData()
//        {
//            List<string> users = new List<string>();
//            var random = new Random();
//            for (int i = 1; i <= 100000; i++)
//            {
//                var d = new Employee()
//                {
//                    Id = i.ToString(),
//                    Name = $"user_{random.Next()}",
//                    DepartmentId = random.Next(1, 20).ToString(),
//                    Address = $"address_{random.Next()}",
//                    DOB = "15-03-1993"
//                };
//                users.Add(JsonConvert.SerializeObject(d, new JsonSerializerSettings
//                {
//                    ContractResolver = new CamelCasePropertyNamesContractResolver()
//                }));
//            }

//            //using(var x = new StreamWriter("E:\\Employee_Test_Data.json"))
//            //{
//            //    var serData = JsonConvert.SerializeObject(users);
//            //    x.Write(serData);
//            //}
//            return users;
//        }
//    }

//    public class CosmoDB
//    {
//        public DocumentClient Client;
//        public string Database { get; set; }
//        public string Collection { get; set; }
//        public string Endpoint = "https://localhost:8081";
//        public string Authkey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

//        public DocumentCollection DataCollection;


//        ConnectionPolicy connectionPolicy = new ConnectionPolicy
//        {
//            ConnectionMode = ConnectionMode.Direct,
//            ConnectionProtocol = Protocol.Tcp
//        };

//        public CosmoDB()
//        {
//            Client = new DocumentClient(new Uri(Endpoint), Authkey, connectionPolicy);
//            Database = "TestDb";
//            Collection = "Employee";
//            CreateDatabase();
//            CreateCollection();
//            Client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
//            Client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

//            DataCollection = GetCollectionIfExists(Client, Database, Collection);
//        }




//        public async Task Create()
//        {

//            IBulkExecutor BulkExecutor = new BulkExecutor(Client, DataCollection);
//            await BulkExecutor.InitializeAsync();

//            Client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
//            Client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;


//            BulkImportResponse x = null;

//            var tokenSource = new CancellationTokenSource();
//            var token = tokenSource.Token;

//            for (int i = 0; i < 10; i++)
//            {
//                var data = new Employee().GenerateDummyData();
//                var tasks = new List<Task>();

//                tasks.Add(Task.Run(async () =>
//                {
//                    Trace.TraceInformation(String.Format("Executing bulk import for batch {0}", i));
//                    do
//                    {
//                        try
//                        {
//                            x = await BulkExecutor.BulkImportAsync(
//                                documents: data,
//                                enableUpsert: true,
//                                disableAutomaticIdGeneration: true,
//                                maxConcurrencyPerPartitionKeyRange: null,
//                                maxInMemorySortingBatchSize: null,
//                                cancellationToken: token);
//                        }
//                        catch (DocumentClientException de)
//                        {
//                            Trace.TraceError("Document client exception: {0}", de);
//                            break;
//                        }
//                        catch (Exception e)
//                        {
//                            Trace.TraceError("Exception: {0}", e);
//                            break;
//                        }
//                    }
//                    while (x.NumberOfDocumentsImported < 100000);
//                }));

//                Trace.WriteLine(String.Format("\nSummary for batch {0}:", i));
//                Trace.WriteLine("--------------------------------------------------------------------- ");
//                Trace.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
//                    x.NumberOfDocumentsImported,
//                    Math.Round(x.NumberOfDocumentsImported / x.TotalTimeTaken.TotalSeconds),
//                    Math.Round(x.TotalRequestUnitsConsumed / x.TotalTimeTaken.TotalSeconds),
//                    x.TotalTimeTaken.TotalSeconds));
//                Trace.WriteLine(String.Format("Average RU consumption per document: {0}",
//                    (x.TotalRequestUnitsConsumed / x.NumberOfDocumentsImported)));
//                Trace.WriteLine("---------------------------------------------------------------------\n ");

//                await Task.WhenAll(tasks);
//            }
//        }

//        public DocumentCollection GetCollectionIfExists(DocumentClient client, string databaseName, string collectionName)
//        {
//            var x =  Client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
//                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
//            return x;
//        }

//        public Database GetDatabaseIfExists(DocumentClient client, string databaseName)
//        {
//            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
//        }

//        public void CreateDatabase()
//        {
//            Client.CreateDatabaseIfNotExistsAsync(new Database { Id = Database }).Wait();
//        }

//        public void CreateCollection()
//        {
//            PartitionKeyDefinition partitionKey = new PartitionKeyDefinition
//            {
//                Paths = new Collection<string> { "/Address" }
//            };
//            Client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(Database),
//                        new DocumentCollection { Id = Collection, PartitionKey = partitionKey }, new RequestOptions { OfferThroughput = 100000 }).Wait();
//        }

//        public string GenerateRandomDocumentString(int id)
//        {
//            return "{\n" +
//                "    \"id\": \"" + id + "\",\n" +
//                "    \"Name\": \"TestDoc\",\n" +
//                "    \"description\": \"1.99\",\n" +
//                "    \"f1\": \"3hrkjh3h4h4h3jk4h\",\n" +
//                "    \"f2\": \"dhfkjdhfhj4434434\",\n" +
//                "    \"f3\": \"nklfjeoirje434344\",\n" +
//                "    \"f4\": \"pjfgdgfhdgfgdhbd6\",\n" +
//                "    \"f5\": \"gyuerehvrerebrhjh\",\n" +
//                "    \"f6\": \"3434343ghghghgghj\"" +
//                "}";
//        }

//    }

//}

//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace BulkImportSample
{
    using System;
    using System.Configuration;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;
    using Cosmo_Db_POC;

    public class Employee
    {
        public string Id { get; set; }
        public string DepartmentId { get; set; }
        public string Name { get; set; }
        public string Address { get; set; }
        public string DOB { get; set; }


        public List<string> GenerateDummyData()
        {
            List<string> users = new List<string>();
            var random = new Random();
            for (int i = 1; i <= 100000; i++)
            {
                var d = new Employee()
                {
                    Id = i.ToString()+ Guid.NewGuid().ToString(),
                    Name = $"user_{random.Next()}",
                    DepartmentId = random.Next(1, 20).ToString(),
                    Address = $"address_{random.Next()}",
                    DOB = "15-03-1993"
                };
                users.Add(JsonConvert.SerializeObject(d, new JsonSerializerSettings
                {
                    ContractResolver = new CamelCasePropertyNamesContractResolver()
                }));
            }

            //using(var x = new StreamWriter("E:\\Employee_Test_Data.json"))
            //{
            //    var serData = JsonConvert.SerializeObject(users);
            //    x.Write(serData);
            //}
            return users;
        }
    }

    class Program
    {
        private static readonly string EndpointUrl = "https://localhost:8081";
        private static readonly string AuthorizationKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private static readonly string DatabaseName = "TestDb";
        private static readonly string CollectionName = "TestEmployee";
        private static readonly int CollectionThroughput = 100000;

        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp
        };

        private DocumentClient client;

        /// <summary>
        /// Initializes a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private Program(DocumentClient client)
        {
            this.client = client;
        }

        public static void Main(string[] args)
        {
            Trace.WriteLine("Summary:");
            Trace.WriteLine("--------------------------------------------------------------------- ");
            Trace.WriteLine(String.Format("Endpoint: {0}", EndpointUrl));
            Trace.WriteLine(String.Format("Collection : {0}.{1}", DatabaseName, CollectionName));
            Trace.WriteLine("--------------------------------------------------------------------- ");
            Trace.WriteLine("");

            try
            {
                using (var client = new DocumentClient(
                    new Uri(EndpointUrl),
                    AuthorizationKey,
                    ConnectionPolicy))
                {
                    var program = new Program(client);
                    program.RunBulkImportAsync().Wait();
                }
            }
            catch (AggregateException e)
            {
                Trace.TraceError("Caught AggregateException in Main, Inner Exception:\n" + e);
                Console.ReadKey();
            }

        }

        /// <summary>
        /// Driver function for bulk import.
        /// </summary>
        /// <returns></returns>
        private async Task RunBulkImportAsync()
        {
            // Cleanup on start if set in config.

            DocumentCollection dataCollection = null;
            try
            {
                
                    Database database = Utils.GetDatabaseIfExists(client, DatabaseName);
                    if (database != null)
                    {
                        await client.DeleteDatabaseAsync(database.SelfLink);
                    }

                    Trace.TraceInformation("Creating database {0}", DatabaseName);
                    database = await client.CreateDatabaseAsync(new Database { Id = DatabaseName });

                    Trace.TraceInformation(String.Format("Creating collection {0} with {1} RU/s", CollectionName, CollectionThroughput));
                    dataCollection = await Utils.CreatePartitionedCollectionAsync(client, DatabaseName, CollectionName, CollectionThroughput);
            }
            catch (Exception de)
            {
                Trace.TraceError("Unable to initialize, exception message: {0}", de.Message);
                throw;
            }

            // Prepare for bulk import.

            // Creating documents with simple partition key here.
            string partitionKeyProperty = dataCollection.PartitionKey.Paths[0].Replace("/", "");

            long numberOfDocumentsToGenerate = 1000000;
            int numberOfBatches = 10;
            long numberOfDocumentsPerBatch = (long)Math.Floor(((double)numberOfDocumentsToGenerate) / numberOfBatches);

            // Set retry options high for initialization (default values).
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

            IBulkExecutor bulkExecutor = new BulkExecutor(client, dataCollection);
            await bulkExecutor.InitializeAsync();

            // Set retries to 0 to pass control to bulk executor.
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

            BulkImportResponse bulkImportResponse = null;
            long totalNumberOfDocumentsInserted = 0;
            double totalRequestUnitsConsumed = 0;
            double totalTimeTakenSec = 0;

            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            for (int i = 0; i < numberOfBatches; i++)
            {
                // Generate JSON-serialized documents to import.

                long prefix = i * numberOfDocumentsPerBatch;

                Trace.TraceInformation(String.Format("Generating {0} documents to import for batch {1}", numberOfDocumentsPerBatch, i));
                var data = new Employee().GenerateDummyData();

                // Invoke bulk import API.

                var tasks = new List<Task>();

                tasks.Add(Task.Run(async () =>
                {
                    Trace.TraceInformation(String.Format("Executing bulk import for batch {0}", i));
                    do
                    {
                        try
                        {
                            bulkImportResponse = await bulkExecutor.BulkImportAsync(
                                documents: data,
                                enableUpsert: true,
                                disableAutomaticIdGeneration: true,
                                maxConcurrencyPerPartitionKeyRange: null,
                                maxInMemorySortingBatchSize: null,
                                cancellationToken: token);
                        }
                        catch (DocumentClientException de)
                        {
                            Trace.TraceError("Document client exception: {0}", de);
                            break;
                        }
                        catch (Exception e)
                        {
                            Trace.TraceError("Exception: {0}", e);
                            break;
                        }
                    } while (bulkImportResponse.NumberOfDocumentsImported < data.Count);

                    Trace.WriteLine(String.Format("\nSummary for batch {0}:", i));
                    Trace.WriteLine("--------------------------------------------------------------------- ");
                    Trace.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                        bulkImportResponse.NumberOfDocumentsImported,
                        Math.Round(bulkImportResponse.NumberOfDocumentsImported / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                        Math.Round(bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                        bulkImportResponse.TotalTimeTaken.TotalSeconds));
                    Trace.WriteLine(String.Format("Average RU consumption per document: {0}",
                        (bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.NumberOfDocumentsImported)));
                    Trace.WriteLine("---------------------------------------------------------------------\n ");

                    totalNumberOfDocumentsInserted += bulkImportResponse.NumberOfDocumentsImported;
                    totalRequestUnitsConsumed += bulkImportResponse.TotalRequestUnitsConsumed;
                    totalTimeTakenSec += bulkImportResponse.TotalTimeTaken.TotalSeconds;
                },
                token));
                await Task.WhenAll(tasks);
            }

            Trace.WriteLine("Overall summary:");
            Trace.WriteLine("--------------------------------------------------------------------- ");
            Trace.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                totalNumberOfDocumentsInserted,
                Math.Round(totalNumberOfDocumentsInserted / totalTimeTakenSec),
                Math.Round(totalRequestUnitsConsumed / totalTimeTakenSec),
                totalTimeTakenSec));
            Trace.WriteLine(String.Format("Average RU consumption per document: {0}",
                (totalRequestUnitsConsumed / totalNumberOfDocumentsInserted)));
            Trace.WriteLine("--------------------------------------------------------------------- ");


            Trace.WriteLine("\nPress any key to exit.");
            Console.ReadKey();
        }
    }
}

