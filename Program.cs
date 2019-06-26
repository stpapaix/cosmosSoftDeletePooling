using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;


    using Microsoft.Azure.Documents.Linq;






namespace softDeletePooling
{
    class Program
    {

        private DocumentClient client;

        // Assign an id for your database & collection
        private static readonly string WebCustomerCollectionName = "WebCustomers";

        // Read the DocumentDB endpointUrl and authorizationKeys from config
        private static readonly string endpointUrl = ConfigurationManager.AppSettings["accountEndpoint"];
        private static readonly string authorizationKey = ConfigurationManager.AppSettings["accountKey"];
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string demoPrintStringValue = ConfigurationManager.AppSettings["DemoPrint"];



        private async Task BasicOperations()
        {

            // Cosmo DB Validation and creation ifNotExist
            this.client = new DocumentClient(new Uri(ConfigurationManager.AppSettings["accountEndpoint"]), ConfigurationManager.AppSettings["accountKey"]);
            await this.client.CreateDatabaseIfNotExistsAsync(new Database { Id = "Users" });
            await this.client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri("Users"), new DocumentCollection { Id = "WebCustomers" });
            Console.WriteLine("Database and collection validation complete");

            Console.WriteLine("Configuring connection to Collection");

            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(DatabaseName, WebCustomerCollectionName);

   
            // Get partition key range map of conso collection
            string pkRangesResponseContinuation = null;
            List<PartitionKeyRange> partitionKeyRanges = new List<PartitionKeyRange>();

            do
            {
                FeedResponse<PartitionKeyRange> pkRangesResponse = await this.client.ReadPartitionKeyRangeFeedAsync(
                    collectionUri,
                    new FeedOptions { RequestContinuation = pkRangesResponseContinuation }
                    );

                partitionKeyRanges.AddRange(pkRangesResponse);
                pkRangesResponseContinuation = pkRangesResponse.ResponseContinuation;
            }
            while (pkRangesResponseContinuation != null);


            // Returns all documents in the collection.
            Console.WriteLine("Reading all changes from the beginning");
            Dictionary<string, string> checkpoints = await GetChanges(this.client,
                collectionUri,
                new Dictionary<string, string>(), partitionKeyRanges);


        }



/////

       /// <summary>
        /// Get changes within the collection since the last checkpoint. This sample shows how to process the change
        /// feed from a single worker. When working with large collections, this is typically split across multiple
        /// workers each processing a single or set of partition key ranges.
        /// </summary>
        /// <param name="client">DocumentDB client instance</param>
        /// <param name="collection">Collection to retrieve changes from</param>
        /// <param name="checkpoints"></param>
        /// <returns></returns>
        private static async Task<Dictionary<string, string>>  GetChanges(
            DocumentClient client,
            Uri collectionUri,
            Dictionary<string, string> checkpoints,
            List<PartitionKeyRange> partitionKeyRanges)
        {
            int numChangesRead = 0;

            foreach (PartitionKeyRange pkRange in partitionKeyRanges)
            {

                string continuation = null;
                checkpoints.TryGetValue(pkRange.Id, out continuation);

                IDocumentQuery<Document> query = client.CreateDocumentChangeFeedQuery(
                    collectionUri,
                    new ChangeFeedOptions
                    {
                        PartitionKeyRangeId = pkRange.Id,
                        StartFromBeginning = true,
                        RequestContinuation = continuation,
                        MaxItemCount = -1,
                        // Set reading time: only show change feed results modified since StartTime
                        // StartTime = DateTime.Now - TimeSpan.FromSeconds(30)
                    });

                while (query.HasMoreResults)
                {
                    FeedResponse<User> readChangesResponse = query.ExecuteNextAsync<User>().Result;

                    foreach (User changedDocument in readChangesResponse)
                    {
                 
                        numChangesRead++;
                        if (changedDocument.SDTag == "y") 
                        {
                            Console.WriteLine("Write record Id :{0} / {1} to blob storage because SoftDeleteTag={2}", changedDocument.Id,changedDocument.UserId ,changedDocument.SDTag);
                         }
                    }


                    checkpoints[pkRange.Id] = readChangesResponse.ResponseContinuation;
                }
            }

            Console.WriteLine("Read {0} documents from the change feed", numChangesRead);

            return checkpoints;
        }






        static void Main(string[] args)
        {
            Console.WriteLine("Debut");

        
            try
                {
                    Program p = new Program();
                    p.BasicOperations().Wait();
                }
                catch (DocumentClientException de)
                {
                    Exception baseException = de.GetBaseException();
                    Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
                }
                catch (Exception e)
                {
                    Exception baseException = e.GetBaseException();
                    Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
                }
                finally
                {
                    Console.WriteLine("End, press any key to exit.");
                    
                    //Console.ReadKey();
                }
            
            
            
        }
    }

        public class User
        {
            [JsonProperty("id")]
            public string Id { get; set; }
            [JsonProperty("ttl")]
            public int ttl { get; set; }
            [JsonProperty("userId")]
            public string UserId { get; set; }
            [JsonProperty("sdTag")]
            public string SDTag { get; set; }
            [JsonProperty("lastName")]
            public string LastName { get; set; }
            [JsonProperty("firstName")]
            public string FirstName { get; set; }
            [JsonProperty("email")]
            public string Email { get; set; }
            [JsonProperty("dividend")]
            public string Dividend { get; set; }
            [JsonProperty("OrderHistory")]
            public OrderHistory[] OrderHistory { get; set; }
            [JsonProperty("ShippingPreference")]
            public ShippingPreference[] ShippingPreference { get; set; }
            [JsonProperty("CouponsUsed")]
            public CouponsUsed[] Coupons { get; set; }
            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }

        public class OrderHistory
        {
            public string OrderId { get; set; }
            public string DateShipped { get; set; }
            public string Total { get; set; }
        }

        public class ShippingPreference
        {
            public int Priority { get; set; }
            public string AddressLine1 { get; set; }
            public string AddressLine2 { get; set; }
            public string City { get; set; }
            public string State { get; set; }
            public string ZipCode { get; set; }
            public string Country { get; set; }
        }

        public class CouponsUsed
        {
            public string CouponCode { get; set; }

        }

}
