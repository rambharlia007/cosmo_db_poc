﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Cosmo_Db_POC
{
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    using System;
    using System.Collections.ObjectModel;
    using System.Configuration;
    using System.Linq;
    using System.Threading.Tasks;

    class Utils
    {
        /// <summary>
        /// Get the collection if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested collection.</returns>
        static internal DocumentCollection GetCollectionIfExists(DocumentClient client, string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(client, databaseName) == null)
            {
                return null;
            }

            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested database.</returns>
        static internal Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        static internal async Task<DocumentCollection> CreatePartitionedCollectionAsync(DocumentClient client, string databaseName,
            string collectionName, int collectionThroughput)
        {
            PartitionKeyDefinition partitionKey = new PartitionKeyDefinition
            {
                Paths = new Collection<string> { "/Address"}
            };
            DocumentCollection collection = new DocumentCollection { Id = collectionName, PartitionKey = partitionKey };

            try
            {
                collection = await client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    collection,
                    new RequestOptions { OfferThroughput = collectionThroughput });
            }
            catch (Exception e)
            {
                throw e;
            }

            return collection;
        }

        static internal String GenerateRandomDocumentString(String id, String partitionKeyProperty, object parititonKeyValue)
        {
            return "{\n" +
                "    \"id\": \"" + id + "\",\n" +
                "    \"" + partitionKeyProperty + "\": \"" + parititonKeyValue + "\",\n" +
                "    \"Name\": \"TestDoc\",\n" +
                "    \"description\": \"1.99\",\n" +
                "    \"f1\": \"3hrkjh3h4h4h3jk4h\",\n" +
                "    \"f2\": \"dhfkjdhfhj4434434\",\n" +
                "    \"f3\": \"nklfjeoirje434344\",\n" +
                "    \"f4\": \"pjfgdgfhdgfgdhbd6\",\n" +
                "    \"f5\": \"gyuerehvrerebrhjh\",\n" +
                "    \"f6\": \"3434343ghghghgghj\"" +
                "}";
        }
    }
}
