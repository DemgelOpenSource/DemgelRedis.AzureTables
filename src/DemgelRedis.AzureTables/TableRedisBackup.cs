using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using Castle.Core.Internal;
using DemgelRedis.Common;
using DemgelRedis.Extensions;
using DemgelRedis.Interfaces;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using StackExchange.Redis;
using System.Threading.Tasks;
using System.Threading;

namespace DemgelRedis.BackingManager
{
    /// <summary>
    /// Does an ongoing (real time) back up to Azure Tables
    /// 
    /// Format is Table:PartitionKey:RowKey (as in: user:1201212:info)
    /// Format could be PartitionKey:RowKey (as in: user:1201212) Table is provided as param. 
    /// </summary>
    public class TableRedisBackup : AbstractRedisBackup
    {
        // Call from Autofac
        public delegate TableRedisBackup Factory(string storageName, string accessKey, bool useHttps = true);

        /// <summary>
        /// _tablesDictionary contains all tables that have been created/referenced
        /// </summary>
        private readonly Dictionary<string, CloudTable> _tablesDictionary = new Dictionary<string, CloudTable>();

        private readonly CloudStorageAccount _storageAccount;
        private CloudTableClient Client => _tableClient ?? (_tableClient = _storageAccount.CreateCloudTableClient());
        private CloudTableClient _tableClient;

        private readonly object _lock = new object();

        /// <summary>
        /// It is recommended to use the Factory Method
        /// and pass in your credientials that way
        /// </summary>
        /// <param name="storageName"></param>
        /// <param name="accessKey"></param>
        /// <param name="useHttps"></param>
        public TableRedisBackup(string storageName, string accessKey, bool useHttps)
        {
            var creds = new StorageCredentials(storageName, accessKey);
            _storageAccount = new CloudStorageAccount(creds, useHttps);
        }

        /// <summary>
        /// Initialize with a specific storage account.
        /// 
        /// Can be used with Autofac, but do not register both CloudStorageAccount and StorageCredentials
        /// with Autofac
        /// </summary>
        /// <param name="storageAccount"></param>
        public TableRedisBackup(CloudStorageAccount storageAccount)
        {
            _storageAccount = storageAccount;
        }

        /// <summary>
        /// Initialize with a specific storageCredentials
        /// 
        /// Can be used with Autofac (or IoC), but do not register both CloudStorageAccount and StorageCredientials
        /// unless you have full control of instanciation
        /// </summary>
        /// <param name="storageCredentials"></param>
        public TableRedisBackup(StorageCredentials storageCredentials)
        {
            _storageAccount = new CloudStorageAccount(storageCredentials, true);
        }

        private CloudTable GetCloudTable(string tableName)
        {
            return Task.Run(async () => await GetCloudTableAsync(tableName)).Result;
        }

        /// <summary>
        /// Retrieve, Create, and register CloudTables in use, Caching for quick lookup.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private async Task<CloudTable> GetCloudTableAsync(string tableName, CancellationToken token = default(CancellationToken))
        {
            CloudTable table;
            lock (_lock)
            {
                if (_tablesDictionary.TryGetValue(tableName, out table)) return table;
            }

            table = Client.GetTableReference(tableName);
            await table.CreateIfNotExistsAsync();

            lock (_lock)
            {
                if (!_tablesDictionary.ContainsKey(tableName))
                {
                    _tablesDictionary.Add(tableName, table);
                }    
            }
            return table;
        }

        /// <summary>
        /// Will process all hash entries (need to come from same hash)
        /// </summary>
        /// <param name="entries"></param>
        /// <param name="hashKey"></param>
        public override async Task UpdateHashAsync(IEnumerable<HashEntry> entries, RedisKeyObject hashKey, CancellationToken token = default(CancellationToken))
        {
            var operation = new TableBatchOperation();
            var cloudTable = await GetCloudTableAsync(hashKey.Prefix, token);

            foreach (var entry in entries)
            {
                var entity = new DynamicTableEntity
                {
                    PartitionKey = GetPartitionKey(hashKey),
                    RowKey = entry.Name
                };

                entity.Properties.Add("value",
                    entry.Value.IsByteArray()
                        ? new EntityProperty((byte[])entry.Value)
                        : new EntityProperty((string)entry.Value));

                operation.InsertOrReplace(entity);
            }

            await cloudTable.ExecuteBatchAsync(operation, token);
        }

        /// <summary>
        /// Not a very effecient way to delete a hash, better to use
        /// DeleteHashValues if you have the whole hash from the cache.
        /// </summary>
        /// <param name="hashKey"></param>
        public override async Task DeleteHashAsync(RedisKeyObject hashKey, CancellationToken token = default(CancellationToken))
        {
            var cloudTable = await GetCloudTableAsync(hashKey.Prefix, token);

            var query = new TableQuery<DynamicTableEntity>
            {
                FilterString =
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, GetPartitionKey(hashKey))
            };

            var dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, null, token);

            do
            {
                var batch = new TableBatchOperation();
                foreach (var row in dynamicTableEntities)
                {
                    batch.Delete(row);
                }

                if (!batch.IsNullOrEmpty())
                    await cloudTable.ExecuteBatchAsync(batch, token);

                dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, dynamicTableEntities.ContinuationToken, token);
            } while (dynamicTableEntities.ContinuationToken != null);
        }

        public override async Task UpdateHashValueAsync(HashEntry entry, RedisKeyObject hashKey, CancellationToken token = default(CancellationToken))
        {
            var cloudTable = await GetCloudTableAsync(hashKey.Prefix, token);

            var partKey = GetPartitionKey(hashKey);

            dynamic entity = new DynamicTableEntity();
            entity.PartitionKey = partKey;
            entity.RowKey = entry.Name;

            entity.Properties.Add("value", new EntityProperty((string)entry.Value));

            var operation = TableOperation.InsertOrReplace(entity);

            await cloudTable.ExecuteAsync(operation, token);
        }

        public override async Task DeleteHashValueAsync(HashEntry entry, RedisKeyObject hashKey, CancellationToken token = default(CancellationToken))
        {
            await DeleteHashValueAsync(entry.Name, hashKey, token);
        }

        public override async Task DeleteHashValueAsync(string valueKey, RedisKeyObject hashKey, CancellationToken token = default(CancellationToken))
        {
            var cloudTable = await GetCloudTableAsync(hashKey.Prefix, token);

            var partKey = GetPartitionKey(hashKey);

            var operation = TableOperation.Delete(new DynamicTableEntity(partKey, valueKey) { ETag = "*" });

            try
            {
                await cloudTable.ExecuteAsync(operation, token);
            }
            catch
            {
                Debug.WriteLine("Object to Delete not found.");
            }
        }

        public override async Task<HashEntry[]> GetHashAsync(RedisKeyObject key, CancellationToken token = default(CancellationToken))
        {
            var cloudTable = await GetCloudTableAsync(key.Prefix, token);

            var query = new TableQuery<DynamicTableEntity>
            {
                FilterString =
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, GetPartitionKey(key))
            };

            var result = new List<HashEntry>();

            var dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, null, token);

            do
            {
                foreach (var row in dynamicTableEntities)
                {
                    EntityProperty value;
                    if (row.Properties.TryGetValue("value", out value))
                    {
                        try
                        {
                            result.Add(new HashEntry(row.RowKey, value.StringValue));
                        }
                        catch
                        {
                            result.Add(new HashEntry(row.RowKey, value.BinaryValue));
                        }
                    }
                }

                dynamicTableEntities =
                    await cloudTable.ExecuteQuerySegmentedAsync(query, dynamicTableEntities.ContinuationToken, token);
            } while (dynamicTableEntities.ContinuationToken != null);


            return result.ToArray();
        }

        public override async Task<HashEntry[]> RestoreHashAsync(IDatabase redisDatabase, RedisKeyObject hashKey, CancellationToken token = default(CancellationToken))
        {
            if (redisDatabase.KeyExists(hashKey.RedisKey)) return new HashEntry[0];
            var hashes = await GetHashAsync(hashKey, token);
            if (hashes.Length != 0)
            {
                await redisDatabase.HashSetAsync(hashKey.RedisKey, hashes);
            }
            return hashes;
        }

        public override async Task<HashEntry> GetHashEntryAsync(string valueKey, RedisKeyObject hashKey, CancellationToken token = default(CancellationToken))
        {
            var cloudTable = await GetCloudTableAsync(hashKey.Prefix, token);
            var operation = TableOperation.Retrieve<DynamicTableEntity>(GetPartitionKey(hashKey), valueKey);
            var result = await cloudTable.ExecuteAsync(operation, token);
            var dynamicTableEntity = result.Result as DynamicTableEntity;
            if (dynamicTableEntity == null) return new HashEntry("null", "null");
            EntityProperty resultString;
            return dynamicTableEntity.Properties.TryGetValue(valueKey, out resultString)
                ? new HashEntry(valueKey, resultString.StringValue)
                : new HashEntry("null", "null");
        }

        /// <summary>
        /// Will update the table database to the current value of the redisDatabase given and key.
        /// </summary>
        /// <param name="redisDatabase"></param>
        /// <param name="key"></param>
        /// <param name="table"></param>
        public override async Task UpdateStringAsync(IDatabase redisDatabase, RedisKeyObject key, string table = "string", CancellationToken token = default(CancellationToken))
        {
            var value = await redisDatabase.StringGetAsync(key.RedisKey);
            if (value.IsNullOrEmpty) return;

            var cloudTable = GetCloudTable(table);
            var entity = new DynamicTableEntity(key.Prefix, GetPartitionKey(key));
            entity.Properties.Add("value", new EntityProperty((string)value));
            var operation = TableOperation.InsertOrReplace(entity);

            await cloudTable.ExecuteAsync(operation);
        }

        public override async Task DeleteStringAsync(RedisKeyObject key, string table = "string", CancellationToken token = default(CancellationToken))
        {
            var cloudTable = GetCloudTable(table);
            var operation = TableOperation.Delete(new DynamicTableEntity(key.Prefix, GetPartitionKey(key)));

            await cloudTable.ExecuteAsync(operation);
        }

        /// <summary>
        /// TODO this will be renamed to RestoreString
        /// Gets and restores the string to the redisDatabase given from table storage
        /// </summary>
        /// <param name="key"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public override async Task<RedisValue> GetStringAsync(RedisKeyObject key, string table = "string", CancellationToken token = default(CancellationToken))
        {
            var cloudTable = await GetCloudTableAsync(table);
            var operation = TableOperation.Retrieve<DynamicTableEntity>(key.Prefix, GetPartitionKey(key));
            var result = await cloudTable.ExecuteAsync(operation);
            var dynamicResult = result.Result as DynamicTableEntity;
            if (dynamicResult == null) return "";
            EntityProperty resultProperty;
            return dynamicResult.Properties.TryGetValue("value", out resultProperty) ? resultProperty.StringValue : "";
        }

        /// <summary>
        /// Gets and restores the string to the redisDatabase given from table storage
        /// </summary>
        /// <param name="redisDatabase"></param>
        /// <param name="key"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public override async Task<string> RestoreStringAsync(IDatabase redisDatabase, RedisKeyObject key, string table = "string", CancellationToken token = default(CancellationToken))
        {
            var cloudTable = await GetCloudTableAsync(table, token);
            var operation = TableOperation.Retrieve<DynamicTableEntity>(key.Prefix, GetPartitionKey(key));
            var result = await cloudTable.ExecuteAsync(operation, token);
            var dynamicResult = result.Result as DynamicTableEntity;

            EntityProperty resultProperty;
            string value = dynamicResult != null && dynamicResult.Properties.TryGetValue("value", out resultProperty)
                ? resultProperty.StringValue
                : null;

            if (string.IsNullOrEmpty(value)) return value;
            // Assume redis database is most upto date?
            if (redisDatabase.StringSet(key.RedisKey, value, null, When.NotExists)) return value;
            // value already exists, so update the new value
            await UpdateStringAsync(redisDatabase, key, table, token);
            value = redisDatabase.StringGet(key.RedisKey);

            return value;
        }

        public override async Task RestoreCounterAsync(IDatabase redisDatabase, RedisKeyObject key, string table = "demgelcounter", CancellationToken token = default(CancellationToken))
        {
            var value = redisDatabase.StringGet($"{table}:{key.CounterKey}");

            if (!value.IsNullOrEmpty) return;

            var cloudTable = await GetCloudTableAsync(table, token);
            var operation = TableOperation.Retrieve<DynamicTableEntity>(key.Prefix, key.CounterKey);
            var result = await cloudTable.ExecuteAsync(operation, token);
            var dynamicResult = result.Result as DynamicTableEntity;
            EntityProperty resultProperty;

            value = dynamicResult != null && dynamicResult.Properties.TryGetValue("value", out resultProperty)
                ? resultProperty.StringValue
                : null;

            if (string.IsNullOrEmpty(value)) return;
            // Assume redis database is most upto date?
            redisDatabase.StringSet($"{table}:{key.CounterKey}", value, null, When.NotExists);
        }

        public override async Task UpdateCounterAsync(IDatabase redisDatabase, RedisKeyObject key, string table = "demgelcounter", CancellationToken token = default(CancellationToken))
        {
            var value = redisDatabase.StringGet($"{table}:{key.CounterKey}");
            if (value.IsNullOrEmpty) return;
            var cloudTable = await GetCloudTableAsync(table, token);

            var entity = new DynamicTableEntity(key.Prefix, key.CounterKey);
            entity.Properties.Add("value", new EntityProperty((string)value));
            var operation = TableOperation.InsertOrReplace(entity);

            await cloudTable.ExecuteAsync(operation, token);
        }

        public override async Task<List<RedisValue>> RestoreListAsync(IDatabase redisDatabase, RedisKeyObject listKey, CancellationToken token = default(CancellationToken))
        {
            // Don't bother if a key already exists (Redis first)
            if (redisDatabase.KeyExists(listKey.RedisKey)) return new List<RedisValue>();

            var cloudTable = await GetCloudTableAsync(listKey.Prefix, token);

            var query = new TableQuery<DynamicTableEntity>
            {
                FilterString =
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, GetPartitionKey(listKey))
            };

            var dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, null, token);

            var listList = new List<RedisValue>();

            do
            {
                foreach (var item in dynamicTableEntities)
                {
                    var propType = item["Value"].PropertyType;
                    for (int i = 0; i < item["Count"].Int32Value; i++)
                    {
                        switch (propType)
                        {
                            case EdmType.Binary:
                                listList.Add(item["Value"].BinaryValue);
                                break;
                            case EdmType.String:
                                listList.Add(item["Value"].StringValue);
                                break;
                        }
                    }
                }

                dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, dynamicTableEntities.ContinuationToken, token);
            } while (dynamicTableEntities.ContinuationToken != null);

            redisDatabase.ListLeftPush(listKey.RedisKey, listList.ToArray());

            return listList;
        }

        public override async Task DeleteListAsync(RedisKeyObject key, CancellationToken token = default(CancellationToken))
        {
            var cloudTable = await GetCloudTableAsync(key.Prefix, token);

            var query = new TableQuery<DynamicTableEntity>
            {
                FilterString =
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, GetPartitionKey(key))
            };

            var dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, null, token);

            do
            {
                var batch = new TableBatchOperation();
                foreach (var row in dynamicTableEntities)
                {
                    batch.Delete(row);
                }

                if (!batch.IsNullOrEmpty())
                    await cloudTable.ExecuteBatchAsync(batch);

                dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, dynamicTableEntities.ContinuationToken, token);
            } while (dynamicTableEntities.ContinuationToken != null);
        }

        /// <summary>
        /// Adds an entry into the Table Database
        /// key.Prefix = table
        /// GetPartitionKey = partition
        /// SHAhash = {c} rowkey (c is count, allowing for mulitple entries)
        /// value = value
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public override async Task AddListItemAsync(RedisKeyObject key, RedisValue value, CancellationToken token = default(CancellationToken))
        {
            var hash = GetSHAHash(value);

            // First we need to look for the item
            var table = await GetCloudTableAsync(key.Prefix, token);
            var operation = TableOperation.Retrieve<DynamicTableEntity>(GetPartitionKey(key), hash);
            var result = await table.ExecuteAsync(operation, token);

            // If result is not found
            if (result.Result == null)
            {
                var entry = new DynamicTableEntity
                {
                    PartitionKey = GetPartitionKey(key),
                    RowKey = hash,
                };
                entry.Properties.Add("Count", new EntityProperty(1));

                if (value.IsByteArray())
                {
                    entry.Properties.Add("Value", new EntityProperty((byte[])value));
                    operation = TableOperation.Insert(entry);
                }
                else
                {
                    entry.Properties.Add("Value", new EntityProperty((string)value));
                    operation = TableOperation.Insert(entry);
                }

            }
            else
            {
                ((DynamicTableEntity)result.Result)["Count"].Int32Value++;
                operation = TableOperation.Replace(((DynamicTableEntity)result.Result));
            }

            await table.ExecuteAsync(operation, token);
        }

        public override async Task RemoveListItemAsync(RedisKeyObject key, RedisValue value, CancellationToken token = default(CancellationToken))
        {
            var hash = GetSHAHash(value);

            // First we need to look for the item
            var table = await GetCloudTableAsync(key.Prefix, token);
            var operation = TableOperation.Retrieve<DynamicTableEntity>(GetPartitionKey(key), hash);
            var result = await table.ExecuteAsync(operation, token);

            if (result == null) return;
            var dynResult = (DynamicTableEntity)result.Result;
            dynResult["Count"].Int32Value--;
            operation = dynResult["Count"].Int32Value < 1 ? TableOperation.Delete(dynResult) : TableOperation.Replace(dynResult);

            await table.ExecuteAsync(operation, token);
        }

        public override async Task UpdateListItemAsync(RedisKeyObject key, RedisValue oldValue, RedisValue newValue, CancellationToken token = default(CancellationToken))
        {
            var hash = GetSHAHash(oldValue);

            // First we need to look for the item
            var table = await GetCloudTableAsync(key.Prefix, token);
            var operation = TableOperation.Retrieve<DynamicTableEntity>(GetPartitionKey(key), hash);
            var result = await table.ExecuteAsync(operation, token);

            if (result.Result != null) RemoveListItem(key, oldValue);

            AddListItem(key, newValue);
        }

        public override async Task RestoreSetAsync(IDatabase redisDatabase, RedisKeyObject key, CancellationToken token = default(CancellationToken))
        {
            // Don't bother if a key already exists (Redis first)
            if (redisDatabase.KeyExists(key.RedisKey)) return;

            var cloudTable = await GetCloudTableAsync(key.Prefix, token);

            var query = new TableQuery<DynamicTableEntity>
            {
                FilterString =
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, GetPartitionKey(key))
            };

            var dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, null, token);

            var listList = new List<SortedSetEntry>();

            do
            {
                foreach (var item in dynamicTableEntities)
                {
                    var propType = item["Value"].PropertyType;
                    double score = double.Parse(item.RowKey);
                    switch (propType)
                    {
                        case EdmType.Binary:
                            listList.Add(new SortedSetEntry(item["Value"].BinaryValue, score));
                            break;
                        case EdmType.String:
                            listList.Add(new SortedSetEntry(item["Value"].StringValue, score));
                            break;
                    }
                }

                dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, dynamicTableEntities.ContinuationToken, token);
            } while (dynamicTableEntities.ContinuationToken != null);

            redisDatabase.SortedSetAdd(key.RedisKey, listList.ToArray());

            return;
        }

        public override async Task UpdateSetItemAsync(RedisKeyObject key, SortedSetEntry entry, SortedSetEntry oldValue, CancellationToken token = default(CancellationToken))
        {
            // We remove the oldValue and add the newValue
            await DeleteSetItemAsync(key, oldValue.Score, token);
            await AddSetItemAsync(key, entry, token);
        }

        /**
         * Will add or update the value of an item if the score is the same
         */
        public override async Task AddSetItemAsync(RedisKeyObject key, SortedSetEntry entry, CancellationToken token = default(CancellationToken))
        {
            var table = await GetCloudTableAsync(key.Prefix, token);
            var tableEntry = new DynamicTableEntity
            {
                PartitionKey = GetPartitionKey(key),
                RowKey = entry.Score.ToString()
            };

            if (entry.Element.IsByteArray())
            {
                tableEntry.Properties.Add("Value", new EntityProperty((byte[])entry.Element));
            }
            else
            {
                tableEntry.Properties.Add("Value", new EntityProperty((string)entry.Element));
            }

            var operation = TableOperation.InsertOrReplace(tableEntry);
            await table.ExecuteAsync(operation, token);
        }

        public async Task DeleteSetItemAsync(RedisKeyObject key, double score, CancellationToken token = default(CancellationToken))
        {
            var table = await GetCloudTableAsync(key.Prefix);
            var operation = TableOperation.Delete(new TableEntity { ETag = "*", PartitionKey = GetPartitionKey(key), RowKey = score.ToString() });
            try {
                await table.ExecuteAsync(operation, token);
            } catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404) { /* Do Nothing  */ }
            }
        }

        public override async Task DeleteSetAsync(RedisKeyObject setKey, CancellationToken token = default(CancellationToken))
        {
            var cloudTable = await GetCloudTableAsync(setKey.Prefix, token);

            var query = new TableQuery<DynamicTableEntity>
            {
                FilterString =
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, GetPartitionKey(setKey))
            };

            var dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, null, token);

            do
            {
                var batch = new TableBatchOperation();
                foreach (var row in dynamicTableEntities)
                {
                    batch.Delete(row);
                }

                if (!batch.IsNullOrEmpty())
                    await cloudTable.ExecuteBatchAsync(batch);

                dynamicTableEntities = await cloudTable.ExecuteQuerySegmentedAsync(query, dynamicTableEntities.ContinuationToken, token);
            } while (dynamicTableEntities.ContinuationToken != null);
        }

        private string GetPartitionKey(RedisKeyObject key)
        {
            return key.Suffix != null ? $"{key.Id}:{key.Suffix}" : key.Id;
        }

        private string GetSHAHash(RedisValue value)
        {
            string hash;
            using (var crypto = new SHA1CryptoServiceProvider())
            {
                hash = BitConverter.ToString(crypto.ComputeHash(value));
            }

            return hash;
        }
    }
}
