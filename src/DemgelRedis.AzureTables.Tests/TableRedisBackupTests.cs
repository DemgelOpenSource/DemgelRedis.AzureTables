using System;
using System.Collections.Generic;
using DemgelRedis.BackingManager;
using DemgelRedis.Common;
using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using StackExchange.Redis;
using System.Diagnostics;
using System.Threading.Tasks;
using DemgelRedis.ObjectManager;

namespace DemgelRedis.AzureTables.Tests
{
    [TestFixture]
    public class TableRedisBackupTests
    {
        private readonly TableRedisBackup _tableRedisBackup = new TableRedisBackup(CloudStorageAccount.DevelopmentStorageAccount);
        private readonly RedisObjectManager _objectManager;

        private readonly IConnectionMultiplexer _connection = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable("REDIS"));
        private IDatabase _database
        {
            get { return _connection.GetDatabase(1); }
        }

        public TableRedisBackupTests()
        {
            _objectManager = new RedisObjectManager(_tableRedisBackup);
        }

        [Test]
        public async Task UpdateHashTest()
        {
            var hashes = new List<HashEntry>
            {
                new HashEntry("testbyte", Guid.NewGuid().ToByteArray()),
                new HashEntry("teststring", "some string")
            };

            var firstKey = new RedisKeyObject()
            {
                Prefix = "testhash7",
                Id = "13"
            };

            var secondKey = new RedisKeyObject()
            {
                Prefix = "testhash7",
                Id = "22",
                Suffix = "testsuffix"
            };

            await _tableRedisBackup.UpdateHashAsync(hashes, firstKey);
            await _tableRedisBackup.UpdateHashAsync(hashes, secondKey);

            await _tableRedisBackup.DeleteHashAsync(secondKey);

            hashes[1] = new HashEntry("teststring", "testing update");
            _tableRedisBackup.UpdateHashValue(hashes[1], firstKey);
            _tableRedisBackup.DeleteHashValue(hashes[0], secondKey);

            // Final Clean up
            _tableRedisBackup.DeleteHashValue(hashes[0], firstKey);
            _tableRedisBackup.DeleteHashValue(hashes[1], firstKey);
        }

        [Test]
        public async Task AddSetItemTests()
        {
            var firstKey = new RedisKeyObject()
            {
                Prefix = "TestSet",
                Id = "1"
            };

            var secondKey = new RedisKeyObject()
            {
                Prefix = "TestSet2",
                Id = "2",
                Suffix = "TestSuffix"
            };

            var entry1 = new SortedSetEntry("testValueOne", 12384828182L);
            var entry2 = new SortedSetEntry("testValueTwo", 12357263237L);

            await _tableRedisBackup.AddSetItemAsync(firstKey, entry1);
            await _tableRedisBackup.AddSetItemAsync(secondKey, entry2);

            // Clean Up
            await _tableRedisBackup.DeleteSetItemAsync(firstKey, entry1.Score);
            await _tableRedisBackup.DeleteSetItemAsync(secondKey, entry2.Score);
        }

        [Test]
        public async Task DeleteSetTest()
        {
            var firstKey = new RedisKeyObject()
            {
                Prefix = "TestSet",
                Id = "1"
            };

            var secondKey = new RedisKeyObject()
            {
                Prefix = "TestSet",
                Id = "1",
            };

            var entry1 = new SortedSetEntry("testValueOne", 12384828182L);
            var entry2 = new SortedSetEntry("testValueTwo", 12357263237L);

            await _tableRedisBackup.AddSetItemAsync(firstKey, entry1);
            await _tableRedisBackup.AddSetItemAsync(secondKey, entry2);

            // Clean Up
            await _tableRedisBackup.DeleteSetAsync(firstKey);
        }

        [Test]
        public async Task RestoreSetTest()
        {
            // Add items to restore to Redis
            var firstKey = new RedisKeyObject()
            {
                Prefix = "TestSetForDelete",
                Id = "1"
            };

            var secondKey = new RedisKeyObject()
            {
                Prefix = "TestSetForDelete",
                Id = "1",
            };

            var entry1 = new SortedSetEntry("testValueOne", 12384828182L);
            var entry2 = new SortedSetEntry("testValueTwo", 12357263237L);

            await _tableRedisBackup.AddSetItemAsync(firstKey, entry1);
            await _tableRedisBackup.AddSetItemAsync(secondKey, entry2);

            await _tableRedisBackup.RestoreSetAsync(_database, firstKey);

            // Clean Up
            await _tableRedisBackup.DeleteSetAsync(firstKey);
        }
    }
}
