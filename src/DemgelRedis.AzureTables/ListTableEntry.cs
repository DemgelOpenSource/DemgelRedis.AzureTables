using Microsoft.WindowsAzure.Storage.Table;

namespace DemgelRedis.AzureTables
{
    public class ListTableEntry : TableEntity
    {
        public int Count { get; set; }
        public object Value { get; set; }
    }

    public class ListTableStringEntry : TableEntity
    {
        public int Count { get; set; }
        public string Value { get; set; }
    }

    public class ListTableByteEntry : TableEntity
    {
        public int Count { get; set; }
        public byte[] Value { get; set; }
    }
}
