using System;
using System.Linq;

namespace Qdbm.Net
{
    internal class QdbmRecord
    {
        public QdbmRecord(QdbmKey key, QdbmRecordHeader header, byte[] value)
        {
            Key = key;
            Header = header;
            Value = value;
        }
        
        public QdbmRecordHeader Header { get; }
        public QdbmKey Key { get; }
        public byte[] Value { get; internal set; }
        public byte[] PaddingData { get; internal set; }
    }
}