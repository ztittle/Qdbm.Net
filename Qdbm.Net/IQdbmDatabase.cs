using System.Collections.Generic;

namespace Qdbm.Net
{
    public interface IQdbmDatabase
    {
        long UsedBucketCount { get; }
        QdbmHeader Header { get; }
        int SetAlignment(int alignment);
        IReadOnlyDictionary<QdbmKey, byte[]> GetAll();
        byte[] Get(QdbmKey key);
        void Put(QdbmKey key, byte[] value);
    }
}