using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Qdbm.Net
{
    /*
        From <https://fallabs.com/qdbm/spex.html#depotapi>
        
        magic number: from offset 0, contains "[DEPOT]\n\f" for big endian or "[depot]\n\f" for little endian.
        version number: decimal string of the version number of the library.
        flags for wrappers: from offset 16, type of `int'.
        file size: from offset 24, type of `int'.
        number of the bucket: from offset 32, type of `int'.
        number of records: from offset 40, type of `int'.
        
        The bucket section places after the header section and its length is determined according to the number of the bucket.
         Each element of the bucket stores an offset of the root node of each separate chain.

        The record section places after the bucket section and occupies to the end of the file.
         The element of the record section contains the following information.
        
        flags: type of `int'.
        second hash value: type of `int'.
        size of the key: type of `int'.
        size of the value: type of `int'.
        size of the padding: type of `int'.
        offset of the left child: type of `int'.
        offset of the right child: type of `int'.
        entity of the key: serial bytes with variable length.
        entity of the value: serial bytes with variable length.
        padding data: void serial bytes with variable length.
     */
    public class QdbmDatabase : IQdbmDatabase
    {
        private readonly Stream _databaseStream;
        private readonly QdbmHeader _qdbmHeader;
        private readonly BinaryReader _databaseBinaryReader;
        private readonly BinaryWriter _databaseBinaryWriter;
        private readonly List<int> _buckets;
        private readonly int _bucketNumber;
        private int _alignment;

        public long UsedBucketCount => _buckets.LongCount(l => l != 0);

        public QdbmHeader Header => _qdbmHeader;

        public QdbmDatabase(Stream databaseStream, int initialCapacity = Constants.DefaultBucketNumber)
        {
            _bucketNumber = GetNearestPrimeNumberLessThan(initialCapacity);
            _databaseStream = databaseStream;
            _qdbmHeader = new QdbmHeader();
            _databaseBinaryReader = new BinaryReader(_databaseStream);
            _databaseBinaryWriter = new BinaryWriter(_databaseStream);
            _buckets = new List<int>(_bucketNumber);
            _alignment = 0;

            if (databaseStream.Length != 0)
            {
                Open();
            }
            else
            {
                Create();
            }
        }

        private void Create()
        {
            var bucketNumber = _bucketNumber;

            _qdbmHeader.ByteOrder = Endianess.Little;
            _qdbmHeader.Version = Constants.QdbmVersion;
            _qdbmHeader.BucketNumber = bucketNumber;
                        
            // Bucket section start - position 48
            for (var i = 0; i < bucketNumber; i++)
            {
                _buckets.Add(0);
            }

            _databaseBinaryWriter.Write(Constants.MagicStringLittleEndian.ToCharArray());
            _databaseStream.Seek(Constants.OffsetHeaderVersion, SeekOrigin.Begin);
            _databaseBinaryWriter.Write(Constants.QdbmVersion.ToCharArray());
            _databaseStream.Seek(Constants.OffsetHeaderBucketNumber, SeekOrigin.Begin);
            _databaseBinaryWriter.Write(bucketNumber);

            _databaseStream.Seek(Constants.OffsetBuckets, SeekOrigin.Begin);

            var buckets = new byte[bucketNumber * sizeof(int)];
            _databaseBinaryWriter.Write(buckets);

            _databaseStream.Seek(Constants.OffsetHeaderFileSize, SeekOrigin.Begin);
            _databaseBinaryWriter.Write(_databaseStream.Length);
            _qdbmHeader.FileSize = _databaseStream.Length;
        }

        private void Open()
        {            
            // Header section start - position 0
            var magicNumber = ReadStringFromDbStream(_databaseBinaryReader, 12);
            switch (magicNumber)
            {
                case Constants.MagicStringLittleEndian:
                    _qdbmHeader.ByteOrder = Endianess.Little;
                    break;
                case Constants.MagicStringBigEndian:
                    throw new NotImplementedException("Big endian is not supported");
                default:
                    throw new InvalidOperationException("Unknown file format");
            }

            _qdbmHeader.Version = ReadStringFromDbStream(_databaseBinaryReader, 4);
            _qdbmHeader.WrapperFlags = _databaseBinaryReader.ReadInt64();
            _qdbmHeader.FileSize = _databaseBinaryReader.ReadInt64();
            _qdbmHeader.BucketNumber = _databaseBinaryReader.ReadInt64();
            _qdbmHeader.RecordCount = _databaseBinaryReader.ReadInt64();
            
            var bucketNumber = _qdbmHeader.BucketNumber;
            
            // Bucket section start - position 48
            for (var i = 0; i < bucketNumber; i++)
            {
                _buckets.Add(_databaseBinaryReader.ReadInt32());
            }
        }
        
        private static readonly int[] _primes = new int[] {
            1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 43, 47, 53, 59, 61, 71, 79, 83,
            89, 103, 109, 113, 127, 139, 157, 173, 191, 199, 223, 239, 251, 283, 317, 349,
            383, 409, 443, 479, 509, 571, 631, 701, 761, 829, 887, 953, 1021, 1151, 1279,
            1399, 1531, 1663, 1789, 1913, 2039, 2297, 2557, 2803, 3067, 3323, 3583, 3833,
            4093, 4603, 5119, 5623, 6143, 6653, 7159, 7673, 8191, 9209, 10223, 11261,
            12281, 13309, 14327, 15359, 16381, 18427, 20479, 22511, 24571, 26597, 28669,
            30713, 32749, 36857, 40949, 45053, 49139, 53239, 57331, 61417, 65521, 73727,
            81919, 90107, 98299, 106487, 114679, 122869, 131071, 147451, 163819, 180221,
            196597, 212987, 229373, 245759, 262139, 294911, 327673, 360439, 393209, 425977,
            458747, 491503, 524287, 589811, 655357, 720887, 786431, 851957, 917503, 982981,
            1048573, 1179641, 1310719, 1441771, 1572853, 1703903, 1835003, 1966079,
            2097143, 2359267, 2621431, 2883577, 3145721, 3407857, 3670013, 3932153,
            4194301, 4718579, 5242877, 5767129, 6291449, 6815741, 7340009, 7864301,
            8388593, 9437179, 10485751, 11534329, 12582893, 13631477, 14680063, 15728611,
            16777213, 18874367, 20971507, 23068667, 25165813, 27262931, 29360087, 31457269,
            33554393, 37748717, 41943023, 46137319, 50331599, 54525917, 58720253, 62914549,
            67108859, 75497467, 83886053, 92274671, 100663291, 109051903, 117440509,
            125829103, 134217689, 150994939, 167772107, 184549373, 201326557, 218103799,
            234881011, 251658227, 268435399, 301989881, 335544301, 369098707, 402653171,
            436207613, 469762043, 503316469, 536870909, 603979769, 671088637, 738197503,
            805306357, 872415211, 939524087, 1006632947, 1073741789, 1207959503,
            1342177237, 1476394991, 1610612711, 1744830457, 1879048183, 2013265907, -1
        };
            
        private static int GetNearestPrimeNumberLessThan(int num)
        {
            int i;
            if (num <= 0)
            {
                throw new InvalidOperationException("num must be positive");
            }
            for(i = 0; _primes[i] > 0; i++){
                if(num <= _primes[i]) return _primes[i];
            }
            return _primes[i-1];
        }

        public int SetAlignment(int alignment) => _alignment = alignment;

        public IReadOnlyDictionary<QdbmKey, byte[]> GetAll()
        {
            var records = new List<QdbmRecord>();
            
            foreach (var bucketAddr in _buckets)
            {
                if (bucketAddr == 0) continue;

                records.AddRange(ReadRecordsFromBucket(bucketAddr));
            }

            return records.ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        private List<QdbmRecord> ReadRecordsFromBucket(int bucketAddr)
        {
            var records = new List<QdbmRecord>();

            var qdbmRecordHeader = ReadRecordHeader(bucketAddr);

            var qdbmRecord = ReadRecord(qdbmRecordHeader);

            records.Add(qdbmRecord);

            var leftChildOffset = qdbmRecordHeader.LeftChildOffset;
            if (leftChildOffset != 0)
            {
                records.AddRange(ReadRecordsFromBucket(leftChildOffset));
            }
            
            var rightChildOffset = qdbmRecordHeader.RightChildOffset;
            if (rightChildOffset != 0)
            {
                records.AddRange(ReadRecordsFromBucket(rightChildOffset));
            }

            return records;
        }

        private QdbmRecordHeader ReadRecordHeader(int recordOffset)
        {
            _databaseStream.Seek(recordOffset, SeekOrigin.Begin);

            var header = new QdbmRecordHeader
            {
                Offset = recordOffset,
                Flags = _databaseBinaryReader.ReadInt32(),
                SecondHashValue = _databaseBinaryReader.ReadInt32(),
                KeySize = _databaseBinaryReader.ReadInt32(),
                ValueSize = _databaseBinaryReader.ReadInt32(),
                PaddingSize = _databaseBinaryReader.ReadInt32(),
                LeftChildOffset = _databaseBinaryReader.ReadInt32(),
                RightChildOffset = _databaseBinaryReader.ReadInt32()
            };

            return header;
        }

        private QdbmRecord ReadRecord(QdbmRecordHeader recordHeader)
        {
            _databaseStream.Seek(recordHeader.Offset + Constants.OffsetRecordKeyOffset, SeekOrigin.Begin);
            
            var key = _databaseBinaryReader.ReadBytes(recordHeader.KeySize);
            var value = _databaseBinaryReader.ReadBytes(recordHeader.ValueSize);

            var qdbmKey = new QdbmKey(key);

            if (qdbmKey.SecondaryHashCode != recordHeader.SecondHashValue)
            {
                throw new InvalidOperationException("Database is corrupt.");
            }

            var qdbmRecord = new QdbmRecord(qdbmKey, recordHeader, value);
            
            return qdbmRecord;
        }

        private string ReadStringFromDbStream(BinaryReader database, int count)
        {
            var stringBytes = database.ReadBytes(count);
            
            return Encoding.ASCII.GetString(stringBytes).TrimEnd((char)0);
        }

        private int CalculateBucketOffset(QdbmKey key, out int bucketId)
        {
            bucketId = (int)(key.GetHashCode() % _qdbmHeader.BucketNumber);
            var bucketOffset = Constants.OffsetBuckets + bucketId * sizeof(int);

            return bucketOffset;
        }

        private QdbmRecordHeader GetRecordHeader(QdbmKey key)
        {
            var bucketOffset = CalculateBucketOffset(key, out _);

            _databaseStream.Seek(bucketOffset, SeekOrigin.Begin);
            var recordOffset = _databaseBinaryReader.ReadInt32();

            if (recordOffset == 0)
            {
                return null;
            }

            var header = ReadRecordHeader(recordOffset);

            while (true)
            {            
                if (header.LeftChildOffset != 0 && key.SecondaryHashCode > header.SecondHashValue)
                {
                    header = ReadRecordHeader(header.LeftChildOffset);
                    continue;
                }

                if (header.RightChildOffset != 0 && key.SecondaryHashCode < header.SecondHashValue)
                {
                    header = ReadRecordHeader(header.RightChildOffset);
                    continue;
                }

                break;
            }

            if (header.SecondHashValue != key.SecondaryHashCode)
            {
                return null;
            }

            return header;
        }

        public byte[] Get(QdbmKey key)
        {
            var header = GetRecordHeader(key);

            if (header == null)
            {
                return null;
            }

            var record = ReadRecord(header);

            if (record.Key.KeyBytes.Length == key.KeyBytes.Length &&
                record.Key.KeyBytes.SequenceEqual(key.KeyBytes))
            {
                return record.Value;
            }
            
            return null;
        }

        public void Put(QdbmKey key, byte[] value)
        {
            if (key == null || key.KeyBytes.Length == 0) throw new ArgumentException("key must be specified", nameof(key));

            if (value == null || value.Length == 0) throw new ArgumentException("value must be specified", nameof(value));
            
            _databaseStream.Seek(Constants.OffsetHeaderRecordCount, SeekOrigin.Begin);

            var existingRecordHeader = GetRecordHeader(key);
            
            var keySize = key.KeyBytes.Length;
            var valueSize = value.Length;

            var recordHeader = new QdbmRecordHeader
            {
                KeySize = keySize,
                ValueSize = valueSize
            };
            
            var record = new QdbmRecord(key, recordHeader, value);

            if (existingRecordHeader?.Size >= recordHeader.Size)
            {
                record.Header.Offset = existingRecordHeader.Offset;
                WriteRecordToDbStream(record);
            }
            else
            {
                AppendRecordToDbStream(record);
            }

            var recordOffset = record.Header.Offset;
            
            var bucketOffset = CalculateBucketOffset(record.Key, out var bucketId);

            _databaseStream.Seek(bucketOffset, SeekOrigin.Begin);

            var existingRecordOffset = _databaseBinaryReader.ReadInt32();
                    
            if (existingRecordOffset == 0)
            {
                _databaseStream.Seek(bucketOffset, SeekOrigin.Begin);
                _databaseBinaryWriter.Write(recordOffset);

                _buckets[bucketId] = bucketOffset;
            }
            else
            {
                SetChildOffset(record.Key.SecondaryHashCode, existingRecordOffset, recordOffset);
            }
        }

        private void SetChildOffset(int secondHash, int existingRecordOffset, int recordOffset)
        {
            _databaseStream.Seek(existingRecordOffset, SeekOrigin.Begin);
            while (true)
            {
                var position = (int)_databaseStream.Position;
                _databaseStream.Seek(Constants.OffsetRecordSecondHashOffset, SeekOrigin.Current);
                
                var childSecondHash = _databaseBinaryReader.ReadInt32();

                if (secondHash == childSecondHash)
                {
                    // duplicate insert?
                    break;
                }
                
                _databaseStream.Seek(position + Constants.OffsetRecordLeftChildOffset, SeekOrigin.Begin);

                // Append Record to left or right child
                var leftChildOffset = _databaseBinaryReader.ReadInt32();

                if (secondHash > childSecondHash)
                {
                    if (leftChildOffset == 0)
                    {
                        _databaseStream.Seek(position + Constants.OffsetRecordLeftChildOffset, SeekOrigin.Begin);
                        _databaseBinaryWriter.Write(recordOffset);
                        return;
                    }
                    
                    _databaseStream.Seek(leftChildOffset, SeekOrigin.Begin);
                    continue;
                }

                var rightChildOffset = _databaseBinaryReader.ReadInt32();

                if (secondHash < childSecondHash)
                {
                    if (rightChildOffset == 0)
                    {
                        _databaseStream.Seek(position + Constants.OffsetRecordRightChildOffset, SeekOrigin.Begin);
                        _databaseBinaryWriter.Write(recordOffset);
                        return;
                    }
                    
                    _databaseStream.Seek(rightChildOffset, SeekOrigin.Begin);
                }
            }
        }

        private void AppendRecordToDbStream(QdbmRecord rec)
        {
            var paddingSize = CalculatePaddingSize(rec.Header.KeySize, rec.Header.ValueSize);
            rec.PaddingData = new byte[paddingSize];
            
            _databaseStream.Seek(0, SeekOrigin.End);

            var recordOffset = (int) _databaseStream.Position;

            rec.Header.Offset = recordOffset;

            WriteRecordToDbStream(rec);
            
            _databaseStream.Seek(Constants.OffsetHeaderRecordCount, SeekOrigin.Begin);
            _databaseBinaryWriter.Write(Header.RecordCount++);
                
            _databaseBinaryWriter.Seek(Constants.OffsetHeaderFileSize, SeekOrigin.Begin);
            _databaseBinaryWriter.Write(_databaseStream.Length);
            Header.FileSize = _databaseStream.Length;
        }

        private void WriteRecordToDbStream(QdbmRecord rec)
        { 
            _databaseStream.Seek(rec.Header.Offset, SeekOrigin.Begin);
            _databaseBinaryWriter.Write(rec.Header.Flags);
            _databaseBinaryWriter.Write(rec.Key.SecondaryHashCode);
            _databaseBinaryWriter.Write(rec.Header.KeySize);
            _databaseBinaryWriter.Write(rec.Header.ValueSize);
            _databaseBinaryWriter.Write(rec.Header.PaddingSize);
            _databaseBinaryWriter.Write(0);
            _databaseBinaryWriter.Write(0);
            
            if (rec.Key.KeyBytes.Length > 0)
            {
                _databaseBinaryWriter.Write(rec.Key.KeyBytes);                
            }

            if (rec.Value.Length > 0)
            {
                _databaseBinaryWriter.Write(rec.Value);
            }

            if (rec.PaddingData?.Length > 0)
            {
                _databaseBinaryWriter.Write(rec.PaddingData);
            }
        }
        
        private int CalculatePaddingSize(int keySize, int valueSize)
        {
            var fileSize = Header.FileSize;

            if (_alignment == 0)
            {
                return 0;
            }
            
            if(_alignment > 0){
                return _alignment - (int)((fileSize + Constants.OffsetHeaderRecordCount * sizeof(int) + keySize + valueSize) % _alignment);
            }
            
            var pad = (int) (valueSize * (2.0 / (1 << -(_alignment))));
            if (valueSize + pad < Constants.DefaultBlockSize)
            {
                return pad >= Constants.DefaultBlockSize * sizeof(int)
                    ? pad
                    : Constants.OffsetHeaderRecordCount * sizeof(int);                
            }
            
            if (valueSize <= Constants.DefaultBlockSize) pad = 0;
            if (fileSize % Constants.DefaultBlockSize == 0)
            {
                return (pad / Constants.DefaultBlockSize) * Constants.DefaultBlockSize +
                       Constants.DefaultBlockSize -
                       (int) (fileSize + Constants.OffsetHeaderRecordCount * sizeof(int) + keySize +
                              valueSize) % Constants.DefaultBlockSize;
            }
                
            return (pad / (Constants.DefaultBlockSize / 2)) * (Constants.DefaultBlockSize / 2) +
                   (Constants.DefaultBlockSize / 2) -
                   (int) (fileSize + Constants.OffsetHeaderRecordCount * sizeof(int) + keySize +
                          valueSize) % (Constants.DefaultBlockSize / 2);

        }
    }
}