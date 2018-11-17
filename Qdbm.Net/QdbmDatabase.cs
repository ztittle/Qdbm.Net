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
        private int _alignment;

        public long UsedBucketCount => _buckets.LongCount(l => l != 0);

        public QdbmHeader Header => _qdbmHeader;

        public QdbmDatabase(Stream databaseStream)
        {
            _databaseStream = databaseStream;
            _qdbmHeader = new QdbmHeader();
            _databaseBinaryReader = new BinaryReader(_databaseStream);
            _databaseBinaryWriter = new BinaryWriter(_databaseStream);
            _buckets = new List<int>(Constants.DefaultBucketNumber);
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
            const long bucketNumber = Constants.DefaultBucketNumber;

            _qdbmHeader.ByteOrder = Endianess.Little;
            _qdbmHeader.Version = Constants.QdbmVersion;
            _qdbmHeader.BucketNumber = Constants.DefaultBucketNumber;
                        
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