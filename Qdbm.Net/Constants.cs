namespace Qdbm.Net
{
    public static class Constants
    {
        public const string MagicStringLittleEndian = "[depot]\n\f";
        public const string MagicStringBigEndian = "[DEPOT]\n\f";
        public const string QdbmVersion = "14";
        public const int OffsetHeaderVersion = 12;
        public const int OffsetHeaderFlagsForWrappers = 16;
        public const int OffsetHeaderFileSize = 24;
        public const int OffsetHeaderBucketNumber = 32;
        public const int OffsetHeaderRecordCount = 40;
        public const int OffsetBuckets = 48;
        public const int DefaultBucketNumber = 4093;
        public const int DefaultBlockSize = 4096;

        public const int OffsetRecordSecondHashOffset = 4;
        public const int OffsetRecordLeftChildOffset = 20;
        public const int OffsetRecordRightChildOffset = 24;
        public const int OffsetRecordKeyOffset = 28;
    }
}