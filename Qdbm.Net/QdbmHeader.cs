namespace Qdbm.Net
{
    public class QdbmHeader
    {
        public Endianess ByteOrder { get; internal set; }
        
        public string Version { get; internal set; }

        public long WrapperFlags { get; internal set; }
        
        public long FileSize { get; internal set; }
        
        public long BucketNumber { get; internal set; }
        
        public long RecordCount { get; internal set; }
    }
}