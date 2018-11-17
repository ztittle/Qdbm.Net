namespace Qdbm.Net
{
    internal class QdbmRecordHeader
    {
        public int Offset { get; internal set; }
        public int Flags { get; internal set; }
        public int SecondHashValue { get; internal set; }
        public int KeySize { get; internal set; }
        public int ValueSize { get; internal set; }
        public int PaddingSize { get; internal set; }
        public int LeftChildOffset { get; internal set; }
        public int RightChildOffset { get; internal set; }
        public int Size => KeySize + ValueSize + PaddingSize;
    }
}