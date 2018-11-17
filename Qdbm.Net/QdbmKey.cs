using System;
using System.Linq;

namespace Qdbm.Net
{
    public class QdbmKey
    {
        private object _key;
        
        public QdbmKey(int key)
        {
            _key = key;
            KeyBytes = BitConverter.GetBytes(key);
        }
        
        public QdbmKey(string key)
        {
            _key = key;
            KeyBytes = key.ToCharArray().Select(Convert.ToByte).ToArray();
        }
        
        public QdbmKey(byte[] key)
        {
            _key = key;
            KeyBytes = key;
        }
        
        public byte[] KeyBytes { get; }

        public int SecondaryHashCode => GetSecondaryHashCode();

        public override int GetHashCode()
        {
            var key = KeyBytes;
            
            var result = key.Length == sizeof(int) ? BitConverter.ToInt32(key, 0) : 751;

            result = key.Aggregate(result, (current, kb) => current * 31 + kb);

            result = (result * 87767623) & int.MaxValue;

            return result;
        }

        public int GetSecondaryHashCode()
        {          
            var key = KeyBytes;
            
            var result = 19780211;
            
            result = key.Reverse().Aggregate(result, (current, kb) => current * 37 + kb);

            result = (result * 43321879) & int.MaxValue;

            return result;
        }

        public override string ToString()
        {
            if (_key is string || _key is int)
                return _key.ToString();
            
            return string.Concat(KeyBytes.Select(b => b.ToString("X")));
        }
    }
}