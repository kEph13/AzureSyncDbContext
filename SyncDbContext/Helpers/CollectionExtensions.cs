using System.Collections.Generic;
using System.Linq;

namespace SyncDbContext.Helpers
{
    public static class CollectionExtensions
    {
        public static bool HasSameKeys<TKey, TValue>(this IDictionary<TKey, TValue> self, IDictionary<TKey, TValue> other)
        {
            return HasSameKeys(self, other.Keys);
        }

        public static bool HasSameKeys<TKey, TValue>(this IDictionary<TKey, TValue> self, ICollection<TKey> other)
        {
            if (self.Count != other.Count)
            {
                return false;
            }

            foreach (var key in other)
            {
                if (!self.ContainsKey(key))
                {
                    return false;
                }
            }
            return true;
        }

        public static int CombineHashCodes(params int[] hashes)
        {
            return CombineHashCodes(hashes.ToList());
        }

        public static int CombineHashCodes(List<int> hashes)
        {
            switch (hashes.Count)
            {
                case 2:
                    return CombineHashCodes(hashes[0], hashes[1]);
                case 3:
                    return CombineHashCodes(hashes[0], hashes[1], hashes[2]);
                case 4:
                    return CombineHashCodes(hashes[0], hashes[1], hashes[2], hashes[3]);
                case 5:
                    return CombineHashCodes(hashes[0], hashes[1], hashes[2], hashes[3], hashes[4]);
                case 6:
                    return CombineHashCodes(hashes[0], hashes[1], hashes[2], hashes[3], hashes[4], hashes[5]);
                case 7:
                    return CombineHashCodes(hashes[0], hashes[1], hashes[2], hashes[3], hashes[4], hashes[5], hashes[6]);
                case 8:
                    return CombineHashCodes(hashes[0], hashes[1], hashes[2], hashes[3], hashes[4], hashes[5], hashes[6], hashes[7]);
                default:
                    throw new System.Exception("Must pass between 2 and 8 hashes");
            }
        }

        public static int CombineHashCodes(int h1, int h2)
        {
            // this is where the magic happens
            return (((h1 << 5) + h1) ^ h2);
        }

        public static int CombineHashCodes(int h1, int h2, int h3)
        {
            return CombineHashCodes(CombineHashCodes(h1, h2), h3);
        }

        public static int CombineHashCodes(int h1, int h2, int h3, int h4)
        {
            return CombineHashCodes(CombineHashCodes(h1, h2), CombineHashCodes(h3, h4));
        }

        public static int CombineHashCodes(int h1, int h2, int h3, int h4, int h5)
        {
            return CombineHashCodes(CombineHashCodes(h1, h2, h3, h4), h5);
        }

        public static int CombineHashCodes(int h1, int h2, int h3, int h4, int h5, int h6)
        {
            return CombineHashCodes(CombineHashCodes(h1, h2, h3, h4), CombineHashCodes(h5, h6));
        }

        public static int CombineHashCodes(int h1, int h2, int h3, int h4, int h5, int h6, int h7)
        {
            return CombineHashCodes(CombineHashCodes(h1, h2, h3, h4), CombineHashCodes(h5, h6, h7));
        }

        public static int CombineHashCodes(int h1, int h2, int h3, int h4, int h5, int h6, int h7, int h8)
        {
            return CombineHashCodes(CombineHashCodes(h1, h2, h3, h4), CombineHashCodes(h5, h6, h7, h8));
        }
    }
}
