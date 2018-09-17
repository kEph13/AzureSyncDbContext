using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
                return false;
            foreach (var key in other)
            {
                if (!self.ContainsKey(key))
                    return false;
            }
            return true;
        }
    }
}
