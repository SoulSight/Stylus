using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Util
{
    public static class Extension
    {
        public static void AddAll(this HashSet<long> set, IEnumerable<long> elements)
        {
            foreach (var item in elements)
            {
                set.Add(item);
            }
        }

        public static TResult GetWithDefault<TKey, TResult>(this Dictionary<TKey, TResult> dict, TKey key) 
        {
            TResult result = default(TResult);
            dict.TryGetValue(key, out result);
            return result;
        }
    }
}
