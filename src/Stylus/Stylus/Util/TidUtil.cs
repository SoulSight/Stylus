using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity;
using Trinity.Core.Lib;

using Stylus.DataModel;

namespace Stylus.Util
{
    public unsafe class TidUtil
    {
        public static long CloneMaskTid(long eid, ushort tid = 0)
        {
            long clone_id = eid;
            ushort* sp = (ushort*)&clone_id;
            *(sp + 3) = tid;
            return clone_id;
        }

        public static long MaskTid(long eid, ushort tid = 0)
        {
            ushort* sp = (ushort*)&eid;
            *(sp + 3) = tid;
            return eid;
        }

        public static ushort GetTid(long eid)
        {
            ushort* sp = (ushort*)&eid;
            ushort tid = *(sp + 3);
            return tid;
        }

        public static void SortByTid(long[] ids) 
        {
            Array.Sort(ids, (id1, id2) => GetTid(id1).CompareTo(GetTid(id2)));
        }

        public static void SortByTid(List<long> ids) 
        {
            ids.Sort((id1, id2) => GetTid(id1).CompareTo(GetTid(id2)));
        }

        public static List<long> SelectByTid(List<long> ids, ushort tid) 
        {
            return ids.Where(id => GetTid(id) == tid).ToList();
        }

        public static long[] SelectByTid(long[] ids, ushort tid)
        {
            return SelectByTid(ids.ToList(), tid).ToArray();
        }
    }
}
