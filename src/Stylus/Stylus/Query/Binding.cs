using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.Util;

namespace Stylus.Query
{
    public abstract class Binding : Dictionary<ushort, HashSet<long>>
    {
        public abstract void AddEid(long eid);

        public abstract void AddEids(IEnumerable<long> eids);

        public abstract bool ContainEid(long eid);

        public abstract bool RemoveEid(long eid);

        public abstract IEnumerable<long> FilterEids(IEnumerable<long> eids);

        public abstract HashSet<long> FilterEidSet(HashSet<long> eids);

        /// Prune the other tid and its eid set by this binding
        public abstract HashSet<long> PruneEidSet(ushort tid, HashSet<long> eids);

        public abstract Binding PruneBinding(Binding other);
    }

    public class UniEidBinding : Binding
    {
        public long Id { set; get; }

        public UniEidBinding(long id)
        {
            this.Id = id;
        }

        public override void AddEid(long eid)
        {
            throw new NotSupportedException();
        }

        public override void AddEids(IEnumerable<long> eid)
        {
            throw new NotSupportedException();
        }

        public override bool ContainEid(long eid)
        {
            return this.Id == eid;
        }

        public override bool RemoveEid(long eid)
        {
            throw new NotSupportedException();
        }

        public override IEnumerable<long> FilterEids(IEnumerable<long> eids)
        {
            foreach (var eid in eids)
            {
                if (eid == this.Id)
                {
                    yield return eid;
                }
            }
        }

        public override HashSet<long> FilterEidSet(HashSet<long> eids)
        {
            if (eids.Contains(this.Id))
            {
                eids = new HashSet<long>() { this.Id };
            }
            else
            {
                eids = new HashSet<long>();
            }
            return eids;
        }

        public override HashSet<long> PruneEidSet(ushort tid, HashSet<long> eids)
        {
            eids = FilterEidSet(eids);
            return eids;
        }

        public override Binding PruneBinding(Binding other)
        {
            ushort identy_tid = TidUtil.GetTid(this.Id);
            foreach (var key in other.Keys)
            {
                if (key == identy_tid && other[key].Contains(this.Id))
                {
                    other[key] = new HashSet<long>() { Id };
                }
                else
                {
                    other[key] = new HashSet<long>();
                }
            }
            return other;
        }

        public override string ToString()
        {
            return "UniEidBinding: " + this.Id;
        }
    }

    public class TidBinding : Binding
    {
        public HashSet<ushort> Tids { set; get; }

        public TidBinding(IEnumerable<ushort> tids)
        {
            this.Tids = new HashSet<ushort>(tids);
        }

        public override void AddEid(long eid)
        {
            throw new NotSupportedException();
        }

        public override void AddEids(IEnumerable<long> eid)
        {
            throw new NotSupportedException();
        }

        public override bool ContainEid(long eid)
        {
            return Tids.Contains(TidUtil.GetTid(eid));
        }

        public override bool RemoveEid(long eid)
        {
            throw new NotSupportedException();
        }

        public override IEnumerable<long> FilterEids(IEnumerable<long> eids)
        {
            foreach (var eid in eids)
            {
                if (this.ContainEid(eid))
                {
                    yield return eid;
                }
            }
        }

        public override HashSet<long> FilterEidSet(HashSet<long> eids)
        {
            eids.RemoveWhere(eid => !this.ContainEid(eid));
            return eids;
        }

        public override HashSet<long> PruneEidSet(ushort tid, HashSet<long> eids)
        {
            if (!this.Tids.Contains(tid))
            {
                eids = new HashSet<long>();
            }
            return eids;
        }

        public override Binding PruneBinding(Binding other)
        {
            foreach (var other_key in other.Keys)
            {
                if (!this.Tids.Contains(other_key))
                {
                    other[other_key] = new HashSet<long>();
                }
            }
            return other;
        }

        public override string ToString()
        {
            return "TidBinding: " + string.Join(", ", this.Tids);
        }
    }

    // xUDT -> Eids
    public class EidSetBinding : Binding
    {
        public EidSetBinding() { }

        public EidSetBinding(IEnumerable<long> eids)
        {
            this.AddEids(eids);
        }

        public override void AddEid(long eid)
        {
            var tid = TidUtil.GetTid(eid);
            HashSet<long> eid_set;
            if (!this.TryGetValue(tid, out eid_set))
            {
                eid_set = new HashSet<long>() { eid };
                this.Add(tid, eid_set);
            }
            else
            {
                eid_set.Add(eid);
            }
        }

        public override void AddEids(IEnumerable<long> eids)
        {
            foreach (var eid in eids)
            {
                this.AddEid(eid);
            }
        }

        public override bool ContainEid(long eid)
        {
            var tid = TidUtil.GetTid(eid);
            if (!this.ContainsKey(tid))
            {
                return false;
            }
            return this[tid].Contains(eid);
        }

        public override bool RemoveEid(long eid)
        {
            var tid = TidUtil.GetTid(eid);
            if (!this.ContainsKey(tid))
            {
                return false;
            }
            return this[tid].Remove(eid);
        }

        public override IEnumerable<long> FilterEids(IEnumerable<long> eids)
        {
            foreach (var eid in eids)
            {
                if (this.ContainEid(eid))
                {
                    yield return eid;
                }
            }
        }

        public override HashSet<long> FilterEidSet(HashSet<long> eids)
        {
            eids.RemoveWhere(eid => !this.ContainEid(eid));
            return eids;
        }

        /// Prune the other tid and its eid set by this binding
        public override HashSet<long> PruneEidSet(ushort tid, HashSet<long> eids)
        {
            if (!this.ContainsKey(tid))
            {
                eids = new HashSet<long>();
            }

            var filter_set = this[tid];
            eids.RemoveWhere(eid => !filter_set.Contains(eid));
            return eids;
        }

        public override Binding PruneBinding(Binding other)
        {
            foreach (var other_key in other.Keys)
            {
                if (!this.ContainsKey(other_key))
                {
                    other[other_key] = new HashSet<long>();
                }
                PruneEidSet(other_key, other[other_key]);
            }
            return other;
        }

        public override string ToString()
        {
            //return "EidSetBinding: " + string.Join("; ", this.Values.Select(l => string.Join(", ", l)));
            return "EidSetBinding: count = " + this.Values.Sum(v => v.Count);
        }
    }

    public class ConcurrentEidSetBinding : Binding
    {
        public override void AddEid(long eid)
        {
            var tid = TidUtil.GetTid(eid);
            if (!this.ContainsKey(tid))
            {
                lock (this)
                {
                    if (!this.ContainsKey(tid))
                    {
                        this.Add(tid, new HashSet<long>());
                    }
                }
            }
            lock (this[tid])
            {
                this[tid].Add(eid);
            }
        }

        public override void AddEids(IEnumerable<long> eids)
        {
            var tid = TidUtil.GetTid(eids.First());
            if (!this.ContainsKey(tid))
            {
                lock (this)
                {
                    if (!this.ContainsKey(tid))
                    {
                        this.Add(tid, new HashSet<long>());
                    }
                }
            }
            lock (this[tid])
            {
                this[tid].AddAll(eids);
            }
        }

        public override bool ContainEid(long eid)
        {
            var tid = TidUtil.GetTid(eid);
            if (!this.ContainsKey(tid))
            {
                return false;
            }
            return this[tid].Contains(eid);
        }

        public override bool RemoveEid(long eid)
        {
            var tid = TidUtil.GetTid(eid);
            if (!this.ContainsKey(tid))
            {
                return false;
            }
            return this[tid].Remove(eid);
        }

        public override IEnumerable<long> FilterEids(IEnumerable<long> eids)
        {
            foreach (var eid in eids)
            {
                if (this.ContainEid(eid))
                {
                    yield return eid;
                }
            }
        }

        public override HashSet<long> FilterEidSet(HashSet<long> eids)
        {
            eids.RemoveWhere(eid => !this.ContainEid(eid));
            return eids;
        }

        /// Prune the other tid and its eid set by this binding
        public override HashSet<long> PruneEidSet(ushort tid, HashSet<long> eids)
        {
            if (!this.ContainsKey(tid))
            {
                eids = new HashSet<long>();
            }

            var filter_set = this[tid];
            eids.RemoveWhere(eid => !filter_set.Contains(eid));
            return eids;
        }

        public override Binding PruneBinding(Binding other)
        {
            foreach (var other_key in other.Keys)
            {
                if (!this.ContainsKey(other_key))
                {
                    other[other_key] = new HashSet<long>();
                }
                PruneEidSet(other_key, other[other_key]);
            }
            return other;
        }

        public override string ToString()
        {
            return "ConcurrentEidSetBinding: " + string.Join(", ", this.Values.SelectMany(l => l));
        }
    }
}
