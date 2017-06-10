using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Stylus.Util;

namespace Stylus.Query
{
    /// <summary>
    /// Flattened tuples as intermediate results
    /// </summary>
    public class QuerySolutions
    {
        public List<string> Heads { set; get; }

        public List<long[]> Records { set; get; }

        // public ConcurrentBag<List<long>> Records { set; get; }

        public QuerySolutions()
        {
            this.Heads = new List<string>();
            this.Records = new List<long[]>();
        }

        public QuerySolutions(QuerySolutions other)
        {
            this.Heads = new List<string>(other.Heads);
            this.Records = new List<long[]>(other.Records);
        }

        public void AppendRecord(long[] tuple)
        {
            this.Records.Add(tuple);
        }

        public void AppendRecords(IEnumerable<long[]> tuples)
        {
            this.Records.AddRange(tuples);
        }

        private bool Invalid(long[] record)
        {
            return record == null;
        }

        public QuerySolutions Join(QuerySolutions other)
        {
            if (this.Heads.Count == 0)
            {
                return other;
            }
            if (other.Heads.Count == 0)
            {
                return this;
            }

            var self_dict = GetHeadDict();
            var other_dict = other.GetHeadDict();
            var join_cols = new Dictionary<string, Tuple<int, int>>();
            List<int> self_join_indexes = new List<int>();
            List<int> other_join_indexes = new List<int>();
            foreach (var skvp in self_dict)
            {
                string key = skvp.Key;
                if (other_dict.ContainsKey(key))
                {
                    join_cols.Add(key, Tuple.Create(skvp.Value, other_dict[key]));
                    self_join_indexes.Add(skvp.Value);
                    other_join_indexes.Add(other_dict[key]);
                }
            }

            if (join_cols.Count == 0)
            {
                this.Product(other);
                return this;
            }

            List<string> union_head = new List<string>(this.Heads);
            union_head.AddRange(other.Heads);
            union_head = union_head.Distinct().ToList();
            QuerySolutions join_binding = new QuerySolutions();
            join_binding.Heads = union_head;

            int[] signed_indexes = new int[union_head.Count];
            for (int i = 0; i < union_head.Count; i++)
            {
                string col_name = union_head[i];
                if (self_dict.ContainsKey(col_name))
                {
                    signed_indexes[i] = self_dict[col_name];
                }
                else
                {
                    signed_indexes[i] = -(other_dict[col_name] + 1);
                }
            }

            join_binding.Records = new List<long[]>();

            Dictionary<int, List<int>> hash_self_indexes = new Dictionary<int, List<int>>();
            for (int i = 0; i < this.Records.Count; i++)
            {
                var srecord = this.Records[i];
                if (Invalid(srecord))
                {
                    continue;
                }
                int sr_hash = ListUtil.GetHashCode(srecord, self_join_indexes);
                List<int> recs;
                if (!hash_self_indexes.TryGetValue(sr_hash, out recs))
                {
                    recs = new List<int>() { i };
                    hash_self_indexes[sr_hash] = recs;
                }
                else
                {
                    recs.Add(i);
                }
            }

            // Parallel:
            //foreach (var orecord in other.Records)
            Parallel.ForEach(other.Records, orecord =>
            {
                if (Invalid(orecord))
                {
                    //continue;
                    return;
                }
                int or_hash = ListUtil.GetHashCode(orecord, other_join_indexes);
                List<int> sr_indexes;
                if (!hash_self_indexes.TryGetValue(or_hash, out sr_indexes))
                {
                    //continue;
                    return;
                }
                else
                {
                    foreach (var sr_index in sr_indexes)
                    {
                        var srecord = this.Records[sr_index];
                        if (PartialEquals(srecord, self_join_indexes, orecord, other_join_indexes))
                        {
                            lock (join_binding)
                            {
                                join_binding.Records.Add(MergeRecord(srecord, orecord, signed_indexes));
                            }
                        }
                    }
                }
            });

            return join_binding;
        }

        private bool PartialEquals(long[] array1, List<int> indexes1, long[] array2, List<int> indexes2)
        {
            int cnt = indexes1.Count;
            if (cnt != indexes2.Count)
            {
                return false;
            }
            for (int i = 0; i < cnt; i++)
            {
                if (array1[indexes1[i]] != array2[indexes2[i]])
                {
                    return false;
                }
            }
            return true;
        }

        private long[] MergeRecord(long[] srecord, long[] orecord, int[] signed_indexes)
        {
            long[] result = new long[signed_indexes.Length];
            for (int i = 0; i < signed_indexes.Length; i++)
            {
                int index = signed_indexes[i];
                if (index >= 0)
                {
                    result[i] = srecord[index];
                }
                else
                {
                    result[i] = orecord[-index - 1];
                }
            }
            return result;
        }

        private void ThrowIfNoCol(params string[] col_names) 
        {
            foreach (var col_name in col_names)
            {
                ThrowUtil.ThrowIf(!ContainCol(col_name), "Column not found: " + col_name);
            }
        }

        public void Filter(Func<long[], bool> func)
        {
            for (int i = 0; i < this.Records.Count; i++)
            {
                var record = this.Records[i];
                if (!Invalid(record) && !func(record))
                {
                    this.Records[i] = null;
                }
            }
            //this.Records = this.Records.Where(r => func(r)).ToList();
        }

        public void Filter(string col, Func<long, bool> func)
        {
            ThrowIfNoCol(col);
            int index = this.GetHeadDict()[col];
            for (int i = 0; i < this.Records.Count; i++)
            {
                var record = this.Records[i];
                if (!Invalid(record) && !func(record[index]))
                {
                    this.Records[i] = null;
                }
            }
            //this.Records = this.Records.Where(r => func(r[index])).ToList();
        }

        public void Filter(string col1, string col2, Func<long, long, bool> func)
        {
            ThrowIfNoCol(col1, col2);

            int index1 = this.GetHeadDict()[col1];
            int index2 = this.GetHeadDict()[col2];
            for (int i = 0; i < this.Records.Count; i++)
            {
                var record = this.Records[i];
                if (!Invalid(record) && !func(record[index1], record[index2]))
                {
                    this.Records[i] = null;
                }
            }

            //this.Records = this.Records.Where(r => func(r[index1], r[index2])).ToList();
        }

        public void Filter(string col1, string col2, string col3, Func<long, long, long, bool> func)
        {
            ThrowIfNoCol(col1, col2, col3);

            int index1 = this.GetHeadDict()[col1];
            int index2 = this.GetHeadDict()[col2];
            int index3 = this.GetHeadDict()[col3];
            for (int i = 0; i < this.Records.Count; i++)
            {
                var record = this.Records[i];
                if (!Invalid(record) && !func(record[index1], record[index2], record[index3]))
                {
                    this.Records[i] = null;
                }
            }

            //this.Records = this.Records.Where(r => func(r[index1], r[index2], r[index3])).ToList();
        }

        public QuerySolutions Expand(Func<List<string>, List<string>> head_op, Func<long[], List<long[]>> expand_op)
        {
            QuerySolutions tb = new QuerySolutions();
            tb.Heads = head_op(this.Heads);
            tb.RefreshDict();
            foreach (var record in this.Records)
            {
                if (Invalid(record))
                {
                    continue;
                }
                tb.AppendRecords(expand_op(record));
            }
            return tb;
        }

        public QuerySolutions Expand(string col, string expand_col, Func<long, List<long>> expand_op)
        {
            ThrowIfNoCol(col);

            int op_index = this.GetHeadDict()[col];
            QuerySolutions tb = new QuerySolutions();
            tb.Heads = new List<string>(this.Heads);
            tb.Heads.Add(expand_col);
            tb.RefreshDict();

            int nlen = tb.Heads.Count;
            foreach (var record in this.Records)
            {
                if (Invalid(record))
                {
                    continue;
                }
                long element = record[op_index];
                List<long> expansion = expand_op(element);

                tb.AppendRecords(expansion.Select(item =>
                {
                    long[] nr = new long[nlen];
                    Array.Copy(record, nr, record.Length);
                    nr[nlen - 1] = item;
                    return nr;
                }));
            }
            return tb;
        }

        public void Product(QuerySolutions other)
        {
            if (this.Heads.Count == 0)
            {
                this.Heads = new List<string>(other.Heads);
                this.Records = new List<long[]>(other.Records);
            }
            else
            {
                int tr_len = this.Heads.Count;
                int or_len = other.Heads.Count;

                this.Heads.AddRange(other.Heads);
                List<long[]> nrs = new List<long[]>();

                foreach (var tr in this.Records)
                {
                    if (Invalid(tr))
                    {
                        continue;
                    }
                    foreach (var or in other.Records)
                    {
                        if (Invalid(or))
                        {
                            continue;
                        }
                        long[] nr = new long[tr_len + or_len];
                        Array.Copy(tr, nr, tr_len);
                        Array.Copy(or, 0, nr, tr_len, or_len);
                        nrs.Add(nr);
                    }
                }
                this.Records = nrs;
            }

            RefreshDict();
        }

        public void Product(string col, IEnumerable<long> values)
        {
            ThrowIfNoCol(col);

            if (this.Heads.Count == 0)
            {
                this.Heads = new List<string>() { col };
                this.Records = new List<long[]>(values.Select(v => new long[] { v }));
            }
            else
            {
                this.Heads.Add(col);
                List<long[]> nrs = new List<long[]>();

                int nlen = this.Heads.Count;

                foreach (var tr in this.Records)
                {
                    if (Invalid(tr))
                    {
                        continue;
                    }

                    nrs.AddRange(values.Select(item =>
                    {
                        long[] nr = new long[nlen];
                        Array.Copy(tr, nr, tr.Length);
                        nr[nlen - 1] = item;
                        return nr;
                    }));
                }
                this.Records = nrs;
            }

            RefreshDict();
        }

        private void RefreshDict()
        {
            this.dict = new Dictionary<string, int>();
            for (int i = 0; i < Heads.Count; i++)
            {
                dict.Add(Heads[i], i);
            }
        }

        private Dictionary<string, int> dict = null;

        private Dictionary<string, int> GetHeadDict()
        {
            if (this.dict == null)
            {
                this.dict = new Dictionary<string, int>();
                for (int i = 0; i < Heads.Count; i++)
                {
                    dict.Add(Heads[i], i);
                }
            }
            return this.dict;
        }

        public bool ContainCol(string col_name)
        {
            return GetHeadDict().ContainsKey(col_name);
        }

        public QuerySolutions Select(params string[] col_names)
        {
            ThrowIfNoCol(col_names);

            List<int> select_cols = new List<int>(col_names.Select(c => GetHeadDict()[c]));
            QuerySolutions binding = new QuerySolutions();
            binding.Heads = new List<string>(col_names);

            int len = select_cols.Count;
            foreach (var record in this.Records)
            {
                if (Invalid(record))
                {
                    continue;
                }
                long[] select_record = new long[len];

                for (int i = 0; i < len; i++)
                {
                    select_record[i] = record[select_cols[i]];
                }

                binding.AppendRecord(select_record);
            }
            return binding;
        }

        public HashSet<long> DistinctValues(string col)
        {
            ThrowIfNoCol(col);

            int index = GetHeadDict()[col];
            return new HashSet<long>(this.Records.Where(r => !Invalid(r)).Select(r => r[index]));
        }

        public Dictionary<string, HashSet<long>> DistinctValues()
        {
            Dictionary<string, HashSet<long>> distincts = new Dictionary<string, HashSet<long>>();
            foreach (var col in this.Heads)
            {
                distincts.Add(col, new HashSet<long>());
            }
            foreach (var record in this.Records)
            {
                if (Invalid(record))
                {
                    continue;
                }
                for (int i = 0; i < this.Heads.Count; i++)
                {
                    distincts[this.Heads[i]].Add(record[i]);
                }
            }
            return distincts;
        }

        public void Shrink()
        {
            this.Records = this.Records.Where(r => !Invalid(r)).ToList();
        }

        public string Brief()
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendLine("Heads: " + string.Join(", ", this.Heads));
            builder.AppendLine("#Records: " + this.Records.Count);
            return builder.ToString();
        }

        public string Detail()
        {
            return this.ToString();
        }

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendLine(string.Join(", ", this.Heads));
            foreach (var record in this.Records)
            {
                if (Invalid(record))
                {
                    continue;
                }
                builder.AppendLine(string.Join(", ", record));
            }
            return builder.ToString();
        }
    }
}
