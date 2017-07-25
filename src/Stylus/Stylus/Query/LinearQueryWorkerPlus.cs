using Stylus.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Query
{
    public class LinearQueryWorkerPlus : BaseQueryWorkerPlus
    {
        #region Bindings
        private Dictionary<string, Binding> query_bindings = new Dictionary<string, Binding>();

        public override bool ContainsBinding(string variable)
        {
            return query_bindings.ContainsKey(variable);
        }

        public override IEnumerable<long> EnumerateBinding(string variable)
        {
            foreach (var tuple in EnumerateBindingValues(query_bindings[variable]))
            {
                foreach (var item in tuple.Item2)
                {
                    yield return item;
                }
            }
        }

        public override Binding GetBinding(string variable)
        {
            Binding b;
            if (query_bindings.TryGetValue(variable, out b))
            {
                return b;
            }
            return null;
        }

        public override void SetBinding(string variable, Binding binding)
        {
            query_bindings[variable] = binding;
        }

        public override void ReplaceBinding(string variable, IEnumerable<long> bindings)
        {
            query_bindings[variable] = new EidSetBinding(bindings);
        }

        public override void ReplaceBinding(string variable, Binding bindings)
        {
            query_bindings[variable] = bindings;
        }

        public override void AppendBinding(string variable, IEnumerable<long> bindings)
        {
            if (!query_bindings.ContainsKey(variable))
            {
                query_bindings[variable] = new EidSetBinding(bindings);
            }
            else
            {
                query_bindings[variable].AddEids(bindings);
            }
        }

        private IEnumerable<Tuple<ushort, ICollection<long>>> EnumerateBindingValues(Binding binding)
        {
            if (binding is UniEidBinding)
            {
                long id = (binding as UniEidBinding).Id;
                ushort tid = TidUtil.GetTid(id);
                ICollection<long> values = new List<long>() { id };
                yield return Tuple.Create(tid, values);
            }
            else if (binding is TidBinding)
            {
                var tids = (binding as TidBinding).Tids;
                foreach (var tid in tids)
                {
                    ICollection<long> values = this.Storage.LoadEids(tid);
                    yield return Tuple.Create(tid, values);
                }
            }
            else if (binding is EidSetBinding)
            {
                EidSetBinding esbinding = (binding as EidSetBinding);
                foreach (var kvp in esbinding)
                {
                    ICollection<long> values = kvp.Value;
                    yield return Tuple.Create(kvp.Key, values);
                }
            }
            else
            {
                throw new NotSupportedException();
            }
        }
        #endregion

        #region BaseQueryWorkerPlus
        public override List<xTwigPlusAnswer> ExecuteToXTwigAnswerPlus(xTwigPlusHead head)
        {
            throw new NotImplementedException();
        }

        public override QuerySolutions ExecuteFlattenPlus(xTwigPlusHead head)
        {
            var results = ExecuteToXTwigAnswerPlus(head);
            return PruneFlattenPlus(head, results);
        }

        public override QuerySolutions ExecuteSingleTwigPlus(xTwigPlusHead head)
        {
            throw new NotImplementedException();
        }

        public override QuerySolutions ExecutePlus(List<xTwigPlusHead> heads)
        {
            throw new NotImplementedException();
        }

        private QuerySolutions PruneFlattenPlus(xTwigPlusHead head, List<xTwigPlusAnswer> answers)
        {
            QuerySolutions global_ans = new QuerySolutions();
            global_ans.Heads.Add(head.Root);
            global_ans.Heads.AddRange(head.SelectLeaves.Select(l => l.Item2));

            foreach (var ans in answers)
            {
                if (!this.query_bindings[head.Root].ContainEid(ans.Root))
                {
                    continue;
                }
                QuerySolutions local_ans = new QuerySolutions();
                local_ans.Heads = new List<string>() { head.Root };
                local_ans.Records = new List<long[]>() { new long[] { ans.Root } };
                for (int i = 0; i < head.SelectLeaves.Count; i++)
                {
                    var leaf = head.SelectLeaves[i].Item2;
                    local_ans.Product(leaf, this.query_bindings[leaf].FilterEids(ans.Leaves[i]));
                }
                global_ans.Records.AddRange(local_ans.Records);
            }
            return global_ans;
        }

        private QuerySolutions FinalJoin(List<QuerySolutions> intermediate_results)
        {
            QuerySolutions results = null;
            for (int i = 0; i < intermediate_results.Count; i++)
            {
                if (i == 0)
                {
                    results = intermediate_results[i];
                }
                else
                {
                    results = results.Join(intermediate_results[i]);
                }
            }
            return results;
        }
        #endregion
    }
}
