using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Query
{
    public class xTwigPlusHead
    {
        public string Root { set; get; }

        public List<string> LeavePreds { set; get; }

        // Handling variable preidcates, such as 's ?p ?o'
        public List<string> SelectVarPreds { set; get; }

        // Pred => Variable
        public List<Tuple<string, string>> SelectLeaves { set; get; }

        // Variable => Candidates
        public Dictionary<string, Binding> Bindings { set; get; }

        public string ToBriefString()
        {
            StringBuilder builder = new StringBuilder();
            builder.Append("{");
            builder.Append("Root: " + Root + "; ");
            builder.Append("LeavePreds: " + string.Join(", ", LeavePreds) + "; ");
            builder.Append("SelectLeaves: " +
                string.Join(", ", SelectLeaves.Select(sl => sl.Item1 + "-" + sl.Item2)) + "; ");
            builder.Append("}");
            return builder.ToString();
        }

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            builder.Append("{");
            builder.Append("Root: " + Root + "; ");
            builder.Append("LeavePreds: " + string.Join(", ", LeavePreds) + "; ");
            builder.Append("SelectLeaves: " +
                string.Join(", ", SelectLeaves.Select(sl => sl.Item1 + "-" + sl.Item2)) + "; ");
            builder.Append("Bindings: " +
                string.Join(", ", Bindings.Select(b => b.Key + "=[" + b.Value + "]")));
            builder.Append("}");
            return builder.ToString();
        }
    }

    public class xTwigPlusAnswer
    {
        public xTwigHead Head { set; get; }

        public long Root { set; get; }

        public List<long[]> Leaves { set; get; }

        public List<VarPredLeaf> VarPredLeaves { set; get; }

        public bool IsValid()
        {
            return this.Leaves.All(l => l.Length > 0);
        }
    }

    public class VarPredLeaf
    {
        public long Pred { set; get; }

        public List<long> Leaves { set; get; }
    }
}
