using Stylus.Storage;
using Stylus.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Query
{
    public interface IQueryServer : ITripleServer
    {
        IStorage Storage { set; get; }

        XDictionary<string, long> LiteralToId { set; get; }

        bool ContainsBinding(string variable);

        IEnumerable<long> EnumerateBinding(string variable);

        Binding GetBinding(string variable);

        void SetBinding(string variable, Binding binding);

        void ReplaceBinding(string variable, IEnumerable<long> bindings);

        void ReplaceBinding(string variable, Binding bindings);

        void AppendBinding(string variable, IEnumerable<long> bindings);

        IEnumerable<List<string>> ResolveQuerySolutions(QuerySolutions querySolutions);
    }
}
