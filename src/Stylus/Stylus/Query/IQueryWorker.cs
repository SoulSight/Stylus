using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.Parsing;
using Stylus.Storage;
using Stylus.DataModel;

namespace Stylus.Query
{
    public interface IQueryWorker
    {
        IStorage Storage { set; get; }

        Dictionary<string, long> LiteralToId { set; get; }

        List<xTwigHead> Plan(QueryGraph qg);

        bool ContainsBinding(string variable);

        IEnumerable<long> EnumerateBinding(string variable);

        Binding GetBinding(string variable);

        void SetBinding(string variable, Binding binding);

        void ReplaceBinding(string variable, IEnumerable<long> bindings);

        void ReplaceBinding(string variable, Binding bindings);

        void AppendBinding(string variable, IEnumerable<long> bindings);

        List<xTwigAnswer> ExecuteToXTwigAnswer(xTwigHead head);

        TwigAnswers ExecuteToTwigAnswer(xTwigHead head);

        QuerySolutions ExecuteFlatten(xTwigHead head);

        QuerySolutions ExecuteSingleTwig(xTwigHead head);

        QuerySolutions Execute(List<xTwigHead> heads);
    }
}
