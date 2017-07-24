using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.Parsing;
using Stylus.Storage;
using Stylus.DataModel;
using Stylus.Util;

namespace Stylus.Query
{
    public interface IQueryWorker : IQueryServer
    {
        List<xTwigHead> Plan(QueryGraph qg);

        List<xTwigAnswer> ExecuteToXTwigAnswer(xTwigHead head);

        TwigAnswers ExecuteToTwigAnswer(xTwigHead head);

        QuerySolutions ExecuteFlatten(xTwigHead head);

        QuerySolutions ExecuteSingleTwig(xTwigHead head);

        QuerySolutions Execute(List<xTwigHead> heads);
    }
}
