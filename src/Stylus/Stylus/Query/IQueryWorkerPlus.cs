using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.DataModel;
using Stylus.Parsing;

namespace Stylus.Query
{
    public interface IQueryWorkerPlus : IQueryServer
    {
        List<xTwigPlusHead> PlanPlus(QueryGraph qg);

        List<xTwigPlusAnswer> ExecuteToXTwigAnswerPlus(xTwigPlusHead head);

        QuerySolutions ExecuteFlattenPlus(xTwigPlusHead head);

        QuerySolutions ExecuteSingleTwigPlus(xTwigPlusHead head);

        QuerySolutions ExecutePlus(List<xTwigPlusHead> heads);
    }
}
