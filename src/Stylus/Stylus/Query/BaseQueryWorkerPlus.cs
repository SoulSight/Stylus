using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity;

using Stylus.Storage;
using Stylus.Util;
using Stylus.DataModel;
using Stylus.Parsing;

namespace Stylus.Query
{
    public abstract class BaseQueryWorkerPlus : BaseQueryServer, IQueryWorkerPlus
    {
        public BaseQueryWorkerPlus() : base() { }

        #region IQueryWorkerPlus
        public List<xTwigPlusHead> PlanPlus(QueryGraph qg)
        {
            throw new NotImplementedException();
        }

        public abstract List<xTwigPlusAnswer> ExecuteToXTwigAnswerPlus(xTwigPlusHead head);

        public abstract QuerySolutions ExecuteFlattenPlus(xTwigPlusHead head);

        public abstract QuerySolutions ExecuteSingleTwigPlus(xTwigPlusHead head);

        public abstract QuerySolutions ExecutePlus(List<xTwigPlusHead> heads);
        #endregion
    }
}
