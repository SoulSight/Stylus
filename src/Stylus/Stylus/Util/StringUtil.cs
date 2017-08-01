using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Util
{
    public class StringUtil
    {
        public static bool IsVar(string pred) 
        {
            return pred.StartsWith("?") || pred.StartsWith("_?")
                || pred.StartsWith("$") || pred.StartsWith("_$");
        }
    }
}
