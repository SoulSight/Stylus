using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Util
{
    public static class ThrowUtil
    {
        public static void ThrowIf(bool expr, string msg = "") 
        {
            if (expr)
            {
                throw new Exception(msg);
            }
        }

        public static void ThrowIfNot(bool expr, string msg = "") 
        {
            ThrowIf(!expr, msg);
        }
    }
}
