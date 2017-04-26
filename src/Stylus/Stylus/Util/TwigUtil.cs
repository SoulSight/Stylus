using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.DataModel;

namespace Stylus.Util
{
    public class TwigUtil
    {
        public static long CalcActualSize(List<TwigAnswer> answers)
        {
            long total_size = 0;
            foreach (var ta in answers)
            {
                long ta_cnt = 1;
                foreach (var leaf in ta.LeaveValues)
                {
                    ta_cnt *= leaf.Count;
                }
                total_size += ta_cnt;
            }
            return total_size;
        }

        public static long CountElementSize(List<TwigAnswer> answers)
        {
            long total_size = 0;
            foreach (var ta in answers)
            {
                total_size += 1; // root
                foreach (var leaf in ta.LeaveValues)
                {
                    total_size += leaf.Count;
                }
            }
            return total_size;
        }
    }
}
