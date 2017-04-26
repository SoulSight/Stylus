using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;

namespace Stylus.Util
{
    public class ClusterUtil
    {
        // Finish = n + n^2 + n^3 + ... + n^(p+1) = {n^(p + 2) - n} / (n - 1)
        public static int GetFinishProceedingCount(int path_len)
        {
            int server_cnt = Global.CloudStorage.ServerCount;
            int pow = 1;
            for (int i = 0; i <= path_len + 1; i++)
            {
                pow *= server_cnt;
            }
            return (pow - server_cnt) / (server_cnt - 1);
        }

        public static int PowUnsafeInt(int x, int y)
        {
            if (y == 0)
            {
                return 1;
            }
            else if (y == 1)
            {
                return x;
            }
            else if (y % 2 == 0)
            {
                int half = PowUnsafeInt(x, y / 2);
                return half * half;
            }
            else
            {
                int half = PowUnsafeInt(x, (y - 1) / 2);
                return half * half * x;
            }
        }

        public static int GetBelongedServerId(long cell_id)
        {
            return (int)(cell_id % Global.CloudStorage.ServerCount);
        }

        public static bool IsLocalEntity(long id)
        {
            return TrinityConfig.CurrentRunningMode == RunningMode.Embedded ? true :
                (int)(id % TrinityConfig.Servers.Count) == Global.MyServerId;
        }
    }
}
