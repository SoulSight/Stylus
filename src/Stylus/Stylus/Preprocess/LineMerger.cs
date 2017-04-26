using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity.Diagnostics;

using Stylus.Util;

namespace Stylus.Preprocess
{
    public interface ILineMerger
    {
        void Merge(string[] srcFilename, string desFilename);
    }

    public class NTripleMerger : ILineMerger
    {
        public void Merge(string[] srcFilename, string desFilename)
        {
            ILineMerger merger;
            if (srcFilename.Length == 2)
            {
                merger = new BinaryMerger();
                merger.Merge(srcFilename, desFilename);
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }

    class BinaryMerger : ILineMerger
    {
        public void Merge(string[] srcFilename, string desFilename)
        {
            if (srcFilename.Length != 2)
            {
                throw new Exception("Too many/less input files.");
            }
            StreamReader left = new StreamReader(srcFilename[0]);
            StreamReader right = new StreamReader(srcFilename[1]);

            StreamWriter des = new StreamWriter(desFilename);

            string ll = left.ReadLine();
            string rl = right.ReadLine();
            long lcount = 0, rcount = 0;

            while (ll != null && rl != null)
            {
                if (ll.CompareTo(rl) > 0)
                {
                    des.WriteLine(rl);
                    rl = right.ReadLine();
                    rcount++;
                }
                else
                {
                    des.WriteLine(ll);
                    ll = left.ReadLine();
                    lcount++;
                }
                if (lcount + rcount % 1000000 == 0)
                {
                    Log.WriteLine(LogLevel.Info, "Left {0} - Right {1}", lcount, rcount);
                }
            }
            // Log.WriteLine(LogLevel.Info, "Left {0} - Right {1}", lcount, rcount);

            if (ll == null)
            {
                Log.WriteLine(LogLevel.Info, "Left finished. ");
                while (rl != null)
                {
                    des.WriteLine(rl);
                    rl = right.ReadLine();
                    rcount++;
                    if (lcount + rcount % 1000000 == 0)
                    {
                        Log.WriteLine(LogLevel.Info, "Left " + lcount + " - Right " + rcount);
                    }
                }
            }
            else
            {
                Log.WriteLine(LogLevel.Info, "Right finished. ");
                while (ll != null)
                {
                    des.WriteLine(ll);
                    ll = left.ReadLine();
                    lcount++;
                    if (lcount + rcount % 1000000 == 0)
                    {
                        Log.WriteLine(LogLevel.Info, "Left " + lcount + " - Right " + rcount);
                    }
                }
            }
            Log.WriteLine(LogLevel.Info, "Left " + lcount + " - Right " + rcount);

            left.Close();
            right.Close();

            des.Close();
        }
    }
}
