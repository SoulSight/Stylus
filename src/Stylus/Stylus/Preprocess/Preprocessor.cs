using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity.Diagnostics;

using Stylus.Util;
using System.Diagnostics;

namespace Stylus.Preprocess
{
    public class Preprocessor
    {
        public static void PrepareFile(string inputFilename, string targetFilename, char sep, long bufferSize) 
        {
            NTripleUtil.FieldSeparator = sep;
            ILineMerger merger = new BinaryMerger();
            ILineGrouper grouper = new HashLineGrouper();
            string revFilename = inputFilename + ".rev";
            string mergeFilename = inputFilename + ".merge";

            Log.WriteLine(LogLevel.Info, "Generating Reverse Triple File...");
            GenerateReverseTripleFile(inputFilename, revFilename);

            Log.WriteLine(LogLevel.Info, "Merging File...");
            merger.Merge(new string[]{ inputFilename, revFilename }, mergeFilename);
            File.Delete(revFilename);

            Log.WriteLine(LogLevel.Info, "Grouping File...");
            grouper.Group(mergeFilename, targetFilename, "./temp/", bufferSize);
            File.Delete(mergeFilename);

            Log.WriteLine(LogLevel.Info, "Data Prepared.");
        }

        public static void PrepareFile(string inputFilename, string targetFilename, char sep) 
        {
            PerformanceCounter totalAvailableRAMCounter = new PerformanceCounter("Memory", "Available Bytes");
            long bufferSize = totalAvailableRAMCounter.RawValue * 2 / 3;
            PrepareFile(inputFilename, targetFilename, sep, bufferSize);
        }

        public static void GenerateReverseTripleFile(string inputFilename, string reverseFilename) 
        {
            using (StreamWriter writer = new StreamWriter(reverseFilename))
            {
                foreach (var line in File.ReadLines(inputFilename))
                {
                    string[] splits = NTripleUtil.FastSplit(line);
                    if (splits.Length < 3)
                    {
                        continue;
                    }
                    writer.WriteLine(splits[2] + NTripleUtil.FieldSeparator 
                        + "_" + splits[1] + NTripleUtil.FieldSeparator + splits[0]);
                }
            }
        }
    }
}
