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
    public class XPreprocessor
    {
        public static void PrepareFile(string inputFilename, string targetFilename, char sep, long bufferSize = 1L << 32)
        {
            NTripleUtil.FieldSeparator = sep;
            ILineGrouper grouper = new HashLineGrouper();
            string pairedFilename = inputFilename + ".paired";
            string mapFilename = inputFilename + ".map";

            Log.WriteLine(LogLevel.Info, "Generating Compact Paired Triple File...");
            GenerateCompactPairedTripleFile(inputFilename, pairedFilename, mapFilename);

            Log.WriteLine(LogLevel.Info, "Grouping File...");
            grouper.Group(pairedFilename, targetFilename, "./temp/", bufferSize);
            File.Delete(pairedFilename);

            Log.WriteLine(LogLevel.Info, "Data Prepared.");
        }

        public static void GenerateCompactPairedTripleFile(string inputFilename, string pairedFilename, string mapFilename)
        {
            XDictionary<string, long> str2id = new XDictionary<string, long>();
            using (StreamWriter paired_writer = new StreamWriter(pairedFilename))
            using (StreamWriter map_writer = new StreamWriter(mapFilename))
            {
                long number = 0;
                foreach (var line in File.ReadLines(inputFilename))
                {
                    string[] splits = NTripleUtil.FastSplit(line);
                    if (splits.Length < 3)
                    {
                        continue;
                    }

                    string subj = splits[0];
                    string pred = splits[1];
                    string obj = splits[2];
                    long subj_no, pred_no, obj_no;

                    if (!str2id.ContainsKey(subj))
                    {
                        str2id.Add(subj, number++);
                        subj_no = number;
                        map_writer.WriteLine(subj + "\t" + subj_no);
                    }
                    else
                    {
                        subj_no = str2id[subj];
                    }

                    if (!str2id.ContainsKey(pred))
                    {
                        str2id.Add(pred, number++);
                        pred_no = number;
                        map_writer.WriteLine(pred + "\t" + pred_no);
                    }
                    else
                    {
                        pred_no = str2id[pred];
                    }

                    if (!str2id.ContainsKey(obj))
                    {
                        str2id.Add(obj, number++);
                        obj_no = number;
                        map_writer.WriteLine(obj + "\t" + obj_no);
                    }
                    else
                    {
                        obj_no = str2id[obj];
                    }

                    paired_writer.WriteLine(subj_no + NTripleUtil.FieldSeparator
                        + pred_no + NTripleUtil.FieldSeparator + obj_no);
                    paired_writer.WriteLine(obj_no + NTripleUtil.FieldSeparator
                        + "_" + pred_no + NTripleUtil.FieldSeparator + subj_no);
                }
            }
        }
    }
}
