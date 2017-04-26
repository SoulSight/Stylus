using Stylus.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Console.Backup
{
    public class CleanToNTriples
    {
        private static Dictionary<string, string> prefix_mapping = new Dictionary<string, string>() {
            { "owl", "http://www.w3.org/2002/07/owl#" },
            { "rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#" },
            { "rdfs", "http://www.w3.org/2000/01/rdf-schema#" },
            { "skos", "http://www.w3.org/2004/02/skos/core#" },
            { "y", "http://mpii.de/yago/resource/"},
            { "x", "http://www.w3.org/2001/XMLSchema#" }
        };
        private static string yago1_prefix = @"http://mpii.de/yago/resource/";
        private static string yago2_prefix = @"http://mpii.de/yago/resource/";

        public static void ConvertYago2(string tsvDir, string ntFilename,
            int subjIndex, int predIndex, int objIndex, char sep = ' ')
        {
            NTripleUtil.FieldSeparator = '\t';
            StreamWriter nt_writer = new StreamWriter(ntFilename);
            foreach (var tsv_file in Directory.EnumerateFiles(tsvDir))
            {
                if (!tsv_file.EndsWith(".tsv"))
                {
                    continue;
                }
                long cnt = 0;
                System.Console.WriteLine("Converting: " + tsv_file);
                foreach (var line in File.ReadLines(tsv_file))
                {
                    if (++cnt % 1000000 == 0)
                    {
                        System.Console.WriteLine(cnt);
                    }
                    string[] splits = NTripleUtil.FastSplit(line);
                    if (splits.Length < 3)
                    {
                        System.Console.WriteLine("Skip: " + line);
                    }
                    string subj = Resolve(splits[subjIndex].Trim(), ToYago2);
                    string pred = Resolve(splits[predIndex].Trim(), ToYago2);
                    string obj = Resolve(splits[objIndex].Trim(), ToYago2);
                    nt_writer.WriteLine(subj + ' ' + pred + ' ' + obj + " .");
                }
                System.Console.WriteLine(cnt);
            }
            nt_writer.Close();
        }

        public static void ConvertYago1(string n3Filename, string resultFilename)
        {
            NTripleUtil.FieldSeparator = ' ';
            StreamWriter nt_writer = new StreamWriter(resultFilename);

            long cnt = 0;
            System.Console.WriteLine("Converting: " + n3Filename);
            foreach (var line in File.ReadLines(n3Filename))
            {
                if (++cnt % 1000000 == 0)
                {
                    System.Console.WriteLine(cnt);
                }
                if (line.StartsWith("@prefix"))
                {
                    //prefix_mapping.Add();
                    continue;
                }

                string[] splits = NTripleUtil.FastSplit(line);
                if (splits.Length < 3)
                {
                    System.Console.WriteLine("Skip: " + line);
                }
                string subj = Resolve(splits[0].Trim(), ToYago1);
                string pred = Resolve(splits[1].Trim(), ToYago1);
                string obj = Resolve(splits[2].Trim(), ToYago1);
                nt_writer.WriteLine(subj + " " + pred + " " + obj + " .");
            }
            System.Console.WriteLine(cnt);
            nt_writer.Close();
        }

        public static void ConvertWatDiv(string n3Filename, string resultFilename)
        {
            StreamWriter nt_writer = new StreamWriter(resultFilename);

            long cnt = 0;
            System.Console.WriteLine("Converting: " + n3Filename);
            foreach (var line in File.ReadLines(n3Filename))
            {
                if (++cnt % 1000000 == 0)
                {
                    System.Console.WriteLine(cnt);
                }

                nt_writer.WriteLine(line.Replace("\t", " "));
            }
            System.Console.WriteLine(cnt);
            nt_writer.Close();
        }

        private static string ToYago1(string str)
        {
            if (str.StartsWith("<") && str.EndsWith(">"))
            {
                str = str.Substring(1, str.Length - 2);
                return "<" + yago1_prefix + str + ">";
            }
            else if (str.StartsWith("\"") && str.EndsWith("\""))
            {
                return str;
            }
            else
            {
                return "<" + yago1_prefix + str + ">";
            }
        }

        private static string ToYago2(string str)
        {
            if (str.StartsWith("<") && str.EndsWith(">"))
            {
                str = str.Substring(1, str.Length - 2);
                return "<" + Format(yago2_prefix + str) + ">";
            }
            else if (str.StartsWith("\"") && str.EndsWith("\""))
            {
                return str;
            }
            else
            {
                return "<" + Format(yago2_prefix + str) + ">";
            }
        }

        private static string Resolve(string str, Func<string, string> trans_func)
        {
            if (str.Contains("http:"))
            {
                return str;
            }

            int index = str.IndexOf(':');
            if (index < 0)
            {
                return trans_func(str);
            }

            // QName
            string prefix = str.Substring(0, index).Trim();
            if (prefix_mapping.ContainsKey(prefix))
            {
                string prefix_str = prefix_mapping[prefix];
                return "<" + Format(prefix_str + str.Substring(index + 1)) + ">";
            }
            else
            {
                return trans_func(str);
            }
        }

        private static string Format(string str)
        {
            return Uri.EscapeUriString(str);
        }
    }

    public class FileCleaner
    {
        public static void ConvertYago2Dataset(string yago2Dir, string ntFilename)
        {
            //CleanToNTriples.ConvertYago2(yago2Dir, ntFilename, 1, 2, 3);
            CleanToNTriples.ConvertYago2(yago2Dir, ntFilename, 1, 2, 3, '\t');
        }

        public static void ConvertYago1Dataset(string yago1Filename, string resultFilename)
        {
            CleanToNTriples.ConvertYago1(yago1Filename, resultFilename);
        }

        public static void ConvertWatDivDataset(string wat_div_filename, string result_filename)
        {
            CleanToNTriples.ConvertWatDiv(wat_div_filename, result_filename);
        }
    }
}
