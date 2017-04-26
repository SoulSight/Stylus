using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Util
{
    public class NTripleUtil
    {
        private static char field_separator = ' ';

        public static char FieldSeparator { set { field_separator = value; } get { return field_separator; } }

        public static string[] FastSplitOld(string line)
        {
            List<string> result = new List<string>();
            StringBuilder builder = new StringBuilder();
            bool inQuate = false;
            bool inPara = false;
            for (int i = 0; i < line.Length; i++)
            {
                if (line[i] == FieldSeparator)
                {
                    if (!inPara && !inQuate)
                    {
                        result.Add(builder.ToString());
                        builder.Clear();
                    }
                    else
                    {
                        builder.Append(line[i]);
                    }
                }
                else
                {
                    builder.Append(line[i]);
                }

                if (line[i] == '<')
                {
                    inPara = true;
                }

                if (line[i] == '>')
                {
                    inPara = false;
                }

                if (line[i] == '"')
                {
                    if (inPara)
                    {
                        inQuate = false;
                    }
                    else
                    {
                        inQuate = !inQuate;
                    }
                }
            }
            result.Add(builder.ToString());
            return result.ToArray();
        }

        public static string[] FastSplit(string line)
        {
            List<string> result = new List<string>();
            StringBuilder builder = new StringBuilder();

            char indicator = field_separator; // < or "
            bool complete = true;
            for (int i = 0; i < line.Length; i++)
            {
                if (line[i] == field_separator)
                {
                    if (complete)
                    {
                        result.Add(builder.ToString());
                        builder.Clear();
                        indicator = field_separator;
                    }
                    else
                    {
                        builder.Append(line[i]);
                    }
                }
                else
                {
                    if ((line[i] == '"' && indicator == '"') || (line[i] == '>' && indicator == '<'))
                    {
                        complete = true;
                    }
                    if (indicator == field_separator && (line[i] == '"' || line[i] == '<'))
                    {
                        indicator = line[i];
                        complete = false;
                    }
                    builder.Append(line[i]);
                }
            }
            result.Add(builder.ToString());
            return result.ToArray();
        }
    }
}
