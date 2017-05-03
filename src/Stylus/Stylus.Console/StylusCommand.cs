using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Console
{
    public class StylusCommand
    {
        public string Name { set; get; }

        private Dictionary<string, string> options { set; get; }

        public List<string> Parameters { set; get; }

        public StylusCommand() : this(string.Empty) { }

        public StylusCommand(string cmdName)
        {
            this.Name = cmdName;
            this.options = new Dictionary<string, string>();
            this.Parameters = new List<string>();
        }

        public void AddOption(string optKey) 
        {
            this.options.Add(optKey, string.Empty);
        }

        public void AddOption(string optKey, string optValue)
        {
            this.options.Add(optKey, optValue);
        }

        public bool HasOption(string optKey) 
        {
            return this.options.ContainsKey(optKey);
        }

        public bool HasOption(string optKey, string optValue)
        {
            string val;
            if (this.options.TryGetValue(optKey, out val))
            {
                return this.options[optKey] == optValue;
            }
            return false;
        }

        public string GetOption(string optKey)
        {
            return this.options[optKey];
        }

        public void AddParam(string parameter) 
        {
            this.Parameters.Add(parameter);
        }

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            builder.Append("StylusCommand: \nname=[" + this.Name + "]\n");
            builder.Append("param=[" + string.Join(", ", this.Parameters) + "]\n");
            builder.Append("opts=[" + string.Join(", ", this.options.Select(kvp => kvp.Key + "=" + kvp.Value)) + "]\n");
            return builder.ToString();
        }
    }

    public class StylusCommandParser 
    {
        public StylusCommand Parse(string line)
        {
            StylusCommand cmd = new StylusCommand();
            var list = Tokenize(line);
            int i = 0;
            while (i < list.Count)
            {
                string cur = list[i];

                if (cur.StartsWith("-"))
                {
                    if (i + 1 < list.Count && !list[i + 1].StartsWith("-"))
                    {
                        cmd.AddOption(cur.TrimStart('-'), list[i + 1]);
                        i += 1;
                    }
                    else
                    {
                        cmd.AddOption(cur.TrimStart('-'));
                    }
                }
                else
                {
                    if (cmd.Name == string.Empty)
                    {
                        cmd.Name = cur;
                    }
                    else
                    {
                        cmd.AddParam(cur);
                    }
                }

                i++;
            }
            return cmd;
        }

        private List<string> Tokenize(string line) 
        {
            List<string> tokens = new List<string>();
            int index = 0;
            while (index < line.Length)
            {
                int next_index;
                string cur = ReadNext(line, index, out next_index);
                index = next_index;
                if (string.IsNullOrWhiteSpace(cur))
                {
                    continue;
                }

                tokens.Add(cur);
            }
            return tokens;
        }

        private string ReadNext(string line, int start, out int index) 
        {
            StringBuilder builder = new StringBuilder();
            char quote = ' ';
            int offset;
            for (offset = start; offset < line.Length; offset++)
            {
                if (builder.Length == 0 && quote == ' ' 
                    && (line[offset] == '\'' || line[offset] == '\"'))
                {
                    quote = line[offset];
                    continue;
                }

                if ((char.IsWhiteSpace(line, offset) && quote == ' ') 
                    || (quote != ' ' && quote == line[offset]))
                {
                    break;
                }
                else
                {
                    builder.Append(line[offset]);
                }
            }
            index = offset + 1;
            return builder.ToString();
        }
    }
}
