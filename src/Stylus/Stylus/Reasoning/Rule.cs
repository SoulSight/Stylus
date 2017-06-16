using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Reasoning
{
    public class RuleHead
    {

    }

    public class RuleBody
    {

    }

    public abstract class Rule
    {
        public RuleHead Head { set; get; }

        public RuleBody Body { set; get; }

        public abstract void Apply();
    }
}
