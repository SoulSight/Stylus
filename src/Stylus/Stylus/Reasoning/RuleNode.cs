using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Reasoning
{
    public class RuleNode { }

    public class RuleLiteralNode : RuleNode { }

    public class RuleVariableNode : RuleNode { }

    public class RuleExpr : RuleNode { }

    public class RuleTree : RuleNode { }
}
