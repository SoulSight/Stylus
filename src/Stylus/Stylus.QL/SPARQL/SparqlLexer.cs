using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.QL.SPARQL
{
    internal class SparqlLexer
    {
        private string input;
        private int pos;
        private int tokenStart;
        private int tokenEnd;
        private Token putBack;
        private bool hasTokenEnd;

        internal SparqlLexer(string input)
        {
            this.input = input;
            this.pos = 0;
            this.tokenStart = 0;
            this.tokenEnd = 0;
            this.putBack = Token.None;
            this.hasTokenEnd = false;
        }

        internal Token GetNext()
        {
            if (putBack != Token.None)
            {
                Token result = putBack;
                putBack = Token.None;
                return result;
            }

            hasTokenEnd = false;

            while (pos != input.Length - 1)
            {
                tokenStart = pos;
                switch (input[pos++])
                {
                    // Whitespace
                    case ' ':
                    case '\t':
                    case '\n':
                    case '\r':
                    case '\f':
                        continue;
                    // Single line comment
                    case '#':
                        while (pos != input.Length - 1)
                        {
                            if ((input[pos] == '\n') || (input[pos] == '\r'))
                                break;
                            ++pos;
                        }
                        if (pos != input.Length - 1) ++pos;
                        continue;
                    // Simple tokens
                    case ':': return Token.Colon;
                    case ';': return Token.Semicolon;
                    case ',': return Token.Comma;
                    case '.': return Token.Dot;
                    case '_': return Token.Underscore;
                    case '{': return Token.LCurly;
                    case '}': return Token.RCurly;
                    case '(': return Token.LParen;
                    case ')': return Token.RParen;
                    case '@': return Token.At;
                    case '+': return Token.Plus;
                    case '-': return Token.Minus;
                    case '*': return Token.Mul;
                    case '/': return Token.Div;
                    case '=': return Token.Equal;
                    case '!':
                        if ((pos == input.Length - 1) || (input[pos] != '='))
                            return Token.Not;
                        ++pos;
                        return Token.NotEqual;
                    case '[':
                        while (pos != input.Length - 1)
                        {
                            switch (input[pos])
                            {
                                case ' ':
                                case '\t':
                                case '\n':
                                case '\r':
                                case '\f': ++pos; continue;
                            }
                            break;
                        }
                        if ((pos != input.Length - 1) && (input[pos] == ']'))
                        {
                            ++pos;
                            return Token.Anon;
                        }
                        return Token.LBracket;
                    case ']': return Token.RBracket;
                    case '>':
                        if ((pos != input.Length - 1) && (input[pos] == '='))
                        {
                            ++pos;
                            return Token.GreaterOrEqual;
                        }
                        return Token.Greater;
                    case '^':
                        if ((pos == input.Length - 1) || (input[pos] != '^'))
                            return Token.Error;
                        ++pos;
                        return Token.Type;
                    case '|':
                        if ((pos == input.Length - 1) || (input[pos] != '|'))
                            return Token.Error;
                        ++pos;
                        return Token.Or;
                    case '&':
                        if ((pos == input.Length - 1) || (input[pos] != '&'))
                            return Token.Error;
                        ++pos;
                        return Token.And;
                    case '<':
                        tokenStart = pos;
                        // Try to parse as URI
                        for (; pos != input.Length - 1; ++pos)
                        {
                            char c = input[pos];
                            // Escape chars
                            if (c == '\\')
                            {
                                if ((++pos) == input.Length - 1) break;
                                continue;
                            }
                            // Fast tests
                            if ((c >= 'a') && (c <= 'z')) continue;
                            if ((c >= 'A') && (c <= 'Z')) continue;

                            // Test for invalid characters
                            if ((c == '<') || (c == '>') || (c == '\"') || (c == '{') || (c == '}') || (c == '^') || (c == '|') || (c == '`') || ((c & 0xFF) <= 0x20))
                                break;
                        }

                        if ((pos != input.Length - 1) && (input[pos] == '>'))
                        {
                            tokenEnd = pos; hasTokenEnd = true;
                            ++pos;
                            return Token.IRI;
                        }
                        pos = tokenStart;

                        if (((pos + 1) != input.Length - 1) && (input[pos + 1] == '='))
                        {
                            pos++;
                            return Token.LessOrEqual;
                        }
                        return Token.Less;
                    case '\'':
                        tokenStart = pos;
                        while (pos != input.Length - 1)
                        {
                            if (input[pos] == '\\')
                            {
                                ++pos;
                                if (pos != input.Length - 1) ++pos;
                                continue;
                            }
                            if (input[pos] == '\'')
                                break;
                            ++pos;
                        }
                        tokenEnd = pos;
                        hasTokenEnd = true;
                        if (pos != input.Length - 1)
                            ++pos;
                        return Token.String;
                    case '\"':
                        tokenStart = pos;
                        while (pos != input.Length - 1)
                        {
                            if (input[pos] == '\\')
                            {
                                ++pos;
                                if (pos != input.Length - 1) ++pos;
                                continue;
                            }
                            if (input[pos] == '\"')
                                break;
                            ++pos;
                        }
                        tokenEnd = pos; hasTokenEnd = true;
                        if (pos != input.Length - 1) ++pos;
                        return Token.String;
                    case '?':
                    case '$':
                        tokenStart = pos;
                        while (pos != input.Length - 1)
                        {
                            char c = input[pos];
                            if (((c >= '0') && (c <= '9')) || ((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z')))
                            {
                                ++pos;
                            }
                            else
                                break;
                        }
                        tokenEnd = pos;
                        hasTokenEnd = true;
                        return Token.Variable;
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                        while (pos != input.Length - 1)
                        {
                            char c = input[pos];
                            if ((c >= '0') && (c <= '9'))
                            {
                                ++pos;
                            }
                            else
                                break;
                        }
                        tokenEnd = pos;
                        hasTokenEnd = true;
                        return Token.Integer;
                    default:
                        --pos;
                        while (pos != input.Length - 1)
                        {
                            char c = input[pos];
                            if (((c >= '0') && (c <= '9')) || ((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z')))
                            {
                                ++pos;
                            }
                            else break;
                        }
                        if (pos == tokenStart)
                            return Token.Error;
                        return Token.Identifier;
                }
            }
            return Token.Eof;
        }

        internal string GetTokenValue()
        {
            if (hasTokenEnd)
            {
                return input.Substring(tokenStart, tokenEnd - tokenStart);
            }
            else
            {
                return input.Substring(tokenStart, pos - tokenStart);
            }
        }

        internal string GetIRIValue()
        {
            int limit = (hasTokenEnd ? tokenEnd : pos);
            StringBuilder result = new StringBuilder();
            for (int iter = tokenStart; iter != limit; ++iter)
            {
                char c = input[iter];
                if (c == '\\')
                {
                    if ((++iter) == limit)
                    {
                        break;
                    }
                    c = input[iter];
                }
                result.Append(c);
            }
            return result.ToString();
        }

        internal string GetLiteralValue()
        {
            int limit = (hasTokenEnd ? tokenEnd : pos);
            StringBuilder result = new StringBuilder();
            for (int iter = tokenStart; iter != limit; ++iter)
            {
                char c = input[iter];
                if (c == '\\')
                {
                    if ((++iter) == limit)
                    {
                        break;
                    }
                    c = input[iter];
                }
                result.Append(c);
            }
            return result.ToString();
        }

        internal bool IsKeyword(string keyword)
        {
            int limit = (hasTokenEnd ? tokenEnd : pos);
            string snippet = input.Substring(tokenStart, limit - tokenStart).ToLower();
            return keyword.ToLower() == snippet;
        }

        internal void Unget(Token value)
        {
            this.putBack = value;
        }

        internal bool HasNext(Token value)
        {
            Token peek = GetNext();
            Unget(peek);
            return peek == value;
        }

        internal int GetReader()
        {
            return (putBack != Token.None) ? tokenStart : pos;
        }
    }
}
