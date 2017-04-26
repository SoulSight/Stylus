﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Util
{
    /// Codes from Trinity.Lib.XHashSet.cs
    public class XHashSet<T> : IEnumerable<T>, IEnumerable
    {
        private HashSet<T>[] HashSetArray;
        private int m;

        public XHashSet(int k = 2)
        {
            this.m = k;
            HashSetArray = new HashSet<T>[m];
            for (int i = 0; i < m; i++)
            {
                HashSetArray[i] = new HashSet<T>();
            }
        }

        public bool Add(T item)
        {
            return HashSetArray[(item.GetHashCode() & 0x7fffffff) % m].Add(item);
        }

        public bool Contains(T item)
        {
            return HashSetArray[(item.GetHashCode() & 0x7fffffff) % m].Contains(item);
        }

        public void Add(ICollection<T> collection)
        {
            foreach (T item in collection)
            {
                HashSetArray[(item.GetHashCode() & 0x7fffffff) % m].Add(item);
            }
        }

        public void Remove(T item)
        {
            HashSetArray[(item.GetHashCode() & 0x7fffffff) % m].Remove(item);
        }

        public void Remove(ICollection<T> collection)
        {
            foreach (T item in collection)
            {
                HashSetArray[(item.GetHashCode() & 0x7fffffff) % m].Remove(item);
            }
        }

        public void Clear()
        {
            for (int i = 0; i < m; i++)
            {
                HashSetArray[i].Clear();
            }
        }

        public Int64 Size()
        {
            Int64 size = 0;
            for (int i = 0; i < m; i++)
            {
                size += HashSetArray[i].Count;
            }
            return size;
        }

        public Int64 Count
        {
            get
            {
                return this.Size();
            }
        }

        internal void PrintInternalInfo()
        {
            for (int i = 0; i < m; i++)
            {
                Console.WriteLine("The Hashset[" + i + "].Count=" + HashSetArray[i].Count);
            }
        }

        public System.Collections.Generic.IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < m; i++)
            {
                foreach (T guid in HashSetArray[i])
                {
                    yield return guid;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
