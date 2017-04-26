using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Stylus.Util;

namespace Stylus.Preprocess
{
    /// <summary>
    /// Sort lines in the given file. Current implementation is only used for NTriples file.
    /// </summary>
    public interface ILineGrouper
    {
        void Group(string srcFilename, string desFilename, string tempDir, long bufferSize);
    }

    /// <summary>
    /// Just guarantee that lines with a same subject are placed sequentially in the file.
    /// </summary>
    public class HashLineGrouper : ILineGrouper 
    {
        private int hf_index = 0;

        public int HashFieldIndex { set { hf_index = value; } get { return hf_index; } }

        public void Group(string srcFilename, string desFilename, string tempDir, long bufferSize)
        {
            FileInfo srcFile = new FileInfo(srcFilename);
            long file_size = srcFile.Length;
            long bucket_cnt = bufferSize >> 10;

            if (File.Exists(desFilename))
            {
                File.Delete(desFilename);
            }

            if (file_size < bufferSize)
            {
                // in-memory hashing
                List<string>[] buckets = HashGroupFile(srcFilename, bucket_cnt);

                using (StreamWriter writer = new StreamWriter(desFilename))
                {
                    foreach (var bucket in buckets)
                    {
                        foreach (var item in bucket)
                        {
                            writer.WriteLine(item);
                        }
                    }
                }
            }
            else
            {
                // disk & in-memory hashing
                if (!Directory.Exists(tempDir))
                {
                    Directory.CreateDirectory(tempDir);
                }

                long partition_cnt = (file_size * 3) / (bufferSize * 2);
                if (partition_cnt % 2 == 0)
                {
                    partition_cnt = partition_cnt + 1;
                }
                List<string>[] buffers = new List<string>[partition_cnt];
                for (int i = 0; i < partition_cnt; i++)
                {
                    buffers[i] = new List<string>();
                }

                using (StreamReader reader = new StreamReader(srcFilename))
                {
                    string line;
                    long current_buffer_size = 0;
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] splits = NTripleUtil.FastSplit(line);
                        if (splits.Length < hf_index + 1)
                        {
                            continue;
                        }
                        int hashcode = splits[hf_index].GetHashCode();
                        buffers[Math.Abs(hashcode % partition_cnt)].Add(line);
                        current_buffer_size += line.Length * 2;

                        if (current_buffer_size >= bufferSize)
                        {
                            for (int i = 0; i < partition_cnt; i++)
                            {
                                File.AppendAllLines(tempDir + i + ".part", buffers[i]);
                                buffers[i].Clear();
                            }
                            current_buffer_size = 0;
                        }
                    }

                    for (int i = 0; i < partition_cnt; i++)
                    {
                        File.AppendAllLines(tempDir + i + ".part", buffers[i]);
                        buffers[i].Clear();
                    }
                }

                for (int i = 0; i < partition_cnt; i++)
                {
                    List<string>[] buckets = HashGroupFile(tempDir + i + ".part", bucket_cnt);
                    File.AppendAllLines(desFilename, buckets.SelectMany(b => b));
                }

                if (Directory.Exists(tempDir))
                {
                    Directory.Delete(tempDir, true);
                }
            }
        }

        private List<string>[] HashGroupFile(string srcFilename, long bucket_cnt) 
        {
            List<string>[] buckets = new List<string>[bucket_cnt];
            for (int i = 0; i < bucket_cnt; i++)
            {
                buckets[i] = new List<string>();
            }
            using (StreamReader reader = new StreamReader(srcFilename))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] splits = NTripleUtil.FastSplit(line);
                    if (splits.Length < hf_index + 1)
                    {
                        continue;
                    }
                    int hashcode = splits[hf_index].GetHashCode();
                    buckets[Math.Abs(hashcode % bucket_cnt)].Add(line);
                }
                //foreach (var bucket in buckets)
                //{
                //    bucket.Sort();
                //}
                for (int i = 0; i < bucket_cnt; i++)
                {
                    buckets[i].Sort();
                    buckets[i] = buckets[i].Distinct().ToList();
                }
            }
            return buckets;
        }
    }
}
