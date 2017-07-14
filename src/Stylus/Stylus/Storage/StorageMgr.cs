using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.DataModel;
using Trinity;

namespace Stylus.Storage
{
    public class StorageMgr
    {
        public static xEntity Convert(xEntity record, long new_eid, ushort new_tid) 
        {
            return Convert(record, new_eid, new_tid, new Dictionary<long, List<long>>(), new Dictionary<long,List<long>>());
        }

        public static xEntity Convert(xEntity record, long new_eid, ushort new_tid, 
            Dictionary<long, List<long>> add_pos, Dictionary<long, List<long>> remove_pos)
        {
            var old_pids = StylusSchema.Tid2Pids[record.Tid];
            var new_pids = StylusSchema.Tid2Pids[new_tid];
            List<int> offsets = new List<int>();
            List<long> obj_vals = new List<long>();

            Dictionary<long, List<long>> pid_oids_dict = new Dictionary<long, List<long>>();

            for (int offsetIndex = 0; offsetIndex < old_pids.Count; offsetIndex++)
            {
                int start = offsetIndex == 0 ? 0 : record.Offsets[offsetIndex - 1];
                int end = record.Offsets[offsetIndex];
                int count = end - start;
                var oids = new List<long>(count);
                for (int i = 0; i < count; i++)
                {
                    oids.Add(record.ObjVals[i + start]);
                }
                pid_oids_dict.Add(old_pids[offsetIndex], oids);
            }

            foreach (var kvp in add_pos)
            {
                var pid = kvp.Key;
                var oids = kvp.Value;
                if (pid_oids_dict.ContainsKey(pid))
                {
                    pid_oids_dict[pid].AddRange(oids);
                }
                else
                {
                    pid_oids_dict.Add(pid, oids);
                }
            }

            foreach (var kvp in remove_pos)
            {
                var pid = kvp.Key;
                if (pid_oids_dict.ContainsKey(pid))
                {
                    var oids = new HashSet<long>(kvp.Value);
                    pid_oids_dict[pid].RemoveAll(oid => oids.Contains(oid));
                }
            }

            for (int order = 0; order < new_pids.Count; order++)
            {
                var pid = new_pids[order];
                // if the pid exists, add all its associated oids; otherwise, repeat the current obj_vals.size
                if (pid_oids_dict.ContainsKey(pid))
                {
                    obj_vals.AddRange(pid_oids_dict[pid]);
                }
                offsets.Add(obj_vals.Count);
            }

            xEntity xentity = new xEntity(new_eid, new_tid, offsets, obj_vals);
            return xentity;
        }

        public static xEntity Convert(GenericPropEntity record, long new_eid, ushort new_tid)
        {
            Dictionary<long, List<long>> pid_oids_dict = new Dictionary<long, List<long>>();
            foreach (var prop in record.Props)
            {
                pid_oids_dict.Add(prop.Name, (List<long>)prop.Values);
            }

            var new_pids = StylusSchema.Tid2Pids[new_tid];

            List<int> offsets = new List<int>();
            List<long> obj_vals = new List<long>();
            for (int order = 0; order < new_pids.Count; order++)
            {
                var pid = new_pids[order];
                // if the pid exists, add all its associated oids; otherwise, repeat the current obj_vals.size
                if (pid_oids_dict.ContainsKey(pid))
                {
                    obj_vals.AddRange(pid_oids_dict[pid]);
                }
                offsets.Add(obj_vals.Count);
            }

            xEntity xentity = new xEntity(new_eid, new_tid, offsets, obj_vals);
            return xentity;
        }

        public static xEntity ConvertFromGeneric(long eid, long new_eid, ushort new_tid) 
        {
            using (var cell = Global.LocalStorage.UseGenericPropEntity(eid))
            {
                Dictionary<long, List<long>> pid_oids_dict = new Dictionary<long, List<long>>();
                foreach (var prop in (List<Property>)cell.Props)
                {
                    pid_oids_dict.Add(prop.Name, (List<long>)prop.Values);
                }

                var new_pids = StylusSchema.Tid2Pids[new_tid];

                List<int> offsets = new List<int>();
                List<long> obj_vals = new List<long>();
                for (int order = 0; order < new_pids.Count; order++)
                {
                    var pid = new_pids[order];
                    // if the pid exists, add all its associated oids; otherwise, repeat the current obj_vals.size
                    if (pid_oids_dict.ContainsKey(pid))
                    {
                        obj_vals.AddRange(pid_oids_dict[pid]);
                    }
                    offsets.Add(obj_vals.Count);
                }

                xEntity xentity = new xEntity(new_eid, new_tid, offsets, obj_vals);
                return xentity;
            }
        }

        public static GenericPropEntity ConvertToGeneric(xEntity record, long new_eid)
        {
            return ConvertToGeneric(record, new_eid, new Dictionary<long,List<long>>(), new Dictionary<long,List<long>>());
        }

        public static GenericPropEntity ConvertToGeneric(xEntity record, long new_eid,
            Dictionary<long, List<long>> add_pos, Dictionary<long, List<long>> remove_pos)
        {
            var old_pids = StylusSchema.Tid2Pids[record.Tid];

            Dictionary<long, List<long>> pid_oids_dict = new Dictionary<long, List<long>>();

            for (int offsetIndex = 0; offsetIndex < old_pids.Count; offsetIndex++)
            {
                int start = offsetIndex == 0 ? 0 : record.Offsets[offsetIndex - 1];
                int end = record.Offsets[offsetIndex];
                int count = end - start;
                var oids = new List<long>(count);
                for (int i = 0; i < count; i++)
                {
                    oids.Add(record.ObjVals[i + start]);
                }
                pid_oids_dict.Add(old_pids[offsetIndex], oids);
            }

            foreach (var kvp in add_pos)
            {
                var pid = kvp.Key;
                var oids = kvp.Value;
                if (pid_oids_dict.ContainsKey(pid))
                {
                    pid_oids_dict[pid].AddRange(oids);
                }
                else
                {
                    pid_oids_dict.Add(pid, oids);
                }
            }

            foreach (var kvp in remove_pos)
            {
                var pid = kvp.Key;
                if (pid_oids_dict.ContainsKey(pid))
                {
                    var oids = new HashSet<long>(kvp.Value);
                    pid_oids_dict[pid].RemoveAll(oid => oids.Contains(oid));
                }
            }

            List<Property> properties = new List<Property>();
            foreach (var pid_oids in pid_oids_dict)
            {
                properties.Add(new Property(pid_oids.Key, pid_oids.Value));
            }

            GenericPropEntity gp_entity = new GenericPropEntity(new_eid, StylusConfig.GenericTid, properties);
            return gp_entity;
        }

        public static GenericPropEntity ConvertToGeneric(long eid, long new_eid) 
        {
            return ConvertToGeneric(eid, new_eid, 
                new Dictionary<long,List<long>>(), new Dictionary<long,List<long>>());
        }

        public static GenericPropEntity ConvertToGeneric(long eid, long new_eid,
            Dictionary<long, List<long>> add_pos, Dictionary<long, List<long>> remove_pos)
        {
            using (var cell = Global.LocalStorage.UsexEntity(eid))
            {
                var old_pids = StylusSchema.Tid2Pids[cell.Tid];

                Dictionary<long, List<long>> pid_oids_dict = new Dictionary<long, List<long>>();

                for (int offsetIndex = 0; offsetIndex < old_pids.Count; offsetIndex++)
                {
                    int start = offsetIndex == 0 ? 0 : cell.Offsets[offsetIndex - 1];
                    int end = cell.Offsets[offsetIndex];
                    int count = end - start;
                    var oids = new List<long>(count);
                    for (int i = 0; i < count; i++)
                    {
                        oids.Add(cell.ObjVals[i + start]);
                    }
                    pid_oids_dict.Add(old_pids[offsetIndex], oids);
                }

                foreach (var kvp in add_pos)
                {
                    var pid = kvp.Key;
                    var oids = kvp.Value;
                    if (pid_oids_dict.ContainsKey(pid))
                    {
                        pid_oids_dict[pid].AddRange(oids);
                    }
                    else
                    {
                        pid_oids_dict.Add(pid, oids);
                    }
                }

                foreach (var kvp in remove_pos)
                {
                    var pid = kvp.Key;
                    if (pid_oids_dict.ContainsKey(pid))
                    {
                        var oids = new HashSet<long>(kvp.Value);
                        pid_oids_dict[pid].RemoveAll(oid => oids.Contains(oid));
                    }
                }

                List<Property> properties = new List<Property>();
                foreach (var pid_oids in pid_oids_dict)
                {
                    properties.Add(new Property(pid_oids.Key, pid_oids.Value));
                }

                GenericPropEntity gp_entity = new GenericPropEntity(new_eid, StylusConfig.GenericTid, properties);
                return gp_entity;
            }
        }
    }
}
