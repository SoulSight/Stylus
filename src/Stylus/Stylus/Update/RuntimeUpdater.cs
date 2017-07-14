using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity;

using Stylus.DataModel;
using Stylus.Util;

namespace Stylus.Update
{
    public class RuntimeUpdater
    {
        private static Dictionary<string, ushort> plist_tid_dict;
        private static Dictionary<ushort, long> tid_cur_pos;

        public static void Initialize()
        {
            plist_tid_dict = new Dictionary<string, ushort>();
            tid_cur_pos = new Dictionary<ushort, long>();
            foreach (var kvp in StylusSchema.Tid2Pids)
            {
                string str = string.Join(" ", kvp.Value);
                plist_tid_dict.Add(str, kvp.Key);
            }
        }

        private static ushort GetTid(List<long> pids)
        {
            pids.Sort();
            string plist_str = string.Join(" ", pids);
            ushort tid = StylusConfig.GenericTid;
            plist_tid_dict.TryGetValue(plist_str, out tid);
            return tid;
        }

        /// <summary>
        /// Update an xEntity with adding properties, and return the new entity id
        /// </summary>
        /// <param name="eid">The id of the entity to update</param>
        /// <param name="add_props">Additional properties</param>
        /// <returns>New id of the updated entity</returns>
        public static long UpdateEntity(long eid, Dictionary<long, List<long>> add_props, Dictionary<long, List<long>> remove_props) 
        {
            var xentity = Global.LocalStorage.LoadxEntity(eid);
            Global.LocalStorage.RemoveCell(eid);
            HashSet<long> pids = new HashSet<long>(StylusSchema.Tid2Pids[xentity.Tid]);
            foreach (var prop in add_props)
            {
                pids.Add(prop.Key);
            }
            var new_tid = GetTid(pids.ToList()); // Todo

            if (new_tid == StylusConfig.GenericTid)
            {
                long new_eid = eid; // Todo: need to change the entity id if the ID-Encoding optimization is enabled
                var new_entity = StorageConverter.ConvertToGeneric(xentity, new_eid, add_props, remove_props);
                Global.LocalStorage.SaveGenericPropEntity(new_entity);
                return new_eid;
            }
            else
            {
                long new_eid = eid; // Todo: need to change the entity id if the ID-Encoding optimization is enabled
                var new_entity = StorageConverter.Convert(xentity, new_eid, new_tid, add_props, remove_props);
                Global.LocalStorage.SavexEntity(new_entity);
                return new_eid;
            }
        }

        public static long AddToEntity(long eid, Dictionary<long, List<long>> add_props) 
        {
            return UpdateEntity(eid, add_props, new Dictionary<long,List<long>>());
        }

        public static long RemoveFromEntity(long eid, Dictionary<long, List<long>> remove_props) 
        {
            return UpdateEntity(eid, new Dictionary<long, List<long>>(), remove_props);            
        }

        public static void AddEntity(long eid, Dictionary<long, List<long>> props) 
        {
            var tid = GetTid(props.Select(p => p.Key).ToList());
            if (tid == StylusConfig.GenericTid)
            {
                AddGenericEntity(eid, props);
            }
            else
            {
                AddxUDTEntity(eid, tid, props);
            }
        }

        public static void AddGenericEntity(long eid, Dictionary<long, List<long>> props) 
        {
            List<Property> properties = new List<Property>();
            foreach (var pid_oids in props)
            {
                properties.Add(new Property(pid_oids.Key, pid_oids.Value));
            }

            GenericPropEntity gp_entity = new GenericPropEntity(eid, StylusConfig.GenericTid, properties);
            Global.LocalStorage.SaveGenericPropEntity(gp_entity);
        }

        public static void AddxUDTEntity(long eid, ushort tid, Dictionary<long, List<long>> props) 
        {
            var pids = StylusSchema.Tid2Pids[tid];
            List<int> offsets = new List<int>();
            List<long> obj_vals = new List<long>();

            for (int order = 0; order < pids.Count; order++)
            {
                var pid = pids[order];
                // if the pid exists, add all its associated oids; otherwise, repeat the current obj_vals.size
                if (props.ContainsKey(pid))
                {
                    obj_vals.AddRange(props[pid]);
                }
                offsets.Add(obj_vals.Count);
            }

            xEntity xentity = new xEntity(eid, tid, offsets, obj_vals);
            Global.LocalStorage.SavexEntity(xentity);
        }

        public static void DeleteEntity(long eid) 
        {
            Global.LocalStorage.RemoveCell(eid);
        }
    }
}
