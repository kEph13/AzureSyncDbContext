using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncDbContext.Helpers
{
    public static class Reflection
    {
        public static object GetPropValue(this object target, string propName)
        {
            try
            {
                var myType = target.GetType();
                var prop = myType.GetProperty(propName);
                return prop.GetValue(target, null);
            }
            catch (Exception) { }
            return null;
        }
    }
}