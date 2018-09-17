using Mono.Linq.Expressions;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SyncDbContext.Helpers
{
    public static class ExpressionHelper
    {
        public static Expression<Func<T, bool>> CheckItemEquality<T>(string keyName, object value)
        {
            var itemParameter = Expression.Parameter(typeof(T), "item");
            return Expression.Lambda<Func<T, bool>>(Expression.Equal(
                        Expression.Property(
                            itemParameter,
                            keyName
                            ),
                        Expression.Constant(value)
                            ),
                    new[] { itemParameter }
                    );
        }

        public static Expression<Func<T, bool>> CheckItemInequality<T>(string keyName, object value)
        {
            var itemParameter = Expression.Parameter(typeof(T), "item");
            return Expression.Lambda<Func<T, bool>>(Expression.NotEqual(
                        Expression.Property(
                            itemParameter,
                            keyName
                            ),
                        Expression.Constant(value)
                            ),
                    new[] { itemParameter }
                    );
        }

        public static string[] GetKeyNames<T>(DbContext context, T entity) where T : class
        {
            var objectSet = ((IObjectContextAdapter)context).ObjectContext.CreateObjectSet<T>();
            string[] keyNames = objectSet.EntitySet.ElementType.KeyMembers
                                                               .Select(k => k.Name)
                                                               .ToArray();
            return keyNames;
        }

        public static Expression<Func<T, bool>> ExistingPredicate<T>(DbContext context, T entity) where T : class
        {
            Expression<Func<T, bool>> predicate = null;
            var keyNames = GetKeyNames(context, entity);
            Type type = typeof(T);

            object[] keys = new object[keyNames.Length];
            for (int i = 0; i < keyNames.Length; i++)
            {
                var keyName = keyNames[i];
                var value = entity.GetPropValue(keyName);
                if (predicate == null)
                {
                    predicate = ExpressionHelper.CheckItemEquality<T>(keyName, value);

                }
                else
                {
                    predicate = predicate.AndAlso(ExpressionHelper.CheckItemEquality<T>(keyName, value));
                }
            }
            return predicate;
        }
    }
}
