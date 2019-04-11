using SyncDbContext.Attributes;
using SyncDbContext.Exceptions;
using SyncDbContext.Models;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.Mapping;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SyncDbContext.Helpers
{
    public static partial class EFExtensions
    {
        /// <summary>
        /// Do an InsertWhereNotExists operation. Note that this executes immediately - savechanges is not necessary
        /// </summary>
        public static async Task<int> InsertWhereNotExists<TEntity>(this DbContext context, TEntity entity, UpsertModel<TEntity> model) where TEntity : class
        {

            var list = new List<TEntity>()
                {
                    entity
                };

            return await new InsertWhereNotExistsOp<TEntity>(context, list, model).Execute();
        }

        public static async Task<int> InsertWhereNotExists<TEntity>(this DbContext context, List<TEntity> entities, UpsertModel<TEntity> model) where TEntity : class
        {
            var propCount = model.PropertyNames.Count;
            if ((propCount * entities.Count) > 2000)
            {
                throw new TooManyItemsException();
             
            }
            else
            {
                return await new InsertWhereNotExistsOp<TEntity>(context, entities, model).Execute();
            }
        }
    }

    public class InsertWhereNotExistsOp<TEntity> : EntityOp<TEntity, int> where TEntity : class
    {
        public InsertWhereNotExistsOp(DbContext context, IEnumerable<TEntity> entityList, UpsertModel<TEntity> model) : base(context, entityList, model)
        {
        }

        public async override Task<int> Execute()
        {

            StringBuilder sql = new StringBuilder("insert into " + _tableName + " (values ");
            int nextIndex = 0;
            var valueList = new List<object>(_propNames.Count * _entityList.Count());
            var propInfos = _propNames.Keys.Select(k => typeof(TEntity).GetProperty(k)).ToList();
            foreach (var entity in _entityList)
            {
                sql.Append('(' + string.Join(",", Enumerable.Range(nextIndex, _propNames.Count)
                    .Select(r => "@p" + r.ToString())) + "),");
                nextIndex += _propNames.Count;
                var toAdd = new List<object>();
                foreach (var info in propInfos)
                {
                    var value = info.GetValue(entity);
                    if (value == null)
                    {
                        //Handle types that dbnull doesn't work for
                        var type = info.PropertyType;
                        if (type == typeof(byte[]))
                        {
                            toAdd.Add(SqlBinary.Null);
                        }
                        else
                        {
                            toAdd.Add(DBNull.Value);
                        }
                    }
                    else
                    {
                        toAdd.Add(value);
                    }
                }
                valueList.AddRange(toAdd);
                //valueList.AddRange(propInfos.Select(pi => pi.GetValue(entity, null) ?? DBNull.Value ));
            }
            sql.Length -= 1;//remove last comma
            sql.Append(") as S (");
            sql.Append(string.Join(",", _propNames.Values));
            sql.Append(") ");

            sql.Append("on (");
            sql.Append(string.Join(" and ", MatchPropertyNames.Select(kn => "T." + kn + "=S." + kn)));
            sql.Append(") when matched then update set ");
            sql.Append(string.Join(",", from p in _propNames
                                        where !_entityPrimaryKeyNames.Contains(p.Key)
                                        select "T." + p.Value + "=S." + p.Value));

            var insertables = (from p in _propNames
                               where !_storeGeneratedPrimaryKeyNames.Contains(p.Key)
                               select p.Value).ToList();
            sql.Append(" when not matched then insert (");
            sql.Append(string.Join(",", insertables));
            sql.Append(") values (S.");
            sql.Append(string.Join(",S.", insertables));
            sql.Append(");");
            var command = sql.ToString();
            var result = await _context.Database.ExecuteSqlCommandAsync(command, valueList.ToArray());
            return result;
        }
    }
}