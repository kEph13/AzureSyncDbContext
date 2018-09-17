using SyncDbContext.Attributes;
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
    public static class EFExtensions
    {


        /// <summary>
        /// Do an upsert operation. Note that this executes immediately - savechanges is not necessary
        /// </summary>
        public static async Task<TEntity> Upsert<TEntity>(this DbContext context, TEntity entity) where TEntity : class
        {

            var list = new List<TEntity>()
                {
                    entity
                };

            await new UpsertOp<TEntity>(context, list).Execute();

            context.Entry(entity).State = EntityState.Unchanged;

            return entity;
        }

        public static async Task<int> Upsert<TEntity>(this DbContext context, List<TEntity> entities) where TEntity : class
        {
            return await new UpsertOp<TEntity>(context, entities).Execute();
        }
    }

    public abstract class EntityOp<TEntity, TRet> where TEntity : class
    {
        public readonly DbContext _context;
        public readonly IEnumerable<TEntity> _entityList;
        protected readonly string _tableName;
        protected readonly string[] _entityPrimaryKeyNames;
        protected readonly string[] _storeGeneratedPrimaryKeyNames;
        protected readonly Dictionary<string, string> _propNames;

        protected List<string> _matchPropertyNames;

        public IEnumerable<string> MatchPropertyNames => (IEnumerable<string>)_matchPropertyNames ?? _entityPrimaryKeyNames;
        //private readonly List<string> _excludeProperties = new List<string>();

        private static string GetMemberName<T>(Expression<Func<TEntity, T>> selectMemberLambda)
        {
            var member = selectMemberLambda.Body as MemberExpression;
            if (member == null)
            {
                throw new ArgumentException("The parameter selectMemberLambda must be a member accessing labda such as x => x.Id", "selectMemberLambda");
            }
            return member.Member.Name;
        }

        public EntityOp(DbContext context, IEnumerable<TEntity> entityList) 
        {
            _context = context;
            _entityList = entityList;

            var mapping = GetEntitySetMapping(typeof(TEntity), context);
            // Get the name of the primary key for the table as we wish to exclude this from the column mapping (we are assuming Identity insert is OFF)
            //https://romiller.com/2015/08/05/ef6-1-get-mapping-between-properties-and-columns/
            _propNames = mapping
                .EntityTypeMappings.Single()
                .Fragments.Single()
                .PropertyMappings
                .OfType<ScalarPropertyMapping>()
                .ToDictionary(m => m.Property.Name, m => '[' + m.Column.Name + ']');

            //_propNames = mapping.EntitySet.ElementType.DeclaredProperties
            //    .ToDictionary(p => p.ToString(), p=>'[' + p.Name + ']');

            var keyNames = mapping.EntitySet.ElementType.KeyMembers
                .ToLookup(k => k.IsStoreGeneratedIdentity, k => k.Name);

            _entityPrimaryKeyNames = keyNames.SelectMany(k => k).ToArray();

            if (_propNames.HasSameKeys(_entityPrimaryKeyNames))
            {
                throw new Exception("Can't upsert entity that has only primary keys");
            }

            var syncColumn = typeof(TEntity).GetProperties().Where(prop => Attribute.IsDefined(prop, typeof(SyncColumnAttribute))).First().Name;

            //Don't need the sync column on the targets
            _propNames.Remove(syncColumn);

            _storeGeneratedPrimaryKeyNames = keyNames[true].ToArray();

            // Find the storage entity set (table) that the entity is mapped
            var table = mapping
                .EntityTypeMappings.Single()
                .Fragments.Single()
                .StoreEntitySet;

            // Return the table name from the storage entity set
            _tableName = (string)table.MetadataProperties["Table"].Value ?? table.Name;
            var schemaName = (string)table.MetadataProperties["Schema"].Value ?? table.Schema;
            _tableName = $"[{schemaName}].[{_tableName}]";
        }

        public abstract Task<TRet> Execute();

        public EntityOp<TEntity, TRet> Key<TKey>(Expression<Func<TEntity, TKey>> selectKey)
        {
            (_matchPropertyNames ?? (_matchPropertyNames = new List<string>())).Add(GetMemberName(selectKey));
            return this;
        }

        public EntityOp<TEntity, TRet> ExcludeField<TField>(Expression<Func<TEntity, TField>> selectField)
        {
            _propNames.Remove(GetMemberName(selectField));
            return this;
        }
        
        private static IEnumerable<string> GetKeyNames(Type type, DbContext context)
        {
            ObjectContext objectContext = ((IObjectContextAdapter)context).ObjectContext;
            ObjectSet<TEntity> set = objectContext.CreateObjectSet<TEntity>();
            IEnumerable<string> keyNames = set.EntitySet.ElementType
                                                        .KeyMembers
                                                        .Select(k => k.Name);

            return keyNames;
        }

        //Only works for database first
        private static EntitySetMapping GetEntitySetMapping(Type type, DbContext context)
        {
            var objContext = ((IObjectContextAdapter)context).ObjectContext;
            var metadata = objContext.MetadataWorkspace;

            // Get the part of the model that contains info about the actual CLR types
            var objectItemCollection = ((ObjectItemCollection)metadata.GetItemCollection(DataSpace.OSpace));

            var entityItems = metadata.GetItems<EntityType>(DataSpace.OSpace);

            // Get the entity type from the model that maps to the CLR type
            var entityType = entityItems
                    .Single(e => objectItemCollection.GetClrType(e) == type);

            // Get the entity set that uses this entity type
            var entitySet = metadata
                .GetItems<EntityContainer>(DataSpace.CSpace)
                .Single()
                .EntitySets
                .Single(s => s.ElementType.Name == entityType.Name);

            // Find the mapping between conceptual and storage model for this entity set
            return metadata.GetItems<EntityContainerMapping>(DataSpace.CSSpace)
                    .Single()
                    .EntitySetMappings
                    .Single(s => s.EntitySet == entitySet);

        }
    }

    public class UpsertOp<TEntity> : EntityOp<TEntity, int> where TEntity : class
    {

        public UpsertOp(DbContext context, IEnumerable<TEntity> entityList) : base(context, entityList)
        { }

        public async override Task<int> Execute()
        {

            StringBuilder sql = new StringBuilder("merge into " + _tableName + " as T using (values ");
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
