﻿using Mono.Linq.Expressions;
using SyncDbContext.Attributes;
using SyncDbContext.Helpers;
using SyncDbContext.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SyncDbContext
{
    public class SyncDbContext : DbContext
    {
        private readonly bool isTarget;
        private readonly List<string> targetConnectionStrings;
        private readonly long completeStatusValue;
        private readonly ConcurrentDictionary<object, long> flagStatuses;
        private readonly List<ISyncModel> models = new List<ISyncModel>();

        public SyncDbContext(string sourceConnectionString, List<string> targetConnectionStrings) : base(sourceConnectionString)
        {
            isTarget = false;
            this.targetConnectionStrings = targetConnectionStrings;

            //this represents all targets sync'd - i.e. 1111 for 4 targets
            completeStatusValue = (2 ^ targetConnectionStrings.Count()) - 1;
            flagStatuses = new ConcurrentDictionary<object, long>();
        }

        private SyncDbContext(string targetConnectionString) : base(targetConnectionString)
        {
            isTarget = true;
        }

        public void AddToSyncList<T>() where T : class
        {
            var model = new SyncModel<T>();
            var typeName = typeof(T).Name;

            var syncColumn = typeof(T).GetProperties().Where(prop => Attribute.IsDefined(prop, typeof(SyncColumnAttribute)));
            if (syncColumn.Count() != 1)
            {
                throw new Exception("Type " + typeName + " must have one and only one property marked as SyncColumn");
            }

            var syncColumnName = syncColumn.First().Name;

            model.SyncColumnName = syncColumnName;

            model.SyncNeeded = ExpressionHelper.CheckItemInequality<T>(syncColumnName, completeStatusValue);

            models.Add(model);
        }

        //This runs on the source connection
        public async Task Sync()
        {
            var errors = new List<Exception>();
            try
            {
                //Make new list to allow removal of broken models
                foreach (var model in new List<ISyncModel>(models))
                {
                    try
                    {
                        //Load items that need sync
                        await model.LoadItemsNeedingSync(this);
                    }
                    catch (Exception ex)
                    {
                        errors.Add(new Exception("Error occurred during item load", ex));
                        models.Remove(model);
                    }

                }

                var syncTasks = new List<Task>();
                for (int x = 0; x < targetConnectionStrings.Count; x++)
                {
                    syncTasks.Add(SyncToTarget(targetConnectionStrings[x], x));
                }
                var whenAll = Task.WhenAll(syncTasks);
                try
                {
                    await whenAll;
                }
                catch
                {
                    var aggregate = whenAll.Exception;
                    errors.AddRange(whenAll.Exception.InnerExceptions);
                }
                foreach (var model in models)
                {
                    try
                    {
                        await model.UpdateStatuses(this);
                        errors.AddRange(model.Errors);
                    }
                    catch (Exception ex)
                    {
                        errors.Add(new Exception("Error occurred during status update", ex));
                    }

                }
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }

            if (errors.Count > 0)
            {
                throw new AggregateException("Errors occurred: ", errors);
            }
        }


        private async Task SyncToTarget(string target, int targetNumber)
        {
            var targetContext = new SyncDbContext(target);
            var toReturn = new List<Exception>();
            foreach (var model in models)
            {
                await model.SyncItems(targetContext, targetNumber);
            }
        }

        //protected sealed override void OnModelCreating(DbModelBuilder modelBuilder)
        //{
        //    var syncTypes = modelBuilder.Types().Where(t => t.IsSubclassOf(typeof(SyncEntityBase)));
        //    if (isTarget)
        //    {
        //        //Don't expect the SyncStatus column to exist on the targets
        //        syncTypes.Configure(c => c.Ignore("SyncStatus"));
        //    }
        //    base.OnModelCreating(modelBuilder);
        //}

    }
}