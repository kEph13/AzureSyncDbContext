using SyncDbContext.Attributes;
using SyncDbContext.Helpers;
using SyncDbContext.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SyncDbContext
{
    public class SyncDbContext : DbContext
    {
        private readonly List<string> targetConnectionStrings;
        private readonly long completeStatusValue;
        private readonly ConcurrentDictionary<object, long> flagStatuses;
        private readonly List<ISyncModel> models = new List<ISyncModel>();
        private static SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

        public Action FinishedAction { get; set; }

        public Action<string> Log { get; set; }

        public SyncDbContext(string sourceConnectionString, List<string> targetConnectionStrings) : base(sourceConnectionString)
        {
            this.targetConnectionStrings = targetConnectionStrings;

            //this represents all targets sync'd - i.e. 1111 for 4 targets
            completeStatusValue = Convert.ToInt64(Math.Pow(2, targetConnectionStrings.Count()) - 1);
            flagStatuses = new ConcurrentDictionary<object, long>();
        }

        private SyncDbContext(string targetConnectionString) : base(targetConnectionString)
        {
        }

        protected void AddToSyncList<TEntity>() where TEntity : class
        {
            var model = new SyncModel<TEntity>();
            var typeName = typeof(TEntity).Name;

            var syncColumn = typeof(TEntity).GetProperties().Where(prop => Attribute.IsDefined(prop, typeof(SyncColumnAttribute)));
            if (syncColumn.Count() != 1)
            {
                throw new Exception("Type " + typeName + " must have one and only one property marked as SyncColumn");
            }

            var syncColumnName = syncColumn.First().Name;

            model.SyncColumnName = syncColumnName;

            model.SyncNeeded = ExpressionHelper.CheckItemInequality<TEntity>(syncColumnName, completeStatusValue);

            model.UpsertModel = EntityHelper<TEntity>.GetUpsertModel(this);
            model.UpsertModel.SyncColumn = syncColumnName;

            model.Log = (string msg) => Log?.Invoke(msg);

            models.Add(model);
        }

        //This runs on the source connection
        public async Task Sync()
        {
            var errors = new List<Exception>();

            //Make sure we're only syncing once at a time
            //since this is async, have to use Semaphore
            if (await semaphore.WaitAsync(0))
            {
                try
                {
                    //Make new list to allow removal of broken models
                    foreach (var model in new List<ISyncModel>(models))
                    {
                        try
                        {
                            //Load items that need sync
                            var itemsToSync = await model.LoadItemsNeedingSync(this);
                            if (!itemsToSync)
                            {
                                models.Remove(model);
                            }
                        }
                        catch (Exception ex)
                        {
                            errors.Add(new Exception("Error occurred during item load", ex));
                            models.Remove(model);
                        }
                    }

                    if (models.Count == 0)
                    {
                        Log?.Invoke("Nothing to sync");
                        return;
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
                            model.UpdateStatuses();
                            errors.AddRange(model.Errors);
                        }
                        catch (Exception ex)
                        {
                            errors.Add(new Exception("Error occurred during status update", ex));
                        }

                    }
                    FinishedAction?.Invoke();
                    await SaveChangesAsync();
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }
                finally
                {
                    semaphore.Release();
                }
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
    }
}