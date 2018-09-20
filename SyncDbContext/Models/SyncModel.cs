using SyncDbContext.Exceptions;
using SyncDbContext.Helpers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SyncDbContext.Models
{
    public interface ISyncModel
    {
        Task LoadItemsNeedingSync(SyncDbContext context);
        Task SyncItems(SyncDbContext targetContext, int targetNumber);
        void UpdateStatuses();
        ConcurrentBag<Exception> Errors { get; set; }
    }

    public class SyncModel<T> : ISyncModel where T : class
    {
        public string SyncColumnName { get; set; }
        public Expression<Func<T, bool>> SyncNeeded { get; set; }

        public List<T> ItemsChanged { get; set; }

        private ConcurrentDictionary<T, long> SyncSuccess { get; set; }

        public ConcurrentBag<Exception> Errors { get; set; } = new ConcurrentBag<Exception>();

        public UpsertModel<T> UpsertModel { get; set; }

        //This should run first
        public async Task LoadItemsNeedingSync(SyncDbContext sourceContext)
        {
            try
            {
                ItemsChanged = await sourceContext.Set<T>().Where(SyncNeeded).ToListAsync();
                SyncSuccess = new ConcurrentDictionary<T, long>();
                if (ItemsChanged.Count > 0)
                {
                    Console.WriteLine($"{ItemsChanged.Count} {typeof(T).Name} rows to sync.");
                    foreach (var item in ItemsChanged)
                    {
                        SyncSuccess[item] = GetSyncValue(item);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error during item load: ", ex);
            }

        }

        private void UpdateSuccess(T item, long successFlag)
        {
            SyncSuccess.AddOrUpdate(item, successFlag, (key, oldValue) =>
            {
                return oldValue | successFlag;
            });
        }

        //Then this one, on each target
        public async Task SyncItems(SyncDbContext targetContext, int targetNumber)
        {
            if (ItemsChanged.Count == 0)
            {
                return;
            }
            var successFlag = Convert.ToInt64(1 << targetNumber);

            var itemsToSync = ItemsChanged.Where(i => !CheckSyncColumn(i, successFlag)).ToList();

            if (itemsToSync.Count == 0)
            {
                return;
            }

            bool success = true;
            try
            {

                //Try to one-shot the updates
                var result = await targetContext.Upsert(itemsToSync, UpsertModel);
                //todo: check value
                foreach (var item in itemsToSync)
                {
                    UpdateSuccess(item, successFlag);
                }
            }
            catch (TooManyItemsException)
            {
                try
                {
                    //batch
                    var allowedEntityCount = 2000 / UpsertModel.PropertyNames.Count;
                    for (int i = 0; i < Math.Ceiling(itemsToSync.Count / Convert.ToDecimal(allowedEntityCount)); i++)
                    {
                        var items = itemsToSync.Skip(i * allowedEntityCount).Take(allowedEntityCount).ToList();
                        var result = await targetContext.Upsert(items, UpsertModel);
                        //todo: check result
                        foreach (var item in items)
                        {
                            UpdateSuccess(item, successFlag);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Errors.Add(ex);
                    success = false;
                }
            }
            catch (Exception ex)
            {
                //assuming transaction has rolled back
                success = false;
                Errors.Add(ex);
            }

            if (!success)
            {
                //If that failed, run one by one
                foreach (var item in itemsToSync)
                {
                    try
                    {
                        var done = false;
                        if (SyncSuccess.TryGetValue(item, out long value))
                        {
                            done = (value & successFlag) > 0;
                        }
                        if (!done)
                        {
                            var result = await targetContext.Upsert(item, UpsertModel);
                            //todo: check value

                            UpdateSuccess(item, successFlag);
                        }
                    }
                    catch (Exception ex)
                    {
                        Errors.Add(ex);
                    }
                }
            }
        }

        private void UpdateSyncColumn(T item, long flag)
        {
            var syncColumnProp = typeof(T).GetProperty(SyncColumnName);
            syncColumnProp.SetValue(item, flag);

        }

        private long GetSyncValue(T item)
        {
            var syncColumnProp = typeof(T).GetProperty(SyncColumnName);
            var value = (long)syncColumnProp.GetValue(item);
            return value;
        }

        private bool CheckSyncColumn(T item, long flag)
        {
            var value = GetSyncValue(item);
            return (value & flag) > 0;
        }

        //Lastly this to update statuses
        public void UpdateStatuses()
        {
            try
            {
                foreach (var item in ItemsChanged)
                {
                    if (SyncSuccess.TryGetValue(item, out long result))
                    {
                        UpdateSyncColumn(item, result);
                    }
                    else
                    {
                        Errors.Add(new Exception("No stored success result for item with type " + typeof(T).Name));
                    }
                }

            }
            catch (Exception ex)
            {
                Errors.Add(ex);
            }

        }
    }

}