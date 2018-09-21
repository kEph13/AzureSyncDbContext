using SyncDbContext.Exceptions;
using SyncDbContext.Helpers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SyncDbContext.Models
{
    public interface ISyncModel
    {
        Task<bool> LoadItemsNeedingSync(SyncDbContext context);
        Task SyncItems(SyncDbContext targetContext, int targetNumber);
        void UpdateStatuses();
        ConcurrentBag<Exception> Errors { get; set; }
        Action<string> Log { get; set; }
    }

    public class SyncModel<T> : ISyncModel where T : class
    {
        public string SyncColumnName { get; set; }
        public Expression<Func<T, bool>> SyncNeeded { get; set; }

        public List<T> ItemsChanged { get; set; }

        private ConcurrentDictionary<T, long> SyncSuccess { get; set; }

        //Each run fetches the items to update, so the objects aren't the same between runs
        //So, keep track of errors using hash
        //Also track targets separately in case one target has a schema problem
        private static ConcurrentDictionary<(int keyHash, int targetId), int> ItemErrors { get; set; } = new ConcurrentDictionary<(int keyHash, int targetId), int>();

        public ConcurrentBag<Exception> Errors { get; set; } = new ConcurrentBag<Exception>();

        public UpsertModel<T> UpsertModel { get; set; }

        public Action<string> Log { get; set; }

        //This should run first
        public async Task<bool> LoadItemsNeedingSync(SyncDbContext sourceContext)
        {
            try
            {
                var query = sourceContext.Set<T>().Where(SyncNeeded);

                ItemsChanged = await query.ToListAsync();
                if (ItemsChanged.Count == 0)
                {
                    return false;
                }
                SyncSuccess = new ConcurrentDictionary<T, long>();
                Log?.Invoke($"{ItemsChanged.Count} {typeof(T).Name} rows to sync.");
                foreach (var item in ItemsChanged)
                {
                    SyncSuccess[item] = GetSyncValue(item);
                }
                return true;
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

        private int GetKeyHash(T item)
        {
            var keyFields = UpsertModel.KeyFields;
            if (keyFields.Count == 1)
            {
                var colProp = typeof(T).GetProperty(keyFields.First());
                var value = colProp.GetValue(item);
                var hash = value.GetHashCode();
                return hash;

            }
            var hashes = new List<int>();
            if (keyFields.Count > 8)
            {
                throw new Exception("Too many keyfields to combine hashes");
            }
            foreach(var key in keyFields)
            {
                var colProp = typeof(T).GetProperty(key);
                var value = colProp.GetValue(item);
                hashes.Add(value.GetHashCode());
            }
            return CollectionExtensions.CombineHashCodes(hashes);
        }

        private string GetTargetName(SyncDbContext context)
        {
            var serverName = context.Database.Connection.DataSource;
            var dbName = context.Database.Connection.Database;
            return $"{serverName}\\{dbName}";
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

            if (ItemErrors.Any())
            {
                int removed = 0;
                //new list to allow removal
                foreach(var item in new List<T>(itemsToSync))
                {
                    if (CheckItemErrors(item, targetNumber) > 2)
                    {
                        //Don't keep retrying error items
                        itemsToSync.Remove(item);
                        removed++;
                    }
                }
                if (removed > 0)
                {
                    Log?.Invoke($"Skipping {removed} row{(removed > 1 ? "s" : "")} on target {GetTargetName(targetContext)} - too many errors");
                }
            }

            var itemCount = itemsToSync.Count;

            if (itemCount == 0)
            {
                return;
            }


            Log?.Invoke($"Syncing {itemCount} {typeof(T).Name} row{(itemCount > 1 ? "s" : "")} to {GetTargetName(targetContext)}");

            bool success = true;
            try
            {

                //Try to one-shot the updates
                var result = await targetContext.Upsert(itemsToSync, UpsertModel);
                if (result != itemCount)
                {
                    throw new Exception("Item count did not match merge results");
                }
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

                        if (result != items.Count)
                        {
                            throw new Exception("Item count did not match merge results");
                        }
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
                        //If the error came from a batch, some items might be done already. Only update ones that aren't
                        if (SyncSuccess.TryGetValue(item, out long value))
                        {
                            done = (value & successFlag) > 0;
                        }
                        if (!done)
                        {
                            var result = await targetContext.Upsert(item, UpsertModel);
                            if (result != 1)
                            {
                                throw new Exception("Item count did not match merge results");
                            }

                            UpdateSuccess(item, successFlag);
                        }
                    }
                    catch (Exception ex)
                    {
                        UpdateItemErrors(item, targetNumber);
                        Errors.Add(ex);
                    }
                }
            }
        }

        private int CheckItemErrors(T item, int targetNumber)
        {
            if (ItemErrors.TryGetValue((GetKeyHash(item), targetNumber), out int count))
            {
                return count;
            }
            return 0;
        }

        private void UpdateItemErrors(T item, int targetNumber)
        {
            ItemErrors.AddOrUpdate((GetKeyHash(item), targetNumber), 1, (key, oldValue) =>
            {
                return oldValue + 1;
            });
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