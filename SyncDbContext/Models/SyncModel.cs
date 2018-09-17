﻿using SyncDbContext.Helpers;
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
        Task LoadItemsNeedingSync(SyncDbContext context);
        Task SyncItems(SyncDbContext targetContext, int targetNumber);
        Task UpdateStatuses(SyncDbContext sourceContext);
        List<Exception> Errors { get; set; }
    }

    public class SyncModel<T> : ISyncModel where T : class
    {
        public string SyncColumnName { get; set; }
        public Expression<Func<T, bool>> SyncNeeded { get; set; }

        public List<T> ItemsChanged { get; set; }

        private ConcurrentDictionary<T, ulong> SyncSuccess { get; set; }

        public List<Exception> Errors { get; set; }

        //This should run first
        public async Task LoadItemsNeedingSync(SyncDbContext sourceContext)
        {
            ItemsChanged = await sourceContext.Set<T>().Where(SyncNeeded).ToListAsync();
            SyncSuccess = new ConcurrentDictionary<T, ulong>();
            foreach(var item in ItemsChanged)
            {
                SyncSuccess[item] = 0;
            }
        }

        private void updateSuccess(T item, ulong successFlag)
        {
            SyncSuccess.AddOrUpdate(item, successFlag, (key, oldValue) =>
            {
                return oldValue | successFlag;
            });
        }

        //Then this one, on each target
        public async Task SyncItems(SyncDbContext targetContext, int targetNumber)
        {
            bool success = true;
            var successFlag = Convert.ToUInt64(1 << targetNumber);
            try
            {
                //Try to one-shot the updates
                var result = await targetContext.Upsert(ItemsChanged);
                //todo: check value
                foreach(var item in ItemsChanged)
                {
                    updateSuccess(item, successFlag);
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
                foreach(var item in ItemsChanged)
                {
                    try
                    {
                        var result = await targetContext.Upsert(item);
                        //todo: check value

                        updateSuccess(item, successFlag);
                    }
                    catch(Exception ex)
                    {
                        Errors.Add(ex);
                    }
                }
            }
        }

        //Lastly this to update statuses
        public async Task UpdateStatuses(SyncDbContext sourceContext)
        {
            var syncColumnProp = typeof(T).GetProperty(SyncColumnName);
            foreach(var item in ItemsChanged)
            {
                if (SyncSuccess.TryGetValue(item, out ulong result))
                {
                    syncColumnProp.SetValue(item, result);
                }
                else
                {
                    Errors.Add(new Exception("No stored success result for item with type " + typeof(T).Name));
                }
            }

            await sourceContext.SaveChangesAsync();
        }
    }

}