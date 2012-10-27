﻿#region License

//Copyright (C)  2012 Aaron Boxer

//This program is free software: you can redistribute it and/or modify
//it under the terms of the GNU General Public License as published by
//the Free Software Foundation, either version 3 of the License, or
//(at your option) any later version.

//This program is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//GNU General Public License for more details.

//You should have received a copy of the GNU General Public License
//along with this program.  If not, see <http://www.gnu.org/licenses/>.

#endregion

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace DataCache
{

    public class PixelCacheItem : IMemoryCacheItem
    {
        public bool IsCompressed;
        public byte[] PixelData;
        public int Size { get; set; }
    }

    public enum PutResponse
    {
        Success,
        Disabled,
        InvalidData,
        Error
    }

    public enum CacheLogLevel
    {
        Debug,
        Info,
        Warn,
        Error
    }

    public interface IDiskCacheLogger
    {
        void Log(CacheLogLevel level, string message);
    }

    public interface IDiskCache
    {
        bool Enabled { get; }
        PixelCacheItem Get(string topLevelId, string cacheId);
        PutResponse Put(string topLevelId, string cacheId, PixelCacheItem pixelCacheItem);
        bool IsCached(string topLevelId, string cacheId);
    }



    public class DiskCache : IDiskCache
    {
        private static string _rootFolder;

        private readonly NamedReaderWriterLockSlim<string> _pixelLock;
        private readonly IDictionary<string, bool> _cacheStatusRepo;
        private readonly ReaderWriterLockSlim _cacheStatusRepoLock;

        private const int MaxBlockSize = 4096;

        [ThreadStatic] public static DynamicBuffer Scratch;

        private readonly IDiskCacheLogger _diskCacheLogger;

        public bool Enabled { get; private set; }


        public DiskCache(IDiskCacheLogger logger)
            : this(CacheSettings.Default.DiskCacheRootFolder,logger)
        {
        }

        internal DiskCache(String rootFolder, IDiskCacheLogger logger)
        {
            if (!CacheSettings.Default.DiskCacheEnabled)
                return;

            if (rootFolder == null)
                return;
            _diskCacheLogger = logger;
            _rootFolder = rootFolder;
            var tokens = _rootFolder.Split(':');
            string drive = null;
            if (tokens.Length > 1)
                drive = tokens[0];
            if (drive == null)
            {
                Log(CacheLogLevel.Error, String.Format("Root folder {0} does not begin with a drive letter. Cache disabled", rootFolder));
                return;
            }

            DriveInfo driveInfo;
            try
            {
                driveInfo = new DriveInfo(drive);
            }
            catch (Exception e)
            {
                Log(CacheLogLevel.Error, String.Format("Exception getting drive info: {0}. Cache disabled.", e));
                return;
            }
            if (!driveInfo.IsReady)
            {
                Log(CacheLogLevel.Error, String.Format("Disk cache drive {0} is not ready. Cache disabled", drive));
                return;
            }


            try
            {
                if (!Directory.Exists(_rootFolder))
                {
                    Directory.CreateDirectory(_rootFolder);
                }

                _pixelLock = new NamedReaderWriterLockSlim<string>();
                _cacheStatusRepo = new Dictionary<string, bool>();
                _cacheStatusRepoLock = new ReaderWriterLockSlim();
                Enabled = true;
            }
            catch (Exception e)
            {
               Log(CacheLogLevel.Error, String.Format("Exception while creating root folder: {0}. Cache is disabled", e));
            }
        }

 

        public void ClearIsCached(string cacheId)
        {
            Action action = () => _cacheStatusRepo.Remove(cacheId);
            UpdateStatus(action);
        }


        private void SetIsCached(string cacheItemId, bool isCached)
        {
            Action action = () =>
                             {
                                if (_cacheStatusRepo.ContainsKey(cacheItemId))
                                    _cacheStatusRepo[cacheItemId] = isCached;
                                else
                                    _cacheStatusRepo.Add(cacheItemId, isCached);
                             };
            UpdateStatus(action);
        }

        public bool IsCached(string topLevelId, string cacheId)
        {
            if (!Enabled || topLevelId == null || cacheId == null)
                return false;
            try
            {
                _cacheStatusRepoLock.EnterReadLock();
                if (_cacheStatusRepo.ContainsKey(cacheId))
                    return _cacheStatusRepo[cacheId];
            }
            finally
            {
                _cacheStatusRepoLock.ExitReadLock();
            }

            var isCached = false;
            try
            {
                using (_pixelLock.LockRead(cacheId))
                {
                    bool isCompressed;
                    isCached = VerifyDataPath(topLevelId, cacheId, out isCompressed) != null;
                }
            }
            catch (Exception e)
            {
               Log(CacheLogLevel.Debug, String.Format("Exception checking if frame is cached for study {0} :{1}", topLevelId, e));
            }
            SetIsCached(cacheId, isCached);
            return isCached;
        }

        private string VerifyDataPath(string topLevelId, string cacheId,out bool isCompressed)
        {
            isCompressed = false;
            var path = GetPixelPath(topLevelId, cacheId, true);
            if (File.Exists(path))
            {
                isCompressed = true;
                return path;
            }

            path = GetPixelPath(topLevelId, cacheId, false);
            return File.Exists(path) ? path : null;
        }
        public PixelCacheItem Get(string topLevelId, string cacheId)
        {
            if (!Enabled || topLevelId == null)
                return null;
            PixelCacheItem item;
            var st = new Stopwatch();
            st.Start();
            try
            {
                using (_pixelLock.LockRead(cacheId))
                {
                    bool isCompressed;
                    var pixelPath = VerifyDataPath(topLevelId, cacheId,out isCompressed);
                    if (pixelPath == null)
                    {
                        ClearIsCached(cacheId);
                        return null;
                    }

                    item = new PixelCacheItem {IsCompressed = isCompressed};
                    using (var fs = File.OpenRead(pixelPath))
                    {
                        GetScratch().Resize((int) fs.Length, item.IsCompressed);

                        int count;
                        int offset = 0;
                        var length = (int) fs.Length;
                        int blockSize = Math.Min(MaxBlockSize, length - offset);
                        while (offset < length && (count = fs.Read(GetScratch().Buffer, offset, blockSize)) > 0)
                        {
                            offset += count;
                            blockSize = Math.Min(MaxBlockSize, length - offset);
                        }


                        item.Size = (int) fs.Length;
                        item.PixelData = GetScratch().Buffer;
                    }
                    st.Stop();
                    var ms = (st.ElapsedTicks/(Stopwatch.Frequency/(1000.0*1000)))/1000.0;
                    Log(CacheLogLevel.Debug,
                      String.Format("Get:  time elapsed: {1} ms for cache id '{0}',  ",
                        cacheId, ms));
                }
            }
            catch (Exception e)
            {
                Log(CacheLogLevel.Debug, String.Format("Get: Exception getting frame for series {0}: {1}", topLevelId, e));
                ClearIsCached(cacheId);
                item = null;
            }
            return item;
        }

        public PutResponse Put(string topLevelId, string cacheId, PixelCacheItem pixelCacheItem)
        {
            if (!Enabled)
                return PutResponse.Disabled;

            if (pixelCacheItem == null ||
                pixelCacheItem.PixelData == null ||
                topLevelId == null)
                return PutResponse.InvalidData;

            var rc = PutResponse.Error;
            try
            {
                using (_pixelLock.LockWrite(cacheId))
                {
                    Directory.CreateDirectory(GetTopLevelFolder(topLevelId));
                    if (pixelCacheItem.PixelData != null)
                    {
                        using (
                            var fs =
                                new FileStream(
                                    GetPixelPath(topLevelId, cacheId, pixelCacheItem.IsCompressed),
                                    FileMode.CreateNew,
                                    FileAccess.Write))
                        {
                            int offset = 0;
                            var length = pixelCacheItem.PixelData.Length;
                            int blockSize = Math.Min(MaxBlockSize, length - offset);
                            while (offset < length )
                            {
                                fs.Write(pixelCacheItem.PixelData, offset, blockSize);
                                offset += blockSize;
                                blockSize = Math.Min(MaxBlockSize, length - offset);
                            }
                        }
                        rc = PutResponse.Success;
                        SetIsCached(cacheId, true);
                    }
                }
            }
            catch (Exception e)
            {
                Log(CacheLogLevel.Debug, String.Format("Put: Exception putting frame for series {0}: {1}", topLevelId, e));
            }
            return rc;
        }

        private void Log(CacheLogLevel level, string message)
        {
            if (_diskCacheLogger != null)
                _diskCacheLogger.Log(level, "[DiskCache] :" + message);
        }
  

        private static string GetTopLevelFolder(string topLevelId)
        {
            var rc = _rootFolder;
            if (topLevelId != null)
                rc += "\\" + topLevelId;
            return rc;
        }


        private static string GetPixelPath(string topLevelId, string cacheId, bool compressed)
        {
            var cacheFolder = GetTopLevelFolder(topLevelId) + "\\" + cacheId;
            return compressed ? cacheFolder + ".cp" : cacheFolder + ".p";
        }

        private static DynamicBuffer GetScratch()
        {
            return Scratch ?? (Scratch = new DynamicBuffer());
        }

        private void UpdateStatus(Action action)
        {
            try
            {
                _cacheStatusRepoLock.EnterWriteLock();
                action();
            }
            finally
            {
                _cacheStatusRepoLock.ExitWriteLock();
            }
        }
    }
}