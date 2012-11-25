#region License

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
using System.IO.Compression;
using System.Threading;

namespace DataCache
{
    public class StringCacheItem : IMemoryCacheItem
    {
        public string Data { get; set; }

        public int Size
        {
            get { return Data.Length; }
        }
    }

    public class ByteBufferCacheItem : IMemoryCacheItem
    {
        public bool IsCompressed;
        public byte[] Data;
        public int Size { get; set; }
        public Stream ByteStream { get; set; }
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


    public interface IDiskCache
    {
        bool Enabled { get; }
        ByteBufferCacheItem Get(CacheType cacheType, string topLevelId, string cacheId);
        PutResponse Put(string topLevelId, string cacheId, ByteBufferCacheItem byteBufferCacheItem);
        PutResponse Put(string topLevelId, string cacheId, StringCacheItem stringCacheItem);
        bool IsCached(CacheType type, string topLevelId, string cacheId);
        void ClearIsCached(string cacheId);
        IEnumerable<string> EnumerateCachedItems(string topLevelId);
    }

 

    public class DiskCache : IDiskCache
    {
        private class CacheStatus
        {
            public bool IsCached {  get; set; }
            public bool IsCompressed {  get; set; }
            public string Path { get;set; }
        }

        private static string _rootFolder;

        private readonly NamedReaderWriterLockSlim<string> _cacheLock;
        private readonly IDictionary<string, CacheStatus> _cacheStatusRepo;
        private readonly ReaderWriterLockSlim _cacheStatusRepoLock;

        private const int MaxBlockSize = 4096;

        [ThreadStatic] public static DynamicBuffer Scratch;

        private readonly ICacheLogger _cacheLogger;

        public bool Enabled { get; private set; }


        public DiskCache(ICacheLogger logger)
            : this(CacheSettings.Default.DiskCacheRootFolder, logger)
        {
        }

        internal DiskCache(String rootFolder, ICacheLogger logger)
        {
            if (!CacheSettings.Default.DiskCacheEnabled)
                return;

            if (rootFolder == null)
                return;
            _cacheLogger = logger;
            _rootFolder = rootFolder;
            var tokens = _rootFolder.Split(':');
            string drive = null;
            if (tokens.Length > 1)
                drive = tokens[0];
            if (drive == null)
            {
                Log(CacheLogLevel.Error,
                    String.Format("Root folder {0} does not begin with a drive letter. Cache disabled", rootFolder));
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

                _cacheLock = new NamedReaderWriterLockSlim<string>();
                _cacheStatusRepo = new Dictionary<string, CacheStatus>();
                _cacheStatusRepoLock = new ReaderWriterLockSlim();
                Enabled = true;
            }
            catch (Exception e)
            {
                Log(CacheLogLevel.Error,
                    String.Format("Exception while creating root folder: {0}. Cache is disabled", e));
            }
        }

        /// <summary>
        /// Set IsCached flag to false. The data may still be stored on disk; we simply overwrite the next
        /// time the item is cached
        /// </summary>
        /// <param name="cacheId"></param>
        public void ClearIsCached(string cacheId)
        {
           SetIsCached(cacheId, new CacheStatus());
        }

        public IEnumerable<string> EnumerateCachedItems(string topLevelId)
        {
            try
            {
                return Directory.EnumerateFiles(GetTopLevelFolder(topLevelId));
            }
            catch (Exception e)
            {
               _cacheLogger.Log(CacheLogLevel.Error, e.Message);   
               
            }
            return null;

        }


        private void SetIsCached(string cacheItemId, CacheStatus status)
        {
            Action action = () =>
                                {
                                    if (_cacheStatusRepo.ContainsKey(cacheItemId))
                                        _cacheStatusRepo[cacheItemId] = status;
                                    else
                                        _cacheStatusRepo.Add(cacheItemId, status);
                                };
            UpdateStatus(action);
        }


        public PutResponse Put(string topLevelId, string cacheId, StringCacheItem stringCacheItem)
        {
            if (!Enabled)
                return PutResponse.Disabled;

            if (cacheId == null)
                return PutResponse.InvalidData;

            if (stringCacheItem == null || (stringCacheItem.Data == null))
                return PutResponse.InvalidData;

            var rc = PutResponse.Error;
            try
            {
                using (_cacheLock.LockWrite(cacheId))
                {
                    Directory.CreateDirectory(GetTopLevelFolder(topLevelId));
                    var path = GetStringPath(topLevelId, cacheId);
                    Write(path, stringCacheItem.Data);
                    rc = PutResponse.Success;
                    SetIsCached(cacheId, new CacheStatus{IsCached = true, Path = path});
                }
            }
            catch (Exception e)
            {
                Log(CacheLogLevel.Debug,
                    String.Format("Put: Exception putting string for series {0}: {1}", cacheId, e));
            }
            return rc;
        }

        public bool IsCached(CacheType type, string topLevelId, string cacheId)
        {
            return GetCacheStatus(type, topLevelId, cacheId).IsCached;
        }

        private CacheStatus GetCacheStatus(CacheType type, string topLevelId, string cacheId)
        {
            if (!Enabled ||  cacheId == null)
                return new CacheStatus();
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

            var status = new CacheStatus();
            try
            {
                using (_cacheLock.LockRead(cacheId))
                {
                    bool isCompressed;
                    var path = GetVerifiedPath(type, topLevelId, cacheId, out isCompressed);
                    status = new CacheStatus{IsCached = path != null, IsCompressed = isCompressed, Path = path};
                    SetIsCached(cacheId, status);
                }
            }
            catch (Exception e)
            {
                Log(CacheLogLevel.Debug,
                    String.Format("Exception checking if data is cached for cache id {0} :{1}", cacheId, e));
            }
            
            return status;
        }
        public ByteBufferCacheItem Get(CacheType cacheType, string topLevelId, string cacheId)
        {
            if (!Enabled || cacheId == null)
                return null;
            ByteBufferCacheItem item;
            var st = new Stopwatch();
            st.Start();
            try
            {
                var status = GetCacheStatus(cacheType, topLevelId, cacheId);
                if (!status.IsCached)
                    return null;
                var path = GetPath(cacheType, topLevelId, cacheId, status.IsCompressed);
                using (_cacheLock.LockRead(cacheId))
                {
                  
                    item = new ByteBufferCacheItem {IsCompressed = status.IsCompressed};
                    FileStream fs = null;
                    try
                    {
                        fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 0x10100,
                                            FileOptions.SequentialScan);
                        byte[] readBuffer = null;

                        switch (cacheType)
                        {
                            case CacheType.Pixels:
                                GetScratch().Resize((int) fs.Length, item.IsCompressed);
                                readBuffer = GetScratch().Buffer;
                                break;
                            case CacheType.String:
                                readBuffer = new byte[fs.Length];
                                break;
                        }
                        if (readBuffer == null)
                            return item;

                        int count;
                        var offset = 0;
                        var length = (int) fs.Length;
                        var blockSize = Math.Min(MaxBlockSize, length);
                        while ((count = fs.Read(readBuffer, offset, blockSize)) > 0)
                        {
                            offset += count;
                            blockSize = Math.Min(MaxBlockSize, length - offset);
                        }
                        item.Size = (int) fs.Length;
                        item.Data = readBuffer;
                    }
                    finally
                    {
                        if (fs != null)
                            fs.Close();
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
                Log(CacheLogLevel.Debug,
                    String.Format("Get: Exception getting data for cache id {0}: {1}", cacheId, e));
                ClearIsCached(cacheId);
                item = null;
            }
            return item;
        }

        public PutResponse Put(string topLevelId, string cacheId, ByteBufferCacheItem byteBufferCacheItem)
        {
            if (!Enabled)
                return PutResponse.Disabled;

            if (cacheId == null)
                return PutResponse.InvalidData;

            if (byteBufferCacheItem == null ||
                (byteBufferCacheItem.Data == null && byteBufferCacheItem.ByteStream == null))
                return PutResponse.InvalidData;

            var rc = PutResponse.Error;
            try
            {
                using (_cacheLock.LockWrite(cacheId))
                {
                    Directory.CreateDirectory(GetTopLevelFolder(topLevelId));
                    using (
                        var fs =
                            new FileStream(
                                GetPixelPath(topLevelId, cacheId, byteBufferCacheItem.IsCompressed),
                                FileMode.CreateNew,
                                FileAccess.Write))
                    {
                        fs.SetLength(byteBufferCacheItem.Size);
                        if (byteBufferCacheItem.Data != null)
                        {
                            Write(byteBufferCacheItem.Data, fs);
                        }
                        else if (byteBufferCacheItem.ByteStream != null)
                        {
                            Write(byteBufferCacheItem.ByteStream, fs);
                        }
                    }
                    rc = PutResponse.Success;
                    SetIsCached(cacheId, new CacheStatus{IsCached = true,IsCompressed = byteBufferCacheItem.IsCompressed});
                }
            }
            catch (Exception e)
            {
                Log(CacheLogLevel.Debug,
                    String.Format("Put: Exception putting data for cache id {0}: {1}", cacheId, e));
            }
            return rc;
        }

        private void Write(byte[] src, Stream dest)
        {
            int offset = 0;
            var length = src.Length;
            int blockSize = Math.Min(MaxBlockSize, length);
            while (offset < length)
            {
                dest.Write(src, offset, blockSize);
                offset += blockSize;
                blockSize = Math.Min(MaxBlockSize, length - offset);
            }
        }

        private void Write(Stream src, Stream dest)
        {
            int count;
            var scratch = new byte[MaxBlockSize];
            while ((count = src.Read(scratch, 0, MaxBlockSize)) > 0)
            {
                dest.Write(scratch, 0, count);
            }
        }

        private void Log(CacheLogLevel level, string message)
        {
            if (_cacheLogger != null)
                _cacheLogger.Log(level, "[DiskCache] :" + message);
        }


        private static string GetTopLevelFolder(string topLevelId)
        {
            var rc = _rootFolder;
            if (topLevelId != null)
                rc += "\\" + topLevelId;
            return rc;
        }

        private string GetVerifiedPath(CacheType cacheType, string topLevelId, string cacheId, out bool compressed)
        {
            compressed = false;
            switch (cacheType)
            {
                case CacheType.Pixels:
                    var path = GetPixelPath(topLevelId, cacheId, true);
                    if (File.Exists(path))
                    {
                        compressed = true;
                        return path;
                    }
                    path = GetPixelPath(topLevelId, cacheId, false);
                    return File.Exists(path) ? path : null;
                case CacheType.String:
                    path =  GetStringPath(topLevelId, cacheId);
                    return File.Exists(path) ? path : null;
            }
            return null;
        }
        private string GetPath(CacheType cacheType, string topLevelId, string cacheId, bool compressed)
        {
            switch (cacheType)
            {
                case CacheType.Pixels:
                    return GetPixelPath(topLevelId, cacheId, compressed);
                case CacheType.String:
                    return GetStringPath(topLevelId, cacheId);
            }
            return null;
        }
        private static string GetPixelPath(string topLevelId, string cacheId, bool compressed)
        {
            var cacheFolder = GetTopLevelFolder(topLevelId) + "\\" + cacheId;
            return compressed ? cacheFolder + ".cp" : cacheFolder + ".p";
        }

        private static string GetStringPath(string topLevelId, string cacheId)
        {
            return GetTopLevelFolder(topLevelId) + "\\" + cacheId + ".s";
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

        private static void Write(string filename, string data)
        {
            using (var fileStream = new FileStream(filename, FileMode.Create, FileAccess.Write))
            {
                using (var compressionStream = new GZipStream(fileStream, CompressionMode.Compress))
                {
                    using (var writer = new StreamWriter(compressionStream))
                    {
                        writer.Write(data);
                    }
                }
            }
        }

    }
}