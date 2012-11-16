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
        PutResponse Put(string topLevelId, string cacheId, StringCacheItem pixelCacheItem);
        bool IsCached(CacheType type, string topLevelId, string cacheId);
    }


    public class DiskCache : IDiskCache
    {
        private static string _rootFolder;

        private readonly NamedReaderWriterLockSlim<string> _cacheLock;
        private readonly IDictionary<string, bool> _cacheStatusRepo;
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
                _cacheStatusRepo = new Dictionary<string, bool>();
                _cacheStatusRepoLock = new ReaderWriterLockSlim();
                Enabled = true;
            }
            catch (Exception e)
            {
                Log(CacheLogLevel.Error,
                    String.Format("Exception while creating root folder: {0}. Cache is disabled", e));
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


        public PutResponse Put(string topLevelId, string cacheId, StringCacheItem pixelCacheItem)
        {
            if (!Enabled)
                return PutResponse.Disabled;

            if (topLevelId == null)
                return PutResponse.InvalidData;

            if (pixelCacheItem == null || (pixelCacheItem.Data == null))
                return PutResponse.InvalidData;

            var rc = PutResponse.Error;
            try
            {
                using (_cacheLock.LockWrite(cacheId))
                {
                    Directory.CreateDirectory(GetTopLevelFolder(topLevelId));
                    Write(GetStringPath(topLevelId, cacheId), pixelCacheItem.Data);
                    rc = PutResponse.Success;
                    SetIsCached(cacheId, true);
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
                using (_cacheLock.LockRead(cacheId))
                {
                    bool isCompressed;
                    isCached = GetVerifiedPath(type, topLevelId, cacheId, out isCompressed) != null;
                }
            }
            catch (Exception e)
            {
                Log(CacheLogLevel.Debug,
                    String.Format("Exception checking if data is cached for cache id {0} :{1}", cacheId, e));
            }
            SetIsCached(cacheId, isCached);
            return isCached;
        }

        private string VerifyDataPath(string topLevelId, string cacheId, out bool isCompressed)
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

        public ByteBufferCacheItem Get(CacheType cacheType, string topLevelId, string cacheId)
        {
            if (!Enabled || topLevelId == null)
                return null;
            ByteBufferCacheItem item;
            var st = new Stopwatch();
            st.Start();
            try
            {
                using (_cacheLock.LockRead(cacheId))
                {
                    bool isCompressed;
                    var path = GetVerifiedPath(cacheType, topLevelId, cacheId, out isCompressed);
                    if (path == null)
                    {
                        ClearIsCached(cacheId);
                        return null;
                    }
                    item = new ByteBufferCacheItem {IsCompressed = isCompressed};
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

            if (topLevelId == null)
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
                    SetIsCached(cacheId, true);
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
            using (var fileStream = new FileStream(filename, FileMode.CreateNew, FileAccess.Write))
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

        /*
        static string DecompressFromFile(string filename)
        {
            using (var fileStream = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                using (var compressionStream = new GZipStream(fileStream, CompressionMode.Decompress))
                {
                    using (var reader = new StreamReader(compressionStream))
                    {
                        return reader.ReadToEnd();
                    }
                }
            }
        }*/
    }
}