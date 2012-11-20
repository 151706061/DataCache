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

namespace DataCache
{
    public interface ICacheLogger
    {
        void Log(CacheLogLevel level, string message);
    }

    public class GetContext
    {
        public const int UnsetConversionBufferSize = -1;
        public Func<byte[], int, byte[]> Decompressor { get; set; }
        public Func<byte[], byte[]> PostProcessor { get; set; }
        public int ConversionBufferSize { get; set; }
    }

    public enum CacheType
    {
        Pixels,
        String
    }

    public interface IUnifiedCache
    {
        bool IsDiskCacheEnabled { get; }
        ByteBufferCacheItem Get(CacheType cacheType, string topLevelId, string cacheId, GetContext context);
        PutResponse Put(string topLevelId, string cacheId, ByteBufferCacheItem byteBufferCacheItem);
        void Put(string cacheId, ByteBufferCacheItem byteBufferCacheItem);
        ByteBufferCacheItem Get(string cacheId);
        PutResponse Put(string topLevelId, string cacheId, StringCacheItem stringCacheItem);
        bool IsCachedToDisk(CacheType type, string topLevelId, string cacheId);
        void ClearCachedToDisk(string cacheId);
        void ClearFromMemory(string cacheId);
    }


    public class UnifiedCache : IUnifiedCache
    {
        private readonly IDiskCache _diskCache;
        private readonly IDictionary<CacheType, IMemoryCache<ByteBufferCacheItem>>  _memoryCaches;


        public UnifiedCache(ICacheLogger logger)
        {
            _diskCache = new DiskCache(logger);
            _memoryCaches = new Dictionary<CacheType, IMemoryCache<ByteBufferCacheItem>>();
            _memoryCaches.Add(CacheType.Pixels, new MemoryCache<ByteBufferCacheItem>(CacheSettings.Default.PixelMemoryCacheCapacityInMb, logger));
            _memoryCaches.Add(CacheType.String, new MemoryCache<ByteBufferCacheItem>(CacheSettings.Default.StringMemoryCacheCapacityInMb, logger));
        }

        #region IUnifiedCache Members

        public bool IsDiskCacheEnabled
        {
            get { return _diskCache.Enabled; }
        }

        public ByteBufferCacheItem Get(CacheType cacheType, string topLevelId, string cacheId, GetContext context)
        {
            if (_memoryCaches[cacheType].Contains(cacheId))
            {
                return _memoryCaches[cacheType][cacheId];
            }

            ByteBufferCacheItem item = _diskCache.Get(cacheType, topLevelId, cacheId);
            if (item != null)
            {
                if (context != null)
                {
                    var fromCompressed = item.IsCompressed;

                    //1. decompress
                    if (fromCompressed)
                    {
                        if (context.Decompressor == null)
                            throw new Exception("Cannot decompress pixel data: null decompressor");

                        item.Data = context.Decompressor(item.Data, item.Size);
                        item.Size = item.Data.Length;
                        item.IsCompressed = false;
                    }

                    //2. post process
                    if (context.PostProcessor != null)
                    {
                        item.Data = context.PostProcessor(item.Data);
                        if (context.ConversionBufferSize != GetContext.UnsetConversionBufferSize)
                            item.Size = context.ConversionBufferSize;
                    }

                    //3. set buffer
                    var possibleOldBuffer = _memoryCaches[cacheType].PopOldestWithSameSize(item.Size);
                    var newBuffer = possibleOldBuffer != null ? possibleOldBuffer.Data : new byte[item.Size];
                    Buffer.BlockCopy(item.Data, 0, newBuffer, 0, item.Size);
                    item.Data = newBuffer;
                }
 
                //cache in memory
                _memoryCaches[cacheType].Add(cacheId, item);
            }
            return item;
        }

        public PutResponse Put(string topLevelId, string cacheId, ByteBufferCacheItem byteBufferCacheItem)
        {
            var response = _diskCache.Put(topLevelId, cacheId, byteBufferCacheItem);
            if (response == PutResponse.Disabled)
            {
                if (byteBufferCacheItem.Data != null)
                {
                    _memoryCaches[CacheType.Pixels].Add(cacheId, byteBufferCacheItem);
                }
            }
            return response;
        }

        public PutResponse Put(string topLevelId, string cacheId, StringCacheItem stringCacheItem)
        {
            return _diskCache.Put(topLevelId, cacheId, stringCacheItem);
        }

        public void Put(string cacheId, ByteBufferCacheItem byteBufferCacheItem)
        {
            _memoryCaches[CacheType.Pixels].Add(cacheId, byteBufferCacheItem);
        }

        public ByteBufferCacheItem Get(string cacheId)
        {
            if (_memoryCaches[CacheType.Pixels].Contains(cacheId))
            {
                return _memoryCaches[CacheType.Pixels][cacheId];
            }
            return null;
        }

        public bool IsCachedToDisk(CacheType type, string topLevelId, string cacheId)
        {
            return _diskCache.IsCached(type, topLevelId, cacheId);
        }

        public void ClearCachedToDisk(string cacheId)
        {
            _diskCache.ClearIsCached(cacheId);
        }

        public void ClearFromMemory(string cacheId)
        {
            _memoryCaches[CacheType.Pixels].Remove(cacheId);
        }

        #endregion

    }
}