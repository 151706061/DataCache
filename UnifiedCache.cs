﻿﻿#region License

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
using System.Threading;

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


    public interface IUnifiedCache
    {
        bool IsDiskCacheEnabled { get; }
        PixelCacheItem Get(string topLevelId, string cacheId, GetContext context);
        PutResponse Put(string topLevelId, string cacheId, PixelCacheItem pixelCacheItem);
        void Put(string cacheId, PixelCacheItem pixelCacheItem);
        PixelCacheItem Get(string cacheId);
        bool IsCachedToDisk(string topLevelId, string cacheId);
        void ClearFromMemory(string cacheId);
    }


    public class UnifiedCache : IUnifiedCache
    {
        private readonly IDiskCache _diskCache;
        private readonly IMemoryCache<PixelCacheItem> _memoryCache;



        public UnifiedCache(ICacheLogger logger)
        {
            _diskCache = new DiskCache(logger);
            _memoryCache = new MemoryCache<PixelCacheItem>(CacheSettings.Default.MemoryCacheCapacityInMb, logger);
        }

        public bool IsDiskCacheEnabled
        {
            get { return _diskCache.Enabled; }
        }

        public PixelCacheItem Get(string topLevelId, string cacheId, GetContext context)
        {
            if (_memoryCache.Contains(cacheId))
            {
                return _memoryCache[cacheId];
            }

            var item = _diskCache.Get(topLevelId, cacheId);
            if (item != null)
            {
                bool fromCompressed = item.IsCompressed;

                //1. decompress
                if (fromCompressed)
                {
                    if (context.Decompressor == null)
                        throw new Exception("Cannot decompress pixel data: null decompressor");

                    item.PixelData = context.Decompressor(item.PixelData, item.Size);
                    item.Size = item.PixelData.Length;
                    item.IsCompressed = false;
                }

                //2. post process
                if (context.PostProcessor != null)
                {
                    item.PixelData = context.PostProcessor(item.PixelData);
                    if (context.ConversionBufferSize != GetContext.UnsetConversionBufferSize)
                        item.Size = context.ConversionBufferSize;
                }

                //3. create new buffer and copy data in
                var possibleOldBuffer = _memoryCache.PopOldestWithSameSize(item.Size);
                var newBuffer = possibleOldBuffer != null ? possibleOldBuffer.PixelData : new byte[item.Size];
                Buffer.BlockCopy(item.PixelData, 0, newBuffer, 0, item.Size);
                item.PixelData = newBuffer;
               

                //4. cache in memory
                _memoryCache.Add(cacheId, item);       
            }        
            return item;
        }

        public PutResponse Put(string topLevelId, string cacheId, PixelCacheItem pixelCacheItem)
        {
            var response  =  _diskCache.Put(topLevelId, cacheId, pixelCacheItem);
            if (response == PutResponse.Disabled)
            {
                if (pixelCacheItem.PixelData != null)
                {
                    _memoryCache.Add(cacheId, pixelCacheItem);
                }
             }
            return response;
        }

        public void Put(string cacheId, PixelCacheItem pixelCacheItem)
        {
            _memoryCache.Add(cacheId, pixelCacheItem);
        }

        public PixelCacheItem Get(string cacheId)
        {
            if (_memoryCache.Contains(cacheId))
            {
                return _memoryCache[cacheId];
            }
            return null;
        }

        public bool IsCachedToDisk(string topLevelId, string cacheId)
        {
            return _diskCache.IsCached(topLevelId, cacheId);
        }

        public void ClearFromMemory(string cacheId)
        {
            _memoryCache.Remove(cacheId);
        }

  

    }
}