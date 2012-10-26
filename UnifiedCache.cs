using System;

namespace DataCache
{
    public interface IDataCache
    {
        bool IsDiskCacheEnabled { get; }
        PixelCacheItem Get(string topLevelId, string cacheId);
        PutResponse Put(string topLevelId, string cacheId, PixelCacheItem pixelCacheItem);
        bool IsCachedToDisk(string topLevelId, string cacheId);
    }


    public class UnifiedCache : IDataCache
    {
        private readonly IDataCache _diskCache;
        private readonly IMemoryCache<PixelCacheItem> _memoryCache; 

        public UnifiedCache(IDiskCacheLogger logger)
        {
            _diskCache = new DiskCache(logger);
            _memoryCache = new MemoryCache<PixelCacheItem>(CacheSettings.Default.MemoryCacheCapacityInMb);
        }

        public bool IsDiskCacheEnabled
        {
            get { return _diskCache.IsDiskCacheEnabled; }
        }

        public PixelCacheItem Get(string topLevelId, string cacheId)
        {
            if (_memoryCache.Contains(cacheId))
            {
                return _memoryCache[cacheId];
            }

            var item = _diskCache.Get(topLevelId, cacheId);
            var possibleOldBuffer = _memoryCache.PopOldestWithSameSize(item.Size);
            var newBuffer = possibleOldBuffer != null ? possibleOldBuffer.PixelData : new byte[item.Size];
            Buffer.BlockCopy(item.PixelData, 0, newBuffer, 0, item.Size);
            _memoryCache.Add(cacheId, new PixelCacheItem
                                          {
                                              IsCompressed = item.IsCompressed,
                                              PixelData =  newBuffer,
                                              Size =  item.Size
                                          });
            return item;
        }

        public PutResponse Put(string topLevelId, string cacheId, PixelCacheItem pixelCacheItem)
        {
            return _diskCache.Put(topLevelId, cacheId, pixelCacheItem);
        }

        public bool IsCachedToDisk(string topLevelId, string cacheId)
        {
            return _diskCache.IsCachedToDisk(topLevelId, cacheId);
        }
    }
}