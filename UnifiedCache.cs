using System;

namespace DataCache
{
    public class GetContext
    {
        public const int UnsetConversionBufferSize = -1;
        public Func<byte[], int, byte[]> Decompressor { get; set; } 
        public Func<byte[], byte[], byte[]> PostProcessor { get; set; }
        public int ConversionBufferSize { get; set; }
    }


    public interface IUnifiedCache
    {
        bool IsDiskCacheEnabled { get; }
        PixelCacheItem Get(string topLevelId, string cacheId, GetContext context);
        PutResponse Put(string topLevelId, string cacheId, PixelCacheItem pixelCacheItem);
        bool IsCachedToDisk(string topLevelId, string cacheId);
        void ClearFromMemory(string cacheId);
    }


    public class UnifiedCache : IUnifiedCache
    {
        private readonly IDiskCache _diskCache;
        private readonly IMemoryCache<PixelCacheItem> _memoryCache;

        [ThreadStatic] public static DynamicBuffer ConversionBuffer;
        [ThreadStatic] public static DynamicBuffer DecompressBuffer;

        public UnifiedCache(IDiskCacheLogger logger)
        {
            _diskCache = new DiskCache(logger);
            _memoryCache = new MemoryCache<PixelCacheItem>(CacheSettings.Default.MemoryCacheCapacityInMb);
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
                    var conversionBuff = GetConversionBuffer();
                    conversionBuff.Resize(context.ConversionBufferSize, false);
                    item.PixelData = context.PostProcessor(item.PixelData, conversionBuff.Buffer);
                    if (context.ConversionBufferSize != GetContext.UnsetConversionBufferSize)
                        item.Size = context.ConversionBufferSize;
                }

                //3. create a new buffer if item.PixelData uses a thread static scratch buffer
                // (i.e. uncompressed or colour converted)
                if (!fromCompressed || context.ConversionBufferSize != GetContext.UnsetConversionBufferSize)
                {
                    var possibleOldBuffer = _memoryCache.PopOldestWithSameSize(item.Size);
                    var newBuffer = possibleOldBuffer != null ? possibleOldBuffer.PixelData : new byte[item.Size];
                    Buffer.BlockCopy(item.PixelData, 0, newBuffer, 0, item.Size);
                    item.PixelData = newBuffer;
                }

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
                _memoryCache.Add(cacheId, pixelCacheItem);
            }
            return response;
        }

        public bool IsCachedToDisk(string topLevelId, string cacheId)
        {
            return _diskCache.IsCached(topLevelId, cacheId);
        }

        public void ClearFromMemory(string cacheId)
        {
            _memoryCache.Remove(cacheId);
        }

        private static DynamicBuffer GetConversionBuffer()
        {
            return ConversionBuffer ?? (ConversionBuffer = new DynamicBuffer());
        }

        private static DynamicBuffer GetDecompressBuffer()
        {
            return DecompressBuffer?? (DecompressBuffer = new DynamicBuffer());
        }

    }
}