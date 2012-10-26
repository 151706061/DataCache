// Copyright (C) 2009 Robert Rossney <rrossney@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;

namespace DataCache
{
    public interface IMemoryCacheItem
    {
        int Size { get; }
    }

    public interface IMemoryCache<T> 
    {
        T this[string key] { get; }
        void Add(string key, T item);
        bool Contains(string key);
        T PopOldestWithSameSize(int incomingSize);
    }


    public class MemoryCache<T> : IMemoryCache<T> where T : IMemoryCacheItem 
    {
        private const long _defaultCapacity = 100000;

        /// <summary>
        /// The default Capacity that the cache uses if none is provided in the constructor.
        /// </summary>
        public static long DefaultCapacity
        {
            get { return _defaultCapacity; }
        }

        // The list of items in the cache.  New items are added to the end of the list;
        // existing items are moved to the end when added; the items thus appear in
        // the list in the order they were added/used, with the least recently used
        // item being the first.  This is internal because the cacheEnumerator
        // needs to access it.
        internal readonly LinkedList<T> List = new LinkedList<T>();

        // The index into the list, used by Add, Remove, and Contains.
        private readonly Dictionary<T, LinkedListNode<T>> _index = new Dictionary<T, LinkedListNode<T>>();

        private readonly Dictionary<string, T> _keyToValue = new Dictionary<string, T>();
        private readonly Dictionary<T, string> _valueToKey = new Dictionary<T, string>();

        // Add, Clear, CopyTo, and Remove lock on this object to keep them threadsafe.
        private readonly object _lock = new object();

        #region Cache Members

        /// <summary>
        /// Initializes a new instance of the cache class that is empty and has the default
        /// capacity.
        /// </summary>
        public MemoryCache() : this(_defaultCapacity)
        {
        }

        /// <summary>
        /// Initializes a new instance of the cache class that is empty and has the specified
        /// initial capacity.
        /// </summary>
        /// <param name="capacityInMb"></param>
        public MemoryCache(long capacityInMb)
        {
            if (capacityInMb < 0)
            {
                throw new InvalidOperationException("Capacity must be positive.");
            }
            Capacity = capacityInMb * 1048576;
        }

        public T this[string key]
        {
            get
            {
                lock (_lock)
                {
                    T item;
                    if (_keyToValue.TryGetValue(key, out item))
                    {
                        if (!List.Last.Equals(item))
                        {
                            List.Remove(_index[item]);
                            _index[item] = List.AddLast(item);
                        }
                    }
                    return item;
                }
            }
        }

        /// <summary>
        /// Occurs when the cache is about to discard its oldest item
        /// because its capacity has been reached and a new item is being added.  
        /// </summary>
        /// <remarks>The item has not been discarded yet, and thus is still contained in 
        /// the Oldest property.</remarks>
        public event EventHandler DiscardingOldestItem;

        /// <summary>
        /// The maximum number of items that the cache can contain without discarding
        /// the oldest item when a new one is added.
        /// </summary>
        public long Capacity { get; private set; }

        public long Size { get; private set; }



        /// <summary>
        /// The oldest (i.e. least recently used) item in the cache.
        /// </summary>
        public T Oldest
        {
            get { return List.First.Value; }
        }

        #endregion

        #region ICollection<T> Members

        /// <summary>
        /// Add an item to the cache, making it the newest item (i.e. the last
        /// item in the list).  If the item is already in the cache, it is moved to the end
        /// of the list and becomes the newest item in the cache.
        /// </summary>
        /// <param name="key"> </param>
        /// <param name="item">The item that is being used.</param>
        /// <remarks>If the cache has a nonzero capacity, and it is at its capacity, this 
        /// method will discard the oldest item, raising the DiscardingOldestItem event before 
        /// it does so.</remarks>
        public void Add(string key, T item)
        {
            lock (_lock)
            {
                //if already in cache, then just move to "front" of list
                if (_index.ContainsKey(item))
                {
                    List.Remove(_index[item]);
                    _index[item] = List.AddLast(item);
                    return;
                }

                while (Size >= Capacity && Capacity != 0)
                {
                    EventHandler h = DiscardingOldestItem;
                    if (h != null)
                    {
                        h(this, new EventArgs());
                    }
                    Remove(Oldest);
                }
                _index.Add(item, List.AddLast(item));
                _keyToValue.Add(key, item);
                _valueToKey.Add(item, key);
                Size += item.Size;
            }
        }

        public T PopOldestWithSameSize(int incomingSize)
        {
            lock (_lock)
            {
                if (List.Count == 0)
                    return default(T);

                T itemToPop = default(T);
                if ((Oldest.Size == incomingSize) && (Size + incomingSize > Capacity))
                {
                    itemToPop = Oldest;
                    Remove(Oldest);
                }
                return itemToPop;
            }
            
        }

        /// <summary>
        /// Determines whether the cache contains a specific value.
        /// </summary>
        /// <param name="item">The item to locate in the cache.</param>
        /// <returns>true if the item is in the cache, otherwise false.</returns>
        public bool Contains(T item)
        {
            return _index.ContainsKey(item);
        }


        public bool Contains(string key)
        {
            return _keyToValue.ContainsKey(key);
        }


        /// <summary>
        /// Clear the contents of the cache.
        /// </summary>
        public void Clear()
        {
            lock (_lock)
            {
                List.Clear();
                _index.Clear();
                _keyToValue.Clear();
                _valueToKey.Clear();
                Size = 0;
            }
        }

        /// <summary>
        /// Gets the number of items contained in the cache.
        /// </summary>
        public int Count
        {
            get { return List.Count; }
        }

        /// <summary>
        /// Gets a value indicating whether the cache is read-only.
        /// </summary>
        public bool IsReadOnly
        {
            get { return false; }
        }

        /// <summary>
        /// Remove the specified item from the cache.
        /// </summary>
        /// <param name="item">The item to remove from the cache.</param>
        /// <returns>true if the item was successfully removed from the cache,
        /// otherwise false.  This method also returns false if the item was not
        /// found in the cache.</returns>
        public bool Remove(T item)
        {
            lock (_lock)
            {
                if (_index.ContainsKey(item))
                {
                    List.Remove(_index[item]);
                    _index.Remove(item);
                    var key = _valueToKey[item];
                    _valueToKey.Remove(item);
                    _keyToValue.Remove(key);
                    Size -= item.Size;
                    return true;
                }
                return false;
            }
        }

        #endregion

    }
}