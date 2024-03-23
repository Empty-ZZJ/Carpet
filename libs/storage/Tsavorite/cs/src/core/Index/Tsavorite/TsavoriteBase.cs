using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    unsafe struct InternalHashTable
    {
        public long size;
        public long size_mask;
        public int size_bits;
        public HashBucket[] tableRaw;
        public HashBucket* tableAligned;
    }

    public unsafe partial class TsavoriteBase
    {
        // Initial size of the table
        internal long minTableSize = 16;

        // Allocator for the hash buckets
        internal MallocFixedPageSize<HashBucket> overflowBucketsAllocator;
        internal MallocFixedPageSize<HashBucket> overflowBucketsAllocatorResize;

        // An array of size two, that contains the old and new versions of the hash-table
        internal InternalHashTable[] state = new InternalHashTable[2];

        // Array used to denote if a specific chunk is merged or not
        internal long[] splitStatus;

        // Used as an atomic counter to check if resizing is complete
        internal long numPendingChunksToBeSplit;

        internal LightEpoch epoch;

        internal ResizeInfo resizeInfo;

        /// <summary>
        /// LoggerFactory
        /// </summary>
        protected ILoggerFactory loggerFactory;

        /// <summary>
        /// Logger
        /// </summary>
        protected ILogger logger;

        /// <summary>
        /// Constructor
        /// </summary>
        public TsavoriteBase(ILogger logger = null)
        {
            epoch = new LightEpoch();
            overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>(logger);
        }

        internal void Free()
        {
            Free(0);
            Free(1);
            epoch.Dispose();
            overflowBucketsAllocator.Dispose();
        }

        void Free(int version)
        {
        }

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="size"></param>
        /// <param name="sector_size"></param>
        public void Initialize(long size, int sector_size)
        {
            if (!Utility.IsPowerOfTwo(size))
            {
                throw new ArgumentException("Size {0} is not a power of 2");
            }
            if (!Utility.Is32Bit(size))
            {
                throw new ArgumentException("Size {0} is not 32-bit");
            }

            minTableSize = size;
            resizeInfo = default;
            resizeInfo.status = ResizeOperationStatus.DONE;
            resizeInfo.version = 0;
            Initialize(resizeInfo.version, size, sector_size);
        }

        /// <summary>
        /// Initialize，修复了内存溢出的问题
        /// </summary>
        /// <param name="version"></param>
        /// <param name="size"></param>
        /// <param name="sector_size"></param>
        internal void Initialize(int version, long size, int sector_size)
        {
            var size_bytes = size * sizeof(HashBucket);
            var aligned_size_bytes = sector_size + ((size_bytes + (sector_size - 1)) & ~(sector_size - 1));

            // 预分配并按照缓存行对齐表格
            state[version].size = size;
            state[version].size_mask = size - 1;
            state[version].size_bits = Utility.GetLogBase2((int)size);

            var unmanagedMemory = Marshal.AllocHGlobal((int)(aligned_size_bytes / Constants.kCacheLineBytes) * sizeof(HashBucket));
            var sectorAlignedPointer = ((long)unmanagedMemory + (sector_size - 1)) & ~(sector_size - 1);
            state[version].tableAligned = (HashBucket*)sectorAlignedPointer;
        }
        /// <summary>
        /// 老的初始化方法，有内存溢出的问题
        /// </summary>
        /// <param name="version"></param>
        /// <param name="size"></param>
        /// <param name="sector_size"></param>
        internal void _Initialize(int version, long size, int sector_size)
        {
            long size_bytes = size * sizeof(HashBucket);
            long aligned_size_bytes = sector_size +
                ((size_bytes + (sector_size - 1)) & ~(sector_size - 1));

            //Over-allocate and align the table to the cacheline
            state[version].size = size;
            state[version].size_mask = size - 1;
            state[version].size_bits = Utility.GetLogBase2((int)size);

            state[version].tableRaw = GC.AllocateArray<HashBucket>((int)(aligned_size_bytes / Constants.kCacheLineBytes), true);
            long sectorAlignedPointer = ((long)Unsafe.AsPointer(ref state[version].tableRaw[0]) + (sector_size - 1)) & ~(sector_size - 1);
            state[version].tableAligned = (HashBucket*)sectorAlignedPointer;
        }

        ///<summary>
        /// 一个辅助函数，用于在指定版本的哈希表中查找与键对应的插槽
        /// </summary>
        ///<returns>如果存在这样的插槽，则返回true<paramref name="hei"/>，否则返回false</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool FindTag(ref HashEntryInfo hei)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);
            var version = resizeInfo.version;
            var masked_entry_word = hei.hash & state[version].size_mask;
            hei.firstBucket = hei.bucket = state[version].tableAligned + masked_entry_word;
            hei.slot = Constants.kInvalidEntrySlot;
            hei.entry = default;
            hei.bucketIndex = masked_entry_word;

            do
            {
                // 在桶中搜索键。最后一个条目是为溢出指针保留的。
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)hei.bucket) + index);
                    if (0 == target_entry_word)
                        continue;

                    hei.entry.word = target_entry_word;
                    if (hei.tag == hei.entry.Tag && !hei.entry.Tentative)
                    {
                        hei.slot = index;
                        return true;
                    }
                }

                // 转到链中的下一个桶（如果它是一个非零溢出分配）
                target_entry_word = *(((long*)hei.bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                if (target_entry_word == 0)
                {
                    // 我们锁定第一个桶，所以它不能被清除。
                    hei.bucket = default;
                    hei.entry = default;
                    return false;
                }
                hei.bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word);
            } while (true);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void FindOrCreateTag(ref HashEntryInfo hei, long BeginAddress)
        {
            var version = resizeInfo.version;
            var masked_entry_word = hei.hash & state[version].size_mask;
            hei.bucketIndex = masked_entry_word;

            while (true)
            {
                hei.firstBucket = hei.bucket = state[version].tableAligned + masked_entry_word;
                hei.slot = Constants.kInvalidEntrySlot;

                if (FindTagOrFreeInternal(ref hei, BeginAddress))
                    return;

                // 在空闲插槽中安装临时标签
                hei.entry = default;
                hei.entry.Tag = hei.tag;
                hei.entry.Address = Constants.kTempInvalidAddress;
                hei.entry.Tentative = true;

                // 将此标签插入到此插槽中。失败意味着另一个会话将键插入到该插槽中，因此继续循环以查找另一个空闲插槽。
                if (0 == Interlocked.CompareExchange(ref hei.bucket->bucket_entries[hei.slot], hei.entry.word, 0))
                {
                    // 确保此标签不在其他插槽中；如果是，则将此插槽设为“可用”并继续搜索循环。
                    var orig_bucket = state[version].tableAligned + masked_entry_word;  // TODO local var not used; use or change to byval param
                    var orig_slot = Constants.kInvalidEntrySlot;                        // TODO local var not used; use or change to byval param

                    if (FindOtherSlotForThisTagMaybeTentativeInternal(hei.tag, ref orig_bucket, ref orig_slot, hei.bucket, hei.slot))
                    {
                        // 我们通过CAS拥有此插槽，因此可以非CAS将0放回
                        hei.bucket->bucket_entries[hei.slot] = 0;
                        // TODO: Why not return orig_bucket and orig_slot if it's not Tentative?
                    }
                    else
                    {
                        hei.entry.Tentative = false;
                        *((long*)hei.bucket + hei.slot) = hei.entry.word;
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// Find existing entry (non-tentative) entry.
        /// </summary>
        /// <returns>If found, return the slot it is in, else return a pointer to some empty slot (which we may have allocated)</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool FindTagOrFreeInternal(ref HashEntryInfo hei, long BeginAddress = 0)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)hei.bucket) + index);
                    if (0 == target_entry_word)
                    {
                        if (hei.slot == Constants.kInvalidEntrySlot)
                        {
                            // Record the free slot and continue to search for the key
                            hei.slot = index;
                            entry_slot_bucket = hei.bucket;
                        }
                        continue;
                    }

                    // If the entry points to an address that has been truncated, it's free; try to reclaim it by setting its word to 0.
                    hei.entry.word = target_entry_word;
                    if (hei.entry.Address < BeginAddress && hei.entry.Address != Constants.kTempInvalidAddress)
                    {
                        if (hei.entry.word == Interlocked.CompareExchange(ref hei.bucket->bucket_entries[index], Constants.kInvalidAddress, target_entry_word))
                        {
                            if (hei.slot == Constants.kInvalidEntrySlot)
                            {
                                // Record the free slot and continue to search for the key
                                hei.slot = index;
                                entry_slot_bucket = hei.bucket;
                            }
                            continue;
                        }
                    }
                    if (hei.tag == hei.entry.Tag && !hei.entry.Tentative)
                    {
                        hei.slot = index;
                        return true;
                    }
                }

                // Go to next bucket in the chain (if it is a nonzero overflow allocation). Don't mask off the non-address bits here; they're needed for CAS.
                target_entry_word = *(((long*)hei.bucket) + Constants.kOverflowBucketIndex);
                while ((target_entry_word & Constants.kAddressMask) == 0)
                {
                    // There is no next bucket. If slot is Constants.kInvalidEntrySlot then we did not find an empty slot, so must allocate a new bucket.
                    if (hei.slot == Constants.kInvalidEntrySlot)
                    {
                        // Allocate new bucket
                        var logicalBucketAddress = overflowBucketsAllocator.Allocate();
                        var physicalBucketAddress = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(logicalBucketAddress);
                        long compare_word = target_entry_word;
                        target_entry_word = logicalBucketAddress;
                        target_entry_word |= compare_word & ~Constants.kAddressMask;

                        long result_word = Interlocked.CompareExchange(
                            ref hei.bucket->bucket_entries[Constants.kOverflowBucketIndex],
                            target_entry_word,
                            compare_word);

                        if (compare_word != result_word)
                        {
                            // Install of new bucket failed; free the allocation and and continue the search using the winner's entry
                            overflowBucketsAllocator.Free(logicalBucketAddress);
                            target_entry_word = result_word;
                            continue;
                        }

                        // Install of new overflow bucket succeeded; the tag was not found, so return the first slot of the new bucket
                        hei.bucket = physicalBucketAddress;
                        hei.slot = 0;
                        hei.entry = default;
                        return false;   // tag was not found
                    }

                    // Tag was not found and an empty slot was found, so return the empty slot
                    hei.bucket = entry_slot_bucket;
                    hei.entry = default;
                    return false;       // tag was not found
                }

                // The next bucket was there or was allocated. Move to it.
                hei.bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word & Constants.kAddressMask);
            } while (true);
        }


        /// <summary>
        /// Look for an existing entry (tentative or otherwise) for this hash/tag, other than the specified "except for this" bucket/slot.
        /// </summary>
        /// <returns>True if found, else false. Does not return a free slot.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool FindOtherSlotForThisTagMaybeTentativeInternal(ushort tag, ref HashBucket* bucket, ref int slot, HashBucket* except_bucket, int except_entry_slot)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)bucket) + index);
                    if (0 == target_entry_word)
                        continue;

                    HashBucketEntry entry = default;
                    entry.word = target_entry_word;
                    if (tag == entry.Tag)
                    {
                        if ((except_entry_slot == index) && (except_bucket == bucket))
                            continue;
                        slot = index;
                        return true;
                    }
                }

                // Go to next bucket in the chain (if it is a nonzero overflow allocation).
                target_entry_word = *(((long*)bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                if (target_entry_word == 0)
                    return false;
                bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word);
            } while (true);
        }

        /// <summary>
        /// Helper function used to update the slot atomically with the
        /// new offset value using the CAS operation
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="entrySlot"></param>
        /// <param name="expected"></param>
        /// <param name="desired"></param>
        /// <param name="found"></param>
        /// <returns>If atomic update was successful</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool UpdateSlot(HashBucket* bucket, int entrySlot, long expected, long desired, out long found)
        {
            found = Interlocked.CompareExchange(ref bucket->bucket_entries[entrySlot], desired, expected);
            return found == expected;
        }
    }
}