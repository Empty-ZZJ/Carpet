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
        [Obsolete]
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

        ///<summary>
        /// 查找现有的（非临时）条目。
        ///<returns>如果找到，则返回它所在的插槽，否则返回一个指向某个空插槽的指针（可能已经分配了）</returns>
        ///</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool FindTagOrFreeInternal(ref HashEntryInfo hei, long BeginAddress = 0)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);
            do
            {
                // 在桶中搜索键。最后一个条目是为溢出指针保留的。
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)hei.bucket) + index);
                    if (0 == target_entry_word)
                    {
                        if (hei.slot == Constants.kInvalidEntrySlot)
                        {
                            // 记录空闲插槽并继续搜索键
                            hei.slot = index;
                            entry_slot_bucket = hei.bucket;
                        }
                        continue;
                    }

                    // 如果条目指向已被截断的地址，则它是空闲的；尝试通过将其字设置为0来回收它。
                    hei.entry.word = target_entry_word;
                    if (hei.entry.Address < BeginAddress && hei.entry.Address != Constants.kTempInvalidAddress)
                    {
                        if (hei.entry.word == Interlocked.CompareExchange(ref hei.bucket->bucket_entries[index], Constants.kInvalidAddress, target_entry_word))
                        {
                            if (hei.slot == Constants.kInvalidEntrySlot)
                            {
                                // 记录空闲插槽并继续搜索键
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

                // 转到链中的下一个桶（如果它是一个非零溢出分配）。不要在这里屏蔽非地址位，因为CAS需要它们。
                target_entry_word = *(((long*)hei.bucket) + Constants.kOverflowBucketIndex);
                while ((target_entry_word & Constants.kAddressMask) == 0)
                {
                    // 没有下一个桶。如果插槽是Constants.kInvalidEntrySlot，则我们没有找到空插槽，因此必须分配新桶。
                    if (hei.slot == Constants.kInvalidEntrySlot)
                    {
                        // 分配新桶
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
                            // 新桶的安装失败；释放分配并继续使用赢家的条目
                            overflowBucketsAllocator.Free(logicalBucketAddress);
                            target_entry_word = result_word;
                            continue;
                        }

                        // 新溢出桶的安装成功；标签未找到，因此返回新桶的第一个插槽
                        hei.bucket = physicalBucketAddress;
                        hei.slot = 0;
                        hei.entry = default;
                        return false;   // 标签未找到
                    }

                    // 标签未找到且找到了空插槽，因此返回空插槽
                    hei.bucket = entry_slot_bucket;
                    hei.entry = default;
                    return false;       // 标签未找到
                }

                // 下一个桶已存在或已分配。转到它。
                hei.bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word & Constants.kAddressMask);
            } while (true);
        }

        //<summary>
        /// 查找此哈希/标签的现有条目（可能是临时的），除了指定的“除此之外”桶/插槽。
        /// </summary>
        ///<returns>如果找到，则返回true，否则返回false。不返回空闲插槽。</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool FindOtherSlotForThisTagMaybeTentativeInternal(ushort tag, ref HashBucket* bucket, ref int slot, HashBucket* except_bucket, int except_entry_slot)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);
            do
            {
                // 在桶中搜索我们的键。最后一个条目是为溢出指针保留的。
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

                // 转到链中的下一个桶（如果它是一个非零溢出分配）。
                target_entry_word = *(((long*)bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                if (target_entry_word == 0)
                    return false;
                bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word);
            } while (true);
        }

        ///<summary>
        /// 用于使用CAS操作以原子方式更新插槽的辅助函数，新的偏移值
        ///<param name="bucket"></param>
        ///<param name="entrySlot"></param>
        ///<param name="expected"></param>
        ///<param name="desired"></param>
        ///<param name="found"></param>
        ///<returns>是否更新成功</returns>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool UpdateSlot(HashBucket* bucket, int entrySlot, long expected, long desired, out long found)
        {
            found = Interlocked.CompareExchange(ref bucket->bucket_entries[entrySlot], desired, expected);
            return found == expected;
        }
    }
}