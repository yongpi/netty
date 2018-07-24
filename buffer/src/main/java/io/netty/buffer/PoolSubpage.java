/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

final class PoolSubpage<T> {

    final PoolChunk<T> chunk;
    //poolChunk 的 page对应的 memoryMapId
    private final int memoryMapIdx;
    //该PoolSubpage在poolChunk上的偏移量
    private final int runOffset;
    //poolChunk 的page的单个大小
    private final int pageSize;
    private final long[] bitmap;

    PoolSubpage<T> prev;
    PoolSubpage<T> next;

    boolean doNotDestroy;
    //element大小
    int elemSize;
    //最多elements数
    private int maxNumElems;
    //[]bitmap的实际length
    private int bitmapLength;
    //下一个可用的内存位置：对应bitmap和long中位数
    private int nextAvail;
    //可用的个数
    private int numAvail;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /** Special constructor that creates a linked list head */
    PoolSubpage(int pageSize) {
        chunk = null;
        memoryMapIdx = -1;
        runOffset = -1;
        elemSize = -1;
        this.pageSize = pageSize;
        bitmap = null;
    }

    PoolSubpage(PoolChunk<T> chunk, int memoryMapIdx, int runOffset, int pageSize, int elemSize) {
        this.chunk = chunk;
        this.memoryMapIdx = memoryMapIdx;
        this.runOffset = runOffset;
        this.pageSize = pageSize;
        //因为netty每次分配内存最少是16字节，而long的长度为64位，每个long的每一位都可以对应内存的占用情况（占用1,没占用0）
        //所以最多需要8个long
        bitmap = new long[pageSize >>> 10]; // pageSize / 16 / 64
        init(elemSize);
    }

    void init(int elemSize) {
        doNotDestroy = true;
        this.elemSize = elemSize;
        if (elemSize != 0) {
            //elemSize最小为16,maxNumElems最多为512
            maxNumElems = numAvail = pageSize / elemSize;
            nextAvail = 0;
            //bitmapLength最大为8
            bitmapLength = maxNumElems >>> 6;
            //大于0小于64bitmapLength需要加一,比如，当maxNumElems=500时，bitmapLength=7，但是此时8才符合
            if ((maxNumElems & 63) != 0) {
                bitmapLength ++;
            }

            for (int i = 0; i < bitmapLength; i ++) {
                bitmap[i] = 0;
            }
        }

        addToPool();
    }

    /**
     * Returns the bitmap index of the subpage allocation.
     */
    long allocate() {
        if (elemSize == 0) {
            return toHandle(0);
        }

        if (numAvail == 0 || !doNotDestroy) {
            return -1;
        }

        final int bitmapIdx = getNextAvail();
        //除以64获取bitmap的索引
        int q = bitmapIdx >>> 6;
        //获取long 64位中的第几位
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) == 0;
        //把q索引对应的值的第r位置为1
        bitmap[q] |= 1L << r;

        if (-- numAvail == 0) {
            removeFromPool();
        }

        return toHandle(bitmapIdx);
    }

    /**
     * @return {@code true} if this subpage is in use.
     *         {@code false} if this subpage is not used by its chunk and thus it's OK to be released.
     */
    boolean free(int bitmapIdx) {

        if (elemSize == 0) {
            return true;
        }

        int q = bitmapIdx >>> 6;
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) != 0;
        bitmap[q] ^= 1L << r;

        setNextAvail(bitmapIdx);

        if (numAvail ++ == 0) {
            addToPool();
            return true;
        }

        if (numAvail != maxNumElems) {
            return true;
        } else {
            // Subpage not in use (numAvail == maxNumElems)
            if (prev == next) {
                // Do not remove if this subpage is the only one left in the pool.
                return true;
            }

            // Remove this subpage from the pool if there are other subpages left in the pool.
            doNotDestroy = false;
            removeFromPool();
            return false;
        }
    }

    private void addToPool() {
        PoolSubpage<T> head = chunk.arena.findSubpagePoolHead(elemSize);
        assert prev == null && next == null;
        //把this插入到链表中 head<->next 变成 head<->this<->next
        prev = head;
        next = head.next;
        next.prev = this;
        head.next = this;
    }

    private void removeFromPool() {
        assert prev != null && next != null;
        prev.next = next;
        next.prev = prev;
        next = null;
        prev = null;
    }

    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    private int getNextAvail() {
        int nextAvail = this.nextAvail;
        if (nextAvail >= 0) {
            this.nextAvail = -1;
            return nextAvail;
        }
        return findNextAvail();
    }

    /**
     * 返回下个可用的位置，数组位置加上位于long的位数
     * @return
     */
    private int findNextAvail() {
        final long[] bitmap = this.bitmap;
        final int bitmapLength = this.bitmapLength;
        for (int i = 0; i < bitmapLength; i ++) {
            long bits = bitmap[i];
            //从第一个long开始找
            if (~bits != 0) {
                return findNextAvail0(i, bits);
            }
        }
        return -1;
    }

    private int findNextAvail0(int i, long bits) {
        final int maxNumElems = this.maxNumElems;
        //把索引转换成对应的个数基数
        final int baseVal = i << 6;

        for (int j = 0; j < 64; j ++) {
            //最低位为0，表示没被占用
            if ((bits & 1) == 0) {
                //表示baseVal+j
                int val = baseVal | j;
                if (val < maxNumElems) {
                    return val;
                } else {
                    break;
                }
            }
            //被占用右移一位
            bits >>>= 1;
        }
        return -1;
    }

    private long toHandle(int bitmapIdx) {
        //bitmapIdx最大为512+64，所以前32位是bitmapIdx,后二十位是memoryMapIdx
        return 0x4000000000000000L | (long) bitmapIdx << 32 | memoryMapIdx;
    }

    public String toString() {
        if (!doNotDestroy) {
            return "(" + memoryMapIdx + ": not in use)";
        }

        return String.valueOf('(') + memoryMapIdx + ": " + (maxNumElems - numAvail) + '/' + maxNumElems +
               ", offset: " + runOffset + ", length: " + pageSize + ", elemSize: " + elemSize + ')';
    }
}
