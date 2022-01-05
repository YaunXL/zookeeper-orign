/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.recipes.queue;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A <a href="package.html">protocol to implement a distributed queue</a>.
 * 使用zk实现一个分布式队列
 */
public class DistributedQueue {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedQueue.class);

    private final String dir;

    private ZooKeeper zookeeper;
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private final String prefix = "qn-";

    public DistributedQueue(ZooKeeper zookeeper, String dir, List<ACL> acl) {
        this.dir = dir;

        if (acl != null) {
            this.acl = acl;
        }
        this.zookeeper = zookeeper;

    }

    /**
     * Returns a Map of the children, ordered by id.
     * 返回所有孩子节点，按编号排序
     * @param watcher optional watcher on getChildren() operation.
     * @return map from id to child name for all children
     */
    private Map<Long, String> orderedChildren(Watcher watcher) throws KeeperException, InterruptedException {
        Map<Long, String> orderedChildren = new TreeMap<>();

        List<String> childNames;
        //获取所有孩子节点，并设置监听
        childNames = zookeeper.getChildren(dir, watcher);

        for (String childName : childNames) {
            try {
                //Check format
                if (!childName.regionMatches(0, prefix, 0, prefix.length())) {
                    LOG.warn("Found child node with improper name: {}", childName);
                    continue;
                }
                String suffix = childName.substring(prefix.length());
                Long childId = Long.parseLong(suffix);
                orderedChildren.put(childId, childName);
            } catch (NumberFormatException e) {
                LOG.warn("Found child node with improper format : {}", childName, e);
            }
        }

        return orderedChildren;
    }

    /**
     * Find the smallest child node.
     * 找到最小的孩子节点
     * @return The name of the smallest child node.
     */
    private String smallestChildName() throws KeeperException, InterruptedException {
        long minId = Long.MAX_VALUE;
        String minName = "";

        List<String> childNames;

        try {
            childNames = zookeeper.getChildren(dir, false);
        } catch (KeeperException.NoNodeException e) {
            LOG.warn("Unexpected exception", e);
            return null;
        }
        //遍历所有节点，找到节点序号ID最小的
        for (String childName : childNames) {
            try {
                //Check format
                if (!childName.regionMatches(0, prefix, 0, prefix.length())) {
                    LOG.warn("Found child node with improper name: {}", childName);
                    continue;
                }
                String suffix = childName.substring(prefix.length());
                long childId = Long.parseLong(suffix);
                if (childId < minId) {
                    minId = childId;
                    minName = childName;
                }
            } catch (NumberFormatException e) {
                LOG.warn("Found child node with improper format : {}", childName, e);
            }
        }

        if (minId < Long.MAX_VALUE) {
            return minName;
        } else {
            return null;
        }
    }

    /**
     * Return the head of the queue without modifying the queue.
     * 获取队列头部节点的数据data
     * @return the data at the head of the queue.
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] element() throws NoSuchElementException, KeeperException, InterruptedException {
        Map<Long, String> orderedChildren;

        // element, take, and remove follow the same pattern.
        // We want to return the child node with the smallest sequence number.
        // Since other clients are remove()ing and take()ing nodes concurrently,
        // the child with the smallest sequence number in orderedChildren might be gone by the time we check.
        // We don't call getChildren again until we have tried the rest of the nodes in sequence order.
        while (true) {
            try {
                orderedChildren = orderedChildren(null);
            } catch (KeeperException.NoNodeException e) {
                throw new NoSuchElementException();
            }
            if (orderedChildren.size() == 0) {
                throw new NoSuchElementException();
            }
            for (String headNode : orderedChildren.values()) {
                if (headNode != null) {
                    try {
                        return zookeeper.getData(dir + "/" + headNode, false, null);
                    } catch (KeeperException.NoNodeException e) {
                        //Another client removed the node first, try next
                    }
                }
            }

        }
    }

    /**
     * Attempts to remove the head of the queue and return it.
     * 尝试删除队列头部元素
     * @return The former head of the queue
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] remove() throws NoSuchElementException, KeeperException, InterruptedException {
        Map<Long, String> orderedChildren;
        // Same as for element.  Should refactor this.
        while (true) {
            try {
                orderedChildren = orderedChildren(null);
            } catch (KeeperException.NoNodeException e) {
                throw new NoSuchElementException();
            }
            if (orderedChildren.size() == 0) {
                throw new NoSuchElementException();
            }

            for (String headNode : orderedChildren.values()) {
                String path = dir + "/" + headNode;
                try {
                    byte[] data = zookeeper.getData(path, false, null);
                    zookeeper.delete(path, -1);
                    return data;
                } catch (KeeperException.NoNodeException e) {
                    // Another client deleted the node first.
                }
            }

        }
    }

    /**
     * 闭锁监听器,装饰器模式功能增强
     */
    private static class LatchChildWatcher implements Watcher {

        CountDownLatch latch;

        public LatchChildWatcher() {
            latch = new CountDownLatch(1);
        }

        public void process(WatchedEvent event) {
            LOG.debug("Watcher fired: {}", event);
            latch.countDown();
        }
        public void await() throws InterruptedException {
            latch.await();
        }

    }

    /**
     * Removes the head of the queue and returns it, blocks until it succeeds.
     * 出队（删除并返回队首元素），阻塞直到成功
     * @return The former head of the queue
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] take() throws KeeperException, InterruptedException {
        Map<Long, String> orderedChildren;
        // Same as for element.  Should refactor this.
        while (true) {
            LatchChildWatcher childWatcher = new LatchChildWatcher();
            try {
                orderedChildren = orderedChildren(childWatcher);
            } catch (KeeperException.NoNodeException e) {
                zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
                continue;
            }
            //队列为空，阻塞等待
            if (orderedChildren.size() == 0) {
                childWatcher.await();
                continue;
            }
            //获取队首元素数据，并删除节点（触发监听器process方法，闭锁放开）
            for (String headNode : orderedChildren.values()) {
                String path = dir + "/" + headNode;
                try {
                    byte[] data = zookeeper.getData(path, false, null);
                    zookeeper.delete(path, -1);
                    return data;
                } catch (KeeperException.NoNodeException e) {
                    // Another client deleted the node first.
                }
            }
        }
    }

    /**
     * Inserts data into queue.
     * 写数据到队列，直到数据写入成功
     * @param data
     * @return true if data was successfully added
     */
    public boolean offer(byte[] data) throws KeeperException, InterruptedException {
        for (; ; ) {
            try {
                zookeeper.create(dir + "/" + prefix, data, acl, CreateMode.PERSISTENT_SEQUENTIAL);
                return true;
            } catch (KeeperException.NoNodeException e) {
                zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
            }
        }

    }

    /**
     * Returns the data at the first element of the queue, or null if the queue is empty.
     * 获取队首元素数据
     * @return data at the first element of the queue, or null.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] peek() throws KeeperException, InterruptedException {
        try {
            return element();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Attempts to remove the head of the queue and return it. Returns null if the queue is empty.
     * @return Head of the queue or null.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] poll() throws KeeperException, InterruptedException {
        try {
            return remove();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

}
