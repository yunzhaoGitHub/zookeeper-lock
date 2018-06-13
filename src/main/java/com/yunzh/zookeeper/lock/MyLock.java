package com.yunzh.zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class MyLock implements Lock,Watcher {

    private ZooKeeper zookeeper = null;
    private String CURRENT_LOCK;
    private String WAIT_LOCK;
    private String ROOT_LOCK = "/mylocks";

    private CountDownLatch countDownLatch;


    /**
     * 1、初始化zookeeper
     * 2、判断根节点是否存在
     */
    public MyLock() {

        try {
            // 监听可以直接使用this，因为已经实现了watcher接口，具体的watcher处理在process方法中进行
            zookeeper = new ZooKeeper
                    ("192.168.25.130:2181,192.168.25.130:2181,192.168.25.130:2181", 4000, this);
            // 判断根节点
            Stat stat = zookeeper.exists(ROOT_LOCK, null);
            if (stat == null) {
                zookeeper.create
                        (ROOT_LOCK, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }

    /**
     * 获得锁
     * 1.创建有序的临时节点---EPHEMERAL_SEQUENTIAL
     * 2.获取根节点下的所有子节点getChildren
     * 3.对所有子节点进行排序
     *  3.1.如果是最小的表示获得锁
     *  3.2.获取比当前锁更小的节点，设置为WAIT_LOCK---实现了“链式监听”
     * @return
     */
    @Override
    public boolean tryLock() {

        try {
            // 1.创建有序的临时节点---EPHEMERAL_SEQUENTIAL
            CURRENT_LOCK = zookeeper.create
                    (ROOT_LOCK + "/", "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            System.out.println(Thread.currentThread().getName()+"->"+ CURRENT_LOCK+"，尝试竞争锁");
           
            // 2.获取根节点下的所有子节点getChildren
            List<String> childNodes = zookeeper.getChildren(ROOT_LOCK, false);
            // 3.对所有子节点进行排序   TreeSet
            SortedSet<String> sortedSet = new TreeSet<>();
            for (String childNode : childNodes) {
                sortedSet.add(ROOT_LOCK + "/" + childNode);
            }
            String fn = sortedSet.first();
            // 3.1.如果是最小的表示获得锁
            if (CURRENT_LOCK.equals(fn)) {
                return true;
            }

            // 3.2.获取比当前锁更小的节点，设置为WAIT_LOCK---实现了“链式监听”
            /*
             * 相当于{1,2,3,4,5,6} 假设当前CURRENT_LOCK为4，那么比他小的是ln{1,2,3}
             * 我们为当前这个4的节点添加watcher监听前一个节点3
             * ln{1,2,3}.last() = 3
             */

            SortedSet<String> ln=((TreeSet<String>) sortedSet).headSet(CURRENT_LOCK);
            if (!ln.isEmpty()) {
                WAIT_LOCK = ln.last();
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 1.如果获得锁成功了可以直接结束
     * 2.如果没有获得锁，给这个锁绑定watcher监听当前节点的上一个节点
     */
    @Override
    public void lock() {
        // 1.如果获得锁成功了可以直接结束
        if (this.tryLock()) {
            System.out.println(Thread.currentThread().getName()+"->"+CURRENT_LOCK+"->获得锁成功");
            return;
        }
        // 等待获得锁，设置阻塞
        waitForLock(WAIT_LOCK);

    }

    private boolean waitForLock(String preNode) {

        try {
            Stat stat = zookeeper.exists(preNode, true);
            if (stat != null) {
                System.out.println(Thread.currentThread().getName()+"->等待锁"+ROOT_LOCK+"/"+preNode+"释放");
                countDownLatch = new CountDownLatch(1);
                countDownLatch.await();

                //TODO  watcher触发以后，还需要再次判断当前等待的节点是不是最小的
                System.out.println(Thread.currentThread().getName()+"->获得锁成功");
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {

    }


    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    /**
     * 释放锁
     * 删除当前节点，将version设置成-1
     */
    @Override
    public void unlock() {
        System.out.println(Thread.currentThread().getName()+"->释放锁"+CURRENT_LOCK);

        try {
            zookeeper.delete(CURRENT_LOCK, -1);
            CURRENT_LOCK = null;
            zookeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (this.countDownLatch != null) {
            countDownLatch.countDown();
        }
    }
}
