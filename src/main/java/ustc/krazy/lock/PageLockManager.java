package ustc.krazy.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author krazy
 * @date 2025/1/16
 */
public class PageLockManager {

    private Map<Integer, Map<Long, PageLock>> pageLockMap;

    public PageLockManager() {
        pageLockMap = new ConcurrentHashMap<>();
    }
    public class PageLock {
        /**
         * 共享锁
         * 读锁
         */
        public static final int SHARE = 0;
        /**
         * 排他锁
         * 写锁
         */
        public static final int EXCLUSIVE = 1;
        /**
         * 锁类型
         */
        private int type;
        /**
         * 事务ID
         */
        private Long threadId;

        public PageLock(int type, long threadId) {
            this.type = type;
            this.threadId = threadId;
        }

        public int getType() {
            return type;
        }
        public void setType(int type) {
            this.type = type;
        }
    }

    /**
     * LockManager来实现对锁的管理，LockManager中主要有申请锁、释放锁、查看指定数据页的指定事务是否有锁这三个功能，
     * 其中加锁的逻辑比较麻烦，需要基于严格两阶段封锁协议去实现。
     *
     * @param pageId
     * @param tid
     * @param acquireType
     * @return
     * @throws InterruptedException
     */
    public synchronized boolean acquireLock(int pageId, long tid, int acquireType) throws InterruptedException {
        final String lockType = acquireType == 0 ? "read lock" : "write lock";
        final String threadName = Thread.currentThread().getName();
        // 获取当前页面上已经添加的锁
        Map<Long, PageLock> lockMap = pageLockMap.get(pageId);
        if(lockMap == null){
            lockMap = new ConcurrentHashMap<>();
            lockMap.put(tid, new PageLock(acquireType, tid));
            pageLockMap.put(pageId, lockMap);
            //System.out.println(threadName + " acquire " + lockType + " in " + pageId + ", the tid lock size is " + lockMap.size());
            return true;
        }else{
            PageLock lock = lockMap.get(tid);
            if (lock != null) {
                if(lock.getType() == PageLock.EXCLUSIVE){
                    return true;
                }else{
                    if(acquireType == PageLock.EXCLUSIVE){
                        if(lockMap.size()==1){
                            lock.setType(PageLock.EXCLUSIVE);
                            lockMap.put(tid, lock);
                            pageLockMap.put(pageId, lockMap);
                            //System.out.println(threadName + " upgrade " + lockType + " in " + pageId + ", the tid lock size is " + lockMap.size());
                            return true;
                        }else{
                            //System.out.println(threadName + ": the" + pageId + "have many read locks, transaction " + tid + " require" + lockType + " fail");
                        }
                    }else{
                        return true;
                    }
                }
            }else{
                if(lockMap.size()==1){
                    // 当前页面上只存在其他事务添加的一把锁,判断这把锁的类型是什么
                    PageLock l = null;
                    for (PageLock value : lockMap.values()) {
                        l = value;
                    }
                    if(l.getType() == PageLock.EXCLUSIVE){
                        wait(50);
                        return false;
                    }else{
                        if(acquireType== PageLock.SHARE){
                            lockMap.put(tid, new PageLock(acquireType, tid));
                            pageLockMap.put(pageId, lockMap);
                            //System.out.println(threadName + " acquire " + lockType + " in " + pageId + ", the tid lock size is " + lockMap.size());
                            return true;
                        }else{
                            wait(50);
                            return false;
                        }
                    }
                }else{
                    if(acquireType== PageLock.SHARE){
                        lockMap.put(tid, new PageLock(acquireType, tid));
                        pageLockMap.put(pageId, lockMap);
                        //System.out.println(threadName + " acquire " + lockType + " in " + pageId + ", the tid lock size is " + lockMap.size());
                        return true;
                    }else{
                        wait(10);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * 释放指定页面的指定事务加的锁
     *
     * @param pageId 页id
     * @param tid    事务id
     */
    public synchronized void releaseLock(int pageId, long tid) {
        final String threadName = Thread.currentThread().getName();

        Map<Long, PageLock> lockMap = pageLockMap.get(pageId);
        if (lockMap == null) {
            return;
        }
        PageLock lock = lockMap.get(tid);
        if (lock == null) {
            return;
        }
        final String lockType = lockMap.get(tid).getType() == 0 ? "read lock" : "write lock";
        lockMap.remove(tid);
        //System.out.println(threadName + " release " + lockType + " in " + pageId + ", the tid lock size is " + lockMap.size());
        if (lockMap.size() == 0) {
            pageLockMap.remove(pageId);
            //System.out.println(threadName + " release last lock, the page " + pageId + " have no lock, the page locks size is " + pageLockMap.size());
        }
        this.notifyAll();
    }

}
