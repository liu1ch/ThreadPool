package com.hello.juc;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * -----------------------------------------------
 *
 *
 *              pool                     put
 * ThreadPool  --------> BlockingQueue ---------->main
 *
 *
 * -----------------------------------------------
 */
@Slf4j(topic = "c.TestPool")
public class TestPool{
    public static void main(String[] args) {

        ThreadPool threadPool = new ThreadPool(2, 1000, TimeUnit.MILLISECONDS, 10,(queue,task)->{
            //假设死等，就调用put参数
            //queue.put(task);
            //带超时等待
//            queue.offer(task,500,TimeUnit.MICROSECONDS);

        });
        for (int i = 0; i < 15; i++) {
            int j = i;
            threadPool.execute(()->{
                try {
                    Thread.sleep(1);
                    System.out.println("执行任务："+j);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            });
        }

    }
}

/**
 * 拒绝策略
 */
@FunctionalInterface
interface RejectPolicy<T>{
    void reject(BlockingQueue<T> queue, T task);
}

@Slf4j(topic = "c.ThreadPool")
class ThreadPool {
    //任务队列
    private  BlockingQueue<Runnable> taskQueue;

    //线程集合
    private HashSet<Worker> workerSet= new HashSet<>();

    //核心线程数
    private int coreSize;

    //获取任务的超时时间
    private long timeOut;

    private TimeUnit timeUnit;

    private RejectPolicy<Runnable> rejectPolicy;

    /**
     * 执行任务
     * 如果任务数小于核心线程数，直接交给worker
     * 如果任务数大于核心线程数，交给任务队列暂存
     * @param task
     */
    public void execute(Runnable task){
        //为保证线程安全。采用synchronized
        log.info("执行任务：{}",task);
        synchronized (workerSet){
            if(workerSet.size() < coreSize){
                Worker worker = new Worker(task);
                log.info("新增 worker：{}，{}",   worker,task);
                workerSet.add(worker);
                worker.start();
            }else {
                taskQueue.put(task);
                //超过线程池阻塞
                taskQueue.tryPut(rejectPolicy,task);

            }
        }
    }

    /**
     *
     * @param coreSize
     * @param timeOut
     * @param timeUnit
     * @param queueCapcity 队列大小上限
     */
    public ThreadPool(int coreSize, long timeOut, TimeUnit timeUnit, int queueCapcity,RejectPolicy<Runnable> rejectPolicy) {
        this.coreSize = coreSize;
        this.timeOut = timeOut;
        this.timeUnit = timeUnit;
        this.rejectPolicy=rejectPolicy;
        this.taskQueue=new BlockingQueue<>(queueCapcity);
    }

    /**
     *
     */
    class Worker extends Thread{
        private Runnable task;
        public Worker(Runnable task) {
            this.task = task;
        }


        @Override
        public void run(){
            //执行任务
            //1）task不空，执行。并且查询任务队列是否还有，有就继续
//            while(task!=null || (task = taskQueue.take())!=null){
            while(task!=null || (task = taskQueue.poll(timeOut,timeUnit))!=null){
                try{
                    log.info("执行任务：{}",task);
                    task.run();
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    task=null;
                }
            }
            //移除workerSet中的task
            // 并不会移除，因为taskQueue的take方法是阻塞的，也就是说线程会一直活在休息室
            synchronized (workerSet){
                log.info("移除任务：{}",this);
                workerSet.remove(this);
            }
        }
    }
}
@Slf4j
class BlockingQueue<T>{
    //1.任务队列，选用ArrayDeque
    private Deque<T> queue = new ArrayDeque<>();

    //2.锁，多个线程来可能会导致一个任务被重复执行，所以要加一把锁。采用ReentrantLock
    private ReentrantLock lock = new ReentrantLock();

    //3，当没有任务的时候，消费者需要等待。消费者条件变量

    private Condition emptyWaitSet = lock.newCondition();

    // 4.阻塞队列满了之后，生产者等待队.  生产者条件变量
    private Condition fullWaitSet = lock.newCondition();

    //5.容量
    private int capcity;

    public BlockingQueue(int capcity) {
        this.capcity = capcity;
    }

    //带超时时间的阻塞获取
    public T poll(long timeout, TimeUnit unit){
        lock.lock();
        try{
            //时间统一转换纳秒
            long nanosTimeout = unit.toNanos(timeout);
            while (queue.isEmpty()){
                //队列为空，进入等待
                try {
                    //防止虚假唤醒问题，返回的是剩余时间
                    if(nanosTimeout<=0){
                        return null;
                    }
                    nanosTimeout = emptyWaitSet.awaitNanos(nanosTimeout);//将来有任务会被唤醒
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            T t = queue.removeFirst();
            //消费完，去唤醒因为队列满了而等待的生产着线程
            fullWaitSet.signal();
            return t;
        } finally {
            lock.unlock();
        }

    }

    //阻塞获取
    public T take(){
        lock.lock();
        try{
            while (queue.isEmpty()){
                //队列为空，进入等待
                try {
                    emptyWaitSet.await();//将来有任务会被唤醒
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
           T t = queue.removeFirst();
            //消费完，去唤醒因为队列满了而等待的生产着线程
            fullWaitSet.signal();
            return t;
        } finally {
            lock.unlock();
        }

    }

    //阻塞添加
    public void put(T element){
        lock.lock();
        try{

            while (queue.size() == capcity){
               log.info("等待加入任务队列：{}",element);
                try {
                    fullWaitSet.await();//将来有空位会被唤醒
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            log.info("已经加入任务队列：{}",element);
            queue.addLast(element);
            //增加完成新元素，去唤醒因为队列为空而等待的消费者线程
            emptyWaitSet.signal();
        }finally {
            lock.unlock();
        }
    }
    //带超时时间的阻塞添加
    public boolean offer(T element, long timeOut,TimeUnit timeUnit){
        lock.lock();
        try{
            long nanos = timeUnit.toNanos(timeOut);
            while (queue.size() == capcity){
                log.info("等待加入任务队列：{}",element);
                try {

                    if(nanos<=0){return false;}

                    nanos = fullWaitSet.awaitNanos(nanos);//将来有空位会被唤醒
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            log.info("已经加入任务队列：{}",element);
            queue.addLast(element);
            //增加完成新元素，去唤醒因为队列为空而等待的消费者线程
            emptyWaitSet.signal();
            return true;
        }finally {
            lock.unlock();
        }
    }
    public int size(){
        lock.lock();
        try {
            return queue.size();
        }finally {
            lock.unlock();
        }
    }

    public void tryPut(RejectPolicy<T> rejectPolicy, T task) {
        lock.lock();
        try {
            //判断队列是否已满
            if(queue.size()==capcity){
                rejectPolicy.reject(this,task);
            }else {//队列空闲
                log.info("加入队列：{}",task);
                queue.addLast(task);
                emptyWaitSet.signal();
            }
        }finally {
            lock.unlock();
        }
    }
}

