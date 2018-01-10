import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

public class SerializeQueue<E> extends LinkedBlockingQueue<E>
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -2246133139616745902L;
    
    /**
     * 
     * <默认构造函数>
     */
    public SerializeQueue()
    {
        super();
    }
    
    /**
     * 
     * <默认构造函数>
     * @param maxQueueSize 队列长度
     */
    public SerializeQueue(int maxQueueSize)
    {
        super(maxQueueSize);
    }
    
    /**
     * 
     * <默认构造函数>
     * @param collection 集合对象
     */
    public SerializeQueue(Collection<E> collection)
    {
        super(collection);
    }
    
    /**
     * 判断队列是否已满
     * @return boolean
     */
    public boolean isFull()
    {
        return this.isFull(1);
    }
    
    /**
     * 判断队列是否已满
     * @param remainCount 剩余数量
     * @return boolean
     */
    public boolean isFull(int remainCount)
    {
        return remainingCapacity() < remainCount;
    }
}
