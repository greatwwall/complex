/**
 * 滑窗处理类
 * 应用于一个节点.在该节点第一次上报告警的时候创建。通过SlipWindowManage创建并进行管理。
 * 对外提供的函数：
 * slipwindow：构造函数，参数为起始ackNo
 * checkAckNo: 告警检查函数，检查告警合法性。检查时会尝试滑动窗口，并在在特定的时机重置窗口。
 * successRecord: 告警处理完成，在窗口中登记，在回ACK之前设置
 * 滑动机制之类的内部逻辑，不对外展现。这里暂不考虑ackNo的翻转。
 
 */
public final class SlipWindow
{
    /**
     * 日志打印对象
     */
    private static final AppLogger LOGGER = AppLogger.getInstance(SlipWindow.class);
    
    /**
     * ackNo为2时，触发滑窗重置
     */
    private static final int RESET_ACKNO = 2;
    
    //滑窗数组，循环记录
    private WindowBean[] nodewindow;
    
    //当前已经成功处理的告警，在滑窗中的索引
    private int successNo;
    
    //当前的滑窗内有效告警数量,但不包括successNo指向的这个
    private int count;
    
    //滑窗大小
    private int maxSlipWindow = FmsModule.getInstance().getMaxSlipWindow();
    
    /**
     * 构造函数，参数为起始ackNo
     * @param ackNo     起始ackNo
     */
    public SlipWindow(int ackNo)
    {
        nodewindow = new WindowBean[maxSlipWindow];
        
        //初始化数组
        for (int i = 0; i < maxSlipWindow; i++)
        {
            nodewindow[i] = new WindowBean();
        }
        
        init(ackNo);
        LOGGER.info("[fms] create slipWindow. ackNo:{}.", ackNo);
    }
    
    /**
     * Coverity-Unguarded read (GUARDED_BY_VIOLATION)
     * 描述：init方法同时被多个方法调用时会出现同时写内存的操作，需要加锁保护
     * 不修改原因：这个方法不会同时被多个方法调用，因此不会出现同时写内存的操作.
     */
    // 初始化滑窗，并将触发初始化的AckNo记录到滑窗
    private void init(int ackNo)
    {
        // 按照初始ackNo初始化滑窗
        successNo = 0;
        nodewindow[successNo].setAckNo(ackNo - 1);
        nodewindow[successNo].setSuccess(true);
        
        // 初始化滑窗内有效告警数量为0
        count = 0;
        
        // 滑窗其他记录默认为0无效
        for (int i = successNo + 1; i < maxSlipWindow; i++)
        {
            nodewindow[i].setAckNo(0);
            nodewindow[i].setSuccess(false);
        }
    }
    
    /**
     * 对外提供的检查接口,前置检查时调用,检查不通过的告警,不处理,但要回ACK（这个回ACK，不用再到滑窗中登记）
     * @param ackNo  要检查的ackNo
     * @return true检查通过 false检查不通过  0成功，1告警已入库，再次上报重复告警，回ack，2告警未入库，再次上报重复告警，不需要回ack
     */
    public synchronized int checkAckNo(int ackNo)
    {
        LOGGER.debug("[fms] checkAckNo enter. ackNo:{}.", ackNo);
        
        // A侧重启，重置滑窗
        if (ackNo == RESET_ACKNO)
        {
            init(ackNo);
            LOGGER.warn("[fms] checkAckNo reset slipWindow. ackNo:{}.", ackNo);
        }
        
        // 尝试向后滑动窗口，必须放在检查之前。
        slipTheWindow();
        
        // 检查不通过，告警被抑制（历史老告警）
        if (ackNo <= nodewindow[successNo].getAckNo())
        {
            LOGGER.warn("[fms] checkAckNo alarm is duplicate, [{},{}].", new Object[]{ackNo
                , nodewindow[successNo].getAckNo()});
            return AlarmConstant.NEEDACK_CODE;
        }
        
        // 安全距离检查,如果落在滑窗外,则认为异常发生,重新初始化滑窗，以免滑窗挂死。
        //  A侧保证每次发送的数据均在一个窗口内部，但异常情况可能出现在外部
        //  1. 老的A侧重启，重新上报的告警可能落在滑窗之外
        //  2. 某告警处理失败，但A侧由于重启等原因，以后不再发了，滑窗不能死等
        int distance = ackNo - nodewindow[successNo].getAckNo();
        if (distance >= maxSlipWindow)
        {
            init(ackNo);
            
            //重置偏移，重置窗口后的ackNo应该放在第二个位置上
            distance = 1;
            LOGGER.warn("[fms] checkAckNo out of slipWindow, reset slipWindow. ackNo:{}.", ackNo);
        }
        
        // 如果滑窗中已有记录，说明是重发。fms正在处理，还没有入库成功置滑窗标志位
        if (nodewindow[(successNo + distance) % maxSlipWindow].getAckNo() == ackNo)
        {
            // 已有成功记录,该告警为重复。算作检查不通过
            if (nodewindow[(successNo + distance) % maxSlipWindow].isSuccess())
            {
                LOGGER.warn("[fms] checkAckNo alarm is duplicate. ackNo:{}.", ackNo);
                return AlarmConstant.NEEDACK_CODE;
            }
            
            // 已有记录,该告警为重复。算作检查不通过
            LOGGER.warn("[fms] checkAckNo alarm is duplicate. ackNo:{}.", ackNo);
            return AlarmConstant.NO_NEEDACK_CODE;
        }
        
        // 需记录新的告警,计数累加
        count++;
        
        // 检查通过,并记录到滑窗
        nodewindow[(successNo + distance) % maxSlipWindow].setAckNo(ackNo);
        nodewindow[(successNo + distance) % maxSlipWindow].setSuccess(false);
        LOGGER.info("[fms] checkAckNo add new alarm ackNo to slipWindow. ackNo:{}, successNo:{}, count:{}"
            , new Object[]{ackNo, successNo, count});
        
        LOGGER.debug("[fms] checkAckNo leave. ackNo:{}.", ackNo);
        return 0;
    }
    
    // 尝试向后滑动窗口
    private void slipTheWindow()
    {
        LOGGER.debug("[fms] slipTheWindow enter.");
        
        int next;
        boolean canSlip;
        
        while (true)
        {
            canSlip = false;
            
            next = (successNo + 1) % maxSlipWindow;
            
            // 如果后面的告警处理成功，移动一格
            if (nodewindow[next].getAckNo() != 0 && nodewindow[next].isSuccess())
            {
                canSlip = true;
                count--;
            }
            // 如果出现间隔，移动一格跳过它。以免滑窗挂死
            else if (nodewindow[next].getAckNo() == 0 && count > 0)
            {
                canSlip = true;
            }
            
            // 实际移动动作：移动一格,移过去的置为无效
            if (canSlip)
            {
                nodewindow[successNo].setAckNo(0);
                nodewindow[successNo].setSuccess(false);
                successNo = next;
                LOGGER.debug("[fms] can slip the Window. count:{}, successNo:{}", new Object[]{count, successNo});
            }
            else
            {
                LOGGER.debug("[fms] can not slip the Window. count:{}, successNo:{}", new Object[]{count, successNo});
                break;
            }
        }
        
        // 如果此时指向了无效单元，需要特殊处理.此时next肯定是有效ackNo。
        if (nodewindow[successNo].getAckNo() == 0)
        {
            nodewindow[successNo].setAckNo(nodewindow[next].getAckNo() - 1);
            nodewindow[successNo].setSuccess(true);
            LOGGER.warn("[fms] successNo is refer to invalid. count:{}, successNo:{}", new Object[]{count, successNo});
        }
        
        LOGGER.info("[fms] slipTheWindow finished. count:{}, successNo:{}", new Object[]{count, successNo});
        LOGGER.debug("[fms] slipTheWindow leave.");
    }
    
    /**
     * 告警处理成功,在窗口中记录
     * @param ackNo 处理成功的告警ackNo
     */
    public synchronized void successRecord(int ackNo)
    {
        LOGGER.debug("[fms] successRecord enter. ackNo:{}.", ackNo);
        
        // 考虑FMS代码实现的便利,包括检查不通过的告警,也会来滑窗中登记, 事实上这种告警无法登记
        if (ackNo <= nodewindow[successNo].getAckNo())
        {
            LOGGER.info("[fms] current ackNo is equal or smaller than the ackNo which refered by successNo, [{},{},{}]"
                , new Object[]{ackNo, nodewindow[successNo].getAckNo(), successNo});
            return;
        }
        
        // 安全检查,如果落在滑窗外,不处理.可能是A侧突然重启导致滑窗重新初始化
        int distance = ackNo - nodewindow[successNo].getAckNo();
        if (distance >= maxSlipWindow)
        {
            LOGGER.error("[fms] current ackNo out of slipWindow, [{},{},{},{}] "
                , new Object[]{ackNo, nodewindow[successNo].getAckNo(), successNo, distance});
            return;
        }
        
        // 通过距离求模计算滑窗下标
        int index = (successNo + distance) % maxSlipWindow;
        
        // 之前未记录该告警，也是异常情况
        if (nodewindow[index].getAckNo() != ackNo)
        {
            LOGGER.error("[fms] current ackNo is not in slipWindow, [{},{},{},{}] "
                , new Object[]{ackNo, nodewindow[successNo].getAckNo(), successNo, index});
            return;
        }
        
        // 正常情况，置成功标志
        nodewindow[index].setSuccess(true);
        
        LOGGER.info("[fms] successRecord ackNo in slipWindow, [{},{},{},{}] "
            , new Object[]{ackNo, nodewindow[successNo].getAckNo(), successNo, index});
        LOGGER.debug("[fms] successRecord leave. ackNo:{}.", ackNo);
    }

    /**
     * 重写toString方法
     * @return 字符串
     */
    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();
        str.append("SlipWindow [nodewindow=").append(Arrays.toString(nodewindow)).append(", successNo=")
        .append(successNo).append(", count=").append(count).append("]");
        return str.toString();
    }
}
