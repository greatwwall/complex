
/**
 * 
 * 告警接收处理类
 * 告警接收处理类，接收来自FMA模块报过来的告警
 */
public class FmsThriftServiceImpl implements FmsService.Iface
{
    /**
     * 日志打印对象
     */
    private static final AppLogger LOGGER = AppLogger.getInstance(FmsThriftServiceImpl.class);
    
    /**
     * 南向通过thrift机制调用本接口上报告警
     * 
     * @param alarmMessage FMA上报的告警列表
     * @return Response 返回码,成功为0
     * @throws TException 南向RPC通信错误
     */
    @Override
    public Response sendAlarmtoFMS(AlarmsMsg alarmMessage) throws TException
    {
        LOGGER.debug("[fms] FmsThriftServiceImpl: sendAlarmtoFMS, enter.");
        
        //给fma返回值
        Response resp = new Response();
        
        //参数检查,检查FMA上报的告警信息有效性。
        if (!checkParam(alarmMessage))
        {
            //告警消息报为空
            LOGGER.error("[fms] Alarm message content:{} is null or empty.", alarmMessage);
            resp.retCode = AlarmErrorCode.CODE_TOFMA_INVALID_PARAM;
            return resp;
        }
        
        //接收类到处理类的转换，将IP信息塞入处理类中以便后续ACK处理，FMA侧不进行IP的塞入是为了减轻FMA的通行负担。
        List<AlarmHandleModel> alarmList = alarmInfoTranster(alarmMessage);
        if (null == alarmList || alarmList.isEmpty())
        {
            //告警消息报为空
            LOGGER.error("[fms] after Alarm Transter the content is null or empty.");
            resp.retCode = AlarmErrorCode.CODE_TOFMA_FAIELD;
            return resp;
        }
        
        //告警上报数据源OMA节点的标识，ip和端口信息，格式为ip:port
        String omaKey = alarmMessage.getAgentIP() + AlarmConstant.FMA_IPPORT_SEPARATOR + alarmMessage.getAgentPort();
        
        //执行划窗前，将接收到的告警印出来便于问题定位
        printLog(alarmList, omaKey);
        
        //执行窗口滑动操作
        List<AlarmHandleModel> validAlarmList = executeWindowSlip(omaKey, alarmList);
        if (validAlarmList.isEmpty())
        {
            //告警消息报为空
            LOGGER.warn("[fms] after Alarm executeWindowSlip the validAlarmList is empty.");
            resp.retCode = AlarmErrorCode.CODE_TOFMA_SUCCESS;
            return resp;
        }
        
        List<AlarmHandleModel> alarmCacheList = new ArrayList<AlarmHandleModel>(AlarmConstant.MAX_CACHE_QUEUE_SIZE);
        List<AlarmHandleModel> eventCacheList = new ArrayList<AlarmHandleModel>(AlarmConstant.MAX_CACHE_QUEUE_SIZE);
        for (AlarmHandleModel alarm : validAlarmList)
        {
            //根据告警类型判断，将事件放入事件List
            if (AlarmCategory.EVENT == AlarmCategory.findByValue(alarm.getAlarmCategory()))
            {
                eventCacheList.add(alarm);
            }
            else if ((AlarmCategory.ALARM == AlarmCategory.findByValue(alarm.getAlarmCategory()))
                    || (AlarmCategory.CLEAR == AlarmCategory.findByValue(alarm.getAlarmCategory())))
            {
                alarmCacheList.add(alarm);
            }
            else
            {
                //将该对象放入ACK队列，等待ACK返回
                AckModelConvertor.offerToAlarmAckQueue(alarm.getFmaIp(),
                        alarm.getFmaPort(),
                        String.valueOf(alarm.getSequenceNo()));
                LOGGER.error("[fms] alarm category is invalid. alarm:{}", alarm);
            }
        }
        
        //将接收到的告警和事件打印出来便于问题定位
        printLog(alarmCacheList, eventCacheList, omaKey);
        
        //获取告警接收缓存队列，用于存在接收到的告警信息
        SerializeQueue<AlarmHandleModel> alarmCacheQueue = FmsModule.getInstance().getAlarmCacheQueue();
        

        /**
         * Fority:
         * 问题:FORTIFY.Poor_Error_Handling--Overly_Broad_Catch，
         * 描述:使用一个“简约”的 catch 块捕获高级别的异常类（如 Exception），
         *     可能会混淆那些需要特殊处理的异常，或是捕获了不应在程序中这一点捕获的异常。
         * 不整改的原因:代码不会出现需要特殊处理的异常，为防止代码中有不可预知的异常，因此捕获一个大异常即可。
         */
        //同步，防止多个线程往缓存队列放内容
        synchronized (FmsThriftServiceImpl.class)
        {
            StringBuilder sb = new StringBuilder();
            try
            {
                //告警缓存队列不足以容纳消息包内的所有告警，不做处理，返回错误码
                doAlarmProcess(alarmCacheList, alarmCacheQueue, sb);
                
                //有日志打印，说明告警或者事件缓存队列满，返回3队列满
                if (0 < sb.length())
                {
                    //告警或者事件缓存队列满，重置滑窗
                    SlipWindowManager slipWindowManager = FmsModule.getInstance().getSlipWindowManager();
                    slipWindowManager.resetSlipWindow(omaKey);
                    LOGGER.error("[fms] cache Queue is full, need to reset slipWindow. omakey:{}", omaKey);
                    
                    resp.retCode = AlarmErrorCode.CODE_TOFMA_FULL;
                    return resp;
                }
            }
            catch (Exception e)
            {
                //告警或者事件缓存队列满，重置滑窗
                SlipWindowManager slipWindowManager = FmsModule.getInstance().getSlipWindowManager();
                slipWindowManager.resetSlipWindow(omaKey);

                LOGGER.error("[fms] add to cache Queue failed.", e);
                resp.retCode = AlarmErrorCode.CODE_TOFMA_FAIELD;
                return resp;
            }
        }
        
        // 处理完成返回成功
        resp.retCode = AlarmErrorCode.CODE_TOFMA_SUCCESS;
        LOGGER.debug("[fms] FmsThriftServiceImpl: sendAlarmtoFMS, leave.");
        return resp;
    }
    
    private void doAlarmProcess(List<AlarmHandleModel> alarmCacheList, SerializeQueue<AlarmHandleModel> alarmCacheQueue,
            StringBuilder sb)
    {
        if (alarmCacheQueue.isFull(alarmCacheList.size()))
        {
            sb.append("[fms] cache Queue is full, the remainingCapacity of alarmCacheQueue:");
            sb.append(alarmCacheQueue.remainingCapacity());
            sb.append(AlarmConstant.NEW_LINE_SINGLE);
            sb.append("but the received alarm that the size is: ");
            sb.append(alarmCacheList.size());
            LOGGER.warn(sb.toString());
        }
        else
        {
            //将告警直接放入到接收队列，此队列将作为pretreat的处理的输入队列；
            alarmCacheQueue.addAll(alarmCacheList);
        }
    }
    
    //执行窗口滑动操作
    private List<AlarmHandleModel> executeWindowSlip(String omaKey, List<AlarmHandleModel> alarmList)
    {
        //创建处理过窗口滑动操作后的告警list，存放检查通过的告警
        List<AlarmHandleModel> validAlarmList = new ArrayList<AlarmHandleModel>();
        
        //获取滑窗，如果不存在则用首条告警的ackNo创建滑窗（OMA上报的告警是顺序的）
        SlipWindowManager slipWindowManager = FmsModule.getInstance().getSlipWindowManager();
        SlipWindow slipWindow = slipWindowManager.getNodeSlipWindow(omaKey, alarmList.get(0).getSequenceNo());
        
        //检查ackNo，记录到滑窗检查时会尝试滑动窗口，并在在特定的时机重置窗口。
        for (int i = 0; i < alarmList.size(); i++)
        {
            int retCode = slipWindow.checkAckNo(alarmList.get(i).getSequenceNo());
            //检查不通过，FMS还没有入库成功，不能回ack
            if (AlarmConstant.NO_NEEDACK_CODE == retCode)
            {
                continue;
            }
            
            //检查不通过，FMS已成功入库，但是还没有回ack，不做处理，要回ack，放入ack入队列
            if (AlarmConstant.NEEDACK_CODE == retCode)
            {
                //将该对象放入ACK队列，等待ACK返回
                AckModelConvertor.offerToAlarmAckQueue(alarmList.get(i).getFmaIp(),
                        alarmList.get(i).getFmaPort(),
                        String.valueOf(alarmList.get(i).getSequenceNo()));
            }
            else
            {
                //检查通过的告警放入validAlarmList
                validAlarmList.add(alarmList.get(i));
            }
        }
        
        return validAlarmList;
    }
    
    private void printLog(List<AlarmHandleModel> alarmCacheList, List<AlarmHandleModel> eventCacheList, String omaKey)
    {
        int loop = 1;
        StringBuilder strAlarm = new StringBuilder();
        strAlarm.append(AlarmConstant.NEW_LINE_SINGLE).append("alarm size:").append(alarmCacheList.size());
        if (!alarmCacheList.isEmpty())
        {
            for (AlarmHandleModel alarmInfo : alarmCacheList)
            {
                strAlarm.append(AlarmConstant.NEW_LINE_SINGLE).append(loop + ":").append(alarmInfo.toString());
                loop++;
            }
        }
        
        strAlarm.append(AlarmConstant.NEW_LINE_SINGLE).append("event size:").append(eventCacheList.size());
        if (!eventCacheList.isEmpty())
        {
            for (AlarmHandleModel alarmInfo : eventCacheList)
            {
                strAlarm.append(AlarmConstant.NEW_LINE_SINGLE).append(loop + ":").append(alarmInfo.toString());
                loop++;
            }
        }
        
        LOGGER.info("[fms]After executeWindowSlip, received alarms from fma[{}] {}",
                new Object[] {omaKey, strAlarm.toString()});
    }
    
    private void printLog(List<AlarmHandleModel> alarmCacheList, String omaKey)
    {
        int loop = 1;
        StringBuilder strAlarm = new StringBuilder();
        strAlarm.append(AlarmConstant.NEW_LINE_SINGLE).append("alarm size:").append(alarmCacheList.size());
        if (!alarmCacheList.isEmpty())
        {
            for (AlarmHandleModel alarmInfo : alarmCacheList)
            {
                strAlarm.append(AlarmConstant.NEW_LINE_SINGLE).append(loop + ":").append(alarmInfo.toString());
                loop++;
            }
        }
        
        LOGGER.info("[fms]Before executeWindowSlip, Received alarms from fma[{}] {}",
                new Object[] {omaKey, strAlarm.toString()});
    }
    
    /**
     * 
     * 告警信息转换 
     * 本质是将IP信息塞入到结构中以便后续ACK返回。  
     * @param AlarmsMsg     FMA侧通过thrift上报的告警数据类
     * @return  经过处理后含有IP信息的告警数据列表。
     */
    private List<AlarmHandleModel> alarmInfoTranster(AlarmsMsg alarmMessage)
    {
        LOGGER.debug("[fms] FmsThriftServiceImpl: alarmInfoTranster, enter.");
        
        //参数检查
        if (null == alarmMessage)
        {
            LOGGER.error("[fms] alarmInfoTranster param error, the alarmMessage is null");
            return null;
        }
        
        //参数有效性检查
        if (!checkParam(alarmMessage))
        {
            LOGGER.error("[fms] alarmInfoTranster param error, the alarmMessage is : " + alarmMessage.toString());
            return null;
        }
        
        AlarmHandleModel model = null;
        List<Alarm> alarmList = alarmMessage.getAlarms();
        String strFmaIp = alarmMessage.getAgentIP();
        int iFmaPort = alarmMessage.getAgentPort();
        if ((null == alarmList) || (null == strFmaIp) || (0 > iFmaPort))
        {
            LOGGER.error("[fms] the info from FMA is null.");
            return null;
        }
        
        List<AlarmHandleModel> alarmHandleModelList = new ArrayList<AlarmHandleModel>();
        
        //循环取得alarmInfo并构造内部处理数据类AlarmHandleModel
        for (Alarm alarm : alarmList)
        {
            model = new AlarmHandleModel(strFmaIp, iFmaPort, alarm);
            alarmHandleModelList.add(model);
        }
        
        LOGGER.debug("[fms] FmsThriftServiceImpl: alarmInfoTranster, leave.");
        return alarmHandleModelList;
    }
    
    private boolean checkParam(AlarmsMsg alarmMessage)
    {
        return null != alarmMessage && null != alarmMessage.agentIP && 0 <= alarmMessage.agentPort
                && 0 != alarmMessage.getAlarmsSize();
    }
}
