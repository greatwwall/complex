/**
 * 
 * 告警预处理任务类
 * 对FMA侧上报的告警进行有效性检查和在加工(静态表查询等)输出待入库数据结构
 * DFV:需要设置告警原因码
 */
public class ProcessPretreatAlarm extends ConnectRunnable<AlarmHandleModel, AlarmModel>
{
    /**
     * 日志打印对象
     */
    private static final AppLogger LOGGER = AppLogger.getInstance(ProcessPretreatAlarm.class);
    
    /**
     * diffSourceAlarmManager
     */
    private static DiffSourceAlarmManager diffSourceAlarmManager = DiffSourceAlarmManager.getInstance();
    
    /**
     * 构造函数
     * @param inQueue 来料加工队列
     * @param outQueue 加工后队列
     */
    public ProcessPretreatAlarm(SerializeQueue<AlarmHandleModel> inQueue, SerializeQueue<AlarmModel> outQueue)
    {
        super(inQueue, outQueue);
    }
    
    /**
     * 
     * 具体执行处理的方法
     * 对FMA侧上报的告警进行有效性和再加工（静态表读取）等，输出到待入库队列
     * 
     */
    @Override
    protected void processRun()
    {
        LOGGER.debug("[fms] process pretreat alarm enter.");
        
        SerializeQueue<AlarmHandleModel> inQueue = getInQueue();
        SerializeQueue<AlarmModel> outQueue = getOutQueue();
        
        // 检查输入队列（告警上报队列）是否为空，为空则休眠0.5秒
        if (inQueue.isEmpty())
        {
            LOGGER.debug("[fms] inQueue is empty!");
            sleep(AlarmConstant.WAIT_TIME);
            return;
        }
        
        // 检查输出队列（告警待入库队列）是否已满，已满则休眠0.5秒
        if (outQueue.isFull())
        {
            LOGGER.debug("[fms] outQueue is full!");
            sleep(AlarmConstant.WAIT_TIME);
            return;
        }
        
        // 从输入队列中获取一次加工的内容，若原料类型错误，直接删除返回
        AlarmHandleModel alarm = inQueue.peek();
        if (null == alarm)
        {
            LOGGER.error("[fms]alarm get from queue is null.");
            return;
        }
        
        // 参数校验，不合法则直接丢弃，并将此告警ACK信息返回给FMA侧
        if (!validParameter(alarm))
        {
            //将该对象放入ACK队列，等待ACK返回
            //FmsUtil.offerToAlarmAckQueue(alarm.getFmaIp(), alarm.getFmaPort(), String.valueOf(alarm.getSequenceNo()));
            
            //从告警上报队列中移除该对象
            inQueue.poll();
            
            LOGGER.error("[fms]alarm Parameter is ERROR, alarm:{}", alarm);
            return;
        }
        
        // 转换为告警入库对象，等待入库
        AlarmModel destObj = pretreatAlarm(alarm);
        if (null == destObj)
        {
            //将该对象放入ACK队列
            //FmsUtil.offerToAlarmAckQueue(alarm.getFmaIp(), alarm.getFmaPort(), String.valueOf(alarm.getSequenceNo()));
            
            //从告警上报队列中移除该对象
            inQueue.poll();
            
            LOGGER.error("[fms] after pretreat alarm is null, the alarminfo is:{}", alarm.toString());
            return;
        }
        
        //获取不同源告警管理类
        //上报的告警为不同源告警则要放入不同源告警缓存，否则告警放入待入库队列，正常处理
        if (diffSourceAlarmManager.getDiffSourceAlarmMap().containsKey(destObj.getSvAlarmId()))
        {
            //对不同源告警进行预处理，将告警放入缓存
            diffSourceAlarmManager.putToCache(destObj);
            
            //将该对象放入ACK队列
            // FmsUtil.offerToAlarmAckQueue(destObj.getSvFmaip(), destObj.getiFmaport(),
            //     String.valueOf(destObj.getSvSequenceNo()));
            
            //从告警上报队列中移除该对象
            inQueue.poll();
            
            return;
        }
        
        // 加工完成后放入输出队列，此时失败则预处理线程将重新在取得此告警进行处理。
        if (!outQueue.offer(destObj))
        {
            LOGGER.error("[fms] put to outQueue failed! inSize:{}, outSize:{}",
                    new Object[] {inQueue.size(), outQueue.size()});
            return;
        }
        
        // 从输入队列中将已加工的内容删除,一旦失败则将重新被取得处理。
        if (null == inQueue.poll())
        {
            LOGGER.error("[fms] poll from inQueue failed");
            return;
        }
        
        LOGGER.debug("[fms] process pretreat alarm leave.");
    }
    
    /**
     * 
     * 参数校验
     * 针对FMA上报的告警进行参数检查，主要检查各参数时候为空
     * @param alarm     FMA侧上报的告警信息类
     * @return  true：参数合法  false：参数非法
     */
    private boolean validParameter(AlarmHandleModel alarm)
    {
        LOGGER.debug("[fms]ProcessPretreatAlarm: validParameter, enter.");
        
        //参数检查
        if (null == alarm)
        {
            LOGGER.error("[fms] alarm is null.");
            return false;
        }
        
        // 如果alarm非空 则取得alarm的id值
        long alarmId = alarm.getAlarmID();
        
        //判断告警ID是否在
        AlarmDefinition alarmDefinition = AlarmStaticDataManager.getInstance()
                .getAlarmDefinition(StringUtils.trim(String.valueOf(alarm.getAlarmID())));
        if (null == alarmDefinition)
        {
            LOGGER.error("[fms] checkAlarmValid: alarm id is invalid, alarm ID is {}", alarmId);
            return false;
        }
        
        //告警port信息检查
        if (AlarmConstant.ZERO > alarm.getFmaPort())
        {
            LOGGER.error("[fms] port is invalid,alarm ID is {}", alarmId);
            return false;
        }
        
        //告警模块对象检查
        if (FmsUtil.checkIsEmpty(alarm.getMocName()))
        {
            LOGGER.error("[fms] moType is null, alarm ID is {}", alarmId);
            return false;
        }
        
        //告警定位信息检查
        if (FmsUtil.checkIsEmpty(alarm.getLocationInfo()))
        {
            LOGGER.error("[fms] alarmLocationInfo is null,alarm ID is {}", alarmId);
            return false;
        }
        
        //来源ID检查
        if (FmsUtil.checkIsEmpty(alarm.getResourceID()))
        {
            LOGGER.error("[fms] resourceID is null,alarm ID is {}", alarmId);
            return false;
        }
        
        //告警附加信息检查
        if (null == alarm.getAdditionalInfo())
        {
            LOGGER.error("[fms] alarmadditionInfo is null,alarm ID is {}", alarmId);
            return false;
        }
        
        //Fma IP检查
        if (StringUtils.isBlank(alarm.getFmaIp()))
        {
            LOGGER.error("[fms] ip is null or blank,alarm ID is {}", alarmId);
            return false;
        }
        
        LOGGER.debug("[fms]ProcessPretreatAlarm: validParameter,leave.");
        return true;
    }
    
    /**
     * 
     * 告警预处理过程
     * 预处理的过程是将FMA上的告警信息和静态表中信息以及一些固定的默认值信息进行拼接来实例化一个入库信息类
     * @param alarm     FMA上报的告警信息类
     * @return  AlarmModel 告警入库信息类
     */
    private AlarmModel pretreatAlarm(AlarmHandleModel alarm)
    {
        LOGGER.debug("[fms]ProcessPretreatAlarm: pretreatAlarm, enter.");
        
        //参数检查
        if (null == alarm)
        {
            LOGGER.error("[fms] pretreatAlarm alarm is null.");
            return null;
        }
        
        // 将告警输入对象转换为入库对象,分为三个部分进行填充：
        AlarmModel msgModel = new AlarmModel();
        
        //FMA上报上来的信息填充 || 读取静态表填充 || 默认值填充（如否屏蔽等，保留GE的接口，统一填写不屏蔽）
        if (!fillAlarmModelByFma(msgModel, alarm) || !fillAlarmModelByTbl(msgModel) || !fillAlarmModelByDef(msgModel))
        {
            return null;
        }
        
        LOGGER.debug("[fms]ProcessPretreatAlarm: pretreatAlarm, leave.");
        return msgModel;
    }
    
    /**
     * 
     * 入库信息类信息填充
     * 用南向接收到的告警信息填充数据库入库类
     * 
     * @param msgModel    数据库入库类
     * @param alarm     南向上报告警类
     * @return true：数据填充成功 false：数据填充失败
     */
    private boolean fillAlarmModelByFma(AlarmModel msgModel, AlarmHandleModel alarm)
    {
        LOGGER.debug("[fms]ProcessPretreatAlarm: fillAlarmModelByFma, enter.");
        
        //参数检查
        if (null == msgModel)
        {
            LOGGER.error("[fms]the msgModel param of fillAlarmModelByFma is null.");
            return false;
        }
        
        if (null == alarm)
        {
            LOGGER.error("[fms]the alarm param of fillAlarmModelByFma is null.");
            return false;
        }
        //将接收上来的告警信息进行直接填充
        msgModel.setSvAlarmId(StringUtils.trim(String.valueOf(alarm.getAlarmID())));
        msgModel.setSvSequenceNo(StringUtils.trim(String.valueOf(alarm.getSequenceNo())));
        msgModel.setIAlarmCategory(alarm.getAlarmCategory());
        msgModel.setDtOccurTime(StringUtils.trim(String.valueOf(alarm.getOccurTime())));
        msgModel.setSvAlarmCause(String.valueOf(alarm.getAlarmCause()));
        msgModel.setSvMoc(StringUtils.trim(alarm.getMocName()));
        msgModel.setSvDn(StringUtils.trim(alarm.getResourceID()));
        
        //将定位信息和附加信息塞入数据库入库类中
        msgModel.setSvLocationInfo(StringUtils.trim(alarm.getLocationInfo()));
        msgModel.setSvAdditionalInfo(StringUtils.trim(alarm.getAdditionalInfo()));
        msgModel.setSvFmaip(StringUtils.trim(alarm.getFmaIp()));
        msgModel.setiFmaport(alarm.getFmaPort());
        
        //如果未上报level或level非法，统一设置为缺省值，后面通过告警定义替换
        msgModel.setIAlarmLevel(alarm.getAlarmSeverity());
        if (!msgModel.checkLevel())
        {
            msgModel.setIAlarmLevel(AlarmConstant.ALARM_INVALID_LEVEL);
        }
        
        //在此接口接收的都为自动清除
        if (AlarmCategory.CLEAR == AlarmCategory.findByValue(alarm.getAlarmCategory()))
        {
            msgModel.setIClearType(ClearType.NORMAL.getValue());
        }
        
        LOGGER.debug("[fms]ProcessPretreatAlarm: fillAlarmModelByFma, leave.");
        return true;
    }
    
    /**
     * 
     * 入库信息类信息填充
     * 根据告警信息查询静态表来填充数据库入库类
     * 
     * @param msgModel    数据库入库类
     * @return true：数据填充成功 false：数据填充失败
     */
    private boolean fillAlarmModelByTbl(AlarmModel msgModel)
    {
        LOGGER.debug("[fms]ProcessPretreatAlarm: fillAlarmModelByTbl, enter.");
        
        //参数检查
        if (null == msgModel)
        {
            LOGGER.error("[fms]the param of fillAlarmModelByTbl is null.");
            return false;
        }
        
        //读取告警的静态信息定义表进行数据填充
        AlarmDefinition alarmDefinition = AlarmStaticDataManager.getInstance()
                .getAlarmDefinition(msgModel.getSvAlarmId());
        if (null == alarmDefinition)
        {
            LOGGER.error("[fms]can not get the info from static table by {}.", msgModel.getSvAlarmId());
            return false;
        }
        
        // 如果是故障告警，要求告警参数和告警定位信息中的参数个数匹配
        List<AlarmLocationInfo> alarmLocInfos = AlarmStaticDataManager.getInstance()
                .getAlarmLocationInfo(msgModel.getSvAlarmId());
        
        int locParamsNum = AlarmProcessUtil.processParams(msgModel.getSvLocationInfo()).size();
        if (null == alarmLocInfos)
        {
            LOGGER.error("[fms] get alarm loc infos error, alarm:{}.", new Object[] {msgModel});
            return false;
        }
        if (msgModel.getIAlarmCategory() == AlarmConstant.ALARM_TYPE)
        {
            if (alarmLocInfos.size() != locParamsNum)
            {
                LOGGER.error("[fms] Alarm loc params error,alarm:{}, loc define:{}.",
                        new Object[] {msgModel, alarmLocInfos.size()});
                return false;
            }
        }else{
            if (alarmLocInfos.size() < locParamsNum)
            {
                LOGGER.error("[fms] ClearAlarm or Event loc params error,alarm:{}, loc define:{}.",
                        new Object[] {msgModel, alarmLocInfos.size()});
                return false;
            }
        }
        
        msgModel.fillAlarmModelByTbl(alarmDefinition);
        
        LOGGER.debug("[fms]ProcessPretreatAlarm: fillAlarmModelByTbl, leave.");
        return true;
    }
    
    /**
     * 
     * 入库信息类信息填充
     * 保留移植对象GE的接口，将此次不交付的功能点的值设置为默认，如屏蔽值等
     * 
     * @param msgModel    数据库入库类
     * @return true：数据填充成功 false：数据填充失败
     */
    private boolean fillAlarmModelByDef(AlarmModel msgModel)
    {
        LOGGER.debug("[fms]ProcessPretreatAlarm: fillAlarmModelByDef, enter.");
        
        //参数检查
        if (null == msgModel)
        {
            LOGGER.error("[fms]the param of fillAlarmModelByDef is null.");
            return false;
        }
        msgModel.fillAlarmModelByDef();
        
        LOGGER.debug("[fms]ProcessPretreatAlarm: fillAlarmModelByDef, leave.");
        return true;
    }
}
