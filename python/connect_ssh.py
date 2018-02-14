# -*- coding: UTF-8 -*-
"""
本模块提供与具体Layer特性、具体流水线层级无绑定的DFV全工程项目通用的基础库构件
"""
from threading import Thread

import paramiko
import os
import time
import sys
import stat
import shutil
import subprocess
import re
import sqlite3
from collections import OrderedDict

from UniAutos.Device.Host.NasNode.OceanStor import SSHRemoteOperation
from UniAutos.Device.Storage.Fusion.FusionStorageNode import FusionStorageNode as Node
from UniAutos import Log
from UniAutos.Util import Comparision
from lib.common.constant import CLI


# try:
#     from openpyxl.reader.excel import load_workbook
#     from openpyxl.workbook.workbook import Workbook
#     from openpyxl.worksheet.worksheet import Worksheet
# except ImportError, e:
#     ret = subprocess.check_output(r"pip install openpyxl", shell=True)
#     if re.search(r"(?m)^Successfully installed.* openpyxl-[\d.]+", ret) is None:
#         raise ImportError("pip install openpyxl failed:\n %s" % ret)
#     from openpyxl.reader.excel import load_workbook
#     from openpyxl.workbook.workbook import Workbook
#     from openpyxl.worksheet.worksheet import Worksheet

reload(sys)
sys.setdefaultencoding('utf-8')

__version__ = "1.1"

log = Log.getLogger(__name__)

BASH_CHECK_SUCCESS_CMD = r'echo ----$?----'
BASH_SUCCESS_FLAG = r'----0----'

MODE_777 = stat.S_IXUSR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP \
           | stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH


def remove_directory(dir_path):
    for d_path, d_names, f_names in os.walk(dir_path):
        for name in f_names:
            f_path = os.path.join(d_path, name)
            os.chmod(f_path, MODE_777)
            os.remove(os.path.join(d_path, name))
        os.chmod(d_path, MODE_777)
    shutil.rmtree(dir_path)


def build_fusion_storage_node(ipv4, username, password, port=22, node_os='linux'):
    """
    使用ip地址、ssh登录信息快速创建一个 FusionStorageNode 对象，可供其他需要传递 FusionStorageNode 参数的函数/方法，以及对象
    构建（例如常用的 UniAutos.Device.Host.NasNode.OceanStor.SSHRemoteOperation 对象的构建）
    Author: zhouxiao 00204081

    :type ipv4: str
    :param ipv4: 目标节点的ipv4地址
    :type username: str
    :param username: 目标节点的ssh登录用户名
    :type password: str
    :param password: 目标节点的ssh登录密码
    :type port: int
    :param port: 目标节点的ssh登录端口号，默认为22
    :type node_os: str
    :param node_os: 目标节点的OS类型，默认为Linux
    :rtype: Node
    :return: 返回一个 FusionStorageNode 对象
    """
    param_dict = {"type": "standSSH", "port": str(port), "ipv4_address": ipv4, "os": node_os}
    return Node(username, password, param_dict)


def ip2num(ip):
    """将ip_v4地址转换为整形数字"""
    num = 0
    for i, sub_ip in enumerate(ip.split('.')[::-1]):
        num += int(sub_ip) * 256 ** i
    return num


def num2ip(number):
    """将传入的数字转换为ip_v4地址"""
    return '.'.join([str(int(number) / (256 ** i) % 256) for i in range(3, -1, -1)])


def mask2segment(gateway_mask):
    """
    用传入的网关、掩码得到可用的ip_V4地址范围起始值

    :type gateway_mask: str
    :param gateway_mask: 形如 "8.40.0.1/16" 格式的网关掩码字符串
    :rtype: Tuple<str, str>
    :return: 包含有效起始ipv4地址和有效结束ipv4地址的tuple对象
    """
    gateway, mask_num = gateway_mask.split("/")
    gateway_int = ip2num(gateway)
    mask_num = int(mask_num)
    assert mask_num in range(1, 32)

    number = (2 ** mask_num - 1) << (32 - mask_num)
    start_ip = num2ip((gateway_int & number) + 1)
    end_ip = num2ip((gateway_int | ~number) - 1)
    return start_ip, end_ip


def mask2ip(mask_num):
    """将传入的10进制掩码数字转为为ip_v4地址"""
    num = int(mask_num)
    assert num in range(1, 32)
    return num2ip(int('1' * num + '0' * (32 - num), 2))


def get_root_dir_path(path, level=1):
    """
    获取路径中的指定level层次的目录，level默认为1。
    在Windows下示例：
        get_root_dir_path(r"D:\test1\test2\test3\test.sh", 1) 得到 r"D:\test1"
        get_root_dir_path(r"D:\test1\test2\test3\test.sh", 2) 得到 r"D:\test1\test2"
    在Linux下示例：
        get_root_dir_path(r"/test1/test2/test3/test.sh", 1) 得到 r"/test1"
        get_root_dir_path(r"/test1/test2/test3/test.sh", 3) 得到 r"/test1/test2/test3"
    Author: z00204081

    :type path: str
    :param path: 目标路径
    :type level: int
    :param level: 目标层级，默认为1
    :rtype: str
    :return: 目标路径字符串
    """
    sep = "\\" if os.path.splitdrive(path)[0] != "" else "/"
    items = path.split(sep)
    _level = min(max(0, level), len(items))
    return sep.join(items[:_level + 1])


def has_inet_info(dict_info):
    """
    判断符合标准组网多网络平面结构的测试床中，<inet_panels>标签是否是真正有意义的内容，
    如果是空值，则该测试床对应的环境在LCM上录入的信息其实是单网络平面

    :type dict_info: dict
    :param dict_info: 已经解析出来的 <inet_panels>标签对应的dict对象
    :rtype bool
    :return: True or False
    """
    if not isinstance(dict_info, dict):
        return False

    for key, value in dict_info.iteritems():
        has_info = has_inet_info(value) if isinstance(value, dict) else (value != "")
        if has_info:
            return True
    return False


class FusionNodeUtils:
    def __init__(self):
        pass

    @staticmethod
    def get_external_ip(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['external']['ipv4_address'] \
            if has_inet_info(inet_dict) else node.localIP

    @staticmethod
    def get_external_dev(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['external']['bond_name'] \
            if has_inet_info(inet_dict) \
            else r"""`ip a | grep -e "$local_ip/[0-9]\+" | awk '{print $NF}'`"""

    @staticmethod
    def get_external_gateway(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['external']['bond_name'] \
            if has_inet_info(inet_dict) else None

    @staticmethod
    def get_internal_ip(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['internal']['ipv4_address'] \
            if has_inet_info(inet_dict) else node.localIP

    @staticmethod
    def get_internal_dev(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['internal']['bond_name'] \
            if has_inet_info(inet_dict) \
            else r"""`ip a | grep -e "$local_ip/[0-9]\+" | awk '{print $NF}'`"""

    @staticmethod
    def get_internal_gateway(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['internal'].get('gateway', None) \
            if has_inet_info(inet_dict) else None

    @staticmethod
    def get_oam_ip(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['oam']['ipv4_address'] \
            if has_inet_info(inet_dict) else node.localIP

    @staticmethod
    def get_oam_dev(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['oam']['bond_name'] \
            if has_inet_info(inet_dict) \
            else r"""`ip a | grep -e "$local_ip/[0-9]\+" | awk '{print $NF}'`"""

    @staticmethod
    def get_oam_gateway(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['oam'].get('gateway', None) \
            if has_inet_info(inet_dict) else None

    @staticmethod
    def get_paxos_ip(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['paxos']['ipv4_address'] if has_inet_info(inet_dict) else node.localIP

    @staticmethod
    def get_paxos_dev(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['paxos']['bond_name'] \
            if has_inet_info(inet_dict) \
            else r"""`ip a | grep -e "$local_ip/[0-9]\+" | awk '{print $NF}'`"""

    @staticmethod
    def get_paxos_gateway(node):
        inet_dict = node.information.get("inet_panels", None)
        return inet_dict['paxos'].get('gateway', None) \
            if has_inet_info(inet_dict) else None


class LongSSHConnection:
    """
    This SSH class is used for keeping the session, for example, keep a different user and interact with the shell.
    It's not used for stdin, stdout or stderr. So the return is different from the single execution, you have to
    deal with the return string by yourself.

    Author: b00412650
    """
    PS1_DEFAULT = u"[dfv]# "

    def __init__(self, node=None, username=None, password=None, ip=None, ps1=PS1_DEFAULT):
        self.ip = node.localIP if ip is None else ip
        self.username = node.username if username is None else username
        self.password = node.password if password is None else password
        self.ps1 = ps1 if isinstance(ps1, (str, unicode)) else self.PS1_DEFAULT
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.chan = None
        self._open_channel()
        self.sftp = self.ssh.open_sftp()

    def _open_channel(self, timeout=10):
        """
        Open an ssh shell channel, for initialization.
        :param timeout: time to verify the channel
        :return:
        """
        timeout = int(timeout)
        try_times = 1
        max_try_times = 2

        while try_times <= max_try_times:
            try:
                self.ssh.connect(self.ip, 22, self.username, self.password, timeout=timeout, look_for_keys=False)
                self.ssh.get_transport().set_keepalive(30)
                self.chan = self.ssh.invoke_shell('dumb', 500, 500)
                start_time = time.time()
                current_time = time.time()
                while current_time - start_time < timeout:
                    time.sleep(0.5)
                    if self.chan.send_ready():
                        self.chan.send('TMOUT=0;PS1=%s\n' % self.ps1)
                        break
                    current_time = time.time()
                    if current_time - start_time >= timeout:
                        self.close()
                        msg = 'wait for ssh send ready to %s timeout' % self.ip
                        log.error(msg)
                        raise Exception(msg)

                start_time = current_time = time.time()
                while current_time - start_time < timeout:
                    time.sleep(0.5)
                    if self.chan.recv_ready():
                        info = self.chan.recv(65535)
                        log.info("On node(%s): SSH login init msg\n%s" % (self.ip, info))
                        break
                    current_time = time.time()
                    if current_time - start_time >= timeout:
                        self.close()
                        log.error('ssh receive from host %s timeout' % self.ip)
                        raise Exception('ssh receive from host %s timeout' % self.ip)
                break
            except Exception, e:
                import traceback
                stack = traceback.format_exc()
                log.error(('node(%s):' % self.ip) + str(e))
                log.error(('node(%s):' % self.ip) + str(stack))
                if try_times == max_try_times:
                    raise Exception('ssh connect to host %s timeout' % self.ip)
                time.sleep(2)
                try_times += 1
                log.info('node(%s): trying to connect channel %d times' % (self.ip, try_times))

    def copy_channel(self):
        timeout = 2
        try_times = 1
        max_try_times = 2
        chan = None

        while try_times <= max_try_times:
            try:
                chan = self.ssh.invoke_shell('dumb', 500, 500)
                start_time = current_time = time.time()
                while current_time - start_time < timeout:
                    time.sleep(0.5)
                    if chan.send_ready():
                        chan.send('TMOUT=0\n')
                        break
                    current_time = time.time()
                    if current_time - start_time >= timeout:
                        chan.close()
                        msg = 'wait for ssh send ready to %s timeout' % self.ip
                        log.error(msg)
                        raise Exception(msg)

                start_time = current_time = time.time()
                while current_time - start_time < timeout:
                    time.sleep(0.5)
                    if chan.recv_ready():
                        info = self.chan.recv(65535)
                        log.info("On node(%s): SSH login init msg\n%s" % (self.ip, info))
                        break
                    current_time = time.time()
                    if current_time - start_time >= timeout:
                        chan.close()
                        log.error('ssh receive from host %s timeout' % self.ip)
                        raise Exception('ssh receive from host %s timeout' % self.ip)
                break
            except Exception, e:
                import traceback
                stack = traceback.format_exc()
                log.error(('node(%s):' % self.ip) + str(e))
                log.error(('node(%s):' % self.ip) + str(stack))
                if try_times == max_try_times:
                    raise Exception('ssh copy channel from host %s timeout' % self.ip)
                time.sleep(2)
                try_times += 1
                log.info('node(%s): trying to copy channel %d times' % (self.ip, try_times))
        return chan

    def execute_cmd(self, cmd, expected_end=(PS1_DEFAULT.strip(), ), timeout=30, channel=None):
        """
        Execute a cmd. After execution, the ssh channel won't lose, which means this can switch to and keep
        a different user, interact with the ssh, and something like it. (ctrl + c is '\x03')
        :param channel: Cmd can be sent by a specific channel.
        :param cmd: Directly send a cmd.
        :param expected_end: A string or a tuple of strings. If the expected_end(s) is reached at the end of the result,
         return the result. If this parameter is wrong, it will be blocked until timeout seconds for returning.
         Example: expected_end='#' or expected_end=('#', '?')
        :param timeout: After timeout seconds, return the result, no matter if the expected_end(s) is reached.
        :return: result, contains the last PS1 sign(for example '[root@localhost ~]#').
        """
        chan = channel if channel else self.chan
        timeout = int(timeout)
        result = ''
        start_time = time.time()
        try:
            log.info('On node(%s): execute command: %s' % (self.ip, cmd))
            chan.send(cmd + '\n')
            while True:
                time.sleep(0.5)
                if chan.recv_ready():
                    ret = chan.recv(65535)
                    log.info('On node(%s): %s' % (self.ip, ret))
                    result += str(ret)
                current_time = time.time()
                if result.strip().endswith(expected_end) or current_time - start_time >= timeout:
                    start_index = 0
                    if result.startswith(cmd):
                        start_index = len(cmd)
                    if current_time - start_time >= timeout:
                        log.warn('On node(%s): receive timeout(%ds) (not reach the expected_end) '
                                 'and return the temporary output: '
                                 '%s' % (self.ip, timeout, result.strip()[start_index:]))
                    return result.strip()[start_index:]
        except Exception, e:
            log.warn('IP: ' + self.ip + '\n' + str(e))

    def close(self, channel=None):
        if channel is not None:
            channel.close()
        else:
            self.ssh.close()

    def isdir(self, path):
        try:
            return stat.S_ISDIR(self.sftp.stat(path).st_mode)
        except IOError:
            return False

    def remove_dir(self, dst_dir_path):
        if not self.isdir(dst_dir_path):
            log.warn('"%s" is not a directory on SFTP server' % dst_dir_path)
            return

        files = self.sftp.listdir(path=dst_dir_path)

        for f in files:
            file_path = os.path.join(dst_dir_path, f).replace(os.sep, "/")
            if self.isdir(file_path):
                self.remove_dir(file_path)
            else:
                self.sftp.remove(file_path)

        self.sftp.rmdir(dst_dir_path)

    def put_file(self, src_file_path, dst_path, dst_type="DIR"):
        if dst_type.lower() == "dir":
            dst_file_path = os.path.join(dst_path, os.path.basename(src_file_path)).replace(os.sep, "/")
        else:
            dst_file_path = dst_path

        self.sftp.put(src_file_path, dst_file_path)

    def put_dir(self, src_dir_path, dst_dir_path, include_src_root=True):
        if not os.path.isdir(src_dir_path):
            raise TypeError('"%s" is not a valid directory' % src_dir_path)

        if include_src_root:
            dst_root_dir_path = os.path.join(dst_dir_path, os.path.basename(src_dir_path)).replace(os.sep, "/")
        else:
            dst_root_dir_path = dst_dir_path

        if not self.isdir(dst_root_dir_path):
            self.sftp.mkdir(dst_root_dir_path)

        for root, dir_names, file_names in os.walk(src_dir_path):
            if root == src_dir_path:
                if include_src_root:
                    self.remove_dir(dst_root_dir_path)
                    self.sftp.mkdir(dst_root_dir_path)
                latest_dst_dir_path = dst_root_dir_path
            else:
                rel_path = os.path.relpath(root, src_dir_path).replace(os.sep, "/")
                latest_dst_dir_path = os.path.join(dst_root_dir_path, rel_path).replace(os.sep, "/")
                self.remove_dir(latest_dst_dir_path)
                self.sftp.mkdir(latest_dst_dir_path)

            for name in file_names:
                self.put_file(os.path.join(root, name), latest_dst_dir_path)


class CLIConnection:
    """
    Author: b00412650
    """

    def __init__(self, oam_s_node, cli_username=CLI.USERNAME, cli_password=CLI.PASSWORD):
        self.ssh = None
        self.oam_s_node = oam_s_node
        self.cli_username = cli_username
        self.cli_password = cli_password

    def login_cli(self, cli_username=None, cli_password=None, expected_end=CLI.PS):
        if cli_username:
            username = cli_username
            self.cli_username = cli_username
        else:
            username = self.cli_username

        if cli_password:
            password = cli_password
            self.cli_password = cli_password
        else:
            password = self.cli_password
        self.ssh = LongSSHConnection(self.oam_s_node)
        self.ssh.execute_cmd(cmd=CLI.START_SH + ' -u ' + username,
                             expected_end='Please input password:')
        # 登录成功一定是这个结束符，判断一次返回值，如果返回值不是这个结束符，说明超时或者登录失败了
        out = self.ssh.execute_cmd(password, expected_end=expected_end)

        Comparision.compTrue(out.endswith(expected_end), 'login result is:%s' % out)

    def execute_cli(self, cmd, cli_username=None, cli_password=None, expected_end=CLI.PS):
        """
        登录cli且保持连接，默认使用admin账号
        :param cmd: 要执行的cli命令
        :param cli_username:
        :param cli_password:
        :param expected_end:
        :return:
        """
        if cli_username:
            username = cli_username
        else:
            username = self.cli_username

        if cli_password:
            password = cli_password
            self.cli_password = cli_password
        else:
            password = self.cli_password

        # 当提交命令的账号发生改变时，需要重新登录
        if self.cli_username != username:
            self.close()
        if self.ssh is None:
            self.login_cli(username, password, expected_end=expected_end)

        out = self.ssh.execute_cmd(cmd, expected_end=expected_end, timeout=5)
        if out.endswith('#'):
            self.login_cli(username, password, expected_end=expected_end)
            out = self.ssh.execute_cmd(cmd, expected_end=expected_end)
        # 支持高危提示功能
        elif CLI.PS in out and expected_end != CLI.PS:
            self.ssh.execute_cmd('\x03', expected_end=CLI.PS)
        return out

    def close(self):
        if self.ssh is not None:
            self.ssh.execute_cmd('exit', expected_end='#')
            self.ssh.close()
            self.ssh = None


def upload_sth_to_nodes(local_path, remote_path, nodes):
    """
    Author: b00412650
    Uploads one file or one directory to all nodes.
    :param local_path: file or directory to be uploaded
    :param remote_path: upload path on remote nodes
    :param nodes: Hosts needed to be uploaded to
    """

    def __run(one_node, result_list):
        ssh = SSHRemoteOperation(one_node)
        mkdir_cmd = r'mkdir -p {path}; {check}'.format(path=remote_path, check=BASH_CHECK_SUCCESS_CMD)
        out, err = ssh.SSHRoExecuteCommand(mkdir_cmd)
        if BASH_SUCCESS_FLAG in out:
            msg = 'On node(%s): make the directory(%s) succeeded' % (one_node.localIP, remote_path)
            log.info(msg)
        else:
            msg = 'On node(%s): make the directory(%s) failed' % (one_node.localIP, remote_path)
            log.error(msg)
            result_list.append((False, msg))
            return

        upload_type = None
        if os.path.isdir(local_path):
            upload_type = 'DIR'
        elif os.path.isfile(local_path):
            upload_type = 'FILE'

        msg = 'Find the upload file %s, type is %s' % (local_path, str(upload_type))
        Comparision.compTrue(upload_type, msg)
        r = ssh.SSHPutFilesToRemote(local_path, remote_path, upload_type)
        msg = 'Upload to node(%s): %s --- result is %s' % (one_node.localIP, remote_path, r)
        if r == 'true':
            log.info(msg)
            result_list.append((True, msg))
        else:
            log.error(msg)
            result_list.append((False, msg))

    results = []
    upload_thread_list = []
    for node in nodes:
        t = Thread(target=__run, args=(node, results))
        upload_thread_list.append(t)
        t.start()
    for t in upload_thread_list:
        t.join()
    for result in results:
        Comparision.compTrue(result[0], result[1])


def run_cmd_on_nodes(cmd, nodes, expected_end='#'):
    def __run(one_node, result_list):
        ssh = LongSSHConnection(one_node)
        out = ssh.execute_cmd(cmd + ';' + BASH_CHECK_SUCCESS_CMD, expected_end=expected_end, timeout=120)
        log.info("execute_cmd out:%s" % out)
        if BASH_SUCCESS_FLAG in out:
            msg = 'On node(%s): execute the cmd(%s) succeeded' % (one_node.localIP, cmd)
            log.info(msg)
            result_list.append((True, msg))
        else:
            msg = 'On node(%s): execute the cmd(%s) failed' % (one_node.localIP, cmd)
            log.error(msg)
            result_list.append((False, msg))
        ssh.close()

    results = []
    thread_list = []
    for node in nodes:
        t = Thread(target=__run, args=(node, results))
        thread_list.append(t)
        t.start()
    for t in thread_list:
        t.join()
    for result in results:
        Comparision.compTrue(result[0], result[1])

#
# class LogParser(object):
#     """
#     DFV项目中ELK监控模块的日志文件分析，分析基于DFV后台日志列表对应的xlsx文件进行动态分析
#     """
#     COL_SERVICE, COL_COMPONENT, COL_LOGNAME, COL_DESC, COL_PATH, COL_BACKDIR_PATH, COL_ELKKEY, COL_COMMENT, COL_SIZE,\
#         COL_PERIOD = u"归属服务", u"归属组件", u"日志名称", u"日志功能描述", u"日志路径", u"日志转储路径", \
#                      u"ELK监控关键字（描述）", u"备注", u"合计大小", u"时间段"
#
#     col_names = (COL_SERVICE, COL_COMPONENT, COL_LOGNAME, COL_DESC, COL_PATH, COL_BACKDIR_PATH, COL_ELKKEY, COL_COMMENT,
#                  COL_SIZE, COL_PERIOD)
#
#     col_index_dict = OrderedDict()
#     for i, name in enumerate(col_names):
#         col_index_dict[name] = chr(ord(u'A')+i)
#
#     col_name_dict = {
#         COL_SERVICE: u'service', COL_COMPONENT: u'component', COL_LOGNAME: u"log_name",
#         COL_DESC: u"log_func_description", COL_PATH: u"log_path", COL_BACKDIR_PATH: u"log_backdir_path",
#         COL_ELKKEY: u"ELK_key", COL_COMMENT: u"comment", COL_SIZE: u"size", COL_PERIOD: u"period"
#     }
#     DB_NAME, DB_CONN_TIMEOUT = u"log_info.db", 30.0
#
#     def __init__(self, log_list_file_path, node_list):
#         self.__log_list_file_path = log_list_file_path
#         self.__db_path = os.path.join(get_root_dir_path(os.path.curdir), LogParser.DB_NAME)
#         self._wb = load_workbook(self.__log_list_file_path)
#         self._ws = None
#         self._db_connection = self.__connect_deploy_db()
#         self._latest_result_dict = OrderedDict()
#         for name in LogParser.col_names:
#             self._latest_result_dict[name] = None
#         self._latest_result_dict[LogParser.COL_SIZE] = 0
#         self._latest_result_dict[LogParser.COL_PERIOD] = "NotSet"
#
#     @staticmethod
#     def dict_factory(cursor, row):
#         d = {}
#         for idx, col in enumerate(cursor.description):
#             d[col[0]] = row[idx]
#         return d
#
#     def __connect_deploy_db(self):
#         conn = sqlite3.connect(self.__db_path, LogParser.DB_CONN_TIMEOUT)
#         cur = conn.cursor()
#         cur.execute("DROP TABLE IF EXISTS %s" % LogParser.TABLE_COLLECT_NAME)
#         conn.commit()
#         cur.execute("""CREATE TABLE IF NOT EXISTS {tb_name}
#                     (
#                       Id                INTEGER PRIMARY KEY NOT NULL,
#                       {col_service}     TEXT NOT NULL,
#                       {col_component}   TEXT NOT NULL,
#                       {col_logname}     TEXT NOT NULL,
#                       {col_logdesc}     TEXT NOT NULL,
#                       {col_logpath}     TEXT NOT NULL,
#                       {col_logbackpath} TEXT NOT NULL,
#                       {col_elkkey}      TEXT NOT NULL,
#                       {col_comment}     TEXT,
#                       {col_size}        INTEGER NOT NULL,
#                       {col_period}      TEXT NOT NULL
#                     );""".format(
#             tb_name=LogParser.TABLE_COLLECT_NAME, col_service=LogParser.col_name_dict[LogParser.COL_SERVICE],
#             col_component=LogParser.col_name_dict[LogParser.COL_COMPONENT],
#             col_logname=LogParser.col_name_dict[LogParser.COL_LOGNAME],
#             col_logdesc=LogParser.col_name_dict[LogParser.COL_DESC],
#             col_logpath=LogParser.col_name_dict[LogParser.COL_PATH],
#             col_logbackpath=LogParser.col_name_dict[LogParser.COL_BACKDIR_PATH],
#             col_elkkey=LogParser.col_name_dict[LogParser.COL_ELKKEY],
#             col_comment=LogParser.col_name_dict[LogParser.COL_COMMENT],
#             col_size=LogParser.col_name_dict[LogParser.COL_SIZE],
#             col_period=LogParser.col_name_dict[LogParser.COL_PERIOD])
#         )
#         conn.commit()
#         conn.row_factory = LogParser.dict_factory
#         return conn
#
#     def save_row(self, row_values):
#         cursor = self._db_connection.cursor()
#         sql = u"INSERT INTO {tb_name} ({col_service},{col_component},{col_logname},{col_logdesc},{col_logpath}, " \
#               u"{col_logbackpath}, {col_elkkey}, {col_comment}, {col_size}, {col_period}) " \
#               u"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".format(
#                 tb_name=LogParser.TABLE_COLLECT_NAME, col_service=LogParser.col_name_dict[LogParser.COL_SERVICE],
#                 col_component=LogParser.col_name_dict[LogParser.COL_COMPONENT],
#                 col_logname=LogParser.col_name_dict[LogParser.COL_LOGNAME],
#                 col_logdesc=LogParser.col_name_dict[LogParser.COL_DESC],
#                 col_logpath=LogParser.col_name_dict[LogParser.COL_PATH],
#                 col_logbackpath=LogParser.col_name_dict[LogParser.COL_BACKDIR_PATH],
#                 col_elkkey=LogParser.col_name_dict[LogParser.COL_ELKKEY],
#                 col_comment=LogParser.col_name_dict[LogParser.COL_COMMENT],
#                 col_size=LogParser.col_name_dict[LogParser.COL_SIZE],
#                 col_period=LogParser.col_name_dict[LogParser.COL_PERIOD])
#
#         params = row_values[:len(LogParser.col_index_dict)]
#         cursor.execute(sql, params)
#         self._db_connection.commit()
#
#     def __enter__(self):
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         if isinstance(self._wb, Workbook):
#             self._wb.close()
#         if isinstance(self._db_connection, sqlite3.Connection):
#             self._db_connection.close()
#
#     def parse_excel(self, sheet_name=None):
#         self._ws = self._wb.get_sheet_by_name(self._wb.get_sheet_names()[0] if sheet_name is None else sheet_name)
#         for i, row in enumerate(self._ws.rows):
#             if i == 0:
#                 continue
#
#             for key, index in LogParser.col_index_dict.iteritems():
#                 value = self._ws["%s%d" % (index, i+1)].value
#                 if value not in (None, u''):
#                     self._latest_result_dict[key] = value
#
#             self.save_row(self._latest_result_dict.values())
#


if __name__ == "__main__":
    pass
