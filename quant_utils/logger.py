import logging
import os
from logging.handlers import TimedRotatingFileHandler
import quant_utils.constant as constant


class Logger:
    def __init__(
        self,
        log_file=constant.LOG_FILE_PATH,
        log_level=logging.INFO,
        console_output=True,
    ):
        """
        初始化日志类

        :param log_file: 日志文件名，如果为None则不写入文件
        :param log_level: 日志级别，默认为INFO
        :param console_output: 是否在控制台输出日志，默认为True
        """
        self.logger = logging.getLogger(__name__)
        if self.logger.handlers:
            return
        self.logger.setLevel(log_level)

        formatter = logging.Formatter(constant.LOG_FORMAT)

        # 如果指定日志文件，则添加文件处理器
        if log_file:
            # 确保日志目录存在
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)

            # 使用时间轮转文件处理器
            file_handler = TimedRotatingFileHandler(
                log_file, when="midnight", interval=1, backupCount=7
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        # 如果需要在控制台输出日志，则添加控制台处理器
        if console_output:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def debug(self, message):
        """记录DEBUG级别的日志"""
        self.logger.debug(message)

    def info(self, message):
        """记录INFO级别的日志"""
        self.logger.info(message)

    def warning(self, message):
        """记录WARNING级别的日志"""
        self.logger.warning(message)

    def error(self, message):
        """记录ERROR级别的日志"""
        self.logger.error(message)

    def critical(self, message):
        """记录CRITICAL级别的日志"""
        self.logger.critical(message)


# 示例用法
if __name__ == "__main__":
    # 创建日志实例，指定日志文件和日志级别
    logger = Logger()

    # 记录不同级别的日志
    logger.debug("这是一个DEBUG级别的日志")
    logger.info("这是一个INFO级别的日志")
    logger.warning("这是一个WARNING级别的日志")
    logger.error("这是一个ERROR级别的日志")
    logger.critical("这是一个CRITICAL级别的日志")
