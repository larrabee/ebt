import logging
import logging.handlers
import smtplib
from email.utils import formatdate


class BufferingSMTPHandler(logging.handlers.SMTPHandler):
    def __init__(self, mailhost, fromaddr, toaddrs, subject, credentials=None,
                 secure=None, capacity=1024 * 1000):
        logging.handlers.SMTPHandler.__init__(self, mailhost, fromaddr,
                                              toaddrs, subject,
                                              credentials, secure)

        self.capacity = capacity
        self.buffer = list()

    def emit(self, record):
        try:
            self.buffer.append(record)

            if len(self.buffer) >= self.capacity:
                self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def flush(self):
        if not self.buffer:
            return

        try:
            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT
            smtp = smtplib.SMTP(self.mailhost, port)
            msg = "From: {0}\nTo: {1}\nSubject: {2}\nDate: {3}\n\n".format(
                self.fromaddr,
                ",".join(self.toaddrs),
                self.getSubject(self.buffer[0]),
                formatdate())
            for record in self.buffer:
                msg = msg + self.format(record) + "\r\n"

            if self.username:
                if self.secure is not None:
                    smtp.ehlo()
                    smtp.starttls(*self.secure)
                    smtp.ehlo()
                smtp.login(self.username, self.password)
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.quit()
            self.buffer = []
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(self.buffer[0])


class Configurator:
    def __init__(self):
        self.msg_format = "%(asctime)s %(levelname)s: %(message)s"
        self.date_format = "%d-%m-%Y %H:%M:%S"
        self.formatter = logging.Formatter(fmt=self.msg_format, datefmt=self.date_format)
        logging.basicConfig(format=self.msg_format, datefmt=self.date_format, level=logging.DEBUG)
        self.log = logging.getLogger("__main__")

    def get_logger(self):
        return self.log

    def set_level(self, severity):
        log_levels = {'debug': logging.DEBUG, 'info': logging.INFO, 'warn': logging.WARN, 'error': logging.ERROR,
                      'crit': logging.CRITICAL}
        self.log.setLevel(log_levels[severity])

    def add_syslog_handler(self, address='/dev/log', facility='user'):
        syslog_handler = logging.handlers.SysLogHandler(address=address, facility=facility)
        self.log.addHandler(syslog_handler)
        self.log.debug('Syslog handler successfully added')

    def add_file_handler(self, path, max_size):
        file_handler = logging.handlers.RotatingFileHandler(path, maxBytes=max_size)
        file_handler.setFormatter(self.formatter)
        self.log.addHandler(file_handler)
        self.log.debug('File handler successfully added')

    def add_smtp_handler(self, fromaddr, mailhost, toaddrs, subject, secure, credentials):
        smtp_handler = BufferingSMTPHandler(fromaddr=fromaddr, mailhost=mailhost, toaddrs=toaddrs, subject=subject,
                                            secure=secure, credentials=credentials)
        smtp_handler.setFormatter(self.formatter)
        self.log.addHandler(smtp_handler)
        self.log.debug('SMTP handler successfully added')
