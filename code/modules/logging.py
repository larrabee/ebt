__author__ = 'larrabee'
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
