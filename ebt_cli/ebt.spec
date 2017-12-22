[Config]
logmethod = string_list(default=list('',''))
loglevel = option('debug', 'info', 'warn', 'error', 'crit', default=info)
logfile = string(default=/var/log/ebt.log)
max_log_size = float(default=4194304)

[MailConfig]
from = string
subject = string(default=ebt report)
server = string
port = string(default=25)
login = string
password = string
recipients = string_list
tls = boolean(default=True)





