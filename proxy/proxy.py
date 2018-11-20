class AbuyunProxy(object):
    def __init__(self, config):
        config_proxy = config['proxy']
        self.enable = int(config_proxy['enable']) == 1
        if not self.enable:
            self.proxy = None
            return
        proxy_host = config_proxy['proxy_host']
        proxy_port = config_proxy['proxy_port']
        proxy_user = config_proxy['proxy_user']
        proxy_pwd = config_proxy['proxy_pwd']

        proxy_meta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": proxy_host,
            "port": proxy_port,
            "user": proxy_user,
            "pass": proxy_pwd,
        }
        self.proxy = {
            "http": proxy_meta,
            "https": proxy_meta,
        }
    
    def get(self):
        return self.proxy

    def __str__(self):
        return self.proxy
