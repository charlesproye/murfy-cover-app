from appsignal import Appsignal

appsignal = Appsignal(name="EValue", active=True, ignore_actions=["GET /metrics"])
