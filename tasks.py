import time
from celery import Celery
from celery.task.base import periodic_task
import os

from kombu.utils import json

import celeryconfig
import redis

REDIS = redis.StrictRedis()

app = Celery('FUCK')
# app.config_from_object(celeryconfig)
app.conf.broker_url = 'redis://localhost:6379/0'
app.conf.broker_transport_options = { 'master_name': "cluster1" }

def gen_age():
    lst = []
    for i in range(100):
        REDIS.set('FUCK%s' % i, json.dumps({"name": "maks", "age": i+1}))
        REDIS.expire('FUCK%s' % i, 45)

def get_data():
    lst = []
    for i in range(100):
        lst.append(REDIS.get('FUCK%s' % i))
        REDIS.delete('FUCK%s' % i)
    print(lst)

@app.task(bind=True, name='test')
@periodic_task(run_every=5.0)
def test():
    print('Test')
    gen_age()
    # time.sleep(60)
    # get_data()
    return 'STOP'

    # os.mkdir(os.path.join(os.path.dirname(__file__), '/', 'testf'))
    # return ('FUCK', 'TEST')

if __name__ == "__main__":
    app.worker_main()