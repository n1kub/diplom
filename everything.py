import os
import time
import kafka_cons
import kafka_prod
import threading as t
import atexit

print('If you want to run new topic then input "newtopic <name of the Topic>"\n')
print('If you want to run existing topic then just print its name\n')
topic = input()

# os.chdir(r'startKafka')
# os.system(f'run.bat {topic}')
# os.chdir('..')
#
# time.sleep(10)

tr1 = t.Thread(target=kafka_cons.cons, args=(topic,))
tr1.start()
# main.cons(topic)
# os.system(f'python kafka_cons.py {topic}')
print('main is up')
time.sleep(5)
tr2 = t.Thread(target=kafka_prod.prod, args=(topic,))
tr2.start()

# kafka_prod.prod(topic)
# os.system(f'python kafka_prod.py {topic}')
print('prod is up')

# atexit.
