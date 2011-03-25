import sys
import time
import Queue
import urllib2
import itertools
import traceback
import multiprocessing
from multiprocessing import Value
from optparse import OptionParser

DEFAULT_MSGNUM = 1000
DEFAULT_BASEURL = """http://localhost/publish/?id="""
DEFAULT_CONCURRENCY = 5
DEFAULT_MSG = "dummy msg! "
QSIZE = 1000

parser = OptionParser()
parser.add_option("-c", "--chan", dest="chan", help="send message to this chan only")
parser.add_option("-r", "--range", dest="range", 
    help="send message to given chan range. Range consists of 2 integers that are separated by : and the first must be smaller or equal to the second one")
parser.add_option("-n", "--number", dest="number", help="send this many messages", default=DEFAULT_MSGNUM)
parser.add_option("-m", "--message", dest="message", help="message to send", default=DEFAULT_MSG)
parser.add_option("-R", "--repeat", dest="repeat", help="repeat message this many times. example: -m hello -r 2 => hellohello", default=1)
parser.add_option("-u", "--url", dest="url", help="URL where to send messages", default=DEFAULT_BASEURL)
parser.add_option("-C", "--concurrency", dest="concurrency", help="URL where to send messages", default=DEFAULT_CONCURRENCY)
(options, args) = parser.parse_args()

if options.chan is not None:
    chan_from, chan_to = int(options.chan), int(options.chan)+1
elif options.range is not None:
    chan_from, chan_to = (int(x) for x in options.range.split(":"))
    if chan_from > chan_to:
        print "chan. range start has to be smaller than range end"
        sys.exit(0)
    chan_to += 1
else:
    print "you have to specify either single chan with --chan or a range with --range"
    sys.exit(0)

num_msgs = int(options.number)
concurrency = int(options.concurrency)
# XXX: actually we can tell option parser to check that for us
if num_msgs < 0 or concurrency < 0:
    print "illegal arguments"
    sys.exit(1)

chan_generator = itertools.cycle(xrange(chan_from, chan_to))
msg_generator = itertools.repeat(options.message*int(options.repeat), num_msgs)

base_url = options.url

class ChannelStats(dict):
    """container for statistics about single push channel
    """

    def __init__(self, *args):
        dict.__init__(self)
        self['no_listener'] = 0
        self['total'] = 0
        self['errors'] = 0

    def __str__(self):
        return "total/no listener/errors: %d,%d,%d" % (self['total'], self['no_listener'], self['errors'])

def send_worker(queue, kill_flag):
    """worker process that actually sends out messages"""
    worker_stats = {}

    while 1:
        if kill_flag.value == 1:
            print "will die:"
            print(worker_stats)
            return worker_stats

        try:
            chanid, msg = queue.get(True, timeout=1.0)
        except Queue.Empty:
            # just so that we would be able to check the kill_flag periodically
            # we could of course use signals instead but it wouldn't be portable
            continue

        if chanid not in worker_stats:
            statd = ChannelStats()
            worker_stats[chanid] = statd
        else:
            statd = worker_stats[chanid]

        try:
            publish_uri = base_url+str(chanid)
            u = urllib2.urlopen(publish_uri, msg)

            statd['total'] += 1
            if u.code == 202:
                statd['no_listener'] += 1
                print "no listener!",str(chanid)

            u.close()
        except:
            statd['errors'] += 1
            print traceback.print_exc()
        finally:
            queue.task_done()

def spawn_children(queue, num_childs, kill_flag):
    """fork worker processes"""
    childl = []

    for x in range(num_childs):
        p = multiprocessing.Process(target=send_worker, args=(queue, kill_flag))
        # set daemon flag so that exiting the master process would take down workers with it
        p.daemon = True
        p.start()
        childl.append(p)

    return childl

def create_messages(queue, num_msgs, msg_generator, chan_generator):
    """master process loop that generates messages & queues them for sending
    returns number of messages generated
    """
    # chanid -> last_message_id
    chand = {}

    for i, msg in enumerate(msg_generator):
        chanid = str(chan_generator.next())
        chan_msg_id = chand.get(chanid, 0)

        msg += "_"+str(chan_msg_id)
        queue.put((chanid, msg))

        chand[chanid] = chan_msg_id + 1

    return i+1

if __name__ == '__main__':
    queue = multiprocessing.JoinableQueue(QSIZE)
    kill_flag = Value('b', 0)
    children = spawn_children(queue, concurrency, kill_flag)

    start_time = time.time()
    msgs_created = create_messages(queue, num_msgs, msg_generator, chan_generator)
    queue.join()
    end_time = time.time()
    kill_flag.value = 1
    time_spent = end_time-start_time

    for c in children:
        c.join()

    print "%s messages sent in %.2f seconds. mean: %.2f msg/s" % (msgs_created, time_spent, msgs_created/time_spent)
