import time
import urllib2
import traceback
import multiprocessing

REQ_URI = """http://localhost/activity/?id=%d"""
CONCURRENCY = 20
VERBOSE = True

def listener_loop(chan_id):
    last_seen_msg_id = None
    response_stats = {
        409: 0,
        'exceptions': 0,
        'messages': 0,
        'lost_messages': 0,
    }

    while 1:
        bind_timestamp = time.time()
        try:
            open_uri = REQ_URI % (chan_id,)
            print time.time(),open_uri
            r = urllib2.urlopen(open_uri, timeout=5.0)
            msg = r.read()

            msg_id = int(msg.rsplit("_", 1)[-1])
            if last_seen_msg_id is not None and msg_id != last_seen_msg_id + 1:
                print "Unexpected msg ID on channel %d expected %d got %d" % (chan_id, last_seen_msg_id+1, msg_id)
                response_stats['lost_messages'] += 1
            else:
                response_stats['messages'] += 1
            last_seen_msg_id = msg_id

            if VERBOSE:
                print "c:",chan_id,"message:",msg,"id:",last_seen_msg_id
            else:
                print "@c:",chan_id,"id:",last_seen_msg_id

        except urllib2.HTTPError, instance:
            response_stats[int(instance.code)] += 1
            print "c:",chan_id,"409 responses seen:",response_stats[409]
            #time.sleep(1)

        except Exception, reason:
            traceback.print_exc()
            response_stats['exceptions'] += 1
            print "c:",chan_id,"exception occured:",reason
            print "response in %.2fs" % (time.time()-bind_timestamp)
            time.sleep(1)

def spawn_children(child_count):
    """fork worker processes"""
    for x in range(child_count):
        p = multiprocessing.Process(target=listener_loop, args=(x+1,))
        # set daemon flag so that exiting the master process would take down workers with it
        p.daemon = True
        p.start()

if __name__ == '__main__':
    spawn_children(CONCURRENCY)
    time.sleep(2)
    spawn_children(CONCURRENCY)
    for child in multiprocessing.active_children():
        child.join()
