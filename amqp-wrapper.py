import simplejson
import amqplib.client_0_8 as amqp
        
class Queue():
    """Base class for AMQP queue service."""
    def __init__(self, queue_name):
        self.conn = amqp.Connection('HOST_HERE', userid='guest', password='guest', ssl=False) 
        self.queue_name = queue_name
        self.ch = self.conn.channel()

    def declare(self):
        return self.ch.queue_declare(self.queue_name, passive=False, durable=True, exclusive=False, auto_delete=False)
        
    def __len__(self):
        """Return number of messages waiting in this queue"""
        _,n_msgs,_ = self.declare()
        return n_msgs
    
    def consumers(self):
        """Return how many clients are currently listening to this queue."""
        _,_,n_consumers = self.declare()
        return n_consumers
    

class QueueProducer(Queue):
    def __init__(self, queue_name):
        """Create new queue producer (guy that creates messages.) 
        Will create a queue if the given name does not already exist."""
        Queue.__init__(self, queue_name)
        self.ch.access_request('/data',active=True,read=False,write=True)
        self.ch.exchange_declare('sqs_exchange', 'direct', durable=True, auto_delete=False)
        qname, n_msgs, n_consumers  = self.declare()
        print "Connected to %s (%d msgs, %d consumers)" % (qname, n_msgs, n_consumers)
        self.ch.queue_bind(self.queue_name, 'sqs_exchange', self.queue_name)
    
    def delete(self):
        """Delete a queue and closes the queue connection."""
        self.ch.queue_delete(self.queue_name)
        self.ch.close()
        
    def write(self, message):
        """Write a single message to the queue. Message can be a dict or a list or whatever."""
        m = amqp.Message(simplejson.dumps(message), content_type='text/x-json')
        self.ch.basic_publish(m, 'sqs_exchange', self.queue_name)

class QueueConsumer(Queue):
    def __init__(self, queue_name):
        """Create new queue consumer (guy that listens to messages.)"""
        Queue.__init__(self, queue_name)
        self.ch.access_request('/data',active=True, read=True, write=False)
        self.ch.queue_bind(self.queue_name, 'sqs_exchange', self.queue_name)
    
    def ack(self, delivery_tag):
        """Acknowledge receipt of the message (which will remove it off the queue.)
         Do this after you've completed your processing of the message. 
         Otherwise after some amount of time (about a minute) it will go back on the queue.
         e.g. 
        
         (object, tag) = consumer.get()
         if(object is not None):
             error = doSomethingWithMessage(object)
             if(error is None):
                 consumer.ack(tag)
        
        """
        self.ch.basic_ack(delivery_tag)
        
    def get(self):
        """Get a message. Returns the object and a delivery tag.""" 
        m = self.ch.basic_get(self.queue_name)
        if(m is not None):
            try:
                ret = simplejson.loads(m.body)
            except ValueError:
                print "Problem decoding json for body " + str(m.body) + ". deleting."
                self.ack(m.delivery_tag)
                return (None, None)
            return (ret, m.delivery_tag)
        else:
            return (None,None)

