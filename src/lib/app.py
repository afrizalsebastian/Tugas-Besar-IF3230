class MessageQueue :
    def __init__(self) :
        self.queue = []

    def enqueue(self, message : str):
        self.queue.append(message)
    
    def dequeue(self):
        self.queue.pop(0)