import asyncio
import json
import random
import socket
import time
from enum import Enum
from threading import Thread
from typing import List, Any
from xmlrpc.client import ServerProxy
from lib.struct.address import Address


class RaftNode:
    HEARTBEAT_INTERVAL = 1
    ELECTION_TIMEOUT_MIN = 2
    ELECTION_TIMEOUT_MAX = 3
    RPC_TIMEOUT = 0.5

    class NodeType(Enum):
        LEADER = 1
        CANDIDATE = 2
        FOLLOWER = 3

    def __init__(self, application : Any, addr: Address, contact_addr: Address = None):
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        self.app = application
        self.address: Address = addr
        self.type: RaftNode.NodeType = RaftNode.NodeType.FOLLOWER
        self.log: List[str] = []
        self.election_term: int = 0
        self.cluster_addr_list: List[Address] = []
        self.cluster_leader_addr: Address = None
        self.last_heartbeat_time = time.time()
        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)
            self.start_election_timeout()
            self.election_thread.start()
        self.isCommited = True

    # Internal Raft Node methods
    def __print_log(self, text: str):
        print(f"[{self.address}] [{time.strftime('%H:%M:%S')}] {text}")

    def __initialize_as_leader(self):
        self.__print_log("Initialize as leader node...")
        self.cluster_leader_addr = self.address
        self.type = RaftNode.NodeType.LEADER
        self.heartbeat_thread = Thread(target=asyncio.run, args=[self.__leader_heartbeat()])
        self.heartbeat_thread.start()

    async def __leader_heartbeat(self):
        while True:
            if(self.type == RaftNode.NodeType.LEADER):
                self.__print_log("[Leader] Sending heartbeat...")
                await self.send_heartbeat_to_followers()
                await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)

    async def send_heartbeat_to_followers(self):
        for follower_addr in self.cluster_addr_list:
            if self.address != Address(follower_addr['ip'], follower_addr['port']):
                try:
                    follower = ServerProxy(f"http://{follower_addr['ip']}:{follower_addr['port']}")
                    response_json = follower.heartbeat(json.dumps({"leader_addr": str(self.address)}))
                    response = json.loads(response_json)
                except Exception as e:
                    self.__print_log(f"[Leader] Error sending heartbeat to {follower_addr}: {str(e)}")

    def __try_to_apply_membership(self, contact_addr: Address):
        redirected_addr = contact_addr
        response = {
            "status": "redirected",
            "address": {
                "ip": contact_addr.ip,
                "port": contact_addr.port,
            }
        }
        
        while response["status"] != "success":
            redirected_addr = Address(response["address"]["ip"], response["address"]["port"])
            response = self.__send_request(self.address, "apply_membership", redirected_addr)
        self.log = response["log"]
        self.app.queue = response["queue"]
        self.cluster_addr_list = response["cluster_addr_list"]
        self.cluster_leader_addr = redirected_addr

        exists = False
        for follower_addr_list in self.cluster_addr_list:
            if self.address == Address(follower_addr_list['ip'], follower_addr_list['port']):
                exists = True

        if(not exists):
            for follower_addr_list in self.cluster_addr_list:
                self.__send_request(self.address, "add_member", Address(follower_addr_list['ip'], follower_addr_list['port']))

            self.cluster_addr_list.append(self.address)

    def apply_membership(self, addr_json : str):
        addr = json.loads(addr_json)
        addr = Address(addr["ip"], addr["port"])

        response = {
            "status" : "success",
            "queue" : self.app.queue,
            "log" : self.log,
            "cluster_addr_list" : self.cluster_addr_list,
        }
        return json.dumps(response)
    
    def add_member(self, addr_json : str):
        addr = json.loads(addr_json)
        addr = Address(addr["ip"], addr["port"])
        self.cluster_addr_list.append(addr)

        response = {
            "status" : "success",
            "cluster_addr_list" : self.cluster_addr_list,
        }
        return json.dumps(response)
        
    def __send_request(self, request, rpc_name: str, addr: Address) -> "json":
        try:
            node = ServerProxy(f"http://{addr.ip}:{addr.port}")
            json_request = json.dumps(request)
            rpc_function = getattr(node, rpc_name)
            response     = json.loads(rpc_function(json_request))
            return response
        except Exception as e:
            self.__print_log(f"Error sending request to {addr}: {str(e)}")
            return None

    def start_election_timeout(self):
        election_timeout = random.uniform(RaftNode.ELECTION_TIMEOUT_MIN, RaftNode.ELECTION_TIMEOUT_MAX)
        self.election_thread = Thread(target=self.__start_election_timeout_thread, args=(election_timeout,))
        
    def __start_election_timeout_thread(self, timeout: float):
        while True : 
            if self.type == RaftNode.NodeType.FOLLOWER and time.time() -  self.last_heartbeat_time >= timeout:
                self.__print_log("[Follower] Election timeout reached. Starting new election...")
                self.start_election()
                break

    def start_election(self):
        self.type = RaftNode.NodeType.CANDIDATE
        self.election_term += 1
        self.voted_for = self.address
        self.vote_count = 1
        self.last_heartbeat_time = time.time()
        inactive_node = 0
        self.__print_log(f"[Candidate] Starting election for term {self.election_term}...")
        for follower_addr in self.cluster_addr_list:
            if self.address != Address(follower_addr['ip'], follower_addr['port']):
                request = {
                    "candidate_addr": self.address,
                    "term": self.election_term
                }
                response = self.__send_request(request, "vote_request", Address(follower_addr['ip'], follower_addr['port']))

                if(response is not None):
                    if(response['vote_granted']['ip'] == self.address.ip and response['vote_granted']['port'] == self.address.port):
                        self.vote_count += 1
                else:
                    inactive_node += 1
        
        if(self.vote_count >= ((len(self.cluster_addr_list)-inactive_node)//2 ) + 1) :
            self.__initialize_as_leader()
            for follower_addr in self.cluster_addr_list:
                if self.address != Address(follower_addr['ip'], follower_addr['port']):
                    request = {
                        "leader_addr": self.address,
                        "queue": self.app.queue,
                        "log": self.log,
                    }
                    self.__send_request(request, "change_leader_addr", Address(follower_addr['ip'], follower_addr['port']))
            self.vote_count = 1

        self.start_election_timeout()

    def vote_request(self, json_request: str) -> "json":
        request = json.loads(json_request)
        candidate_addr = request["candidate_addr"]
        term = request["term"]

        if term > self.election_term:
            self.__print_log(f"[Follower] Received vote request from {candidate_addr['ip']}:{candidate_addr['port']} for term {term}")
            self.type = RaftNode.NodeType.FOLLOWER
            self.election_term = term
            self.voted_for = None
            self.vote_count = 0
            self.last_heartbeat_time = time.time()

        if term == self.election_term and (self.voted_for is None or self.voted_for == candidate_addr):
            self.__print_log(f"[Follower] Voting for {candidate_addr['ip']}:{candidate_addr['port']} in term {term}")
            self.voted_for = candidate_addr
            

        response = {
            "vote_granted": self.voted_for
        }
        return json.dumps(response)
    
    def change_leader_addr(self, json_request : str) -> "json":
        request = json.loads(json_request)
        leader_addr_dict = request['leader_addr']
        leader_addr = Address(leader_addr_dict['ip'], leader_addr_dict['port'])
        self.cluster_leader_addr = leader_addr
        self.app.queue = request['queue']
        self.log = request['log']
        self.start_election_timeout()
        self.election_thread.start()

        response = {
            "leader_addr_change" : True,
        }

        return json.dumps(response)

    def heartbeat(self, json_request: str) -> "json":
        request = json.loads(json_request)
        leader_addr = request["leader_addr"]
        self.cluster_leader_addr = leader_addr
        self.type = RaftNode.NodeType.FOLLOWER
        self.last_heartbeat_time = time.time()

        response = {
            "heartbeat_response": "ack",
            "address": str(self.address),
        }
        return json.dumps(response)

    # Client RPCs
    def execute(self, json_request: str) -> "json":
        request = json.loads(json_request)
        command = request['command']
        message = request['message']
        response = None
        
        if(self.type == RaftNode.NodeType.LEADER):
            if(command == 'enqueue'):
                self.log.append(f"enqueue('{message}')")
                count = 1
                for addr in self.cluster_addr_list:
                    if self.address != Address(addr["ip"], addr["port"]):
                        req = {
                            'command' : 'enqueue_follower',
                            'message' : message
                        }
                        print("sending enqueue to follower", addr)
                        response = self.__send_request(req, 'execute', Address(addr["ip"], addr["port"]))
                        if(response is not None):
                            if(response['status'] == 1 and response['message'] == f"enqueue('{message}')"):
                                count += 1
                if(count >= ((len(self.cluster_addr_list))//2 ) + 1):
                    self.app.enqueue(message)
                    print("commit")
                    # notify commit
                    self.notify_commit(message)
                    response = {
                        'status' : 1,
                        'message' : f"enqueue('{message}')"
                    }
            elif(command == 'dequeue'):
                self.log.append(f"dequeue")
                count = 1
                for addr in self.cluster_addr_list:
                    if self.address != Address(addr["ip"], addr["port"]):
                        req = {
                            'command' : 'dequeue_follower',
                            'message' : None
                        }
                        print("sending dequeue to follower", addr)
                        response = self.__send_request(req, 'execute', Address(addr["ip"], addr["port"]))
                        if(response is not None):
                            if(response['status'] == 1 and response['message'] == f"dequeue()"):
                                count += 1
                if(count >= ((len(self.cluster_addr_list))//2 ) + 1):
                    print("commit")
                    string = self.app.dequeue()
                    # notify commit
                    self.notify_commit()
                    response = {
                        'status' : 1,
                        'message' : f"dequeue()",
                        'string' : string
                    }
            elif(command == 'log_request'):
                response = {
                    'status' : 1,
                    'message' : self.log
                }
        elif(command == "enqueue_follower"):
            print("Received enqueue log from leader")
            self.log.append(f"enqueue('{message}')")
            response = {
                'status' : 1,
                'message' : f"enqueue('{message}')",
            }
        elif(command == "dequeue_follower"):
            print("Received dequeue log from leader")
            self.log.append(f"dequeue()")
            response = {
                'status' : 1,
                'message' : f"dequeue()",
            }
        else :
            ip = self.cluster_leader_addr.split(':')[0]
            port = int(self.cluster_leader_addr.split(':')[1])
            print('redirected')
            response = self.__send_request(request, 'execute', Address(ip, port))
            # isLeaderCommit = self.__send_request({}, 'check_leader_commit', Address(ip, port))
            if(response is None):
                if(command == 'enqueue'):
                    response = {
                        'status' : 1,
                        'message' : f"enqueue('{message}')"
                    }
                elif(command == 'dequeue'):
                    response = {
                        'status' : 1,
                        'message' : f"dequeue()",
                        'string' : string
                    }
                self.commited = False
        return json.dumps(response)
    
    def check_leader_commit(self, json_req:str):
        return json.dumps({'isCommited': self.isCommited})

    def commit(self, json_request: str):
        request = json.loads(json_request)
        command = request['command']
        message = request['message']
        
        if(command == 'enqueue'):
            if (self.log[-1] != f"enqueue('{message}')"):
                self.log.append(f"enqueue('{message}')")
            self.app.enqueue(message)
            print("commit")
        else:
            if (self.log[-1] != f'dequeue()'):
                self.log.append(f"dequeue()")
            self.app.dequeue()
            print("commit")
        return json.dumps({"status" : "commited"})

    def notify_commit(self, message: str = None):
        self.isCommited = True
        for addr in self.cluster_addr_list:
            if self.address != Address(addr["ip"], addr["port"]):
                if(message is None):
                    request = {
                        "status": "commit",
                        "command": "dequeue",
                        "message": None
                    }
                else:
                    request = {
                        "status": "commit",
                        "command": "enqueue",
                        "message": message
                    }
                print("sending commit to follower", addr)
                self.__send_request(request, "commit", Address(addr["ip"], addr["port"]))