import sys
import json
from xmlrpc.client import ServerProxy

def send_request(ip: str, port: int, request: str) -> str:
    response = None
    try :
        server = ServerProxy(f"http://{ip}:{port}")
        response = server.execute(request)
    except Exception as e :
        print(e)
    return response

if __name__ == "__main__":
    ip = "localhost"
    port = 5005

    if(len(sys.argv) != 3):
        print("client.py <ip> <port>")
        exit()

    ip = sys.argv[1]
    port = int(sys.argv[2])


    while True:
        print("1. Execute command")
        print("2. Exit")
        choice = input("Enter your choice: ")
        response = None
        if choice == "1":
            command = input("Enter the command : ")
            
            if(command == 'enqueue') :
                message = input("Enter the message : ")
                request = {
                    'command' : command,
                    'message' : message
                }
                json_request = json.dumps(request)
                response = send_request(ip, port, json_request)
            elif(command == 'dequeue') :
                request = {
                    'command' : command,
                    'message' : None,
                }
                json_request = json.dumps(request)
                response = send_request(ip, port, json_request)
            elif(command == 'log_request'):
                request = {
                    'command' : command,
                    'message' : None,
                }
                json_request = json.dumps(request)
                response = send_request(ip, port, json_request)
            else :
                print("Invalid choice. Please try again.")
            if(response is not None):
                response = json.loads(response)
                print("\n===============")
                print("Status : ", response['status'])
                print("Response:", response['message'])
                if(command == 'dequeue()') :
                    print("Message Dequeued : ", response['string'])
                print("===============")
        elif choice == "2":
            break
        else:
            print("Invalid choice. Please try again.")
