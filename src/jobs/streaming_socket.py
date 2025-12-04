import json
import socket
import time
import pandas as pd

def send_data_over_socket(file_path, host ='spark-master', port=9999, chunk_size =2):
    s =socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host,port))
    s.listen(1)
    print(f"listening for connection on {host}:{port}")
    
   
    last_sent_index =0
    while True:
         conn, address =s.accept()
         print(f"Connection from {address}")
         try:
            with open(file_path, 'r') as file:
               #skip the lines that were already sent
               for _ in range(last_sent_index): 
                  next(file)
               records =[]
               for line in file:
                  records.append(json.loads(line))
                  if(len(records)) ==chunk_size:
                     print(f"--- Sending a batch of {len(records)} records ---")
                     for record_to_send in records:
                        serialize_data = json.dumps(record_to_send).encode('utf-8')
                        conn.sendall(serialize_data+ b'\n')
                        time.sleep(5)
                     records =[]   
         except (BrokenPipeError, ConnectionResetError):
            print("Client disconnected")

         finally:
                  conn.close()
                  print("Connection closed")
if __name__ =="__main__":
   send_data_over_socket("datasets/yelp_academic_dataset_review.json")