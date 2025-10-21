import struct
import time
import zlib
from sortedcontainers import sortedlist,SortedKeyList,sorteddict

class Memtable:
    def __init__(self,maximum_size_limit:int,log_file_path:str):
        self.maximum_size_limt=maximum_size_limit
        self.log_file_path=log_file_path
        self.data=SortedKeyList(key=lambda x:x[0])  
    

    def put(self,key:str,value:str,op_type:int=1):
        timestamp = int(time.time_ns())
        key_bytes = key.encode()
        val_bytes = value.encode() if value else b""
        key_len = len(key_bytes)
        val_len = len(val_bytes)

        
        body = struct.pack(
            f"<B Q I I {key_len}s {val_len}s",
            op_type, timestamp, key_len, val_len, key_bytes, val_bytes
        )

        
        checksum = zlib.crc32(body)
        header = struct.pack("<I I", checksum, len(body))
        record = header + body

        
        with open(self.log_file_path, "ab") as f:
            f.write(record)
        
        self.data.add((key,value))
    
    def update_record(self,key:str,value:str,op_type:int=2):
        timestamp = int(time.time_ns())
        key_bytes = key.encode()
        val_bytes = value.encode() if value else b""
        key_len = len(key_bytes)
        val_len = len(val_bytes)

        
        body = struct.pack(
            f"<B Q I I {key_len}s {val_len}s",
            op_type, timestamp, key_len, val_len, key_bytes, val_bytes
        )

       
        checksum = zlib.crc32(body)
        header = struct.pack("<I I", checksum, len(body))
        record = header + body

        
        with open(self.log_file_path, "ab") as f:
            f.write(record)
    


        self.data.remove((key,self.get(key)))
        self.data.add((key,value))

    def delete_record(self,key:str,op_type:int=3):
        timestamp = int(time.time_ns())
        key_bytes = key.encode()
        key_len = len(key_bytes)
        

        
        body = struct.pack(
            f"<B Q I I {key_len}s",
            op_type, timestamp, key_len, key_bytes,
        )

        
        checksum = zlib.crc32(body)
        header = struct.pack("<I I", checksum, len(body))
        record = header + body

        
        with open(self.log_file_path, "ab") as f:
            f.write(record)
    


        self.data.remove(key)
        
    
    def get(self,key:str):
        index=self.data.bisect_key_left(key)
        if index<len(self.data) and self.data[index][0]==key:
            return self.data[index][1]
        return None
    
    def restore_from_log(self,log_file_path):
        records = []
        with open(log_file_path, "rb") as f:
            while True:
                header = f.read(8)
                if not header:
                    break
                checksum, length = struct.unpack("<I I", header)
                body = f.read(length)
                if zlib.crc32(body) != checksum:
                    print("Corruption detected, skipping record")
                    break

                op_type, timestamp, key_len, val_len = struct.unpack("<B Q I I", body[:17])
                offset = 17
                key = body[offset:offset + key_len].decode()
                value = body[offset + key_len:offset + key_len + val_len].decode()
                records.append((op_type, timestamp, key, value))
                if(op_type==1):
                    self.data.add((key,value))
                elif(op_type==2):
                    self.data.remove((key,self.data[key]))
                    self.data.add((key,value))
                
                elif(op_type==3):
                    self.data.remove(key)
        return records
    
    
    