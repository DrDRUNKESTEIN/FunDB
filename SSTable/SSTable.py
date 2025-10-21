from sortedcontainers import SortedKeyList
from collections import deque
class SSTable:
    def __init__(self):
        pass
    
    SSTableQueue=deque()
    @staticmethod
    def flush_to_disk(data:SortedKeyList,data_path):
        # print(data._keys[0][0])
        keys=data._keys[0]
        ct=0
        with open(data_path,"ab") as f:
            for key in keys:
                index=data.bisect_key_left(key)
                v1=data[index][0]
                v2=data[index][1]
                line=f"{v1} : {v2}\n".encode()
                f.write(line)
                ct+=1
            # if index<len(data) and data[index][0]==key:
            #     print(f"{key}: {data[index][1]}")
        
        print(f"Flushed {ct} Records To Disk")

    
    
    

            
                
            
        
    
        