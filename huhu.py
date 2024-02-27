# Create a file first: ./very_big_file.csv as:
# transaction_id,user_id,total_cost,dt
# 1,John,10.99,2023-04-15
# 2,Mary, 4.99,2023-04-12


# Example.py
def etl(item):
    # Do some etl here
    return item.replace("John", '****') 

# Create a generator 
def batch_read_file(file_object, batch_size=25):
    """Lazy function (generator) can read a file in chunks.
    Default chunk: 1024 bytes."""
    while True:
        data = file_object.read(batch_size)
        if not data:
            break
        yield data
# and read in chunks
with open('very_big_file.csv') as f:
    for batch in batch_read_file(f):
        print(etl(batch))
        print('\n')

# In command line run
# Python example.py