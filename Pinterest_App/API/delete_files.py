import os

for n in range(1, 100000):
    os.remove(f'./api_data{n}.json')
print('all api_data files deleted')

