from matplotlib import pyplot as plt
import json

with open('airbnb_datasets/neighbourhoods.geojson') as f:
    data = json.load(f)

i = 0
for feature in data['features']:
    i+= 1
    if(feature['properties']['neighbourhood'] == 'Chelsea'):
        crd = data['features'][i-1]['geometry']['coordinates'][0][0]
        break

fig = plt.figure()
ax = fig.add_subplot(1,1,1)
N = len(crd)
for i in range(0,N):
    x = [crd[i][0], crd[(i+1)%(N-1)][0]]
    y = [crd[i][1], crd[(i+1)%(N-1)][1]]
    ax.plot(x,y,color='black')

plt.show()
