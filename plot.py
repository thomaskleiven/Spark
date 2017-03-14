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

#crd = data['features'][0]['geometry']['coordinates'][0][0]

print len(crd)

fig = plt.figure()
ax = fig.add_subplot(1,1,1)
N = len(crd)
p1 = 4.07209233968
p2 = -7.39938614507
for i in range(0,N):
    x = [crd[i][0], crd[(i+1)%(N-1)][0]]
    y = [crd[i][1], crd[(i+1)%(N-1)][1]]
    print y,x
    ax.plot(x,y,color='black')

#plt.scatter(p1,p2)
plt.show()
