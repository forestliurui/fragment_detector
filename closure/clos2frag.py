import numpy as np

up_down="down"

data=np.genfromtxt(up_down+'closure_raw.txt',delimiter='\t',dtype='int_')
output={}
for i in range(np.shape( data)[0] ):
	key=data[i,0]
	value=data[i,1]
	if not output.has_key(key):
		output[key]=[value]
	else:
		output[key].append(value)


if up_down=="up":
	delimiter_out="#"
else:
	delimiter_out="@"

with open(up_down+"closure.txt","w") as f:
	for key in output:
		f.write(str(key)+delimiter_out)
		f.write(','.join( str(e) for e in output[key] )  )
		f.write("\n")

