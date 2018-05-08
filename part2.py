from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
		

class MRJob_Max(MRJob):

	def steps(self):
		return [ MRStep(mapper=self.mapper,reducer=self.reducer),
			     MRStep(reducer=self.reducer_max) ]

	def mapper(self,_,line):
		n=[]
		for number in line.split(','):
			n.append(number)
		n = list(map(float, n))
		yield n,None

	def reducer(self,line,_):
		top=[]
		top.append(max(line))
		top.sort()
		yield None,top

	def reducer_max (self,_,result):

		x = max(result)
		number  = x[0]

		# yield number with decimal points only if needed
		if type( number ) is str:
			if number == '':
				number = 0
		number = float(number)
		if number.is_integer():
			yield 'Given this CSV file the maximum is:', int(number)
		else:
			yield 'Given this CSV file the maximum is:', number
		

if __name__ == '__main__':
	MRJob_Max.run()