from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol

		
class Mean(MRJob):
	
	def steps(self):
		return [
				MRStep(mapper=self.mapper1),
				MRStep(mapper=self.mapper2),
				MRStep(reducer=self.reducer)
        		]

	def mapper1(self, _, line):
		for number in line.split(','):
			yield (int(number),None)

	def mapper2(self,number,_):
		yield (None ,number)

	def reducer(self, _,total):
		Sum_Of_Numebrs   = 0
		Count_Of_Numbers = 0
		for i in total:
			Sum_Of_Numebrs   += i
			Count_Of_Numbers += 1
		yield 'Given the CSV file, the mean is :', Sum_Of_Numebrs/float(Count_Of_Numbers)
		
if __name__ == '__main__':
	Mean.run()