"""Word Appearence. This program takes as input a file where each line contains a comma
separated set of words and outputs for each word, a listing of the lines on which it appears.

"""

from mrjob.job import MRJob
import csv

#read the csv data.
def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader(line):
        return row

class Appearence(MRJob): 
	def mapper(self, words, lines):
		"""Extracts the words and the lines where they appeared on""" 
		lines= lines.split(',')
		for words in lines: # iterate over the lines to extract the line for each word by occurrence.
			yield words, lines


	def reducer(self, words, lines):
		""" Joining the lines the contains the same word"""
		lines= [','.join([(x) for x in lst]) for lst in lines]
		words= words.center(10) # formating the output layput to justify center.
		yield  words, sorted(lines) # lines are sorted alphabatically.
	
   
if __name__ == '__main__':
    Appearence.run()
