import sys
import random
import math

class SourceGender(object):
    pass

class TableGenerator(object):
    """
    Base class for table generator
    """
    def __init__(self, *args, **kwargs):
        self._colNames = kwargs.pop('colNames', ["a"])
        self._locDict = kwargs.pop('locDict', {'a' : 'a'})
        self._rowNum = kwargs.pop('rowNum', 1000)
        self._dupFactor = kwargs.pop('dupFactor', 0.0)
        self._outPath = kwargs.pop('outPutLoc', "~/tmp/")

    def genOneCol(self):
        '''
        Generate one single column with colName, rowNum, dupFactor, and from the locDict
        '''
        pass

    def genColumns(self):
        '''
        Generate all the data with genOneCol and then concatenate the columns to get whole table.
        '''
        pass

    def getDateSource(self, columnName):
        with open(self._locDict[columnName], 'r') as f:
            source = f.read()
        return source

    def genIndexInRange(self, minVal, maxVal, uniqueFactor, rowNum):
        '''
        This is going to sample the index
        '''
        data = []
        if (uniqueFactor == 1):
            data = range(minVal, minVal + rowNum + 1)
        else:
            dist = math.ceil(rowNum / float(uniqueFactor))
            temp_data = range(minVal, minVal + dist)

            for x in range(0, uniqueFactor * 2):
                prev_data += temp_data[:]
            data = filter(lambda x: randint(1, 2) % 2 == 0, prev_data)
        return data

    def genDataWithSource(self, source, rowNum):
        if rowNum > len(source) :
            index = range(0, rowNum)

        else :
            return source[0:rowNum + 1]









class JoinTableGener(object):
    def __init__(self):
        self.filterFactor = 1
        self.joinFactor = 2

