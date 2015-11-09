import sys
import csv
import random

test_dir = "/Users/liuyang/dev/spark/test"
test1 = test_dir + "/" + "test1.csv"
test2 = test_dir + "/" + "test2.csv"
test1x = test_dir + "/" + "test1x"
test2x = test_dir + "/" + "test2x"
out1 = test_dir + "/" + "output1"
out2 = test_dir + "/" + "output2"
out3 = test_dir + "/" + "output3"

def genJoinTable(fromTable1, fromTable2, table2JoinColIndex, toTable1 = "temp"):
    table1 = []
    joinCol = []
    joinColLen = 0

    with open(fromTable2, "rb") as csvfile:
        table2 = csv.reader(csvfile)
        joinCol = [row[table2JoinColIndex] for row in table2]
        joinColLen = len(joinCol)
    with open(fromTable1, "rb") as readerFile:
        with open(toTable1, "wb") as writerFile:
            reader = csv.reader(readerFile)
            writer = csv.writer(writerFile)
            for row in reader:
                row.append(joinCol[random.randint(0, joinColLen - 1)])
                writer.writerow(row)

def genDataFromSource(fromTable, genFactor, startColumn, startValue, toTable):
    data = []
    with open(fromTable, "rb") as csvfile:
        table1 = csv.reader(csvfile)
        for row in table1:
            data +=[row[startColumn:]]
    data_length = len(data)
    rowNum = data_length * genFactor;
    with open(toTable, "wb") as writeFile:
        writer = csv.writer(writeFile)
        for i in range(startValue, startValue + rowNum + 1):
            row = [i] + data[random.randint(0, data_length - 1)]
            # print row
            writer.writerow(row)


genJoinTable(test1, test2, 0, out1)
genDataFromSource(test1x, 1000, 1, 1000, out2)
genJoinTable(out2, test2x, 0, out3)




















