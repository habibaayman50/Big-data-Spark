#map-reduce
# Import findspark and initialize
import findspark
findspark.init()

from pyspark import SparkContext
from operator import add


sc = SparkContext.getOrCreate()

# Load the data
data = sc.textFile("C:/Users/ahmed/OneDrive/Desktop/pagecounts-20160101-000000_parsed.out")


# Function to parse each line of the input data
def parse_line(line):
    fields = line.split()
    if len(fields) == 4:  # Check if line has at least 4 fields
        return (fields[0], fields[1], int(fields[3]))
    else:
        return None


# Parse the data
parsed_data = data.map(parse_line).filter(lambda x: x is not None)


# 1. Compute min, max, and average page size
page_sizes = parsed_data.map(lambda x: x[2])
min_page_size = page_sizes.min()
max_page_size = page_sizes.max()
total_page_sizes = page_sizes.reduce(add)
average_page_size = total_page_sizes / page_sizes.count()

# 2. Determine the number of page titles that start with "The" and are not part of the English project
the_pages = parsed_data.filter(lambda x: x[1].startswith("The"))
english_the_pages = the_pages.filter(lambda x: not x[0].startswith("en"))
number_of_the_pages = the_pages.count()
number_of_english_the_pages = english_the_pages.count()

# 3. Determine the number of unique terms appearing in the page titles
# Normalization and splitting terms by underscores
unique_terms_count = parsed_data.flatMap(lambda x: x[1].lower().split('_')).distinct().count()

# 4. Extract each title and the number of times it was repeated
title_counts = parsed_data.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)


# 5. Combine data of pages with the same title and save each pair of pages data
grouped_data = parsed_data.map(lambda x: (x[1], x)).groupByKey()

# Save results to a document
with open("results.txt", "w") as f:
    f.write("Min page size: {}\n".format(min_page_size))
    f.write("Max page size: {}\n".format(max_page_size))
    f.write("Average page size: {}\n".format(average_page_size))
    f.write("Number of page titles that start with 'The': {}\n".format(number_of_the_pages))
    f.write("Number of 'The' page titles not part of English project: {}\n".format(number_of_english_the_pages))
    f.write("Number of unique terms: {}\n".format(unique_terms_count))


with open("results.txt", "a", encoding="utf-8") as f:
    f.write("\n Title\t\t\tCount\n")
    for title, count in title_counts.collect():
        f.write("{}\t\t{}\n".format(title, count))
        f.write("\n Pairs of pages with the same title:\n")
    for title, pages in grouped_data.collect():
        pages_list = list(pages)
        for i in range(len(pages_list)):
            for j in range(i+1, len(pages_list)):
                f.write("Page 1: {}\nPage 2: {}\n\n".format(pages_list[i], pages_list[j]))


# Stop SparkContext
sc.stop()

#_____________________________________________________________________________________________________________________
#spark loops
# Import findspark and initialize
import findspark
findspark.init()

# Initialize SparkContext
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# Load the data
data = sc.textFile("C:/Users/ahmed/OneDrive/Desktop/pagecounts-20160101-000000_parsed.out")


# Function to parse each line of the input data
def parse_line(line):
    fields = line.split()
    if len(fields) == 4:  # Check if line has at least 4 fields
        return (fields[0], fields[1], int(fields[3]))
    else:
        return None

# Parse the data
parsed_data = data.map(parse_line).filter(lambda x: x is not None)

# 1. Compute min, max, and average page size
min_page_size = float('inf')
max_page_size = float('-inf')
total_page_sizes = 0
page_count = 0

for line in parsed_data.collect():
    page_size = line[2]
    min_page_size = min(min_page_size, page_size)
    max_page_size = max(max_page_size, page_size)
    total_page_sizes += page_size
    page_count += 1

average_page_size = total_page_sizes / page_count



# 2. Determine the number of page titles that start with "The" and are not part of the English project
number_of_the_pages = 0
number_of_english_the_pages = 0

for line in parsed_data.collect():
    if line[1].startswith("The"):
        number_of_the_pages += 1
        if not line[0].startswith("en"):
            number_of_english_the_pages += 1


# 3. Determine the number of unique terms appearing in the page titles
unique_terms = set()

for line in parsed_data.collect():
    terms = line[1].lower().split('_')
    for term in terms:
        unique_terms.add(term)

unique_terms_count = len(unique_terms)


# 4. Extract each title and the number of times it was repeated
title_counts = {}

for line in parsed_data.collect():
    title = line[1]
    if title in title_counts:
        title_counts[title] += 1
    else:
        title_counts[title] = 1



# 5. Combine data of pages with the same title and save each pair of pages data
grouped_data = {}

for line in parsed_data.collect():
    title = line[1]
    if title in grouped_data:
        grouped_data[title].append(line)
    else:
        grouped_data[title] = [line]


# Save results to a document
with open("results2.txt", "w") as f:
    f.write("Min page size: {}\n".format(min_page_size))
    f.write("Max page size: {}\n".format(max_page_size))
    f.write("Average page size: {}\n".format(average_page_size))
    f.write("Number of page titles that start with 'The': {}\n".format(number_of_the_pages))
    f.write("Number of 'The' page titles not part of English project: {}\n".format(number_of_english_the_pages))
    f.write("Number of unique terms: {}\n".format(unique_terms_count))

with open("results.txt", "a", encoding="utf-8") as f:
    f.write("\n Title\t\t\tCount\n")
    for title, count in title_counts.collect():
        f.write("{}\t\t{}\n".format(title, count))
        f.write("\n Pairs of pages with the same title:\n")
    for title, pages in grouped_data.collect():
        pages_list = list(pages)
        for i in range(len(pages_list)):
            for j in range(i+1, len(pages_list)):
                f.write("Page 1: {}\nPage 2: {}\n\n".format(pages_list[i], pages_list[j]))


# Stop SparkContext
sc.stop()