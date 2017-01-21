#Page rank assignment

# Name : Gaurav Dhamdhere
# Student ID: 800934991


from pyspark import SparkConf, SparkContext #Import required packages from 'pyspark'
import sys # Import package to fire system commands used to get input and output paths from user
import re  # Import regex package

conf = SparkConf().setAppName("PageRank")	#Initialize Spark configuration
sc   = SparkContext(conf=conf)			#Initialize Spark Context object


iterationCount = 10	#Number of iterations


inputData = sc.textFile(sys.argv[1]).filter(lambda x: x!='')	# Get input data from the user specified input file into an RDD

# Function to get Source URL
def hasTitle(line):
	title = re.search(r'<title>(.*?)</title>',line).group() #Using regex to get Source URL
	return title[7:-8]

# Function to get Target URLs
def isLink(line):
	link = re.findall(r'\[\[(.*?)\]\]',line) #Using regex to get Target URL
	outlinks = []
	for i in link:
		i = re.sub("\[\[","",i) # Substituting unwanted square brackets in Target URLs
		i = re.sub("\]\]","",i)
		outlinks.append(i)
	return tuple(outlinks)

# Function to assign rank contributions to Target URLs
def getLinkContribs(src,links,value):
	outlinksCount = len(links)	
	contribList=[(src,0.0)];	#If empty list of target URLs, just return tuple (Source URL, 0.0)
	for link in links:
		contribList.append((link,1.0*value/outlinksCount)) # Assign each target URL equal part of Source URL's rank
	return tuple(contribList)



src_url = inputData.map(lambda x: hasTitle(x)) # Get Source URLs
links = inputData.flatMap(lambda x: isLink(x)) # Get Target URLs
totalNodeCount = src_url.union(links).distinct().count() # Get Total number of unique URLs


linksGraphRDD = inputData.map(lambda x: (hasTitle(x),isLink(x))) # Form Links graph



initRanks = linksGraphRDD.map(lambda x: (x[0],1.0/totalNodeCount)) # Initialize each source URL with a rank = 1 / total_number_of_nodes


# While loop to calculate page rank iteratively
count = 0
while (count < iterationCount):
	count = count + 1;
	jointGraph = linksGraphRDD.join(initRanks) # Add previous ranks to Link Graph
	rankContribs = jointGraph.flatMap(lambda x: getLinkContribs(x[0],x[1][0],x[1][1])) # Get contributions for each target URL
	ranksTotalContribs = rankContribs.reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], x[1])) #Sum up all contributions for each URL
	initRanks = ranksTotalContribs.map(lambda x: (x[0],(x[1]*0.85)+(1 - 0.85))) # Using page rank equation, include Damping Factor of 0.85


#After finishing all iterations, we get final ranks in the variable initRanks

finalRanks = initRanks.coalesce(1).sortBy(lambda x: -x[1]) #Sort in descending order of ranks

finalRanks.coalesce(1).map(lambda x: "%s\t%s"%(x[0],x[1])).saveAsTextFile(sys.argv[2]) # Save them to a file user given output path

