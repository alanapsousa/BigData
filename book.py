import requests
from pyspark import SparkContext


#Download "Bautiful Stories from Shakespeare" from Project Gutenberg
url = "https://www.gutenberg.org/cache/epub/1430/pg1430.txt"

response = requests.get(url)

book = response.text

print(book[1000:10000])  # Print a few characters of the book

# Save the book to a text file
#with open("beautiful_stories_from_shakespeare.txt", "w", encoding="utf-8") as f:
#    f.write(book)

sc = SparkContext('local[*]', 'book_analysis')
#rdd = sc.textFile("BeautifulStories.txt")
rdd = sc.parallelize(book.split("\r\n"))

# Display the first 5 lines of the RDD
print('\n',rdd.take(5),'\n') 

# Count the number of lines in the book
line_count = rdd.count()
print("Number of lines in the book:", line_count)

# most common words in the book
words_rdd = rdd.flatMap(lambda line: line.split(" "))
words_rdd = words_rdd.map(lambda word: word.lower().strip('.,!?"();:'))

#stopwords = {"the", "and", "of", "to", "in", "a", "is", "for", "with", "on", "by", "was", "his", "he"}  #words to ignore

stopwords = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", 
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have",
    "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
    "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've",
    "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me",
    "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on",
    "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over",
    "own", "same", "said", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't",
    "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them",
    "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll",
    "they're", "they've", "this", "those", "through", "to", "too", "under", "until",
    "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while",
    "who", "who's", "whom", "why", "why's", "with", "will", "won't", "would", "wouldn't", "you",
    "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
}



filtered_rdd = words_rdd.filter(lambda w: w not in stopwords and len(w) > 3)

word_counts = (filtered_rdd.map(lambda w: (w, 1))
                .reduceByKey(lambda a, b: a + b)
                .sortBy(lambda x: x[1], ascending=False)

#    filtered_rdd.map(lambda word: word.lower().strip('.,!?"();:') not in stopwords)
#             .filter(lambda x: x[0] != "")
#             .reduceByKey(lambda a, b: a + b)
#             .sortBy(lambda x: x[1], ascending=False)
)   
print("Most common words in the book:", word_counts.take(10))
sc.stop()   


