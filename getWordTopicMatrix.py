import sys
topic = -1
topics_map = {}
unique_words = []
matrix = {}
with open("data.csv") as topics:
  for line in topics:
    word = line.strip("\n")
    words = word.split(",")
    topic = int(words[0])
    word = words[1]
    wordlist = topics_map.get(topic)
    if wordlist != None:
      wordlist.append(word)
    else:
      topics_map[topic] = [word]
    if word not in unique_words:
      unique_words.append(word)
#print topics_map
#print unique_words
for key in topics_map.keys():
  matrix[key] = []
for word in unique_words:
  for key in topics_map.keys():
    if word in topics_map[key]:
      matrix[key].append(1)
    else:
      matrix[key].append(0)
#for key in topics_map.keys():
#  print str(matrix[key])
topics = ""
for key in topics_map.keys():
  topics += ",Topic "+  str(key)
print "Unique Words" + topics
for word in unique_words:
  cntr = ""
  for key in topics_map.keys():
    if word in topics_map[key]:
      cntr = cntr + ",1"
    else:
      cntr = cntr + ",0"
  print word + cntr
#for key in topics_map.keys():
#  print str(topics_map[key])
