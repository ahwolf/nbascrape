import csv
import MySQLdb

db = MySQLdb.connect(host="localhost", port=3306, user="root", db="nba_salary")

cursor = db.cursor()


db.close()

with open("../data/nba_advanced_stats.csv", "rU") as stats, open("../data/nba_salary_csv.csv","rU") as salary, open("../data/nba_salary_stats_merged.csv","wb") as merge:
  stats_reader = csv.reader(stats)
  salary_reader = csv.reader(salary)  
  writer = csv.writer(merge)

  stats_header = stats_reader.next()
  salary_reader.next()
  salary_header = salary_reader.next()

  header = stats_header[1:] + salary_header[3:]

  print header

  stats_dict = {}
  description_set = set()
  for row in stats_reader:
    stats_dict[row[1]] = row[1:]

  for row in salary_reader:
    row = [x.replace("$","") for x in row]
    try:
      stats_dict[row[1]] = stats_dict[row[1]] + row[3:]
    except KeyError:
      print "fuck!", row

  writer.writerow(header)
  for player, stats_list in stats_dict.iteritems():
    
    if len(stats_list) == 41:
      stats_list = stats_list[:26] + stats_list[34:]
    elif not len(stats_list) == 33:
      continue

    writer.writerow(stats_list)


