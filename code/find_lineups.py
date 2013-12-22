"""
This python script query's the databases and finds the best team possible given salary cap limitations
"""

import sys
import time
import datetime
import MySQLdb
import os
import logging
import pickle
import datetime
from collections import defaultdict
from pulp import *


sys.path.append("../")
from libscrape.config import constants
from libscrape.config import db
from libscrape.config import config



LOGDIR_SOURCE = constants.LOGDIR_SOURCE
LOGDIR_EXTRACT = constants.LOGDIR_EXTRACT

logging.basicConfig(filename='etl.log',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

dbobj = db.Db(config.config['db'])
logging.info("Starting create table with database: %s" % (dbobj))


def aggregate_possessions_from_dates(cursor,
                                     start_date=datetime.date(2010,10,26),
                                     end_date=datetime.date.today(),
                                     max_salary=45):

  # get the stats

  sql = """
  SELECT pgp.player_id, 
         p.full_name, 
         sum(pgp.points_scored) / sum(pgp.offensive_possessions), 
         sum(pgp.points_allowed) / sum(pgp.defensive_possessions),
         pgp.total_time,
         s.pos,
         s.2013_14/1000000,
         s.2014_15/1000000,
         s.2015_16/1000000,
         s.2016_17/1000000,
         s.2017_18/1000000
  FROM player_game_possessions pgp 
  INNER JOIN player p on pgp.player_id = p.id 
  INNER JOIN salary s on pgp.player_id = s.player_id
  WHERE pgp.game_id in (select g.id from game g where g.date_played <= "%s" and g.date_played >= "%s")
  GROUP BY pgp.player_id 
  ORDER BY sum(pgp.points_scored)/sum(pgp.offensive_possessions);
  """%(end_date.strftime('%Y-%m-%d'), start_date.strftime('%Y-%m-%d'))

  cursor.execute(sql)

  player_dict = {}
  offensive_ppp_dict = {}
  defensive_ppp_dict = {}
  diff_ppp_dict = {}
  salary_dict = {}
  pg_dict = {}
  sg_dict = {}
  sf_dict = {}
  pf_dict = {}
  c_dict = {}
  minute_dict = {}

  name_list = []
  
  for values in cursor.fetchall():
    player_dict[values[1]] = {
      "id":values[0],
      "offensive_ppp":values[2],
      "defensive_ppp":values[3],
      "minutes": values[4],
      "name":values[1],
      "position": values[5],
      "2013_14":values[6],
      "2014_15":values[7],
      "2015_16":values[8],
      "2016_17":values[9],    
      "2017_18":values[10],
    } 

    if values[4] > 150:
      name_list.append(values[1])
      offensive_ppp_dict[values[1]] = values[2]
      defensive_ppp_dict[values[1]] = float(values[3])
      diff_ppp_dict[values[1]] = values[2] - float(values[3])
      salary_dict[values[1]] = float(values[6])

      pg_dict[values[1]] = 1 if values[5] == "PG" else 0
      sg_dict[values[1]] = 1 if values[5] == "SG" else 0
      sf_dict[values[1]] = 1 if values[5] == "SF" else 0
      pf_dict[values[1]] = 1 if values[5] == "PF" else 0
      c_dict[values[1]] = 1 if values[5] == "C" else 0
      minute_dict[values[1]] = values[4]



  # now that we have our dicts, lets do some LP!
  prob = LpProblem("Starting lineup", LpMaximize)
  player_vars = LpVariable.dicts("player",name_list,0,1,cat="Integer")
  prob += lpSum([diff_ppp_dict[i]*player_vars[i] for i in name_list]), "Difference of offensive and defensive"
  prob += lpSum([player_vars[i] for i in name_list]) == 5, "number of players"
  prob += lpSum([pg_dict[i] * player_vars[i] for i in name_list]) <= 1, "number of point guards"
  prob += lpSum([sg_dict[i] * player_vars[i] for i in name_list]) <= 1, "number of shooting guards"  
  prob += lpSum([sf_dict[i] * player_vars[i] for i in name_list]) <= 1, "number of small forwards"  
  prob += lpSum([pf_dict[i] * player_vars[i] for i in name_list]) <= 1, "number of power forwards"  
  prob += lpSum([c_dict[i] * player_vars[i] for i in name_list]) <= 1, "number of centers"    
  prob += lpSum([salary_dict[i] * player_vars[i] for i in name_list]) <= max_salary, "total salary"
  
  prob.writeLP("SalaryModel.lp")
  prob.solve()

  total_salary = 0
  average_o_ppp = 0
  average_d_ppp = 0
  for v in prob.variables():
    if v.varValue == 1:

      player_name = " ".join(v.name.split("_")[1:])

      average_o_ppp += offensive_ppp_dict[player_name]
      average_d_ppp += defensive_ppp_dict[player_name]
      total_salary += salary_dict[player_name]

      print "%s\tPosition: %s\tMinutes: %0.1f\tSalary: %0.1f\tOffensive_ppp:%0.2f\tDefensive_ppp:%0.2f"%(player_name,
                                                                 player_dict[player_name]["position"],
                                                                 minute_dict[player_name],
                                                                 salary_dict[player_name],
                                                                 offensive_ppp_dict[player_name],
                                                                 defensive_ppp_dict[player_name])

  print "\nTotal Salary: $%0.1fmm\tOffensive ppp: %0.2f\tDefensive ppp: %0.2f"%(total_salary,average_o_ppp/5,average_d_ppp/5)

if __name__ == '__main__':
  cursor = dbobj.curs()
  output_list = []
  player_dict = aggregate_possessions_from_dates(cursor)

