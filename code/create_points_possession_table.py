"""
This file is used to generate the table of game_id, player_id, points_scored, offensive_possessions,
points_allowed, defensive possessions

Use ESPN for substitutions, nba for everything else. NBA fucks up substitutions for some reason
"""

import sys
import time
import datetime
import MySQLdb
import os
import logging
import pickle
from collections import defaultdict

sys.path.append("../")
from libscrape.config import constants
from libscrape.config import db
from libscrape.config import config



LOGDIR_SOURCE = constants.LOGDIR_SOURCE
LOGDIR_EXTRACT = constants.LOGDIR_EXTRACT

logging.basicConfig(filename='etl.log',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

dbobj = db.Db(config.config['db'])
logging.info("Starting create table with database: %s" % (dbobj))


# executes the query and updates the point_dict
def execute_update_point_dict(cursor, sql, point_dict):
  cursor.execute(sql)
  rows = cursor.fetchall()
  for row in rows:
    point_dict[(row[0],row[1])] += float(row[2])


# get the points scored 
def get_points_scored(ASSIST_FACTOR=.33):
  point_dict = defaultdict(int)
  cursor = dbobj.curs()

  # first get the two point field goals
  sql = """
  select game_id, player_id, 2*count(*) from playbyplay_nbacom where play_type_nbacom_id in (13,54) group by game_id, player_id
  """
  execute_update_point_dict(cursor,sql,point_dict)

  # now get the three point field goals
  sql = """
  select game_id, player_id, 3*count(*) from playbyplay_nbacom where play_type_nbacom_id in (23) group by game_id, player_id
  """
  execute_update_point_dict(cursor,sql,point_dict)

  # now get free throws made
  sql = """
  select game_id, player_id, count(*) from playbyplay_nbacom where play_type_nbacom_id in (31,32,33,34,35,36) group by game_id, player_id
  """
  execute_update_point_dict(cursor,sql,point_dict)

  # add in the assists as primary for two-pointers
  sql = """
  select game_id, player2_id, %f*2*count(*) from playbyplay_nbacom where play_type_nbacom_id in (55) group by game_id, player2_id
  """%(ASSIST_FACTOR)
  execute_update_point_dict(cursor,sql,point_dict)

  # add in the assists as primary for three-pointers
  sql = """
  select game_id, player2_id, %f*3*count(*) from playbyplay_nbacom where play_type_nbacom_id in (24) group by game_id, player2_id
  """%(ASSIST_FACTOR)
  execute_update_point_dict(cursor,sql,point_dict)

  # add in abbreviated two-point assists
  sql = """
  select game_id, player_id, (1-%f)*2*count(*) from playbyplay_nbacom where play_type_nbacom_id in (55) group by game_id, player_id
  """%(ASSIST_FACTOR)
  execute_update_point_dict(cursor,sql,point_dict)

  # add in abbreviated three-point assists
  sql = """
  select game_id, player_id, (1-%f)*3*count(*) from playbyplay_nbacom where play_type_nbacom_id in (24) group by game_id, player_id
  """%(ASSIST_FACTOR)
  execute_update_point_dict(cursor,sql,point_dict)


  cursor.close()
  return point_dict

def get_off_pos():
  pos_dict = defaultdict(int)
  cursor = dbobj.curs()

  # get the number of possessions
  sql = """
  SELECT game_id, player_id, count(*) 
  FROM playbyplay_nbacom 
  WHERE play_type_nbacom_id in (13,23,24,54,55,7,21,22,52,53,26,28,32,34,16,17,44,45,49,50,51)
  GROUP BY game_id, player_id
  """
  execute_update_point_dict(cursor,sql,pos_dict)
  
  # now for the assists
  sql="""
  SELECT game_id, player2_id, count(*)
  FROM playbyplay_nbacom
  WHERE play_type_nbacom_id in (24,55)
  GROUP BY game_id, player2_id
  """
  execute_update_point_dict(cursor,sql,pos_dict)

  return pos_dict

# gets the score of the game at halftime
def get_begin_end_score(game_id, home_team_bool,cursor, period = 2):
  if home_team_bool:
    column = "away_quarter_score"
  else:
    column = "home_quarter_score"
  sql = """
  select %s from game_stats where game_id = %s
  """%(column, game_id)

  cursor.execute(sql)
  quarter_scores = cursor.fetchone()[0]
  points = quarter_scores.split('|')

  total_points = 0
  for quarter in points[:period-1]:
    total_points += int(quarter)

  return (total_points, total_points + int(points[period-1]))
  
# given the variables, calculate the number of possessions the offense
def get_num_possessions(game_id='2729', 
                        cursor=None, 
                        previous_play_index='1',
                        team_id='2',
                        last_play_index='50'):
  sql = """
  SELECT count(*)
  FROM playbyplay_espn 
  WHERE play_espn_id = ANY (SELECT id 
                            FROM play_espn 
                            WHERE (is_turnover=1 OR
                                   is_shot = 1 OR
                                   is_freethrow_last = 1) 
                            AND id NOT IN (1,4,18))
  AND game_id = %s
  AND team_id <> %s
  AND play_index >= %s AND play_index <= %s
  """%(game_id,team_id, previous_play_index,last_play_index)

  cursor.execute(sql)
  
  return cursor.fetchone()[0]
  


# algorithm for converting the game substitutions into
def substitution_to_points(game_id,
                           player_id, 
                           rows, 
                           home_team_bool, 
                           cursor):

  # initialize some variables
  total_time = 0
  points_allowed = 0
  defensive_possessions = 0

  for row in rows:
#    import pdb; pdb.set_trace()
    # this means the player is coming out
    if row[5] != player_id:

      # get the opponent score
      if home_team_bool:
        current_opponent_score = row[2]
      else:
        current_opponent_score = row[3]

      # update total time, possessions, and points
      total_time += 7200 * (row[0] - period) + previous_time - row[1]
      points_allowed += current_opponent_score - previous_score
      try:
        defensive_possessions += get_num_possessions(game_id, 
                                                     cursor, 
                                                     previous_play_index,
                                                     row[4],
                                                     row[6])
      except:
        import pdb; pdb.set_trace()
    # player is entering the game
    else:
      period = row[0]
      previous_time = row[1]
      previous_play_index = row[6]
      # set the score
      if home_team_bool:
        previous_score = row[2]
      else:
        previous_score = row[3]

    # print "total_time: %i, points_allowed: %i, defensive_possessions: %i, period: %i, previous_score: %i, previous_play_index: %i"%(total_time,
    #                                                                                                                             points_allowed,
    #                                                                                                                             defensive_possessions,
    #                                                                                                                             period,
    #                                                                                                                             previous_score,
    #                                                                                                                             previous_play_index)
  # sometimes there just aren't any possessions! but free throws are scored
  # this is wrong, but too lazy to fix

  if not defensive_possessions:
    defensive_possessions += 1
  return (float(points_allowed), defensive_possessions, float(total_time)/60)

def get_max_min_play_index_period(game_id,
                                  period,
                                  cursor):
  sql ="""
  SELECT MIN(play_index), MAX(play_index)
  FROM playbyplay_espn
  WHERE game_id = %s and period = %s
  """%(game_id, period)
  cursor.execute(sql)

  return cursor.fetchone()

# special case where player enters and exits without subs 
def no_substitution_log(game_id, 
                         player_id,
                         team_id,
                         home_team_bool,
                         cursor,
                         special_case_period = None,
                         enter_then_exit = True):
  
  # First get the score at the start and end of the period in question
  begin_end_score = get_begin_end_score(game_id, home_team_bool,cursor, int(special_case_period))

  # Then get the play indexes for those two periods
  min_max_play_index = get_max_min_play_index_period(game_id,special_case_period,cursor)
  if enter_then_exit:
    rows = [(special_case_period, 7200, begin_end_score[0], begin_end_score[0], team_id, player_id, min_max_play_index[0]),
            (special_case_period, 0, begin_end_score[1], begin_end_score[1], team_id, -1, min_max_play_index[1]),
    ] 
  else:
    rows = [(special_case_period, 7200, begin_end_score[0], begin_end_score[0], team_id, -1, min_max_play_index[0]),
            (special_case_period, 7200, begin_end_score[1], begin_end_score[1], team_id, player_id, min_max_play_index[1]),
    ]
  return rows

# returns True if player made any play in the period, otherwise False
def played_in_period(game_id,
                     player_id,
                     period,
                     cursor):
  sql = """
  SELECT count(*)
  FROM playbyplay_espn
  WHERE game_id = %s and (player_id = %s or player2_id = %s) and period = %s
  """%(game_id, player_id, player_id,period)
  cursor.execute(sql)
  return cursor.fetchone()[0] != 0

def add_multiple_periods(game_id,
                         player_id,
                         team_id,
                         home_team_bool,
                         cursor,
                         old_period,
                         new_period):
  new_rows = []
  for check_period in range(old_period+1, new_period):
    if played_in_period(game_id, player_id, check_period, cursor):
      new_rows += no_substitution_log(game_id,player_id, team_id, home_team_bool, cursor, check_period)

  return new_rows

# take the list of tuoles and inserts data for missing quarters
def fill_substitution_gaps(game_id, 
                           player_id,
                           team_id,
                           rows,
                           home_team_bool, 
                           cursor):
  # Assume no-one starts the game
  player_in = False
  period = 0
  new_rows = []
  bad_data_point = False

  for i, row in enumerate(rows):

    # player is entering the game again! need to insert an exit
    if (player_in and (row[5] == player_id)):
      if period != row[0]:
        # insert the missing row for the exit then any multi-periods
        new_rows.append(no_substitution_log(game_id,player_id, team_id, home_team_bool, cursor, period)[1])
        new_rows += add_multiple_periods(game_id,player_id, team_id, home_team_bool, cursor, period, row[0])
      else:
        bad_data_point = True

    # player is exiting the game again! need to insert an entrance
    elif (not player_in and (row[5] != player_id)):
      if period != row[0]:
        # insert any multi-periods then the missing row for the entrance
        new_rows += add_multiple_periods(game_id,player_id, team_id, home_team_bool, cursor, period, row[0])      
        new_rows.append(no_substitution_log(game_id,player_id, team_id, home_team_bool, cursor, period+1)[0])
      else:
        bad_data_point = True
    # we may have missed a whole period
    elif (int(row[0]) - period > 1):
      new_rows += add_multiple_periods(game_id,player_id, team_id, home_team_bool, cursor, period, row[0])      

    if not bad_data_point:
      new_rows.append(row)  
      bad_data_point = False
    else:
      print "Bad data point", row
    # reset some variables for the next loop
    if row[5] == player_id:
      player_in = True
    else:
      player_in = False
    period = int(row[0])

    # if this is the last hurrah fill in the final exit

  # if there was an exit and maybe the player played later without notice 8 is for 4 OTs
  if not player_in:
    new_rows += add_multiple_periods(game_id,player_id, team_id, home_team_bool, cursor, period, 8)
  # if there was an enter when was the last exit?
  else:
    new_rows.append(no_substitution_log(game_id,player_id, team_id, home_team_bool, cursor, period)[1])
    new_rows += add_multiple_periods(game_id,player_id, team_id, home_team_bool, cursor, period, 8)


  return new_rows

# given a game_id and player_id, calculate the points allowed in the game while
# the player was on the floor
def get_points_allowed_game(game_id,
                            player_id,
                            home_team_id,
                            cursor):

  # first get all of the substitutions in order they occur
  sql = """
  select period, deciseconds_left, away_score, home_score, team_id, player_id, play_index 
  from playbyplay_espn 
  where play_espn_id = 48 and game_id = %s and (player_id = %s or player2_id = %s)
  order by period, deciseconds_left desc
  """%(game_id, player_id, player_id)

  cursor.execute(sql)
  rows = cursor.fetchall()

  # check to see if the player is home or away
  try:
      home_team_bool = rows[0][4] == home_team_id
  except IndexError:
    print "special case where player entered and exited a period"
    sql = """
    SELECT team_id, period
    FROM playbyplay_espn
    WHERE game_id = %s and player_id = %s
    limit 1
    """%(game_id, player_id)

    cursor.execute(sql)
    special_case = cursor.fetchone()
    home_team_bool = special_case[0] == home_team_id
    

    rows = no_substitution_log(game_id,player_id, special_case[0], home_team_bool, cursor, special_case[1])
  else:
    # fill in the gaps for between quarter substitutions
    rows = fill_substitution_gaps(game_id,player_id, rows[0][4], rows, home_team_bool, cursor)
  
  return substitution_to_points(game_id, player_id, rows, home_team_bool, cursor)

def get_points_allowed(cursor):
  points_allowed = defaultdict(int)
  
  # first get all the game_id player_id tuples that also includes the home and away team id
  sql = """
  select playbyplay_espn.game_id, playbyplay_espn.player_id, game.home_team_id
  from playbyplay_espn,game 
  where playbyplay_espn.game_id = game.id and player_id > 0
  group by playbyplay_espn.game_id, playbyplay_espn.player_id
  """
  cursor.execute(sql)


  # loop through each tuple
  rows = cursor.fetchall()
  for i, row in enumerate(rows):
    if i % 100 == 0:
      print "analyzing #%i out of %i"%(i, len(rows))

    game_id = row[0]
    player_id = row[1]
    home_team_id = row[2]

    # now query for each and process
    points_allowed[(game_id,player_id)] = get_points_allowed_game(game_id,
                                                                  player_id,
                                                                  home_team_id,
                                                                  cursor)

  return points_allowed

def create_table(cursor):
  sql="""
  CREATE TABLE player_game_possessions
  (game_id int, player_id int, points_scored float,offensive_possessions int, points_allowed int,defensive_possessions int, total_time float)
  """
  cursor.execute(sql)

def write_to_sql(output_list, cursor):
  sql = """
  INSERT INTO player_game_possessions
  VALUES (%s, %s, %s, %s, %s, %s, %s)
  """

  cursor.executemany( sql, output_list )

def get_rebound_personal_total():
  pass

def get_steals():
  pass


if __name__ == '__main__':
  cursor = dbobj.curs()
  output_list = []
  with open("../data/points_allowed.p", "wb") as datafile:
    pickle.dump(get_points_allowed(cursor), datafile)

  with open("../data/points_allowed.p", "rb") as datafile:
    points_allowed = pickle.load(datafile)

  # creates a dict keyed on (game, player) with a value of how many points they scored
  point_dict = get_points_scored()

  rebound_dict = get_rebound_percentage()

  # creates a dict keyed on (game, player) with a value of how many offensive
  # possessions
  off_poss_dict = get_off_pos()

  for key, value in points_allowed.iteritems():
    output_list.append((key[0], key[1], point_dict[key], off_poss_dict[key], value[0], value[1], value[2]))

  with open("../data/complete_data.p", "wb") as outfile:
    pickle.dump(output_list, outfile)

  with open("../data/complete_data.p", "rb") as datafile:
    output_list = pickle.load(datafile)
    create_table(cursor)
    dbobj.conn.commit()
    write_to_sql(output_list, cursor)
    dbobj.conn.commit()
    dbobj.conn.close()


