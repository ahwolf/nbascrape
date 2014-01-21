"""
This is used to calculate the SRS.
"""

import sys
import time
import datetime
import MySQLdb
import os
import logging
import pickle
from collections import defaultdict
import operator
from numpy.random import poisson
import numpy as np
import random
import copy

sys.path.append("../")
from libscrape.config import constants
from libscrape.config import db
from libscrape.config import config

dbobj = db.Db(config.config['db'])
dts = {"2010-2011":[(2010,10,26),(2011,4,13)],
       "2011-2012":[(2011,12,25),(2012,4,26)],
       "2012-2013":[(2012,10,30),(2013,4,17)],
       "2013-2014":[(2013,10,29),(2014,4,16)]}



def get_dates(season):
  dt_set = dts[season]
  start_date = dt_set[0]
  end_date = dt_set[1]

  # convert to a date object so we can just print them out each date
  d1 = datetime.date(start_date[0],start_date[1],start_date[2])
  d2 = datetime.date(end_date[0],end_date[1],end_date[2])
  return d1, d2

def get_num_games(season, fraction=1):
  cursor = dbobj.curs()
  # get the fraction of games
  sql = """
  select count(*) from game where date_played >= "%s" and date_played <= "%s"
  """%(get_dates(season))
  cursor.execute(sql)
  total = cursor.fetchone()[0]
  cursor.close()
  return int(round(total*fraction))

def get_season_scores(season,
                      fraction=.5):
  cursor = dbobj.curs()
  num_games = get_num_games(season, fraction)
  start_date, end_date = get_dates(season)
  # print "number of games is: %i" %(num_games)
  sql = """
  select game.home_team, game.away_team, (game_stats.home_score - game_stats.away_score)
  from game, game_stats 
  where game_stats.game_id = game.id and
        game.date_played >= "%s" and game.date_played <= "%s"
  order by game.date_played
  limit %s
  """%(start_date, end_date, num_games)
  cursor.execute(sql)
  rows = cursor.fetchall()
  cursor.close()
  return rows

def get_season_scores_off_def(season,
                              fraction=.5):
  cursor = dbobj.curs()
  num_games = get_num_games(season, fraction)
  start_date, end_date = get_dates(season)
  # print "number of games is: %i" %(num_games)
  sql = """
  select game.home_team, game.away_team, game_stats.home_score, game_stats.away_score
  from game, game_stats 
  where game_stats.game_id = game.id and
        game.date_played >= "%s" and game.date_played <= "%s"
  order by game.date_played
  limit %s
  """%(start_date, end_date, num_games)
  cursor.execute(sql)
  rows = cursor.fetchall()
  cursor.close()
  return rows

# breaks off home and away games
def get_initial_season_SRS_off_def(season,
                                   fraction=0.5):

  rows = get_season_scores_off_def(season, fraction)

  # break into points allowed and points scored for home/away
  home_off_dict = defaultdict(int)
  home_def_dict = defaultdict(int)

  away_off_dict = defaultdict(int)
  away_def_dict = defaultdict(int)


  home_opponent_dict = defaultdict(list)  
  away_opponent_dict = defaultdict(list)
  win_dict = defaultdict(int)

  # create the SRS dictionary
  for home_team, away_team, home_score, away_score in rows:
    
    # update the margin of victory for both home and away teams
    home_off_dict[home_team] += home_score
    home_def_dict[home_team] += away_score

    away_off_dict[away_team] += away_score
    away_def_dict[away_team] += home_score

    # add in the opponents
    home_opponent_dict[home_team].append(away_team)
    away_opponent_dict[away_team].append(home_team)

    # get the number of wins, mov > 0 means that the home team won
    if home_score > away_score:
      win_dict[home_team] += 1
    else:
      win_dict[away_team] += 1

  # adjust the margin of victory to be an average
  for team, score in home_off_dict.iteritems():
    home_off_dict[team] = float(score)/len(home_opponent_dict[team])
    home_def_dict[team] = float(home_def_dict[team])/len(home_opponent_dict[team])
    away_off_dict[team] = float(away_off_dict[team])/len(away_opponent_dict[team])
    away_def_dict[team] = float(away_def_dict[team])/len(away_opponent_dict[team])

  return (home_off_dict, 
         home_def_dict,
         away_off_dict,
         away_def_dict,
         home_opponent_dict, 
         away_opponent_dict,
         win_dict)

def get_initial_season_SRS(season,
                           fraction=.5):
  
  rows = get_season_scores(season, fraction)

  margin_of_victory_dict = defaultdict(float)
  opponent_dict = defaultdict(list)  
  win_dict = defaultdict(int)

  # create the SRS dictionary
  for row in rows:
    margin_of_victory = row[2]
    
    # update the margin of victory for both home and away teams
    margin_of_victory_dict[row[0]] += margin_of_victory
    margin_of_victory_dict[row[1]] -= margin_of_victory

    # add in the opponents
    opponent_dict[row[0]].append(row[1])
    opponent_dict[row[1]].append(row[0])

    # get the number of wins, mov > 0 means that the home team won
    if margin_of_victory > 0:
      win_dict[row[0]] += 1
    else:
      win_dict[row[1]] += 1

  # adjust the margin of victory to be an average
  for team, mov in margin_of_victory_dict.iteritems():
    margin_of_victory_dict[team] = float(mov)/len(opponent_dict[team])
  return margin_of_victory_dict, opponent_dict, win_dict

def error_in_SRS(old_srs,
                 new_srs):
  error = 0
  # go through each team calculate the error
  for team, value in old_srs.iteritems():
    error += (new_srs[team] - value)**2
  return error


def iterate_SRS(old_srs_dict,
                margin_of_victory_dict,
                opponent_dict):

  new_srs_dict = {}

  # go through all of the opponents to find their strength
  for team, opponents in opponent_dict.iteritems():
    strength = 0
    for opponent in opponents:
      strength += old_srs_dict[opponent]

    # new srs value is the margin of victory +/- strength of schedule
    new_srs_dict[team] = margin_of_victory_dict[team] + float(strength)/len(opponents)
  return new_srs_dict

def make_srs(margin_of_victory_dict, opponent_dict):
  # set up the while loop
  old_srs_dict = margin_of_victory_dict.copy()
  error = 100
  while error > .00001:
    new_srs_dict = iterate_SRS(old_srs_dict, margin_of_victory_dict, opponent_dict)
    error = error_in_SRS(old_srs_dict, new_srs_dict)
    old_srs_dict.clear()
    old_srs_dict = new_srs_dict.copy()
    print "Error is: %0.2f" %(error)  
  return new_srs_dict


def iterate_one_srs_dict(opponent_dict,
                    orig_score_dict,
                    old_srs_dict):
  new_dict = {}
  for team, opponents in opponent_dict.iteritems():
    strength = 0
    for opponent in opponents:
      strength += old_srs_dict[opponent]

    # new srs value is the margin of victory +/- strength of schedule
    new_dict[team] = orig_score_dict[team] + float(strength)/len(opponents)
  return new_dict

def iterate_SRS_off_def(old_home_off_dict,
                        old_home_def_dict, 
                        old_away_off_dict,
                        old_away_def_dict, 
                        home_off_dict,
                        home_def_dict,
                        away_off_dict,
                        away_def_dict, 
                        home_opponent_dict, 
                        away_opponent_dict):

  
  new_home_off_dict = iterate_one_srs_dict(home_opponent_dict,
                                           home_off_dict,
                                           old_away_def_dict)

  new_home_def_dict = iterate_one_srs_dict(home_opponent_dict,
                                           home_def_dict,
                                           old_away_off_dict)

  new_away_off_dict = iterate_one_srs_dict(away_opponent_dict,
                                           away_off_dict,
                                           old_home_def_dict)

  new_away_def_dict = iterate_one_srs_dict(away_opponent_dict,
                                           away_def_dict,
                                           old_home_off_dict)

  # for team, opponents in home_opponent_dict.iteritems():
  #   strength = 0
  #   for opponent in opponents:
  #     strength += old_away_def_dict[opponent]

  #   # new srs value is the margin of victory +/- strength of schedule
  #   new_home_off_dict[team] = home_off_dict[home_team] + float(strength)/len(opponents)

  return new_home_off_dict, new_home_def_dict, new_away_off_dict, new_away_def_dict

def make_srs_off_def(home_off_dict, 
                     home_def_dict,
                     away_off_dict,
                     away_def_dict,
                     home_opponent_dict, 
                     away_opponent_dict):

  srs_dict = {}
  old_home_off_dict = copy.deepcopy(home_off_dict)
  old_home_def_dict = copy.deepcopy(home_def_dict)
  old_away_off_dict = copy.deepcopy(away_off_dict)
  old_away_def_dict = copy.deepcopy(away_def_dict)

  error = 100
  max_iterations = 10
  iteration_number = 0
  while error > 1e-10 and iteration_number < max_iterations:
    iteration_number += 1
    #print "old %.3f" % old_home_srs_dict["CHI"]
    # print "before ",home_margin_of_victory_dict
    (new_home_off_dict, 
     new_home_def_dict,
     new_away_off_dict,
     new_away_def_dict) = iterate_SRS_off_def(
      old_home_off_dict,
      old_home_def_dict, 
      old_away_off_dict,
      old_away_def_dict, 
      home_off_dict,
      home_def_dict,
      away_off_dict,
      away_def_dict, 
      home_opponent_dict, 
      away_opponent_dict)
    # print "after ", home_margin_of_victory_dict

    # print "new %.3f" % new_home_srs_dict["CHI"]
    error = error_in_SRS(old_home_off_dict, new_home_off_dict) + error_in_SRS(old_home_def_dict, old_home_def_dict)
    error += error_in_SRS(old_away_off_dict, new_away_off_dict) + error_in_SRS(old_away_def_dict, old_away_def_dict)
    
    old_home_off_dict = copy.deepcopy(new_home_off_dict)
    old_home_def_dict = copy.deepcopy(new_home_def_dict)
    old_away_off_dict = copy.deepcopy(new_away_off_dict)
    old_away_def_dict = copy.deepcopy(new_away_def_dict)    
    print "Error is: %0.2f" %(error)  
  return new_home_off_dict, new_home_def_dict, new_away_off_dict, new_away_def_dict

# returns 1 if home team wins, 0 for road team win
def simulate_game_flip_coins(home_team,
                             away_team,
                             srs_dict,
                             average_home_score=None,
                             average_away_score=None):
  return random.random() < 0.5

def get_average_score(season,
                      fraction=0.5):
  cursor = dbobj.curs()
  num_games = get_num_games(season, fraction)
  start_date, end_date = get_dates(season)
  # print "number of games is: %i" %(num_games)
  sql = """
  select avg(game_stats.home_score), 
         avg(game_stats.away_score) 
  from game, game_stats 
  where game_stats.game_id = game.id and
        game.date_played >= "%s" and game.date_played <= "%s"
  order by game.date_played desc 
  limit %s;
  """%(start_date, end_date, num_games)
  cursor.execute(sql)
  avg_home_score, avg_away_score = cursor.fetchone()
  return float(avg_home_score), float(avg_away_score)

# take each teams rate and find the difference 
# and ensure that the average score remains true
def simulate_game_srs(home_team,
                      away_team,
                      srs_dict,
                      average_home_score = 99,
                      average_away_score = 96):
  average_score = (average_away_score + average_home_score) / 2.0
  srs_diff = srs_dict[home_team] - srs_dict[away_team]
  
  home_team_rating = average_score + (srs_diff / 2) 
  away_team_rating = average_score - (srs_diff / 2) 

  home_team_score = poisson(home_team_rating)
  away_team_score = poisson(away_team_rating)
  
  while home_team_score == away_team_score:
    home_team_score = poisson(home_team_rating*(5.0/48))
    away_team_score = poisson(away_team_rating*(5.0/48))

  return home_team_score > away_team_score
  # print home_team_rating, away_team_rating, srs_dict[home_team], srs_dict[away_team], home_team, away_team


def simulate_game_srs_home_away(home_team,
                                away_team,
                                srs_dict,
                                average_home_score = 99,
                                average_away_score = 96):

  srs_diff = srs_dict[home_team] - srs_dict[away_team]

  home_team_rating = average_home_score + (srs_diff / 2) 
  away_team_rating = average_away_score - (srs_diff / 2) 

  home_team_score = poisson(home_team_rating)
  away_team_score = poisson(away_team_rating)
  
  while home_team_score == away_team_score:
    home_team_score = poisson(home_team_rating*(5.0/48))
    away_team_score = poisson(away_team_rating*(5.0/48))

  return home_team_score > away_team_score

def get_team_wins(season_rows):
  # get the actual number of wins
  actual_wins = defaultdict(int)
  for home_team, away_team, margin_of_victory in season_rows:
    if margin_of_victory > 0:
      actual_wins[home_team] += 1
    else:
      actual_wins[away_team] += 1
  return actual_wins

def calculate_error_wins(actual_wins,
                         win_dict):

  error = 0
  for team, predicted_wins in win_dict.iteritems():
    error += abs(predicted_wins - actual_wins[team])
  return error

def output_np_stats(np_array, simulate_func):
  print >> sys.stderr, "Function: %s\tMean: %0.1f\tSTD:%0.1f\tMedian:%0.1f"%(simulate_func.__name__,
                                                                             np_array.mean(), 
                                                                             np_array.std(), 
                                                                             np.median(np_array))


def get_median_outcome(simulated_season_wins):
  median_wins = {}
  for team, win_list in simulated_season_wins.iteritems():
    median_wins[team] = np.median(win_list)
  return median_wins

def simulate_rest_of_season(season,
                            srs_dict,
                            win_dict,
                            fraction=0.5,
                            simulations=1000,
                            simulate_func=simulate_game_flip_coins):


  fraction_season_rows = get_season_scores(season,fraction=fraction)
  full_season_rows = get_season_scores(season,fraction=1)

  unplayed_games = full_season_rows[len(fraction_season_rows):]
  actual_wins = get_team_wins(full_season_rows)

  error_array = np.zeros(simulations)
  avg_home_score, avg_away_score = get_average_score(season, fraction)
  simulated_season_wins = defaultdict(list)
  # start the simulation loop
  for iteration in range(simulations):
    simulated_wins = win_dict.copy()
    for home_team, away_team, mov in unplayed_games:
      if simulate_func(home_team,away_team,srs_dict, avg_home_score, avg_away_score):
        simulated_wins[home_team] += 1
      else:
        simulated_wins[away_team] += 1
    # calculate the error of the simulation
    error_wins = calculate_error_wins(actual_wins, simulated_wins)
    error_array[iteration] = error_wins

    for team, wins in simulated_wins.iteritems():
      simulated_season_wins[team].append(wins)
    # print iteration, error_wins
    #print "Iteration %i error is: %i"%(iteration,error_wins)
  median_simulated_wins = get_median_outcome(simulated_season_wins)

  print >> sys.stderr, "Function: %s\tmedian win error: %0.2f" %(simulate_func.__name__, calculate_error_wins(actual_wins, median_simulated_wins))
  # output_np_stats(error_array, simulate_func) 



# MAIN TIME
if __name__ == '__main__':
  season = "2011-2012"
  fraction = 1
  simulations = 1000
  margin_of_victory_dict, opponent_dict, win_dict = get_initial_season_SRS(season,fraction=fraction)
  srs_dict = make_srs(margin_of_victory_dict, opponent_dict)
  for team, srs in srs_dict.iteritems():
    print "Team: %s\tsrs: %0.2f\twins: %i"%(team,srs_dict[team],win_dict[team])


  simulate_rest_of_season(season, srs_dict, win_dict,fraction, simulations, simulate_game_flip_coins)
  simulate_rest_of_season(season, srs_dict, win_dict,fraction, simulations, simulate_game_srs)
  simulate_rest_of_season(season, srs_dict, win_dict, fraction, simulations, simulate_game_srs_home_away)

  (home_off_dict, 
   home_def_dict,
   away_off_dict,
   away_def_dict,
   home_opponent_dict, 
   away_opponent_dict,
   win_dict) = get_initial_season_SRS_off_def(season,fraction=fraction)

  # # make a separate call for home and away dictionaries
  # srs_dict = {}
  (srs_dict["home_off"], 
   srs_dict["home_def"],
   srs_dict["away_off"],
   srs_dict["away_def"])= make_srs_off_def(home_off_dict, 
                                           home_def_dict,
                                           away_off_dict,
                                           away_def_dict,
                                           home_opponent_dict, 
                                           away_opponent_dict)



