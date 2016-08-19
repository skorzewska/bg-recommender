#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Collect data from MySQL database and use them"""

import itertools
import operator
import os

from scikits.crab.models import MatrixPreferenceDataModel
from scikits.crab.metrics import pearson_correlation
from scikits.crab.similarities import UserSimilarity
from scikits.crab.recommenders.knn import UserBasedRecommender

import numpy as np

from rs_db_connection import RSDBConnection


def download_data(db_conn):
    """Download data
    """
    data = db_conn.get_data_for_rs()
    # data = dict(itertools.islice(data.iteritems(), 190, 300))
    return data


def build_recommender(data):
    """Create recommender system
    """
    print "Building the model..."
    model = MatrixPreferenceDataModel(data)
    print "Building the similarity..."
    similarity = UserSimilarity(model, pearson_correlation)
    print "Building the recommender..."
    return UserBasedRecommender(
        model, similarity, with_preference=True)


def collect_user_ratings(db_conn, user_id):
    """Collect ratings from a new user
    """
    games = db_conn.suggest_games_to_rate(user_id)
    i = 0
    ratings = {}
    print("Please rate some games now (1-10).\n"
          "If you haven't played the game, just hit ENTER.")
    while len(ratings) < 10:
        rating = raw_input('"{}": '.format(games[i][1]))
        if rating:
            rating = float(rating)
            if rating < 1:
                rating = 1.0
            elif rating > 10:
                rating = 10.0
            ratings[games[i][0]] = rating
        i += 1
    return ratings

def add_new_user():
    """ Add new user to database
    """
    rsdbc = RSDBConnection()
    user = raw_input("What's your name? ")
    user_id = rsdbc.add_user(user)
    while not user_id:
        user = raw_input(
            "This name isn't available. Choose another one: ")
        user_id = rsdbc.add_user(user)
    ratings = collect_user_ratings(rsdbc, user_id)
    rsdbc.insert_user_ratings(user_id, ratings)
    rsdbc.finalize()

def generate_individual_recommendation(user_name):
    """Generate recommendation for single user
    """
    rec_file = open('recommendations/'+user_name, 'w+')
    rsdbc = RSDBConnection()
    user_id = rsdbc.get_user_id(user_name)
    data = download_data(rsdbc)
    recommender = build_recommender(data)
    print "Recommending items..."
    recommendation = recommender.recommend(user_id)
    #print "Recommendations:\nprob\tgame"
    for game_id, prob in recommendation[:50]:
        #game = rsdbc.get_game_name(game_id)
        #print '{:.3f}\t{}'.format(prob, game)
        rec_file.write('{};{}\n'.format(game_id, prob))
    rsdbc.finalize()
    rec_file.close()
    return recommendation

def check_recommendation_file(filename):
    """Check if file with given username recommendations already exists
    """
    try:
        if os.stat(filename).st_size > 0:
           return True
        else:
           return False
    except OSError:
        return False

def read_recommendation_from_file(user_name):
    """Get data from file with recommendations
    """
    recommendation = []
    if check_recommendation_file('recommendations/'+user_name):
        with open('recommendations/'+user_name, 'r') as rec_file:
            for line in rec_file:
                game_id, prob = line.split(';')
                game_id = int(game_id)
                prob = float(prob)
                recommendation.append((game_id, prob))
    else:
        recommendation = generate_individual_recommendation(user_name)
    return recommendation

def get_user_ratings_for_game():
    """Function for getting ratings of chosen game_name
    """
    rsdbc = RSDBConnection()
    ratings = {}
    menu = {}
    menu['0'] = 'End.'
    menu['1'] = 'Add rating'
    user_name = raw_input("Give your username\n")
    user_id = rsdbc.get_user_id(user_name)
    while(True):
        options = menu.keys()
        options.sort()
        for entry in options:
            print entry, menu[entry]
        selection = raw_input("Please select: ")
        if selection == '0':
            rsdbc.finalize()
            return user_id, ratings
        elif selection == '1':
            game_name = raw_input("Please give the name of the game you want to rate: ")
            game_name = game_name.lower()
            game_name = '%' + game_name + '%'
            games = rsdbc.get_game_full_name(user_id, game_name)
            print("\nPlease rate the game you meant).\n"
                "If it's not the one you meant, just hit ENTER.\n"
                "If you want to quit, just write '-1' and ENTER\n")
            rating = 100
            i = 0
            while (rating != 0):
                if len(games) == 0:
                    print 'Cannot find the game!\n'
                    break
                rating = raw_input('"{}": '.format(games[i][1]))
                if rating:
                    rating = float(rating)
                    if rating< 1:
                        rating = 1.0
                    elif rating > 10:
                        rating= 10.0
                    ratings[games[i][0]] = rating
                if i +1 < len(games):
                    i += 1
                else:
                    break
            else:
                print "Unknown option selected!\n"


def show_user_ratings():
    """Show ratings for a given user
    """
    rsdbc = RSDBConnection()
    user_name = raw_input("Please enter your username\n")
    user_id = rsdbc.get_user_id(user_name)
    ratings = rsdbc.get_user_ratings(user_id)
    print '\n\nrating\t\t game'
    for elem in ratings:
        game_name = rsdbc.get_game_name(elem[0])
        print '{1}\t\t {0}'.format(game_name, elem[1])
    print "\n\n"
    rsdbc.finalize()

def create_dict_with_group_ratings():
    """Function that creates a dict with user ratings from our group
    """
    rsdbc = RSDBConnection()
    menu = {}
    menu['1'] = 'Add user to group'
    menu['0'] = 'End'
    user_ids = []
    ratings = {}
    game_ratings = {}
    while(True):
        options = menu.keys()
        options.sort()
        for entry in options:
            print entry, menu[entry]
        selection = raw_input("Please select: ")
        if selection == '1':
            user_name = raw_input("Please give the user name of existing user: ")
            user_id = rsdbc.get_user_id(user_name)
            user_ids.append(user_id)
        elif selection == '0':
            break
        else:
            print 'Wrong option!\n'
        for user_id in user_ids:
            rating = rsdbc.get_user_ratings(user_id)
            ratings[user_id] = rating
    rsdbc.finalize()
    for user_id, game_dict in ratings.iteritems():
        for game_id, rating in game_dict:
            if game_id in game_ratings:
                game_ratings[game_id] [user_id] = rating
            else:
                game_ratings[game_id] = {user_id:rating}
    return game_ratings, len(user_ids)

def create_dict_with_group_recommendations():
    """Function that creates a dict with user ratings from our group
    """
    menu = {}
    menu['1'] = 'Add user to group'
    menu['0'] = 'End'
    user_names = []
    game_ratings = {}
    while(True):
        options = menu.keys()
        options.sort()
        for entry in options:
            print entry, menu[entry]
        selection = raw_input("Please select: ")
        if selection == '1':
            user_name = raw_input("Please give the user name of existing user: ")
            user_names.append(user_name)
        elif selection == '0':
            break
        else:
            print 'Wrong option!\n'
        for user_name in user_names:
            single_recommendation = read_recommendation_from_file(user_name)
            for game_id, prob in single_recommendation:
                if game_id in game_ratings:
                    game_ratings[game_id] [user_name] = prob
                else:
                    game_ratings[game_id] = {user_name:prob}
    return game_ratings, len(user_names)

def merge_user_least_misery(user_id, ratings):
    """For each game get minimum from users' from group rating
    """
    rsdbc = RSDBConnection()
    individual_ratings = {}
    for game_id, game_ratings in ratings.iteritems():
        user_id_min_rating = min(game_ratings.iteritems(), key=operator.itemgetter(1))[0]
        min_rating = game_ratings[user_id_min_rating]
        individual_ratings[game_id] = min_rating
    print individual_ratings
    rsdbc.insert_user_ratings(user_id, individual_ratings)
    rsdbc.finalize()

def merge_user_max(user_id, ratings):
    """For each game get maximum from users' from group rating
    """
    rsdbc = RSDBConnection()
    individual_ratings = {}
    for game_id, game_ratings in ratings.iteritems():
        user_id_max_rating = max(game_ratings.iteritems(), key=operator.itemgetter(1))[0]
        max_rating = game_ratings[user_id_max_rating]
        individual_ratings[game_id] = max_rating
    print individual_ratings
    rsdbc.insert_user_ratings(user_id, individual_ratings)
    rsdbc.finalize()

def merge_user_avg(user_id, ratings):
    """For each game get average from users' from group rating
    """
    rsdbc = RSDBConnection()
    individual_ratings = {}
    for game_id, game_ratings in ratings.iteritems():
        ratings = [game_ratings[i] for i in game_ratings]
        avg_rating = sum(ratings)/len(ratings)
        individual_ratings[game_id] = avg_rating
    print individual_ratings
    rsdbc.insert_user_ratings(user_id, individual_ratings)
    rsdbc.finalize()

def merge_rec_least_misery(ratings):
    """For each game get minimum from users' from group rating
    """
    individual_ratings = {}
    for game_id, game_ratings in ratings.iteritems():
        user_id_min_rating = min(game_ratings.iteritems(), key=operator.itemgetter(1))[0]
        min_rating = game_ratings[user_id_min_rating]
        individual_ratings[game_id] = min_rating
    print individual_ratings

def merge_rec_max(ratings):
    """For each game get maximum from users' from group rating
    """
    individual_ratings = {}
    for game_id, game_ratings in ratings.iteritems():
        user_id_max_rating = max(game_ratings.iteritems(), key=operator.itemgetter(1))[0]
        max_rating = game_ratings[user_id_max_rating]
        individual_ratings[game_id] = max_rating
    print individual_ratings

def merge_rec_avg(ratings):
    """For each game get average from users' from group rating
    """
    individual_ratings = {}
    group_name = ''
    for game_id, game_ratings in ratings.iteritems():
        ratings = [game_ratings[i] for i in game_ratings]
        group_name = "_".join(game_ratings.keys())
        avg_rating = sum(ratings)/len(ratings)
        individual_ratings[game_id] = avg_rating
    print individual_ratings
    # with open('recommendations/'+group_name+'_avg', 'w') as save_file:
    #     for
    #     save_file

def check_numplayers(numplayers, game_id):
    rsdbc = RSDBConnection()
    minplayers, maxplayers = rsdbc.get_numplayers(game_id)
    rsdbc.finalize()
    return minplayers <= numplayers and numplayers <= maxplayers

def make_group_recommendation():
    """Display menu for choose of method of group recommendation.
        Call appropriate method of group recommendation.
        Merge multiple users to one user or merge individual recommendations into group one.
    """
    menu_rec = {}
    menu_rec['2'] = 'Merge users approach '
    menu_rec['1'] = 'Merge recommendations approach '
    menu_rec['0'] = 'Back '
    while(True):
        options = menu_rec.keys()
        options.sort()
        for entry in options:
            print entry, menu_rec[entry]
        selection = raw_input("Please select: ")
        if selection == '2':
            rsdbc = RSDBConnection()
            group_ratings, numplayers = create_dict_with_group_ratings()
            group_name = raw_input("Please provide a name for your group: ")
            user_id = rsdbc.add_user(group_name)
            while not user_id:
                group_name = raw_input(
                    "This name isn't available. Choose another one: ")
                user_id = rsdbc.add_user(group_name)
            menu_user_approach = {}
            menu_user_approach['0'] = 'Back'
            menu_user_approach['1'] = 'User Minimum strategy '
            menu_user_approach['2'] = 'User Maximum strategy '
            menu_user_approach['3'] = 'User Avg strategy '
            options2 = menu_user_approach.keys()
            options2.sort()

            rsdbc.finalize()
            for entry2 in options2:
                print entry2, menu_user_approach[entry2]
            selection2 = raw_input("Please select: ")
            if selection2 == '0':
                break
            elif selection2 == '1':
                merge_user_least_misery(user_id, group_ratings)
                generate_and_print_recommendation(group_name, numplayers)
            elif selection2 == '2':
                merge_user_max(user_id, group_ratings)
                generate_and_print_recommendation(group_name, numplayers)
            elif selection2 == '3':
                merge_user_avg(user_id, group_ratings)
                generate_and_print_recommendation(group_name, numplayers)
            else:
                print 'Wrong option!\n'
        elif selection == '0':
            break
        elif selection == '1':
            group_ratings, numplayers = create_dict_with_group_recommendations()
            menu_rec_approach = {}
            menu_rec_approach['0'] = 'Back'
            menu_rec_approach['1'] = 'Minimum strategy '
            menu_rec_approach['2'] = 'Maximum strategy '
            menu_rec_approach['3'] = 'Avg strategy '
            options3 = menu_rec_approach.keys()
            options3.sort()
            for entry3 in options3:
                print entry3, menu_rec_approach[entry3]
            selection3 = raw_input("Please select: ")
            if selection3 == '0':
                break
            elif selection3 == '1':
                merge_rec_least_misery(group_ratings)
            elif selection3 == '2':
                merge_rec_max(group_ratings)
            elif selection3 == '3':
                merge_rec_avg(group_ratings)
            else:
                print 'Wrong option!\n'
        else:
            print 'Wrong option!\n'

def generate_and_print_recommendation(group_name, numplayers):
    """Function for generating and printing recommendation
    """
    rsdbc = RSDBConnection()
    print "\nI'M RECOMMENDING\n"
    recommendation = read_recommendation_from_file(group_name)
    for game_id, prob in recommendation[:50]:
        if check_numplayers(game_id, numplayers):
            game = rsdbc.get_game_name(game_id)
            print '{:.3f}\t{}'.format(prob, game)
            with open('recommendations/'+group_name + '_result') as save_file:
                save_file.write('{} ;{}\n'.format(prob, game))
    rsdbc.finalize()

def main():
    """Do the stuff
    """
    menu = {}
    menu['5'] = 'Add new user'
    menu['1'] = 'Add ratings for games'
    menu['2'] = 'Generate individual recommendation'
    menu['3'] = 'Generate group recommendation'
    menu['4'] = 'Show me my ratings'
    menu['0'] = 'Quit'

    while True:
        rsdbc = RSDBConnection()
        options = menu.keys()
        options.sort()
        for entry in options:
            print entry, menu[entry]

        selection = raw_input("Please select: ")
        if selection == '5':
            add_new_user()
        elif selection == '1':
            user_id, ratings = get_user_ratings_for_game()
            rsdbc.insert_user_ratings(user_id, ratings)
        elif selection == '2':
            user_name = raw_input("Give user name for counting recommendation: ")
            recommendation = read_recommendation_from_file(user_name)
            for game_id, prob in recommendation[:50]:
                 game = rsdbc.get_game_name(game_id)
                 print '{:.3f}\t{}'.format(prob, game)
        elif selection == '3':
            rsdbc.finalize()
            make_group_recommendation()
        elif selection == '4':
            show_user_ratings()
        elif selection == '0':
            print '\n\nBye!'
            break
        else:
            print "Unknown option selected!\n"
        rsdbc.finalize()


if __name__ == "__main__":
    main()
