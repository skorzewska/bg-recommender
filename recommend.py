#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Collect data from MySQL database and use them"""

import itertools

from scikits.crab.models import MatrixPreferenceDataModel
from scikits.crab.metrics import pearson_correlation
from scikits.crab.similarities import UserSimilarity
from scikits.crab.recommenders.knn import UserBasedRecommender

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
    user = raw_input("What's your name? ")
    user_id = rsdbc.add_user(user)
    while not user_id:
        user = raw_input(
            "This name isn't available. Choose another one: ")
        user_id = rsdbc.add_user(user)
    ratings = collect_user_ratings(rsdbc, user_id)
    rsdbc.insert_user_ratings(user_id, ratings)

def generate_individual_recommendation(user_id):
    """Generate recommendation for single user
    """
    data = download_data(rsdbc)
    recommender = build_recommender(data)
    print "Recommending items..."
    recommendation = recommender.recommend(user_id)
    print recommendation
    print "Recommendations:\nprob\tgame"
    for game_id, prob in recommendation[:25]:
        game = rsdbc.get_game_name(game_id)
        print '{:.3f}\t{}'.format(prob, game)
    rsdbc.finalize()
    return recommendation

def main():
    """Do the stuff
    """
    rsdbc = RSDBConnection()

    menu = {}
    menu['0'] = 'Add new user'
    menu['1'] = 'Add ratings for games'
    menu['2'] = 'Generate individual recommendation'
    menu['3'] = 'Generate group recommendation'
    menu['4'] = 'Quit'

    while True:
        options = menu.keys()
        options.sort()
        for entry in options:
            print entry, menu[entry]

        selection = raw_input("Please select: \n")
        if selection == '0':
            print '0'
            add_new_user()
        elif selection == '1':
            print '1'
        elif selection == '2':
            user_id = raw_input("Give user name for counting recommendation ")
            recommendation =generate_individual_recommendation(user_id)
            print '2'
        elif selection == '3':
            print '3'
        elif selection == '4':
            break
        else:
            print "Unknown option selected!\n"


if __name__ == "__main__":
    main()
