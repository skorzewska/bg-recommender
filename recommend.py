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
    rsdbc = RSDBConnection()
    user_id = rsdbc.get_user_id(user_name)
    data = download_data(rsdbc)
    recommender = build_recommender(data)
    print "Recommending items..."
    recommendation = recommender.recommend(user_id)
    print "Recommendations:\nprob\tgame"
    for game_id, prob in recommendation[:50]:
        game = rsdbc.get_game_name(game_id)
        print '{:.3f}\t{}'.format(prob, game)
    rsdbc.finalize()
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

def main():
    """Do the stuff
    """
    rsdbc = RSDBConnection()
    menu = {}
    menu['0'] = 'Add new user'
    menu['1'] = 'Add ratings for games'
    menu['2'] = 'Generate individual recommendation'
    menu['3'] = 'Generate group recommendation'
    menu['4'] = 'Show me my ratings'
    menu['5'] = 'Quit'

    while True:
        options = menu.keys()
        options.sort()
        for entry in options:
            print entry, menu[entry]

        selection = raw_input("Please select: ")
        if selection == '0':
            add_new_user()
        elif selection == '1':
            user_id, ratings = get_user_ratings_for_game()
            rsdbc.insert_user_ratings(user_id, ratings)
        elif selection == '2':
            user_name = raw_input("Give user name for counting recommendation :")
            recommendation =generate_individual_recommendation(user_name)
        elif selection == '3':
            print '3'
        elif selection == '4':
            show_user_ratings()
        elif selection == '5':
            print '\n\nBye!'
            break
        else:
            print "Unknown option selected!\n"
    rsdbc.finalize()


if __name__ == "__main__":
    main()
