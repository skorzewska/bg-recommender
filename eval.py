#!/usr/bin/python
# -*- coding: utf-8 -*-

"""System evaluation"""

import csv
import os
import sys

from scikits.crab.models import MatrixPreferenceDataModel
from scikits.crab.metrics import pearson_correlation
from scikits.crab.similarities import UserSimilarity
from scikits.crab.recommenders.knn import UserBasedRecommender

from rs_db_connection import RSDBConnection


def avg(data_list):
    """Return average of given list of numbers
    """
    return sum(data_list) / len(data_list)


def merge(data, merge_fun):
    """Merge data (ratings or recommendations)
    using given strategy (merge function)
    """
    return {
        game_id: merge_fun(game_data.values())
        for game_id, game_data in data.iteritems()}


def merge_users(db_conn_conf, group_user_id, ratings, merge_fun):
    """Merge users using given strategy (merge function)
    """
    individual_ratings = merge(ratings, merge_fun)
    db_conn = RSDBConnection(db_conn_conf)
    db_conn.insert_user_ratings(group_user_id, individual_ratings)
    db_conn.finalize()


def format_data(raw_data):
    """Return formatted data (ratings or recommendations)
    """
    formatted_data = {}
    for user_id, game_dict in raw_data.iteritems():
        for game_id, data in game_dict:
            if game_id in formatted_data:
                formatted_data[game_id][user_id] = data
            else:
                formatted_data[game_id] = {user_id: data}
    return formatted_data


def get_users_ratings_dict(db_conn_conf, user_ids):
    """Create dict with ratings for given users
    """
    db_conn = RSDBConnection(db_conn_conf)
    raw_ratings = {user_id: db_conn.get_user_ratings(user_id)
                   for user_id in user_ids}
    db_conn.finalize()
    return format_data(raw_ratings), len(user_ids)


def get_users_recommendations_dict(db_conn_conf, user_ids):
    """Create dict with recommendations for given users
    """
    db_conn = RSDBConnection(db_conn_conf)
    raw_recommendations = {
        user_id: read_recommendation_from_file(
            db_conn_conf,
            db_conn.get_user_name(user_id))
        for user_id in user_ids}
    db_conn.finalize()
    return format_data(raw_recommendations), len(user_ids)


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


def gen_individual_recommendation(db_conn_conf, user_name):
    """Generate recommendation for single user
    """
    rec_file = open('recommendations/' + user_name, 'w+')

    db_conn = RSDBConnection(db_conn_conf)
    user_id = db_conn.get_user_id(user_name)
    data = db_conn.get_data_for_rs()
    db_conn.finalize()

    recommender = build_recommender(data)
    print "Recommending items for user:", user_name
    recommendation = recommender.recommend(user_id)
    # recommendation = [(1, 1.0)]
    for game_id, prob in recommendation:
        rec_file.write('{};{}\n'.format(game_id, prob))

    rec_file.close()
    return recommendation


def check_file(file_name):
    """Check if file with given name already exists
    """
    try:
        return os.stat(file_name).st_size > 0
    except OSError:
        return False


def read_recommendation_from_file(db_conn_conf, user_name):
    """Get data from file with recommendations
    """
    recommendation = []
    recommendation_file_name = 'recommendations/' + user_name
    if check_file(recommendation_file_name):
        with open(recommendation_file_name, 'r') as rec_file:
            for line in rec_file:
                game_id, prob = line.split(';')
                recommendation.append((int(game_id), float(prob)))
    else:
        recommendation = gen_individual_recommendation(db_conn_conf, user_name)
    return recommendation


def check_numplayers(db_conn_conf, num_players, game_id):
    """Check if given number of players is relevant to given game
    """
    db_conn = RSDBConnection(db_conn_conf)
    min_players, max_players = db_conn.get_numplayers(game_id)
    db_conn.finalize()

    return min_players <= num_players and num_players <= max_players


def gen_and_print_recommendation(db_conn_conf, group_name, numplayers):
    """Function for generating and printing recommendation
    """
    recommendation = read_recommendation_from_file(db_conn_conf, group_name)
    with open('recommendations/'+group_name + '_result', 'w') as save_file:
        for game_id, prob in recommendation:
            if check_numplayers(db_conn_conf, numplayers, game_id):
                rsdbc = RSDBConnection()
                game = rsdbc.get_game_name(game_id)
                rsdbc.finalize()
                save_file.write('{};{}\n'.format(prob, game))
    return recommendation


def recommend_merge_users(merge_fun):
    """Return function that makes recommendation
    using merge-user strategy
    and a given merging function
    """
    def recommend_fun(db_conn_conf, user_ids, group_name):
        ratings, num_players = get_users_ratings_dict(db_conn_conf, user_ids)

        db_conn = RSDBConnection(db_conn_conf)
        full_group_name = 'group_' + merge_fun.__name__ + '_' + group_name
        group_user_id = db_conn.add_user(full_group_name)
        db_conn.finalize()

        merge_users(db_conn_conf, group_user_id, ratings, merge_fun)
        return gen_and_print_recommendation(db_conn_conf, full_group_name, num_players)
    return recommend_fun


def recommend_merge_recommendations(merge_fun):
    """Return function that makes recommendation
    using merge-recommendations strategy
    and a given merging function
    """
    def recommend_fun(db_conn_conf, user_ids, group_name):
        recommendations, num_players = get_users_recommendations_dict(db_conn_conf, user_ids)
        return merge(recommendations, merge_fun)
    return recommend_fun


def get_user_ratings(user_id):
    """Return function that returns user ratings
    """
    def resulting_fun(db_conn_conf, user_ids, group_name):
        db_conn = RSDBConnection(db_conn_conf)
        ratings = db_conn.get_user_ratings(user_id)
        recommendations = read_recommendation_from_file(
            db_conn_conf,
            db_conn.get_user_name(user_id))
        db_conn.finalize()
        return ratings + recommendations
    return resulting_fun


def main(number_of_groups, max_group_size=4):
    """Evaluate the system on a given number of random groups
    """
    db_conn_conf = {
        "user": "madzia",
        "host": "localhost",
        "database": "bgg",
    }

    recommendation_functions = [
        recommend_merge_users(avg),
        recommend_merge_users(max),
        recommend_merge_users(min),
        recommend_merge_recommendations(avg),
        recommend_merge_recommendations(max),
        recommend_merge_recommendations(min),
    ]

    # user_groups = []
    # for _ in range(number_of_groups):
    #     user_groups.append(db_conn.get_random_user_group(max_group_size))

    user_groups = [[113, 145]]

    for user_group in user_groups:
        db_conn = RSDBConnection(db_conn_conf)
        user_names = [
            db_conn.get_user_name(user_id)
            for user_id in user_group]
        group_name = '_'.join(user_names)
        db_conn.finalize()

        print "Evaluating user group: ", group_name
        user_rating_functions = [
            get_user_ratings(user_id)
            for user_id in user_group
        ]
        evaluation_functions = user_rating_functions + recommendation_functions
        results_list = [
            dict(recommend(db_conn_conf, user_group, group_name))
            for recommend in evaluation_functions]
        print results_list
        game_ids = set.union(*[set(reco.keys()) for reco in results_list])
        print game_ids
        results_dict = {game_id: [] for game_id in game_ids}
        print results_dict
        # convert list of dicts into dict with list values
        results_dict = {game_id: [reco.get(game_id, None) for reco in results_list]
                        for game_id in set.union(*[set(reco.keys()) for reco in results_list])}
        print results_dict
        with open('eval/' + group_name + '.tsv', 'wb') as eval_results_file:
            writer = csv.writer(eval_results_file, delimiter='\t')
            headings = ['game_id'] + user_names + [
                'merge_usr_avg',
                'merge_usr_max',
                'merge_usr_min',
                'merge_rec_avg',
                'merge_rec_max',
                'merge_rec_min',
            ]
            writer.writerow(headings)
            for game_id, results in sorted(results_dict.items()):
                writer.writerow([game_id] + results)


if __name__ == "__main__":
    main(int(sys.argv[1]))
