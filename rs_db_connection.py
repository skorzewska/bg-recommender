#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Recommender system database connection handler"""

from collections import defaultdict
import os
import sys

import xml.etree.ElementTree as ET
import codecs

import mysql.connector
from mysql.connector import errorcode

import os
import operator


MYSQL_CONNECTION_DEFAULT_CONFIG = {
    "user": "madzia",
    "host": "localhost",
    "database": "bgg",
}


class Progress:
    """Progress bar"""

    def __init__(self, total, bar_len=50):
        self.total = total
        self.bar_len = bar_len
        self.part = 0

    def update(self):
        self.part += 1
        percentage = self.part * 100 / self.total
        fraction = self.part * self.bar_len / self.total
        bar = (fraction * "#" + (self.bar_len - fraction) * " ")
        sys.stdout.write("\r[{}] {:>3}%".format(bar, percentage))
        sys.stdout.flush()

    def finalize(self):
        print


class RSDBConnection:
    """A class that connects to the database and performs all the stuff"""

    def __init__(self, config=MYSQL_CONNECTION_DEFAULT_CONFIG):
        try:
            self.cnx = mysql.connector.connect(**config)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print "Something is wrong with your user name or password"
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print "Database does not exist"
            else:
                print err
        else:
            self.cursor = self.cnx.cursor()

    def create_tables(self):
        self.cursor.execute("""
            create table if not exists games (
                id int not null primary key,
                yearpublished int,
                minplayers int,
                maxplayers int,
                name varchar(64),
                age int,
                description varchar(512),
                noofratings int,
                avgrating real,
                bestnumplayers int,
                langdependence varchar(256)
            );

            """)
        self.cursor.execute("""
            create table if not exists categories (
                id int not null auto_increment,
                name varchar(64) unique,
                primary key (id)
            );
        """)

        self.cursor.execute("""
            create table if not exists gamecategories (
                game_id int not null,
                foreign key (game_id) references games(id),
                category_id int not null,
                foreign key (category_id) references categories(id)
            );
        """)

        self.cursor.execute("""
            create table if not exists mechanics (
                id int not null auto_increment,
                name varchar(64) unique,
                primary key(id)
            );
        """)

        self.cursor.execute("""
            create table if not exists gamemechanics (
                game_id int not null,
                foreign key (game_id) references games(id),
                mechanic_id int not null,
                foreign key (mechanic_id) references mechanics(id)
            );
        """)

        self.cursor.execute("""
            create table if not exists users (
                id int not null auto_increment,
                name varchar(64),
                primary key(id)
            )
        """)

        self.cursor.execute("""
            create table if not exists gameratings (
                user_id int not null,
                foreign key (user_id) references users(id),
                game_id int not null,
                foreign key (game_id) references games(id),
                rating float,
                primary key (user_id, game_id)
            )
        """)

    def find_noofplayers(self, game):
        pollresults = {}
        for poll in game.findall("poll"):
            if (poll.get("name") == "suggested_numplayers"):
                for results in poll.findall("results"):
                    numplayers = results.get("numplayers")
                    for result in results.findall("result"):
                        if result.get("value") == "Best":
                            numvotes = result.get("numvotes")
                            pollresults[numplayers] = int(numvotes)
        if pollresults:
            return max(pollresults.iteritems(), key=operator.itemgetter(1))[0]
        else:
            return None

    def find_langdependency(self, game):
        pollresults = {}
        for poll in game.findall("poll"):
            if (poll.get("name") == "language_dependence"):
                for results in poll.findall("results"):
                    for result in results.findall("result"):
                        langdependence = result.get("value")
                        numvotes = result.get("numvotes")
                        pollresults[langdependence] = int(numvotes)
        if pollresults:
            return max(pollresults.iteritems(), key=operator.itemgetter(1))[0]
        else:
            return "None"

    def find_categories(self, game):
        categories = []
        for category in game.findall("boardgamecategory"):
            categories.append(category.text)
        return categories

    def find_mechanics(self, game):
        mechanics = []
        for mechanic in game.findall("boardgamemechanic"):
            mechanics.append(mechanic.text)
        return mechanics

    def process_game(self, filename):
        """Parse single game XML file"""
        try:
            tree = ET.parse(filename)
            game = tree.getroot().find("boardgame")
            primary_name = ''
            if game is None:
                return False
            if game.find("boardgamecategory") is None:
                return False
            if (game.find("statistics").find("ratings")
                    .find("usersrated").text == '0'):
                return False
            for name in game.findall("name"):
                if name.get("primary")=="true":
                    primary_name = name.text
            if not primary_name:
                primary_name = game.find("name").text
            self.cursor.execute(
                "insert ignore into games values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (
                    game.get("objectid"),
                    game.find("yearpublished").text,
                    game.find("minplayers").text,
                    game.find("maxplayers").text,
                    primary_name,
                    game.find("age").text,
                    game.find("description").text,
                    game.find("statistics").find("ratings").find("usersrated").text,
                    game.find("statistics").find("ratings").find("average").text,
                    self.find_noofplayers(game),
                    self.find_langdependency(game)
                    ))
            categories = self.find_categories(game)
            for category in categories:
                self.cursor.execute(
                    "insert ignore into categories (name) values (%s);", ( category,))
            query = ("select id from categories where name = (%s);")
            for category in categories:
                self.cursor.execute(query, (category, ))
                for (cat_id,) in self.cursor:
                    self.cursor.execute(
                        "insert ignore into gamecategories values (%s, %s);", (
                            game.get("objectid"),
                            cat_id
                        ))

            mechanics = self.find_mechanics(game)
            for mechanic in mechanics:
                self.cursor.execute(
                    "insert ignore into mechanics (name) values (%s);", (mechanic,)
                )

            mechanics_query = ("select id from mechanics where name = (%s);")
            for mechanic in mechanics:
                self.cursor.execute(mechanics_query, (mechanic, ))
                for (mechanic_id,) in self.cursor:
                    self.cursor.execute(
                        "insert ignore into gamemechanics values (%s, %s);", (
                            game.get("objectid"),
                            mechanic_id
                        ))
        except ET.ParseError:
            self.failed_files.append(filename)
        return True

    def process_games(self, directory):
        self.failed_files = []
        filenames = os.listdir(directory)
        progress = Progress(len(filenames))
        count_boardgames = 0
        for filename in filenames:
            progress.update()
            path = directory + "/" + filename
            if self.process_game(path):
                count_boardgames += 1
        progress.finalize()
        print "There are {} board games in total {} games.".format(
            count_boardgames, len(filenames))
        print "There are {} bad games".format(len(self.failed_files))

    def show_games(self):
        print "Games in database:"
        self.cursor.execute("select * from games;")
        for data in self.cursor:
            print data

    def process_user(self, filename):
        """Parse single user XML file"""
        try:
            tree = ET.parse(filename)
            items = tree.getroot()
            user = os.path.basename(filename).replace(".xml", "")
            if items is None:
                return False
            self.cursor.execute(
                "insert ignore into users (name) values (%s);", (user, )
            )
            query = ("select id from users where name = (%s);")
            self.cursor.execute(query, (user, ))
            user_ids = []
            for (user_id,) in self.cursor:
                user_ids.append(user_id)
            for user_id in user_ids:
                for item in items.findall("item"):
                    try:
                        self.cursor.execute(
                            "insert ignore into gameratings values (%s, %s, %s);", (
                                user_id,
                                item.get("objectid"),
                                item.find("stats").find("rating").get("value")
                            )
                        )
                    except mysql.connector.errors.IntegrityError:
                        pass
        except ET.ParseError:
            self.failed_files.append(filename)
        return True

    def insert_user_ratings(self, user_id, ratings):
        """Insert user ratings (from dict)"""
        for game_id, rating in ratings.iteritems():
            self.cursor.execute(
                "insert ignore into gameratings values (%s, %s, %s);", (
                    user_id,
                    game_id,
                    rating,
                )
            )

    def process_users(self, directory):
        filenames = os.listdir(directory)
        progress = Progress(len(filenames))
        count_users = 0
        for filename in filenames:
            progress.update()
            path = directory + "/" + filename
            if self.process_user(path):
                count_users += 1
        progress.finalize()
        print "There are {} users in total {} users.".format(
            count_users, len(filenames))

    def get_data_for_rs(self):
        """Returns a dict containing data for recommender system
        in the format expected by Crab
        """
        self.cursor.execute("""
            select * from gameratings
            where game_id in (
                select game_id from gameratings
                group by game_id
                having count(user_id) > 20)
        """)
        result = defaultdict(dict)
        for data in self.cursor:
            result[data[0]][data[1]] = data[2]
        return result

    def get_user_id(self, user_name):
         self.cursor.execute(
            "select id from users where name = %s",
            (user_name,))
         return [item[0] for item in self.cursor][0]

    def add_user(self, user_name):
        """Try to add user with given name.
        Return False if the name is not available
        """
        self.cursor.execute(
            "select name from users where name = %s;",
            (user_name,))
        if self.cursor.fetchall():
            return None
        self.cursor.execute(
            "insert ignore into users (name) values (%s);",
            (user_name,))
        self.cursor.execute(
            "select id from users where name = %s",
            (user_name,))
        return [item[0] for item in self.cursor][0]

    def get_game_name(self, game_id):
        """Return game name for a given id"""
        self.cursor.execute(
            "select name from games where id = %s;",
            (str(game_id),))
        return [item[0].encode('utf-8') for item in self.cursor][0]

    def suggest_games_to_rate(self, user_id):
        """Return a game not rated by the user
        """
        self.cursor.execute("""
            select id, name from games
            where id in (
                select game_id from gameratings
                group by game_id
                having count(user_id) > 20)
            and id not in (
                select game_id from gameratings
                where user_id = %s)
            order by noofratings desc
        """, (user_id,))
        return [(int(item[0]), item[1].encode('utf-8'))
                for item in self.cursor]

    def get_game_full_name(self, user_id, name):
        """Return a list of games not rated by the user
        and where name is like a name given by a user
        """
        self.cursor.execute("""
            select id, name from games
            where id in (
                select game_id from gameratings
                group by game_id
                having count(user_id) > 20)
            and id not in (
                select game_id from gameratings
                where user_id = %s)
            and name like %s
            order by noofratings desc
        """, (user_id,name,))
        return [(int(item[0]), item[1].encode('utf-8'))
                for item in self.cursor]

    def get_user_ratings(self, user_id):
        """Return a games rated by the user
        """
        self.cursor.execute("""
            select game_id, rating from gameratings
            where user_id=%s
            order by rating desc
        """, (user_id,))
        return [(int(item[0]), item[1])
                for item in self.cursor]

    def finalize(self):
        """Close what __init__ opened"""
        self.cnx.commit()
        self.cursor.close()
        self.cnx.close()
