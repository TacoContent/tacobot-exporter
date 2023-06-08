# Copyright 2022 Ryan Conrad

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from prometheus_client import start_http_server, Gauge, Enum
import codecs
import signal
import ssl
import pytz
import yaml
import traceback
import re
import os
import time
from dotenv import load_dotenv, find_dotenv
import datetime

from lib import mongo as mongo

load_dotenv(find_dotenv())


class AppConfig:
    def __init__(self, file: str):
        # set defaults for config from environment variables if they exist
        self.metrics = {
            "port": int(dict_get(os.environ, "TBE_CONFIG_METRICS_PORT", "8932")),
            "pollingInterval": int(dict_get(os.environ, "TBE_CONFIG_METRICS_POLLING_INTERVAL", "30")),
        }

        try:
            # check if file exists
            if os.path.exists(file):
                print(f"Loading config from {file}")
                with codecs.open(file, encoding="utf-8-sig", mode="r") as f:
                    settings = yaml.safe_load(f)
                    self.__dict__.update(settings)
        except yaml.YAMLError as exc:
            print(exc)


class TacoBotMetrics:
    def __init__(self, config):
        self.namespace = "tacobot"
        self.polling_interval_seconds = config.metrics["pollingInterval"]
        self.config = config
        labels = [
            "guild_id",
        ]

        user_labels = [
            "guild_id",
            "user_id",
            "username",
        ]

        live_labels = [
            "guild_id",
            "user_id",
            "username",
            # "platform",
        ]

        # merge labels and config labels
        # labels = labels + [x['name'] for x in self.config.labels]

        self.db = mongo.MongoDatabase()

        self.sum_tacos = Gauge(
            namespace=self.namespace,
            name=f"tacos",
            documentation="The number of tacos give to users",
            labelnames=labels,
        )

        self.sum_taco_gifts = Gauge(
            namespace=self.namespace,
            name=f"taco_gifts",
            documentation="The number of tacos gifted to users",
            labelnames=labels,
        )

        self.sum_taco_reactions = Gauge(
            namespace=self.namespace,
            name=f"taco_reactions",
            documentation="The number of tacos given to users via reactions",
            labelnames=labels,
        )

        self.sum_live_now = Gauge(
            namespace=self.namespace,
            name=f"live_now",
            documentation="The number of people currently live",
            labelnames=labels,
        )

        self.sum_twitch_channels = Gauge(
            namespace=self.namespace,
            name=f"twitch_channels",
            documentation="The number of twitch channels the bot is watching",
            labelnames=labels,
        )

        self.sum_twitch_tacos = Gauge(
            namespace=self.namespace,
            name=f"twitch_tacos",
            documentation="The number of tacos given to twitch users",
            labelnames=labels,
        )

        self.sum_twitch_linked_accounts = Gauge(
            namespace=self.namespace,
            name=f"twitch_linked_accounts",
            documentation="The number of twitch accounts linked to discord accounts",
            labelnames=[],
        )

        self.sum_tqotd_questions = Gauge(
            namespace=self.namespace,
            name=f"tqotd",
            documentation="The number of questions in the TQOTD database",
            labelnames=labels,
        )

        self.sum_tqotd_answers = Gauge(
            namespace=self.namespace,
            name=f"tqotd_answers",
            documentation="The number of answers in the TQOTD database",
            labelnames=labels,
        )

        self.sum_invited_users = Gauge(
            namespace=self.namespace,
            name=f"invited_users",
            documentation="The number of users invited to the server",
            labelnames=labels,
        )

        self.sum_live_twitch = Gauge(
            namespace=self.namespace,
            name=f"live_twitch",
            documentation="The number of twitch users that have gone live",
            labelnames=labels,
        )

        self.sum_live_youtube = Gauge(
            namespace=self.namespace,
            name=f"live_youtube",
            documentation="The number of youtube users that have gone live",
            labelnames=labels,
        )

        self.sum_wdyctw = Gauge(
            namespace=self.namespace,
            name=f"wdyctw_questions",
            documentation="The number of questions in the WDYCTW database",
            labelnames=labels,
        )

        self.sum_wdyctw_answers = Gauge(
            namespace=self.namespace,
            name=f"wdyctw_answers",
            documentation="The number of answers in the WDYCTW database",
            labelnames=labels,
        )


        self.sum_techthurs = Gauge(
            namespace=self.namespace,
            name=f"techthurs",
            documentation="The number of questions in the TechThurs database",
            labelnames=labels,
        )

        self.sum_techthurs_answers = Gauge(
            namespace=self.namespace,
            name=f"techthurs_answers",
            documentation="The number of answers in the TechThurs database",
            labelnames=labels,
        )

        self.sum_mentalmondays = Gauge(
            namespace=self.namespace,
            name=f"mentalmondays",
            documentation="The number of questions in the MentalMondays database",
            labelnames=labels,
        )

        self.sum_mentalmondays_answers = Gauge(
            namespace=self.namespace,
            name=f"mentalmondays_answers",
            documentation="The number of answers in the MentalMondays database",
            labelnames=labels,
        )

        self.sum_tacotuesday = Gauge(
            namespace=self.namespace,
            name=f"tacotuesday",
            documentation="The number of featured posts for TacoTuesday",
            labelnames=labels,
        )

        self.sum_tacotuesday_answers = Gauge(
            namespace=self.namespace,
            name=f"tacotuesday_answers",
            documentation="The number of interactions in the TacoTuesday database",
            labelnames=labels,
        )

        self.sum_game_keys_available = Gauge(
            namespace=self.namespace,
            name=f"game_keys_available",
            documentation="The number of game keys available",
            labelnames=[])

        self.sum_game_keys_claimed = Gauge(
            namespace=self.namespace,
            name=f"game_keys_redeemed",
            documentation="The number of game keys claimed",
            labelnames=[])

        self.sum_minecraft_whitelist = Gauge(
            namespace=self.namespace,
            name=f"minecraft_whitelist",
            documentation="The number of users on the minecraft whitelist",
            labelnames=[])

        self.sum_logs = Gauge(
            namespace=self.namespace,
            name=f"logs",
            documentation="The number of logs",
            labelnames=["guild_id", "level"])

        self.sum_stream_team_requests = Gauge(
            namespace=self.namespace,
            name=f"team_requests",
            documentation="The number of stream team requests",
            labelnames=labels)

        self.sum_birthdays = Gauge(
            namespace=self.namespace,
            name=f"birthdays",
            documentation="The number of birthdays",
            labelnames=labels)

        self.sum_first_messages = Gauge(
            namespace=self.namespace,
            name=f"first_messages_today",
            documentation="The number of first messages today",
            labelnames=labels)

        # self.sum_messages_tracked = Gauge(
        #     namespace=self.namespace,
        #     name=f"messages_tracked",
        #     documentation="The number of messages tracked",
        #     labelnames=labels)

        self.known_users = Gauge(
            namespace=self.namespace,
            name=f"known_users",
            documentation="The number of known users",
            labelnames=["guild_id", "type"])

        self.top_messages = Gauge(
            namespace=self.namespace,
            name=f"messages",
            documentation="The number of top messages",
            labelnames=user_labels)

        self.top_gifters = Gauge(
            namespace=self.namespace,
            name=f"gifters",
            documentation="The number of top gifters",
            labelnames=user_labels)

        self.top_reactors = Gauge(
            namespace=self.namespace,
            name=f"reactors",
            documentation="The number of top reactors",
            labelnames=user_labels)

        self.top_tacos = Gauge(
            namespace=self.namespace,
            name=f"top_tacos",
            documentation="The number of top tacos",
            labelnames=user_labels)

        self.top_live_activity = Gauge(
            namespace=self.namespace,
            name=f"live_activity",
            documentation="The number of top live activity",
            labelnames=live_labels)

        self.build_info = Gauge(
            namespace=self.namespace,
            name=f"build_info",
            documentation="A metric with a constant '1' value labeled with version",
            labelnames=["version", "ref", "build_date", "sha"],
        )


        ver = dict_get(os.environ, "APP_VERSION", "1.0.0-snapshot")
        ref = dict_get(os.environ, "APP_BUILD_REF", "unknown")
        build_date = dict_get(os.environ, "APP_BUILD_DATE", "unknown")
        sha = dict_get(os.environ, "APP_BUILD_SHA", "unknown")
        self.build_info.labels(version=ver, ref=ref, build_date=build_date, sha=sha).set(1)

    def run_metrics_loop(self):
        """Metrics fetching loop"""
        while True:
            print(f"begin metrics fetch")
            self.fetch()
            print(f"end metrics fetch")
            time.sleep(self.polling_interval_seconds)

    def fetch(self):
        error_count = 0
        try:
            guild_id = 935294040386183228

            labels = {
                "guild_id": guild_id,
            }

            error_log_labels = {
                "guild_id": guild_id,
                "level": "ERROR",
            }

            warn_log_labels = {
                "guild_id": guild_id,
                "level": "WARNING",
            }


            guage_values = self.db.get_exporter_sum_data(guild_id=guild_id)
            print(guage_values)
            # q_all_tacos = self.db.get_sum_all_tacos(guild_id=guild_id)
            # all_tacos = q_all_tacos[0]["total"] or 0
            self.sum_tacos.labels(**labels).set(guage_values['all_tacos'])

            # q_all_taco_gifts = self.db.get_sum_all_gift_tacos(guild_id=guild_id)
            # all_taco_gifts = q_all_taco_gifts[0]["total"] or 0
            self.sum_taco_gifts.labels(**labels).set(guage_values['all_gift_tacos'])

            # q_all_taco_reactions = self.db.get_sum_all_taco_reactions(guild_id=guild_id)
            # all_taco_reactions = q_all_taco_reactions or 0
            self.sum_taco_reactions.labels(**labels).set(guage_values['all_reaction_tacos'])

            # q_live_now = self.db.get_live_now_count(guild_id=guild_id)
            # live_now = q_live_now or 0
            self.sum_live_now.labels(**labels).set(guage_values['live_now'])

            # q_twitch_channels = self.db.get_twitch_channel_bot_count(guild_id=guild_id)
            # twitch_channels = q_twitch_channels or 0
            self.sum_twitch_channels.labels(**labels).set(guage_values['twitch_channels'])

            # q_twitch_tacos = self.db.get_sum_all_twitch_tacos(guild_id=guild_id)
            # twitch_tacos = q_twitch_tacos[0]["total"] or 0
            self.sum_twitch_tacos.labels(**labels).set(guage_values['all_twitch_tacos'])

            # q_twitch_linked_accounts = self.db.get_twitch_linked_accounts_count()
            # twitch_linked_accounts = q_twitch_linked_accounts or 0
            self.sum_twitch_linked_accounts.set(guage_values['twitch_linked_accounts'])

            # q_tqotd_questions = self.db.get_tqotd_questions_count(guild_id=guild_id)
            # tqotd_questions = q_tqotd_questions or 0
            self.sum_tqotd_questions.labels(**labels).set(guage_values['tqotd'])

            # q_tqotd_answers = self.db.get_tqotd_answers_count(guild_id=guild_id)
            # tqotd_answers = q_tqotd_answers[0]["total"] or 0
            self.sum_tqotd_answers.labels(**labels).set(guage_values['tqotd_answers'])

            # q_invited_users = self.db.get_invited_users_count(guild_id=guild_id)
            # invited_users = q_invited_users[0]['total'] or 0
            self.sum_invited_users.labels(**labels).set(guage_values['invited_users'])

            # q_live_twitch = self.db.get_sum_live_twitch(guild_id=guild_id)
            # live_twitch = q_live_twitch or 0
            self.sum_live_twitch.labels(**labels).set(guage_values['live_twitch'])

            # q_live_youtube = self.db.get_sum_live_youtube(guild_id=guild_id)
            # live_youtube = q_live_youtube or 0
            self.sum_live_youtube.labels(**labels).set(guage_values['live_youtube'])

            # q_wdyctw_questions = self.db.get_wdyctw_questions_count(guild_id=guild_id)
            # wdyctw_questions = q_wdyctw_questions or 0
            self.sum_wdyctw.labels(**labels).set(guage_values['wdyctw'])

            # q_wdyctw_answers = self.db.get_wdyctw_answers_count(guild_id=guild_id)
            # wdyctw_answers = q_wdyctw_answers[0]["total"] or 0
            self.sum_wdyctw_answers.labels(**labels).set(guage_values['wdyctw_answers'])

            # q_techthurs_questions = self.db.get_techthurs_questions_count(guild_id=guild_id)
            # techthurs_questions = q_techthurs_questions or 0
            self.sum_techthurs.labels(**labels).set(guage_values['techthurs'])

            # q_techthurs_answers = self.db.get_techthurs_answers_count(guild_id=guild_id)
            # techthurs_answers = q_techthurs_answers[0]["total"] or 0
            self.sum_techthurs_answers.labels(**labels).set(guage_values['techthurs_answers'])

            # q_mentalmondays_questions = self.db.get_mentalmondays_questions_count(guild_id=guild_id)
            # mentalmondays_questions = q_mentalmondays_questions or 0
            self.sum_techthurs.labels(**labels).set(guage_values['mentalmondays'])

            # q_mentalmondays_answers = self.db.get_mentalmondays_answers_count(guild_id=guild_id)
            # mentalmondays_answers = q_mentalmondays_answers[0]["total"] or 0
            self.sum_mentalmondays_answers.labels(**labels).set(guage_values['mentalmondays_answers'])

            # q_tacotuesday = self.db.get_tacotuesday_questions_count(guild_id=guild_id)
            # tacotuesday_questions = q_tacotuesday or 0
            self.sum_tacotuesday.labels(**labels).set(guage_values['tacotuesday'])

            # q_tacotuesday_answers = self.db.get_tacotuesday_answers_count(guild_id=guild_id)
            # tacotuesday_answers = q_tacotuesday_answers[0]["total"] or 0
            self.sum_tacotuesday_answers.labels(**labels).set(guage_values['tacotuesday_answers'])

            # q_game_keys_available = self.db.get_game_keys_available_count()
            # game_keys_available = q_game_keys_available or 0
            self.sum_game_keys_available.set(guage_values['game_keys_available'])

            # q_game_keys_claimed = self.db.get_game_keys_redeemed_count()
            # game_keys_claimed = q_game_keys_claimed or 0
            self.sum_game_keys_claimed.set(guage_values['game_keys_redeemed'])

            # q_minecraft_whitelisted = self.db.get_minecraft_whitelisted_count()
            # minecraft_whitelisted = q_minecraft_whitelisted or 0
            self.sum_minecraft_whitelist.set(guage_values['minecraft_whitelisted'])

            q_logs = self.db.get_logs_count(guild_id=guild_id, level="WARNING")
            logs = q_logs or 0
            self.sum_logs.labels(**warn_log_labels).set(logs)

            q_logs = self.db.get_logs_count(guild_id=guild_id, level="ERROR")
            logs = q_logs or 0
            self.sum_logs.labels(**error_log_labels).set(logs)

            # q_team_requests = self.db.get_team_requests_count(guild_id=guild_id)
            # team_requests = q_team_requests or 0
            self.sum_stream_team_requests.labels(**labels).set(guage_values['stream_team_requests'])

            # q_birthdays = self.db.get_birthdays_count(guild_id=guild_id)
            # birthdays = q_birthdays or 0
            self.sum_birthdays.labels(**labels).set(guage_values['birthdays'])

            # q_first_messages = self.db.get_first_messages_today_count(guild_id=guild_id)
            # first_messages = q_first_messages or 0
            self.sum_first_messages.labels(**labels).set(guage_values['first_messages_today'])

            # q_messages_tracked = self.db.get_messages_tracked_count(guild_id=guild_id)
            # messages_tracked = q_messages_tracked or 0
            # self.sum_messages_tracked.labels(**labels).set(messages_tracked)

            q_known_users = self.db.get_known_users(guild_id=guild_id)
            bot_labels = { "guild_id": guild_id, "type": "bot"}

            bot_count = len([ b for b in q_known_users if b["bot"] ])
            self.known_users.labels(**bot_labels).set(bot_count)

            system_labels = { "guild_id": guild_id, "type": "system"}
            system_count = len([ b for b in q_known_users if b["system"] ])
            self.known_users.labels(**system_labels).set(system_count)

            user_labels = { "guild_id": guild_id, "type": "user"}
            user_count = len(q_known_users) - system_count - bot_count
            self.known_users.labels(**user_labels).set(user_count)

            # loop top messages and add to histogram
            q_top_messages = self.db.get_user_messages_tracked(guild_id=guild_id, limit=50)
            for u in q_top_messages:
                user_labels = { "guild_id": guild_id, "user_id": u["_id"], "username": u["user"][0]['username'] }
                self.top_messages.labels(**user_labels).set(u["count"])


            q_top_gifters = self.db.get_top_taco_gifters(guild_id=guild_id, limit=50)
            # print(q_top_gifters)
            for u in q_top_gifters:
                user = {
                    "user_id": u["_id"],
                    "username": u["_id"]
                }
                if u["user"] is not None and len(u["user"]) > 0:
                    user = u["user"][0]

                user_labels = { "guild_id": guild_id, "user_id": u["_id"], "username": user['username'] }
                self.top_gifters.labels(**user_labels).set(u["count"])

            q_top_reactors = self.db.get_top_taco_reactors(guild_id=guild_id, limit=50)
            # print(q_top_reactors)
            for u in q_top_reactors:
                user = {
                    "user_id": u["_id"],
                    "username": u["_id"]
                }
                if u["user"] is not None and len(u["user"]) > 0:
                    user = u["user"][0]

                user_labels = { "guild_id": guild_id, "user_id": u["_id"], "username": user['username'] }
                self.top_reactors.labels(**user_labels).set(u["count"])

            q_top_tacos = self.db.get_top_taco_receivers(guild_id=guild_id, limit=50)
            # print(q_top_tacos)
            for u in q_top_tacos:
                user = {
                    "user_id": u["_id"],
                    "username": u["_id"]
                }
                if u["user"] is not None and len(u["user"]) > 0:
                    user = u["user"][0]

                user_labels = { "guild_id": guild_id, "user_id": u["_id"], "username": user['username'] }
                self.top_tacos.labels(**user_labels).set(u["count"])

            q_top_live = self.db.get_live_activity(guild_id=guild_id, limit=50)
            print(q_top_live)
            for u in q_top_live:
                user = {
                    "user_id": u["_id"],
                    "username": u["_id"]
                }
                if u["user"] is not None and len(u["user"]) > 0:
                    user = u["user"][0]

                user_labels = { "guild_id": guild_id, "user_id": u["_id"], "username": user['username'] }
                self.top_live_activity.labels(**user_labels).set(u["count"])


            # self.not_valid_after.labels(**labels).set(expiration_date.timestamp())
            # self.not_valid_before.labels(**labels).set(issued_date.timestamp())
            # self.expired.labels(**labels).set(int(expiration_date < datetime.datetime.now(pytz.UTC)))
            # self.host_read_errors.labels(host=f"{host['name']}:{host['port']}").set(0)
        except Exception as e:
            traceback.print_exc()
            error_count += 1
            # self.host_read_errors.labels(host=f"{host['name']}:{host['port']}").set(1)
        # self.read_errors.set(error_count)


def dict_get(dictionary, key, default_value=None):
    if key in dictionary.keys():
        return dictionary[key] or default_value
    else:
        return default_value


def sighandler(signum, frame):
    print("<SIGTERM received>")
    exit(0)


def main():
    signal.signal(signal.SIGTERM, sighandler)

    try:
        config_file = dict_get(os.environ, "TBE_CONFIG_FILE", default_value="./config/.configuration.yaml")

        config = AppConfig(config_file)

        print(f"start listening on :{config.metrics['port']}")
        app_metrics = TacoBotMetrics(config)
        start_http_server(config.metrics["port"])
        app_metrics.run_metrics_loop()

    except KeyboardInterrupt:
        exit(0)


if __name__ == "__main__":
    main()
